import 'dart:async';

import 'package:clock/clock.dart';
import 'package:idb_shim/idb.dart';

import 'indexed_db_factory_stub.dart'
    if (dart.library.js_interop) 'indexed_db_factory_web.dart';

/// One schema and transaction owner for cache rows, file metadata and chunks.
/// This internal implementation never silently substitutes an in-memory store.
class IndexedDbDatabase {
  IndexedDbDatabase(this.name, {IdbFactory? factory}) : _factory = factory;

  static const files = 'fcm_files';
  static const chunks = 'fcm_chunks';
  static const metadata = 'fcm_metadata';
  static const staging = 'fcm_staging';
  static const stores = [files, chunks, metadata, staging];
  static const schemaVersion = 1;
  static const stagingLifetime = Duration(days: 1);

  final String name;
  IdbFactory? _factory;
  Database? _database;
  Future<Database>? _opening;
  Completer<Database>? _pendingOpen;
  StreamSubscription<VersionChangeEvent>? _versionChanges;
  var _generation = 0;
  var _closed = false;

  bool get hasConnection => _database != null;

  Future<Database> get database {
    if (_closed) return Future.error(StateError('IndexedDB owner is closed.'));
    if (_database case final database?) return Future.value(database);
    if (_opening case final opening?) return opening;
    final generation = _generation;
    final result = Completer<Database>();
    _pendingOpen = result;
    _opening = result.future;
    try {
      final factory = _factory ??= persistentIndexedDbFactory();
      factory
          .open(
            name,
            version: schemaVersion,
            onUpgradeNeeded: (event) {
              final database = event.database;
              database.createObjectStore(files, keyPath: 'path');
              database.createObjectStore(chunks);
              database.createObjectStore(staging, keyPath: 'generation');
              final rows = database.createObjectStore(
                metadata,
                keyPath: '_id',
                autoIncrement: true,
              );
              rows.createIndex('key', 'key', unique: true);
              rows.createIndex('touched', 'touched');
            },
            onBlocked: (_) {
              if (!result.isCompleted) {
                result.completeError(
                  StateError('IndexedDB open is blocked by another tab: $name'),
                );
              }
            },
          )
          .then(
            (database) {
              // Native open requests cannot be cancelled. A late success may never
              // resurrect a closed, blocked or replaced owner.
              if (_closed || generation != _generation || result.isCompleted) {
                database.close();
                if (!result.isCompleted) {
                  result.completeError(
                    StateError('IndexedDB owner closed while opening.'),
                  );
                }
                return;
              }
              try {
                late final StreamSubscription<VersionChangeEvent>
                versionChanges;
                versionChanges = database.onVersionChange.listen((_) {
                  database.close();
                  unawaited(versionChanges.cancel());
                  if (identical(_database, database)) {
                    _database = null;
                    _opening = null;
                    _versionChanges = null;
                    _generation++;
                  }
                });
                _database = database;
                _versionChanges = versionChanges;
                _pendingOpen = null;
                result.complete(database);
              } on Object catch (error, stack) {
                // Errors in a Future.then success callback are not handled by
                // its sibling onError callback. Never leave the open pending.
                database.close();
                if (!result.isCompleted) result.completeError(error, stack);
              }
            },
            onError: (Object error, StackTrace stack) {
              if (!result.isCompleted) result.completeError(error, stack);
            },
          );
    } on Object catch (error, stack) {
      result.completeError(error, stack);
    }
    return result.future.then(
      (database) => database,
      onError: (Object error, StackTrace stack) {
        if (generation == _generation) _opening = null;
        Error.throwWithStackTrace(error, stack);
      },
    );
  }

  /// Completes only after the transaction commits, not on request success.
  Future<T> run<T>(
    List<String> names,
    bool write,
    Future<T> Function(Transaction transaction) operation, {
    Set<String> protectedPaths = const {},
  }) async {
    try {
      return await _run(names, write, operation);
    } on Object catch (error) {
      if (!write || !_isQuota(error) || !await evictOldest(protectedPaths)) {
        rethrow;
      }
      // Exactly one retry after deleting real bytes as well as their rows.
      return _run(names, write, operation);
    }
  }

  Future<T> _run<T>(
    List<String> names,
    bool write,
    Future<T> Function(Transaction) operation,
  ) async {
    final database = await this.database;
    final transaction = database.transactionList(
      names,
      write ? idbModeReadWrite : idbModeReadOnly,
    );
    final committed = transaction.completed;
    // A failed request and its transaction can reject separately. Observe both
    // immediately while preserving the completion error for the await below.
    committed.ignore();
    try {
      final result = await operation(transaction);
      await committed;
      return result;
    } on Object {
      try {
        transaction.abort();
      } on Object {
        /* Already settled. */
      }
      try {
        await committed;
      } on Object {
        /* Preserve the first error. */
      }
      rethrow;
    }
  }

  static bool _isQuota(Object error) =>
      error.toString().toLowerCase().contains('quotaexceeded');

  static KeyRange generationRange(String generation) =>
      KeyRange.bound('$generation:', '$generation;', false, true);

  static Future<void> removeFile(Transaction transaction, String path) async {
    final store = transaction.objectStore(files);
    final object = await store.getObject(path);
    if (object != null) {
      final row = Map<String, Object?>.from(object as Map);
      await transaction
          .objectStore(chunks)
          .delete(generationRange(row['generation'] as String));
    }
    await store.delete(path);
  }

  /// Eviction removes committed blobs and every metadata alias in one atomic
  /// transaction. A replacement's previous committed file is protected.
  Future<bool> evictOldest(Set<String> protectedPaths) => _run(stores, true, (
    transaction,
  ) async {
    if (await _reclaimExpiredStages(transaction, protectedPaths) > 0) {
      return true;
    }
    final store = transaction.objectStore(metadata);
    final all = (await store.getAll())
        .map((row) => Map<String, Object?>.from(row as Map))
        .toList();
    final candidates =
        all
            .where((row) => !protectedPaths.contains(row['relativePath']))
            .toList()
          ..sort(
            (a, b) => ((a['touched'] as num?) ?? 0).compareTo(
              (b['touched'] as num?) ?? 0,
            ),
          );
    if (candidates.isEmpty) return false;
    final count = (candidates.length / 10).ceil().clamp(1, 50);
    final paths = candidates
        .take(count)
        .map((row) => row['relativePath'] as String)
        .toSet();
    for (final path in paths) {
      await removeFile(transaction, path);
    }
    for (final row in all) {
      if (paths.contains(row['relativePath'])) await store.delete(row['_id']!);
    }
    return true;
  });

  /// A crashed writer cannot leave unreferenced chunks filling the cache
  /// forever. Active writes renew their staging lease on every chunk. A writer
  /// paused beyond this lifetime must fail, never publish missing chunks.
  Future<int> reclaimExpiredStages([Set<String> protectedPaths = const {}]) =>
      _run(
        stores,
        true,
        (transaction) => _reclaimExpiredStages(transaction, protectedPaths),
      );

  static Future<int> _reclaimExpiredStages(
    Transaction transaction,
    Set<String> protectedPaths,
  ) async {
    final store = transaction.objectStore(staging);
    final cutoff = clock.now().subtract(stagingLifetime).millisecondsSinceEpoch;
    var removed = 0;
    for (final value in await store.getAll()) {
      final row = Map<String, Object?>.from(value as Map);
      if ((row['lastWrite'] as int) >= cutoff ||
          protectedPaths.contains(row['path'])) {
        continue;
      }
      final generation = row['generation'] as String;
      await transaction.objectStore(chunks).delete(generationRange(generation));
      await store.delete(generation);
      removed++;
    }
    return removed;
  }

  Future<void> deleteData() async {
    await close();
    final result = Completer<void>();
    final factory = _factory ??= persistentIndexedDbFactory();
    factory
        .deleteDatabase(
          name,
          onBlocked: (_) {
            if (!result.isCompleted) {
              result.completeError(
                StateError(
                  'IndexedDB deletion is blocked by another tab: $name',
                ),
              );
            }
          },
        )
        .then(
          (_) {
            if (!result.isCompleted) result.complete();
          },
          onError: (Object error, StackTrace stack) {
            if (!result.isCompleted) result.completeError(error, stack);
          },
        );
    return result.future;
  }

  Future<void> close() async {
    _closed = true;
    _generation++;
    final opening = _opening;
    final pending = _pendingOpen;
    _pendingOpen = null;
    if (pending != null && !pending.isCompleted) {
      pending.completeError(
        StateError('IndexedDB owner closed while opening.'),
      );
    }
    _opening = null;
    _database?.close();
    _database = null;
    await _versionChanges?.cancel();
    _versionChanges = null;
    // Do not wait on a blocked native open. Its late-success guard closes it.
    opening?.ignore();
  }
}
