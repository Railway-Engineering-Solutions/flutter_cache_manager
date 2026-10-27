import 'dart:async';
import 'dart:convert';
import 'dart:io' as io;
import 'dart:typed_data';

import 'package:clock/clock.dart';
import 'package:file/file.dart' as files;
import 'package:flutter_cache_manager/src/storage/cache_info_repositories/cache_info_repository.dart';
import 'package:flutter_cache_manager/src/storage/cache_object.dart';
import 'package:flutter_cache_manager/src/storage/file_system/file_system.dart'
    as cache_fs;
import 'package:idb_shim/idb.dart';
import 'package:path/path.dart' as paths;
import 'package:uuid/uuid.dart';

import 'indexed_db_database.dart';

part 'indexed_db_file.dart';
part 'indexed_db_writer.dart';

/// Opt-in browser storage for both cached files and their cache-info rows.
///
/// Pass the same instance to `Config(repo: storage, fileSystem: storage)`.
/// Browser clearing, private mode and quota limits can still remove cache data;
/// this is not a durable document store. No persistent-storage grant is requested.
/// The default factory rejects unsupported platforms instead of using memory.
/// [factory] is an optional backend override, useful for deterministic tests.
///
/// [close] closes the connection after the last [open]. Reuse after closing
/// requires another explicit [open]. File reads and writes then work again.
class IndexedDbCache extends CacheInfoRepository
    implements cache_fs.FileSystem {
  IndexedDbCache(this.databaseName, {IdbFactory? factory})
    : _factory = factory {
    if (databaseName.isEmpty) {
      throw ArgumentError.value(databaseName, 'databaseName');
    }
    _database = IndexedDbDatabase(databaseName, factory: factory);
  }

  /// Browser database namespace. Use a distinct name for each cache.
  final String databaseName;
  final IdbFactory? _factory;
  late IndexedDbDatabase _database;
  Future<void>? _initializing;
  Future<void>? _closingFuture;
  final _writers = <_IndexedDbWriter>{};
  var _references = 0;
  var _closed = false;
  var _closing = false;
  var _deleting = false;

  Future<void> _initialize() {
    if (_closed || _closing) {
      return Future.error(StateError('IndexedDB cache is closed.'));
    }
    final database = _database;
    return _initializing ??= () async {
      try {
        await database.database;
        await database.reclaimExpiredStages();
      } on Object {
        if (identical(_database, database)) _initializing = null;
        rethrow;
      }
    }();
  }

  Future<T> _run<T>(
    List<String> stores,
    bool write,
    Future<T> Function(Transaction) operation, {
    Set<String> protectedPaths = const {},
  }) async {
    final database = _database;
    await _initialize();
    if (_closed || _closing || !identical(_database, database)) {
      throw StateError('IndexedDB cache is closed.');
    }
    return database.run(
      stores,
      write,
      operation,
      protectedPaths: protectedPaths,
    );
  }

  @override
  Future<bool> open() async {
    if (_closing || _deleting) throw StateError('IndexedDB cache is closing.');
    if (_closed) {
      _database = IndexedDbDatabase(databaseName, factory: _factory);
      _initializing = null;
      _closed = false;
    }
    final database = _database;
    _references++;
    try {
      await _initialize();
      if (_closed || _closing || !identical(_database, database)) {
        throw StateError('IndexedDB cache closed while opening.');
      }
      return true;
    } on Object {
      if (identical(_database, database) && _references > 0) _references--;
      rethrow;
    }
  }

  /// Whether this namespace has any cache-info rows, not whether its database
  /// connection has merely been opened.
  @override
  Future<bool> exists() => _run(
    [IndexedDbDatabase.metadata],
    false,
    (transaction) async =>
        await transaction.objectStore(IndexedDbDatabase.metadata).count() > 0,
  );

  @override
  Future<files.File> createFile(String name) async {
    if (_closed || _closing) throw StateError('IndexedDB cache is closed.');
    return _IndexedDbFile(this, _relativeName(name));
  }

  String _relativeName(String name) {
    final prefix = '/indexeddb/${Uri.encodeComponent(databaseName)}/';
    if (name.startsWith(prefix)) name = name.substring(prefix.length);
    if (name.isEmpty ||
        name.contains('\u0000') ||
        paths.posix.isAbsolute(name) ||
        paths.posix.split(name).contains('..')) {
      throw ArgumentError.value(
        name,
        'name',
        'Expected a relative cache file name',
      );
    }
    return paths.posix.normalize(name);
  }

  @override
  Future<CacheObject?> get(String key) =>
      _run([IndexedDbDatabase.metadata], false, (transaction) async {
        final value = await transaction
            .objectStore(IndexedDbDatabase.metadata)
            .index('key')
            .get(key);
        return value == null
            ? null
            : CacheObject.fromMap(Map<String, dynamic>.from(value as Map));
      });

  @override
  Future<List<CacheObject>> getAllObjects() => _run(
    [IndexedDbDatabase.metadata],
    false,
    (transaction) async =>
        (await transaction.objectStore(IndexedDbDatabase.metadata).getAll())
            .map(
              (value) =>
                  CacheObject.fromMap(Map<String, dynamic>.from(value as Map)),
            )
            .toList(),
  );

  @override
  Future<CacheObject> insert(
    CacheObject cacheObject, {
    bool setTouchedToNow = true,
  }) {
    if (cacheObject.id != null) {
      throw ArgumentError('Inserted cache objects cannot already have an id.');
    }
    return _run([IndexedDbDatabase.metadata], true, (transaction) async {
      final id = await transaction
          .objectStore(IndexedDbDatabase.metadata)
          .add(cacheObject.toMap(setTouchedToNow: setTouchedToNow));
      return cacheObject.copyWith(id: id as int);
    }, protectedPaths: {cacheObject.relativePath});
  }

  @override
  Future<int> update(CacheObject cacheObject, {bool setTouchedToNow = true}) {
    if (cacheObject.id == null) {
      throw ArgumentError('Updated cache objects must have an id.');
    }
    return _run([IndexedDbDatabase.metadata], true, (transaction) async {
      final store = transaction.objectStore(IndexedDbDatabase.metadata);
      if (await store.getObject(cacheObject.id!) == null) return 0;
      await store.put(cacheObject.toMap(setTouchedToNow: setTouchedToNow));
      return 1;
    }, protectedPaths: {cacheObject.relativePath});
  }

  @override
  Future<CacheObject> updateOrInsert(CacheObject cacheObject) =>
      _run([IndexedDbDatabase.metadata], true, (transaction) async {
        final store = transaction.objectStore(IndexedDbDatabase.metadata);
        final values = cacheObject.toMap();
        final keyed = await store.index('key').get(cacheObject.key);
        if (keyed != null) {
          // Serialize competing writes for the same cache key onto one row.
          values[CacheObject.columnId] = (keyed as Map)[CacheObject.columnId];
        } else if (cacheObject.id != null) {
          // Quota eviction may have removed the row captured before download.
          // Restore that id unless it now belongs to an unrelated cache key.
          final previous = await store.getObject(cacheObject.id!);
          if (previous != null) values.remove(CacheObject.columnId);
        }
        final id = await store.put(values);
        return cacheObject.copyWith(id: id as int);
      }, protectedPaths: {cacheObject.relativePath});

  @override
  Future<int> delete(int id) => deleteAll([id]);

  @override
  Future<int> deleteAll(Iterable<int> ids) =>
      _run([IndexedDbDatabase.metadata], true, (transaction) async {
        final store = transaction.objectStore(IndexedDbDatabase.metadata);
        var count = 0;
        for (final id in ids.toSet()) {
          if (await store.getObject(id) == null) continue;
          await store.delete(id);
          count++;
        }
        return count;
      });

  @override
  Future<List<CacheObject>> getObjectsOverCapacity(int capacity) async {
    if (capacity < 0) throw ArgumentError.value(capacity, 'capacity');
    final objects = await getAllObjects();
    objects.sort(
      (a, b) =>
          (a.touched ?? DateTime(1970)).compareTo(b.touched ?? DateTime(1970)),
    );
    return objects
        .take((objects.length - capacity).clamp(0, objects.length))
        .toList();
  }

  @override
  Future<List<CacheObject>> getOldObjects(Duration maxAge) async {
    final cutoff = clock.now().subtract(maxAge);
    return (await getAllObjects())
        .where((object) => (object.touched ?? DateTime(1970)).isBefore(cutoff))
        .toList();
  }

  @override
  Future<bool> close() async {
    if (_references > 0) _references--;
    if (_references > 0) return false;
    await _forceClose();
    return true;
  }

  Future<void> _forceClose() {
    if (_closed) return Future.value();
    return _closingFuture ??= _close();
  }

  Future<void> _close() async {
    final database = _database;
    _closing = true;
    try {
      await Future.wait(_writers.toList().map((writer) => writer.abort()));
    } finally {
      await database.close();
      _references = 0;
      _initializing = null;
      _closed = true;
      _closing = false;
      _closingFuture = null;
    }
  }

  /// Deletes files, chunks and metadata in this namespace. A blocked browser
  /// delete reports an error; the native request can still finish when the
  /// other tab closes, because IndexedDB delete requests cannot be cancelled.
  @override
  Future<void> deleteDataFile() async {
    if (_deleting) throw StateError('IndexedDB deletion is already pending.');
    _deleting = true;
    final database = _database;
    try {
      _references = 0;
      await _forceClose();
      await database.deleteData();
    } finally {
      _deleting = false;
    }
  }
}
