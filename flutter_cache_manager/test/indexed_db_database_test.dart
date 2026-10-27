import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:flutter_cache_manager/src/storage/indexed_db/indexed_db_cache.dart';
import 'package:flutter_cache_manager/src/storage/indexed_db/indexed_db_database.dart';
import 'package:flutter_cache_manager/src/storage/indexed_db/indexed_db_factory_stub.dart'
    if (dart.library.js_interop) 'package:flutter_cache_manager/src/storage/indexed_db/indexed_db_factory_web.dart'
    as browser_backend;
import 'package:flutter_test/flutter_test.dart';
import 'package:idb_shim/idb.dart';
import 'package:idb_shim/idb_client_memory.dart' show newIdbFactoryMemory;

void main() {
  test('one schema creates every store regardless of first accessor', () async {
    final factory = _factory();
    final first = IndexedDbDatabase('schema', factory: factory);
    expect(
      (await first.database).objectStoreNames.toSet(),
      IndexedDbDatabase.stores.toSet(),
    );
    final second = IndexedDbDatabase('schema', factory: factory);
    await second.run([IndexedDbDatabase.metadata], false, (transaction) async {
      expect(
        await transaction.objectStore(IndexedDbDatabase.metadata).count(),
        0,
      );
    });
    await first.close();
    expect(
      (await second.database).objectStoreNames,
      contains(IndexedDbDatabase.files),
    );
    await second.close();
  });

  test('an operation failure rolls back earlier request successes', () async {
    final db = IndexedDbDatabase('rollback', factory: _factory());
    await expectLater(
      db.run([IndexedDbDatabase.chunks], true, (transaction) async {
        await transaction
            .objectStore(IndexedDbDatabase.chunks)
            .put(Uint8List.fromList([1, 2]), 'partial:0');
        throw StateError('after request success');
      }),
      throwsStateError,
    );
    expect(await _keys(db, IndexedDbDatabase.chunks), isEmpty);
    await db.close();
  });

  test(
    'transaction abort cannot masquerade as successful request completion',
    () async {
      final db = IndexedDbDatabase('abort', factory: _factory());
      await expectLater(
        db.run([IndexedDbDatabase.chunks], true, (transaction) async {
          await transaction.objectStore(IndexedDbDatabase.chunks).put([
            1,
          ], 'aborted:0');
          transaction.abort();
          return 'must not escape';
        }),
        throwsA(anyOf(isA<DatabaseError>(), isA<DatabaseException>())),
      );
      expect(await _keys(db, IndexedDbDatabase.chunks), isEmpty);
      await db.close();
    },
  );

  test(
    'eviction atomically deletes real chunks and every metadata alias',
    () async {
      final db = IndexedDbDatabase('eviction', factory: _factory());
      await _seed(db);
      expect(await db.evictOldest({'keep'}), isTrue);
      expect(await _keys(db, IndexedDbDatabase.files), ['keep']);
      expect(await _keys(db, IndexedDbDatabase.chunks), ['keep-generation:0']);
      final rows = await db.run(
        [IndexedDbDatabase.metadata],
        false,
        (transaction) =>
            transaction.objectStore(IndexedDbDatabase.metadata).getAll(),
      );
      expect(rows, hasLength(1));
      expect((rows.single as Map)['relativePath'], 'keep');
      final bytes = await db.run(
        [IndexedDbDatabase.chunks],
        false,
        (transaction) => transaction
            .objectStore(IndexedDbDatabase.chunks)
            .getObject('keep-generation:0'),
      );
      expect(bytes, [7, 8]);
      await db.close();
    },
  );

  test(
    'quota retry observes rollback and releases bytes before one retry',
    () async {
      final db = IndexedDbDatabase('quota', factory: _factory());
      await _seed(db);
      var attempts = 0;
      await db.run([IndexedDbDatabase.chunks], true, (transaction) async {
        attempts++;
        await transaction
            .objectStore(IndexedDbDatabase.chunks)
            .put(Uint8List.fromList([9]), 'candidate:$attempts');
        if (attempts == 1) throw DatabaseException('QuotaExceededError');
      }, protectedPaths: {'keep'});
      expect(attempts, 2);
      expect(await _keys(db, IndexedDbDatabase.chunks), [
        'candidate:2',
        'keep-generation:0',
      ]);
      expect(await _keys(db, IndexedDbDatabase.files), ['keep']);
      await db.close();
    },
  );

  test(
    'persistent quota failure does not loop or delete protected bytes',
    () async {
      final db = IndexedDbDatabase('quota-failure', factory: _factory());
      await _seed(db);
      var attempts = 0;
      await expectLater(
        db.run([IndexedDbDatabase.chunks], true, (transaction) async {
          attempts++;
          await transaction.objectStore(IndexedDbDatabase.chunks).put([
            9,
          ], 'candidate:$attempts');
          throw DatabaseException('QuotaExceededError');
        }, protectedPaths: {'keep'}),
        throwsA(isA<DatabaseException>()),
      );
      expect(attempts, 2);
      expect(await _keys(db, IndexedDbDatabase.chunks), ['keep-generation:0']);
      expect(await _keys(db, IndexedDbDatabase.files), ['keep']);
      await db.close();
    },
  );

  test(
    'close retires only that connection; a fresh owner reopens stored bytes',
    () async {
      final factory = _factory();
      final first = IndexedDbDatabase('reopen', factory: factory);
      await _seed(first);
      await first.close();
      await expectLater(first.database, throwsStateError);
      final second = IndexedDbDatabase('reopen', factory: factory);
      expect(await _keys(second, IndexedDbDatabase.files), ['keep', 'old']);
      await second.close();
    },
  );

  test('blocked open rejects and closes any late native success', () async {
    final factory = _PendingFactory();
    final db = IndexedDbDatabase('blocked', factory: factory);
    final result = db.database;
    final failed = expectLater(result, throwsStateError);
    factory.blocked!(_BlockedEvent());
    await failed;
    final lateDatabase = _LateDatabase();
    factory.result.complete(lateDatabase);
    await lateDatabase.closed.future;
    await db.close();
  });

  test(
    'failed connection finalization rejects and closes its database',
    () async {
      final factory = _PendingFactory();
      final db = IndexedDbDatabase('finalization', factory: factory);
      final failed = expectLater(
        db.database,
        throwsA(isA<UnimplementedError>()),
      );
      final connection = _LateDatabase();
      factory.result.complete(connection);
      try {
        await failed.timeout(const Duration(seconds: 2));
        await connection.closed.future;
      } finally {
        await db.close();
      }
    },
  );

  test(
    'close immediately rejects a pending open and retires late success',
    () async {
      final factory = _PendingFactory();
      final db = IndexedDbDatabase('late', factory: factory);
      final failed = expectLater(db.database, throwsStateError);
      await db.close();
      await failed;
      final lateDatabase = _LateDatabase();
      factory.result.complete(lateDatabase);
      await lateDatabase.closed.future;
      await expectLater(db.database, throwsStateError);
    },
  );

  test('close during initialization cannot report a successful open', () async {
    final factory = _PendingFactory();
    final connection = _InitializingDatabase();
    factory.result.complete(connection);
    final cache = IndexedDbCache('close-initialization', factory: factory);
    final refused = expectLater(cache.open(), throwsStateError);
    await connection.initialization.started.future;
    await cache.close();
    connection.initialization.finished.complete(connection);
    await refused;
    await cache.close();
  });

  if (!kIsWeb) {
    test('unsupported platforms never silently fall back to memory', () async {
      final db = IndexedDbDatabase('unsupported');
      await expectLater(db.database, throwsUnsupportedError);
      await db.close();
    });
  }
}

// Browser transaction/lifecycle tests exercise native IndexedDB rather than a
// memory adapter. The VM adapter is only a deterministic unit-test backend.
IdbFactory _factory() => kIsWeb
    ? browser_backend.persistentIndexedDbFactory()
    : newIdbFactoryMemory();

Future<List<Object>> _keys(IndexedDbDatabase db, String name) => db.run(
  [name],
  false,
  (transaction) => transaction.objectStore(name).getAllKeys(),
);

Future<void> _seed(IndexedDbDatabase db) =>
    db.run(IndexedDbDatabase.stores, true, (transaction) async {
      final files = transaction.objectStore(IndexedDbDatabase.files);
      final chunks = transaction.objectStore(IndexedDbDatabase.chunks);
      final rows = transaction.objectStore(IndexedDbDatabase.metadata);
      await files.put({
        'path': 'old',
        'generation': 'old-generation',
        'length': 3,
      });
      await files.put({
        'path': 'keep',
        'generation': 'keep-generation',
        'length': 2,
      });
      await chunks.put(Uint8List.fromList([1, 2, 3]), 'old-generation:0');
      await chunks.put(Uint8List.fromList([7, 8]), 'keep-generation:0');
      await rows.add({'key': 'old-first', 'relativePath': 'old', 'touched': 1});
      await rows.add({'key': 'old-alias', 'relativePath': 'old', 'touched': 2});
      await rows.add({'key': 'keep', 'relativePath': 'keep', 'touched': 3});
    });

class _PendingFactory implements IdbFactory {
  final result = Completer<Database>();
  OnBlockedFunction? blocked;
  @override
  Future<Database> open(
    String name, {
    int? version,
    OnUpgradeNeededFunction? onUpgradeNeeded,
    OnBlockedFunction? onBlocked,
  }) {
    blocked = onBlocked;
    return result.future;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) =>
      throw UnimplementedError('${invocation.memberName}');
}

class _LateDatabase implements Database {
  final closed = Completer<void>();
  @override
  void close() {
    if (!closed.isCompleted) closed.complete();
  }

  @override
  dynamic noSuchMethod(Invocation invocation) =>
      throw UnimplementedError('${invocation.memberName}');
}

class _BlockedEvent implements Event {}

class _InitializingDatabase extends _LateDatabase {
  late final initialization = _InitializingTransaction();

  @override
  Stream<VersionChangeEvent> get onVersionChange => const Stream.empty();

  @override
  Transaction transactionList(List<String> storeNames, String mode) =>
      initialization;
}

class _InitializingTransaction implements Transaction {
  final started = Completer<void>();
  final finished = Completer<Database>();

  @override
  Future<Database> get completed {
    if (!started.isCompleted) started.complete();
    return finished.future;
  }

  @override
  ObjectStore objectStore(String name) => _EmptyStagingStore();

  @override
  dynamic noSuchMethod(Invocation invocation) =>
      throw UnimplementedError('${invocation.memberName}');
}

class _EmptyStagingStore implements ObjectStore {
  @override
  Future<List<Object>> getAll([Object? query, int? count]) async => [];

  @override
  dynamic noSuchMethod(Invocation invocation) =>
      throw UnimplementedError('${invocation.memberName}');
}
