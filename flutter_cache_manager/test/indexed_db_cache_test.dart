import 'dart:async';
import 'dart:io';

import 'package:clock/clock.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter_cache_manager/flutter_cache_manager.dart';
import 'package:flutter_cache_manager/src/storage/indexed_db/indexed_db_database.dart';
import 'package:flutter_cache_manager/src/storage/indexed_db/indexed_db_factory_stub.dart'
    if (dart.library.js_interop) 'package:flutter_cache_manager/src/storage/indexed_db/indexed_db_factory_web.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:idb_shim/idb.dart' show IdbFactory;
import 'package:idb_shim/idb_client_memory.dart' show newIdbFactoryMemory;
import 'package:uuid/uuid.dart';

void main() {
  late IdbFactory factory;
  late String name;
  late IndexedDbCache cache;
  final peers = <IndexedDbCache>[];
  final inspectors = <IndexedDbDatabase>[];

  setUp(() {
    factory = kIsWeb ? persistentIndexedDbFactory() : newIdbFactoryMemory();
    name = 'fcm-test-${const Uuid().v4()}';
    // Chrome exercises the default native factory, not a memory substitute.
    cache = IndexedDbCache(name, factory: kIsWeb ? null : factory);
  });
  tearDown(() async {
    for (final peer in peers) {
      await peer.close();
    }
    peers.clear();
    await cache.close();
    for (final inspector in inspectors) {
      await inspector.close();
    }
    inspectors.clear();
    await cache.deleteDataFile();
  });

  CacheObject object(String key, String path, {DateTime? touched}) =>
      CacheObject(
        'https://example.invalid/$key',
        key: key,
        relativePath: path,
        validTill: DateTime(2030),
        touched: touched,
      );

  Future<List<Object>> keys(String store) async {
    final database = IndexedDbDatabase(name, factory: factory);
    // The memory adapter shares a Sembast connection between opens. Closing an
    // inspector would also close the cache's connection. Native Chrome tests
    // additionally verify real independent IndexedDB connections.
    inspectors.add(database);
    return database.run(
      [store],
      false,
      (transaction) => transaction.objectStore(store).getAllKeys(),
    );
  }

  test(
    'first file access and first metadata access share one schema',
    () async {
      final file = await cache.createFile('a');
      await file.writeAsBytes([1, 2]);
      expect(await cache.exists(), isFalse);
      final inserted = await cache.insert(object('key', 'a'));
      expect(inserted.id, isNotNull);
      expect((await cache.get('key'))!.relativePath, 'a');
      expect(await file.readAsBytes(), [1, 2]);
      expect(await cache.exists(), isTrue);
    },
  );

  test(
    'metadata CRUD, unique keys, age and capacity preserve values',
    () async {
      await cache.open();
      final old = await cache.insert(
        object('old', 'a', touched: DateTime(2020)),
        setTouchedToNow: false,
      );
      final recent = await cache.insert(
        object('new', 'b', touched: DateTime(2025)),
        setTouchedToNow: false,
      );
      await expectLater(
        cache.insert(object('old', 'c')),
        throwsA(
          predicate(
            (Object error) =>
                error.toString().contains('already exists') ||
                error.toString().contains('ConstraintError'),
          ),
        ),
      );
      expect((await cache.get('old'))!.touched, DateTime(2020));
      expect(
        await cache.update(
          old.copyWith(relativePath: 'changed'),
          setTouchedToNow: false,
        ),
        1,
      );
      expect((await cache.get('old'))!.relativePath, 'changed');
      expect((await cache.getObjectsOverCapacity(1)).single.key, 'old');
      final aged = await withClock(
        Clock.fixed(DateTime(2026)),
        () => cache.getOldObjects(const Duration(days: 365 * 3)),
      );
      expect(aged.single.key, 'old');
      expect(await cache.deleteAll([old.id!, old.id!, 987654]), 1);
      expect(await cache.delete(recent.id!), 1);
      expect(await cache.getAllObjects(), isEmpty);
    },
  );

  test('bounded chunks and ranged reads preserve typed bytes', () async {
    final file = await cache.createFile('large');
    final bytes = Uint8List.fromList(
      List.generate(64 * 1024 * 5 + 123, (i) => i % 251),
    );
    await file.writeAsBytes(bytes);
    expect(await file.length(), bytes.length);
    final chunks = await file.openRead().toList();
    expect(chunks, hasLength(6));
    expect(
      chunks.every((chunk) => chunk is Uint8List && chunk.length <= 64 * 1024),
      isTrue,
    );
    expect(await file.readAsBytes(), bytes);
    expect(
      await file.openRead(65530, 65550).expand((chunk) => chunk).toList(),
      bytes.sublist(65530, 65550),
    );
    expect(await file.openRead(bytes.length + 1).toList(), isEmpty);
    await expectLater(file.openRead(-1).drain<void>(), throwsRangeError);
    expect(await keys(IndexedDbDatabase.chunks), hasLength(6));
    expect(await keys(IndexedDbDatabase.staging), isEmpty);
  });

  test('exists and length do not read blob chunks', () async {
    final file = await cache.createFile('metadata-only');
    await file.writeAsBytes([1, 2, 3]);
    final database = IndexedDbDatabase(name, factory: factory);
    inspectors.add(database);
    await database.run([IndexedDbDatabase.chunks], true, (transaction) async {
      await transaction.objectStore(IndexedDbDatabase.chunks).clear();
    });
    expect(await file.exists(), isTrue);
    expect(await file.length(), 3);
    await expectLater(file.readAsBytes(), throwsA(isA<FileSystemException>()));
  });

  test(
    'flush never publishes a partial replacement or duplicates it',
    () async {
      final file = await cache.createFile('replace');
      await file.writeAsString('old');
      final sink = file.openWrite();
      sink.add([1, 2]);
      await sink.flush();
      await sink.flush();
      expect(await file.readAsString(), 'old');
      sink.add([3, 4]);
      await sink.flush();
      await sink.close();
      await sink.close();
      expect(await file.readAsBytes(), [1, 2, 3, 4]);
      expect(await keys(IndexedDbDatabase.chunks), hasLength(1));
      expect(await keys(IndexedDbDatabase.staging), isEmpty);
    },
  );

  test('append copies the old generation once across flushes', () async {
    final file = await cache.createFile('append');
    await file.writeAsBytes(List.filled(65535, 7));
    final sink = file.openWrite(mode: FileMode.append);
    sink.add([8]);
    await sink.flush();
    sink.add([9]);
    await sink.close();
    final expected = [...List.filled(65535, 7), 8, 9];
    expect(await file.readAsBytes(), expected);
    expect(await keys(IndexedDbDatabase.chunks), hasLength(2));
  });

  test(
    'a failed source removes staged bytes and preserves committed bytes',
    () async {
      final file = await cache.createFile('failed');
      await file.writeAsString('old');
      final sink = file.openWrite();
      final failure = StateError('source failed');
      final stream = Stream<List<int>>.multi((controller) {
        controller.add(List.filled(70000, 1));
        controller.addError(failure);
        controller.close();
      });
      await expectLater(sink.addStream(stream), throwsA(same(failure)));
      await expectLater(sink.close(), throwsA(same(failure)));
      expect(await file.readAsString(), 'old');
      expect(await keys(IndexedDbDatabase.staging), isEmpty);
      expect(await keys(IndexedDbDatabase.chunks), hasLength(1));
    },
  );

  test(
    'addError exposes the failure through done without committing',
    () async {
      final file = await cache.createFile('sink-error');
      await file.writeAsString('old');
      final sink = file.openWrite();
      sink.add(List.filled(70000, 1));
      final failure = StateError('caller failed');
      sink.addError(failure);
      await expectLater(sink.done, throwsA(same(failure)));
      expect(await file.readAsString(), 'old');
      expect(await keys(IndexedDbDatabase.staging), isEmpty);
    },
  );

  test(
    'close cancels an active upstream subscription and rejects its write',
    () async {
      final file = await cache.createFile('close-stream');
      await file.writeAsString('old');
      final cancelled = Completer<void>();
      final source = StreamController<List<int>>(
        onCancel: () => cancelled.complete(),
      );
      final sink = file.openWrite();
      final writing = sink.addStream(source.stream);
      source.add(List.filled(70000, 1));
      // Wait for a persisted staging row, not a wall-clock delay.
      while ((await keys(IndexedDbDatabase.staging)).isEmpty) {
        await Future<void>.delayed(Duration.zero);
      }
      final rejected = expectLater(writing, throwsStateError);
      await cache.close();
      await cancelled.future.timeout(const Duration(seconds: 2));
      await rejected;
      await expectLater(sink.close(), throwsStateError);
      await cache.open();
      expect(await file.readAsString(), 'old');
      expect(await keys(IndexedDbDatabase.staging), isEmpty);
    },
  );

  test(
    'open references close only the final owner and allow explicit reopen',
    () async {
      await cache.open();
      await cache.open();
      final file = await cache.createFile('reopen');
      await file.writeAsBytes([4, 5]);
      expect(await cache.close(), isFalse);
      expect(await file.readAsBytes(), [4, 5]);
      expect(await cache.close(), isTrue);
      await expectLater(file.readAsBytes(), throwsStateError);
      await cache.open();
      expect(await file.readAsBytes(), [4, 5]);
      final peer = IndexedDbCache(name, factory: factory);
      peers.add(peer);
      expect(await (await peer.createFile('reopen')).readAsBytes(), [4, 5]);
    },
  );

  test(
    'copy rename delete and exclusive create have asynchronous file semantics',
    () async {
      final file = await cache.createFile('dir/a');
      await file.create();
      expect(await file.length(), 0);
      await expectLater(
        file.create(exclusive: true),
        throwsA(isA<FileSystemException>()),
      );
      await file.writeAsString('one\ntwo');
      expect(await file.readAsLines(), ['one', 'two']);
      final copied = await file.copy('copy');
      await (await cache.createFile('renamed')).writeAsString('replaced');
      final renamed = await copied.rename('renamed');
      expect(await copied.exists(), isFalse);
      expect(await renamed.readAsString(), 'one\ntwo');
      expect(await file.readAsString(), 'one\ntwo');
      await renamed.delete();
      expect(await renamed.exists(), isFalse);
      expect(() => file.readAsBytesSync(), throwsUnsupportedError);
      for (final path in ['', '../escape', '/absolute', 'a/../../escape']) {
        await expectLater(cache.createFile(path), throwsArgumentError);
      }
    },
  );

  test(
    'quota eviction invalidates every alias and removes actual bytes',
    () async {
      final file = await cache.createFile('old');
      await file.writeAsBytes(List.filled(150000, 2));
      await cache.insert(object('alias-one', 'old'));
      await cache.insert(object('alias-two', 'old'));
      final database = IndexedDbDatabase(name, factory: factory);
      inspectors.add(database);
      expect(await database.evictOldest({}), isTrue);
      expect(await file.exists(), isFalse);
      expect(await cache.get('alias-one'), isNull);
      expect(await cache.get('alias-two'), isNull);
      expect(await keys(IndexedDbDatabase.chunks), isEmpty);
    },
  );

  test(
    'expired staging is reclaimed without deleting committed data',
    () async {
      final file = await cache.createFile('lease');
      await file.writeAsString('old');
      final sink = file.openWrite();
      sink.add(List.filled(70000, 1));
      await sink.flush();
      final database = IndexedDbDatabase(name, factory: factory);
      inspectors.add(database);
      final future = clock.now().add(const Duration(days: 2));
      expect(
        await withClock(
          Clock.fixed(future),
          () => database.reclaimExpiredStages({'lease'}),
        ),
        0,
      );
      expect(
        await withClock(
          Clock.fixed(future),
          () => database.reclaimExpiredStages(),
        ),
        1,
      );
      await expectLater(sink.close(), throwsA(isA<FileSystemException>()));
      expect(await file.readAsString(), 'old');
      expect(await keys(IndexedDbDatabase.chunks), hasLength(1));
    },
  );

  test(
    'a reader cannot silently splice a newer committed generation',
    () async {
      final file = await cache.createFile('read-generation');
      await file.writeAsBytes(List.filled(150000, 1));
      final iterator = StreamIterator(file.openRead());
      expect(await iterator.moveNext(), isTrue);
      expect(iterator.current, List.filled(65536, 1));
      await file.writeAsBytes(List.filled(150000, 2));
      await expectLater(
        iterator.moveNext(),
        throwsA(isA<FileSystemException>()),
      );
      await iterator.cancel();
      expect(await file.readAsBytes(), List.filled(150000, 2));
    },
  );

  test('namespace deletion does not affect a different cache', () async {
    final peer = IndexedDbCache('$name-other', factory: factory);
    peers.add(peer);
    await (await cache.createFile('a')).writeAsBytes([1]);
    await (await peer.createFile('a')).writeAsBytes([2]);
    await cache.deleteDataFile();
    await cache.open();
    expect(await (await cache.createFile('a')).exists(), isFalse);
    expect(await (await peer.createFile('a')).readAsBytes(), [2]);
    await peer.deleteDataFile();
  });

  test(
    'CacheManager can write then reopen bytes and metadata together',
    () async {
      final manager = CacheManager(
        Config(name, repo: cache, fileSystem: cache),
      );
      await manager.putFile(
        'https://example.invalid/asset',
        Uint8List.fromList([1, 2, 3]),
      );
      await manager.dispose();
      final reopened = IndexedDbCache(name, factory: factory);
      final next = CacheManager(
        Config(name, repo: reopened, fileSystem: reopened),
      );
      final info = await next.getFileFromCache('https://example.invalid/asset');
      expect(info, isNotNull);
      expect(await info!.file.readAsBytes(), [1, 2, 3]);
      await next.dispose();
    },
  );

  test('quota eviction cannot leave a usable memory-only cache hit', () async {
    final manager = CacheManager(Config(name, repo: cache, fileSystem: cache));
    const url = 'https://example.invalid/evicted';
    await manager.putFile(url, Uint8List.fromList([1, 2, 3]));
    expect(await manager.getFileFromMemory(url), isNotNull);
    final database = IndexedDbDatabase(name, factory: factory);
    inspectors.add(database);
    expect(await database.evictOldest({}), isTrue);
    expect(await manager.getFileFromMemory(url), isNull);
    expect(await manager.getFileFromCache(url), isNull);
    expect(await keys(IndexedDbDatabase.files), isEmpty);
    expect(await keys(IndexedDbDatabase.chunks), isEmpty);
    expect(await keys(IndexedDbDatabase.metadata), isEmpty);
    await manager.dispose();
  });

  test(
    'CacheManager.removeFile removes persistent bytes and metadata',
    () async {
      final manager = CacheManager(
        Config(name, repo: cache, fileSystem: cache),
      );
      const url = 'https://example.invalid/remove';
      final file = await manager.putFile(url, Uint8List.fromList([1, 2, 3]));
      try {
        await manager.removeFile(url);
        expect(await file.exists(), isFalse);
        expect(await cache.get(url), isNull);
        expect(await keys(IndexedDbDatabase.chunks), isEmpty);
      } finally {
        await manager.dispose();
      }
    },
  );

  test(
    'CacheManager.emptyCache removes all persistent files and rows',
    () async {
      final manager = CacheManager(
        Config(name, repo: cache, fileSystem: cache),
      );
      final first = await manager.putFile(
        'https://example.invalid/first',
        Uint8List.fromList([1]),
      );
      final second = await manager.putFile(
        'https://example.invalid/second',
        Uint8List.fromList([2]),
      );
      try {
        await manager.emptyCache();
        expect(await first.exists(), isFalse);
        expect(await second.exists(), isFalse);
        expect(await cache.getAllObjects(), isEmpty);
        expect(await keys(IndexedDbDatabase.chunks), isEmpty);
      } finally {
        await manager.dispose();
      }
    },
  );

  test('HTTP refresh survives eviction of its previous metadata row', () async {
    const url = 'https://example.invalid/refresh';
    final database = IndexedDbDatabase(name, factory: factory);
    inspectors.add(database);
    var evictions = 0;
    final client = MockClient((request) async {
      expect(request.url.toString(), url);
      // WebHelper has already captured the old CacheObject, including its id.
      // Exercise the actual quota-eviction transaction before replacement save.
      expect(await database.evictOldest({}), isTrue);
      expect(await cache.get(url), isNull);
      evictions++;
      return http.Response.bytes([4, 5, 6], 200);
    });
    final manager = CacheManager(
      Config(
        name,
        repo: cache,
        fileSystem: cache,
        fileService: HttpFileService(httpClient: client),
      ),
    );
    final old = await manager.putFile(url, Uint8List.fromList([1, 2, 3]));
    final oldId = (await cache.get(url))!.id;
    final downloaded = await manager.downloadFile(url, force: true);
    expect(evictions, 1);
    expect(await old.exists(), isFalse);
    expect(await downloaded.file.readAsBytes(), [4, 5, 6]);
    final row = await cache.get(url);
    expect(row, isNotNull);
    expect(row!.id, oldId);
    await manager.dispose();
    client.close();
    final reopened = IndexedDbCache(name, factory: factory);
    final next = CacheManager(
      Config(name, repo: reopened, fileSystem: reopened),
    );
    try {
      final info = await next.getFileFromCache(url);
      expect(info, isNotNull);
      expect(await info!.file.readAsBytes(), [4, 5, 6]);
    } finally {
      await next.dispose();
    }
  });

  test('concurrent key upserts share one canonical metadata id', () async {
    final results = await Future.wait([
      cache.updateOrInsert(object('shared', 'first')),
      cache.updateOrInsert(object('shared', 'second')),
    ]);
    expect(results.map((value) => value.id).toSet(), hasLength(1));
    expect(await cache.getAllObjects(), hasLength(1));
  });

  test('a stale id cannot overwrite an unrelated cache key', () async {
    final existing = await cache.insert(object('existing', 'first'));
    final replacement = await cache.updateOrInsert(
      object('replacement', 'second').copyWith(id: existing.id),
    );
    expect(replacement.id, isNot(existing.id));
    expect((await cache.get('existing'))!.relativePath, 'first');
    expect((await cache.get('replacement'))!.relativePath, 'second');
    expect(await cache.getAllObjects(), hasLength(2));
  });

  if (kIsWeb) {
    test(
      'native versionchange closes the owner instead of blocking an upgrade',
      () async {
        await cache.open();
        final file = await cache.createFile('versioned');
        await file.writeAsBytes([1]);
        final upgrade = await factory.open(
          name,
          version: 2,
          onUpgradeNeeded: (_) {},
        );
        try {
          // The old owner must not continue to use its retired connection or
          // silently open a different schema version.
          await expectLater(
            file.length(),
            throwsA(
              predicate(
                (Object error) => error.toString().contains('VersionError'),
              ),
            ),
          );
        } finally {
          upgrade.close();
        }
      },
    );

    test(
      'native blocked deletion rejects and late completion never revives the owner',
      () async {
        await cache.open();
        await (await cache.createFile('deleted')).writeAsBytes([1]);
        final foreign = await factory.open(
          name,
          version: 1,
          onUpgradeNeeded: (_) {},
        );
        try {
          await expectLater(cache.deleteDataFile(), throwsStateError);
          await expectLater(cache.createFile('late'), throwsStateError);
        } finally {
          foreign.close();
        }
        // The new open queues behind the browser's already-issued delete.
        await cache.open();
        expect(await (await cache.createFile('deleted')).exists(), isFalse);
      },
    );
  }
}
