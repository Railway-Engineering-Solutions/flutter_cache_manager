import 'dart:async';
import 'dart:convert';
import 'dart:io' as io;
import 'dart:typed_data';
import 'dart:ui' as ui;

import 'package:file/file.dart' as pf;
import 'package:file/memory.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_cache_manager/flutter_cache_manager.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:mockito/mockito.dart';

import 'helpers/mock_cache_info_repository.dart';
import 'helpers/mock_file_fetcher_response.dart';

void main() {
  setUp(WidgetsFlutterBinding.ensureInitialized);

  test(
    'pre-cancelled calls do not start legacy services or resize work',
    () async {
      final f = _Fixture();
      final token = DownloadCancellationToken()..cancel();
      final BaseCacheManager legacyView = f.manager;
      await expectLater(
        f.manager.downloadFileWithCancellation('u', cancellationToken: token),
        throwsA(isA<DownloadCancelledException>()),
      );
      await expectLater(
        legacyView.getSingleFileWithCancellation('u', cancellationToken: token),
        throwsA(isA<DownloadCancelledException>()),
      );
      await expectLater(
        legacyView
            .getFileStreamWithCancellation('u', cancellationToken: token)
            .toList(),
        throwsA(isA<DownloadCancelledException>()),
      );
      await expectLater(
        f.manager
            .getImageFileWithCancellation(
              'u',
              cancellationToken: token,
              maxWidth: 20,
            )
            .toList(),
        throwsA(isA<DownloadCancelledException>()),
      );
      expect(f.service.calls, isEmpty);
      expect(f.fs.files, isEmpty);
    },
  );

  for (final tokenFirst in [true, false]) {
    test(
      'one cancelled caller preserves shared download; tokenFirst=$tokenFirst',
      () async {
        final f = _Fixture();
        final body = f.service.body('u');
        final token = DownloadCancellationToken();
        late Future<FileInfo> cancelled;
        late Future<FileInfo> survivor;
        if (tokenFirst) {
          cancelled = f.manager.downloadFileWithCancellation(
            'u',
            cancellationToken: token,
          );
          survivor = f.manager.downloadFile('u');
        } else {
          survivor = f.manager.downloadFile('u');
          cancelled = f.manager.downloadFileWithCancellation(
            'u',
            cancellationToken: token,
          );
        }
        final failed = expectLater(
          cancelled,
          throwsA(isA<DownloadCancelledException>()),
        );
        await body.listening.future;
        body.controller.add([1, 2]);
        token.cancel();
        await failed;
        expect(body.controller.hasListener, isTrue);
        expect(f.manager.webHelper.concurrentCalls, 1);
        body.controller.add([3, 4]);
        await body.controller.close();
        final result = await survivor;
        expect(await result.file.readAsBytes(), [1, 2, 3, 4]);
        expect(f.service.calls, ['u']);
      },
    );
  }

  test(
    'last caller cancels mid-stream; closes sink and deletes partial UUID',
    () async {
      final f = _Fixture();
      final body = f.service.body('u');
      final token = DownloadCancellationToken();
      final progress = Completer<void>();
      final result = f.manager
          .getFileStreamWithCancellation(
            'u',
            cancellationToken: token,
            withProgress: true,
          )
          .map((response) {
            if (response is DownloadProgress && !progress.isCompleted) {
              progress.complete();
            }
            return response;
          })
          .toList();
      final failed = expectLater(
        result,
        throwsA(isA<DownloadCancelledException>()),
      );
      await body.listening.future;
      body.controller.add([1, 2, 3]);
      await progress.future;
      token.cancel();
      await failed;
      await body.cancelled.future;
      await _until(() => f.manager.webHelper.concurrentCalls == 0);
      expect(f.fs.closedSinks, greaterThanOrEqualTo(1));
      expect(f.fs.files, isEmpty);
      verifyNever(f.repo.updateOrInsert(any));
    },
  );

  test('a cancelled queued request never reaches the service', () async {
    final f = _Fixture()..service.concurrentFetches = 1;
    final active = f.service.body('active');
    final activeResult = f.manager.downloadFile('active');
    await active.listening.future;
    final token = DownloadCancellationToken();
    final queued = f.manager.downloadFileWithCancellation(
      'queued',
      cancellationToken: token,
    );
    final failed = expectLater(
      queued,
      throwsA(isA<DownloadCancelledException>()),
    );
    token.cancel();
    await failed;
    await active.controller.close();
    await activeResult;
    await _until(() => f.manager.webHelper.concurrentCalls == 0);
    expect(f.service.calls, ['active']);
  });

  test('cancelling one queued caller retains its queued sibling', () async {
    final f = _Fixture()..service.concurrentFetches = 1;
    final active = f.service.body('active');
    final queuedBody = f.service.body('queued');
    final activeResult = f.manager.downloadFile('active');
    await active.listening.future;
    final token = DownloadCancellationToken();
    final cancelled = f.manager.downloadFileWithCancellation(
      'queued',
      cancellationToken: token,
    );
    final survivor = f.manager.downloadFile('queued');
    final failed = expectLater(
      cancelled,
      throwsA(isA<DownloadCancelledException>()),
    );
    token.cancel();
    await failed;
    await active.controller.close();
    await activeResult;
    await queuedBody.listening.future;
    queuedBody.controller.add([9]);
    await queuedBody.controller.close();
    expect(await (await survivor).file.readAsBytes(), [9]);
    expect(f.service.calls, ['active', 'queued']);
  });

  test('response error closes the sink and cleans partial file', () async {
    final f = _Fixture();
    final body = f.service.body('u');
    final result = f.manager.downloadFile('u');
    final failed = expectLater(result, throwsStateError);
    await body.listening.future;
    body.controller.add([1, 2]);
    body.controller.addError(StateError('response failed'));
    await failed;
    await _until(() => f.manager.webHelper.concurrentCalls == 0);
    expect(f.fs.closedSinks, greaterThanOrEqualTo(1));
    expect(f.fs.files, isEmpty);
    verifyNever(f.repo.updateOrInsert(any));
  });

  test(
    'late legacy response is cancelled after its last caller leaves',
    () async {
      final f = _Fixture();
      final headers = Completer<FileServiceResponse>();
      f.service.pending['late'] = headers;
      final token = DownloadCancellationToken();
      final result = f.manager.downloadFileWithCancellation(
        'late',
        cancellationToken: token,
      );
      final failed = expectLater(
        result,
        throwsA(isA<DownloadCancelledException>()),
      );
      await _until(() => f.service.calls.contains('late'));
      token.cancel();
      await failed;
      await _until(() => f.manager.webHelper.concurrentCalls == 0);
      final body = f.service.body('late');
      headers.complete(_response(body.controller.stream));
      await body.cancelled.future;
      expect(f.fs.files, isEmpty);
    },
  );

  test(
    'failed replacement retains the old file even if extension changes',
    () async {
      final f = _Fixture();
      final old = f.fs.directory.childFile('old.bin');
      await old.writeAsBytes([7, 8]);
      final cached = CacheObject(
        'u',
        id: 1,
        relativePath: 'old.bin',
        validTill: DateTime(2000),
      );
      when(f.repo.get('u')).thenAnswer((_) async => cached);
      final body = f.service.body('u');
      final result = f.manager.downloadFile('u');
      final failed = expectLater(result, throwsStateError);
      await body.listening.future;
      body.controller.addError(StateError('failed jpg download'));
      await failed;
      expect(await old.readAsBytes(), [7, 8]);
      expect(f.fs.files.map((file) => file.basename), ['old.bin']);
      final updates = verify(
        f.repo.updateOrInsert(captureAny),
      ).captured.cast<CacheObject>();
      expect(updates, hasLength(1));
      expect(
        updates.single.toMap(setTouchedToNow: false),
        cached.toMap(setTouchedToNow: false),
      );
    },
  );

  test(
    'force download map ownership survives previous same-key completion',
    () async {
      final f = _Fixture();
      final old = f.service.body('old');
      final fresh = f.service.body('fresh');
      final first = f.manager.downloadFile('old', key: 'key');
      await old.listening.future;
      final forced = f.manager.downloadFile('fresh', key: 'key', force: true);
      await fresh.listening.future;
      await old.controller.close();
      await first;
      final sibling = f.manager.downloadFile('fresh', key: 'key');
      fresh.controller.add([42]);
      await fresh.controller.close();
      expect(await (await forced).file.readAsBytes(), [42]);
      expect((await sibling).file.path, (await forced).file.path);
      expect(f.service.calls, ['old', 'fresh']);
    },
  );

  test('single-file cancellation reaches the shared owner', () async {
    final f = _Fixture();
    final body = f.service.body('u');
    final token = DownloadCancellationToken();
    final result = f.manager.getSingleFileWithCancellation(
      'u',
      cancellationToken: token,
    );
    final failed = expectLater(
      result,
      throwsA(isA<DownloadCancelledException>()),
    );
    await body.listening.future;
    token.cancel();
    await failed;
    await body.cancelled.future;
    await _until(() => f.manager.webHelper.concurrentCalls == 0);
    expect(f.fs.files, isEmpty);
  });

  test('plain subscription cancellation releases its download owner', () async {
    final f = _Fixture();
    final body = f.service.body('u');
    final subscription = f.manager.getFileStream('u').listen((_) {});
    await body.listening.future;
    await subscription.cancel();
    await body.cancelled.future;
    await _until(() => f.manager.webHelper.concurrentCalls == 0);
    expect(f.fs.files, isEmpty);
  });

  test(
    'cancellation during file creation also releases an unconsumed body',
    () async {
      final f = _Fixture();
      f.fs.blockCreate = Completer<void>();
      final body = f.service.body('u');
      final token = DownloadCancellationToken();
      final result = f.manager.downloadFileWithCancellation(
        'u',
        cancellationToken: token,
      );
      final failed = expectLater(
        result,
        throwsA(isA<DownloadCancelledException>()),
      );
      await f.fs.creationStarted.future;
      token.cancel();
      await failed;
      f.fs.blockCreate!.complete();
      await body.cancelled.future;
      await _until(() => f.manager.webHelper.concurrentCalls == 0);
      expect(f.fs.files, isEmpty);
      expect(f.fs.closedSinks, 0);
    },
  );

  test(
    'existing HttpFileService get overrides remain the dispatch path',
    () async {
      final service = _LegacyHttpService();
      final manager = CacheManager(
        Config(
          'legacy-http',
          fileSystem: _TrackingFileSystem(),
          repo: MockCacheInfoRepository(),
          fileService: service,
        ),
      );
      final file = await manager.downloadFileWithCancellation(
        'gs://bucket/custom-url',
        cancellationToken: DownloadCancellationToken(),
      );
      expect(service.calls, ['gs://bucket/custom-url']);
      expect(await file.file.readAsBytes(), [4, 5]);
    },
  );

  test('abortable HTTP capability cancels a real loopback response', () async {
    final server = await io.HttpServer.bind(io.InternetAddress.loopbackIPv4, 0);
    final client = http.Client();
    server.listen((request) async {
      try {
        request.response.bufferOutput = false;
        request.response.headers.contentType = io.ContentType.binary;
        // Keep an explicitly incomplete response open until the client aborts.
        request.response.contentLength = 100;
        request.response.add([1, 2]);
        await request.response.flush();
        await request.response.done;
      } on Object {
        // Expected when the test aborts its one loopback response.
      }
    });
    try {
      final token = DownloadCancellationToken();
      final service = CancellableHttpFileService(httpClient: client);
      final response = await service.getWithCancellation(
        'http://127.0.0.1:${server.port}/file',
        cancellationToken: token,
      );
      final firstChunk = Completer<void>();
      final received = response.content.map((chunk) {
        if (!firstChunk.isCompleted) firstChunk.complete();
        return chunk;
      }).toList();
      final aborted = expectLater(
        received,
        throwsA(isA<http.RequestAbortedException>()),
      );
      await firstChunk.future;
      token.cancel();
      await aborted;
    } finally {
      client.close();
      await server.close(force: true);
    }
  });

  test(
    'image resize caller cancellation preserves a sibling and real resize',
    () async {
      final f = _Fixture();
      final body = f.service.body('image');
      final token = DownloadCancellationToken();
      final cancelled = f.manager
          .getImageFileWithCancellation(
            'image',
            cancellationToken: token,
            maxWidth: 40,
          )
          .toList();
      final survivor = f.manager
          .getImageFileWithCancellation(
            'image',
            cancellationToken: DownloadCancellationToken(),
            maxWidth: 30,
          )
          .last;
      final failed = expectLater(
        cancelled,
        throwsA(isA<DownloadCancelledException>()),
      );
      await body.listening.future;
      token.cancel();
      await failed;
      body.controller.add(
        await io.File('test/images/image-120.png').readAsBytes(),
      );
      await body.controller.close();
      final result = await survivor as FileInfo;
      final codec = await ui.instantiateImageCodec(
        await result.file.readAsBytes(),
      );
      final frame = await codec.getNextFrame();
      expect(frame.image.width, 30);
      frame.image.dispose();
      codec.dispose();
      expect(f.service.calls, ['image']);
      verifyNever(
        f.repo.updateOrInsert(
          argThat(
            isA<CacheObject>().having(
              (value) => value.key,
              'key',
              'resized_w40_image',
            ),
          ),
        ),
      );
    },
  );
}

Future<void> _until(bool Function() done) async {
  final deadline = DateTime.now().add(const Duration(seconds: 3));
  while (!done()) {
    if (DateTime.now().isAfter(deadline)) fail('owner did not settle');
    await Future<void>.delayed(const Duration(milliseconds: 1));
  }
}

FileServiceResponse _response(Stream<List<int>> content) =>
    MockFileFetcherResponse(
      content,
      null,
      null,
      '.jpg',
      200,
      DateTime.now().add(const Duration(days: 1)),
    );

class _Fixture {
  final fs = _TrackingFileSystem();
  final repo = MockCacheInfoRepository();
  final service = _LegacyService();
  late final manager = _ImageManager(
    Config(
      'cancellation-test',
      fileSystem: fs,
      repo: repo,
      fileService: service,
    ),
  );
}

class _ImageManager extends CacheManager with ImageCacheManager {
  _ImageManager(super.config);
}

// Deliberately uses only the unchanged, published FileService.get signature.
class _LegacyService extends FileService {
  final calls = <String>[];
  final pending = <String, Completer<FileServiceResponse>>{};
  final bodies = <String, _Body>{};
  _Body body(String url) => bodies.putIfAbsent(url, _Body.new);

  @override
  Future<FileServiceResponse> get(
    String url, {
    Map<String, String>? headers,
  }) async {
    calls.add(url);
    if (pending.containsKey(url)) return pending[url]!.future;
    return _response(body(url).controller.stream);
  }
}

class _LegacyHttpService extends HttpFileService {
  final calls = <String>[];
  @override
  Future<FileServiceResponse> get(
    String url, {
    Map<String, String>? headers,
  }) async {
    calls.add(url);
    return _response(Stream.value([4, 5]));
  }
}

class _Body {
  final listening = Completer<void>();
  final cancelled = Completer<void>();
  late final controller = StreamController<List<int>>(
    onListen: () {
      if (!listening.isCompleted) listening.complete();
    },
    onCancel: () {
      if (!cancelled.isCompleted) cancelled.complete();
    },
  );
}

class _TrackingFileSystem implements FileSystem {
  final directory = MemoryFileSystem().systemTempDirectory.createTempSync(
    'cancel',
  );
  var closedSinks = 0;
  Completer<void>? blockCreate;
  final creationStarted = Completer<void>();
  List<pf.File> get files => directory.listSync().whereType<pf.File>().toList();

  @override
  Future<pf.File> createFile(String name) async {
    if (!creationStarted.isCompleted) creationStarted.complete();
    await blockCreate?.future;
    return _TrackingFile(directory.childFile(name), this);
  }
}

class _TrackingFile implements pf.File {
  _TrackingFile(this.file, this.owner);
  final pf.File file;
  final _TrackingFileSystem owner;
  @override
  String get path => file.path;
  @override
  Future<bool> exists() => file.exists();
  @override
  Future<pf.File> delete({bool recursive = false}) async {
    await file.delete(recursive: recursive);
    return this;
  }

  @override
  Future<Uint8List> readAsBytes() => file.readAsBytes();
  @override
  Future<int> length() => file.length();
  @override
  Future<pf.File> writeAsBytes(
    List<int> bytes, {
    io.FileMode mode = io.FileMode.write,
    bool flush = false,
  }) => file.writeAsBytes(bytes, mode: mode, flush: flush);
  @override
  io.IOSink openWrite({
    io.FileMode mode = io.FileMode.write,
    Encoding encoding = utf8,
  }) => _TrackingSink(file.openWrite(mode: mode, encoding: encoding), owner);
  @override
  dynamic noSuchMethod(Invocation invocation) =>
      throw UnimplementedError('${invocation.memberName}');
}

class _TrackingSink implements io.IOSink {
  _TrackingSink(this.sink, this.owner);
  final io.IOSink sink;
  final _TrackingFileSystem owner;
  @override
  Future<void> addStream(Stream<List<int>> stream) => sink.addStream(stream);
  @override
  Future<void> close() async {
    owner.closedSinks++;
    await sink.close();
  }

  @override
  dynamic noSuchMethod(Invocation invocation) =>
      throw UnimplementedError('${invocation.memberName}');
}
