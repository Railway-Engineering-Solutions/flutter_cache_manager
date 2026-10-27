import 'dart:async';
import 'dart:collection';
import 'dart:io';

import 'package:clock/clock.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter_cache_manager/flutter_cache_manager.dart';
import 'package:flutter_cache_manager/src/cache_store.dart';
import 'package:rxdart/rxdart.dart';
import 'package:uuid/uuid.dart';

///Flutter Cache Manager
///Copyright (c) 2019 Rene Floor
///Released under MIT License.

const statusCodesNewFile = [HttpStatus.ok, HttpStatus.accepted];
const statusCodesFileNotChanged = [HttpStatus.notModified];

class WebHelper {
  WebHelper(this._store, FileService? fileFetcher)
    : _memCache = {},
      fileFetcher = fileFetcher ?? CancellableHttpFileService();

  final CacheStore _store;
  @visibleForTesting
  final FileService fileFetcher;
  final Map<String, _DownloadJob> _memCache;
  final Queue<_DownloadJob> _queue = Queue();

  ///Download the file from the url
  Stream<FileResponse> downloadFile(
    String url, {
    String? key,
    Map<String, String>? authHeaders,
    bool ignoreMemCache = false,
  }) {
    final cacheKey = key ?? url;
    var requestedJob =
        (ignoreMemCache ? null : _memCache[cacheKey]) ??
        _DownloadJob(url, cacheKey, authHeaders);
    _schedule(requestedJob);
    return Stream<FileResponse>.multi((controller) {
      var job = requestedJob;
      if (job.cancellation.isCancelled) {
        job =
            (ignoreMemCache ? null : _memCache[cacheKey]) ??
            _DownloadJob(url, cacheKey, authHeaders);
        requestedJob = job;
      }
      final current = job;
      current.listeners++;
      var released = false;
      void release() {
        if (released) return;
        released = true;
        current.listeners--;
        if (current.listeners == 0 && !current.finished) {
          current.cancellation.cancel();
          if (_queue.remove(current)) {
            _finish(current);
          }
          if (identical(_memCache[cacheKey], current)) {
            _memCache.remove(cacheKey);
          }
        }
      }

      final subscription = current.subject.listen(
        controller.addSync,
        onError: controller.addErrorSync,
        onDone: () {
          release();
          controller.closeSync();
        },
      );
      controller.onPause = subscription.pause;
      controller.onResume = subscription.resume;
      controller.onCancel = () async {
        release();
        await subscription.cancel();
      };
      _schedule(current);
    }, isBroadcast: true);
  }

  void _schedule(_DownloadJob job) {
    if (job.scheduled || job.finished) return;
    job.scheduled = true;
    _memCache[job.key] = job;
    unawaited(_downloadOrAddToQueue(job));
  }

  var concurrentCalls = 0;

  Future<void> _downloadOrAddToQueue(_DownloadJob job) async {
    if (job.cancellation.isCancelled) {
      _finish(job);
      return;
    }
    //Add to queue if there are too many calls.
    if (concurrentCalls >= fileFetcher.concurrentFetches) {
      _queue.add(job);
      return;
    }
    cacheLogger.log(
      'CacheManager: Downloading ${job.url}',
      CacheManagerLogLevel.verbose,
    );

    concurrentCalls++;
    try {
      await for (final result in _updateFile(
        job.url,
        job.key,
        authHeaders: job.headers,
        cancellation: job.cancellation,
      )) {
        job.subject.add(result);
      }
    } on Object catch (e, stackTrace) {
      job.subject.addError(e, stackTrace);
    } finally {
      concurrentCalls--;
      _finish(job);
      _checkQueue();
    }
  }

  void _finish(_DownloadJob job) {
    job.finished = true;
    if (identical(_memCache[job.key], job)) _memCache.remove(job.key);
    unawaited(job.subject.close());
  }

  void _checkQueue() {
    if (_queue.isEmpty) return;
    unawaited(_downloadOrAddToQueue(_queue.removeFirst()));
  }

  ///Download the file from the url
  Stream<FileResponse> _updateFile(
    String url,
    String key, {
    Map<String, String>? authHeaders,
    required DownloadCancellationToken cancellation,
  }) async* {
    var cacheObject = await _store.retrieveCacheData(key);
    cancellation.throwIfCancelled();
    cacheObject = cacheObject == null
        ? CacheObject(
            url,
            key: key,
            validTill: clock.now(),
            relativePath: '${const Uuid().v1()}.file',
          )
        : cacheObject.copyWith(url: url);
    final response = await _download(cacheObject, authHeaders, cancellation);
    yield* _manageResponse(cacheObject, response, cancellation);
  }

  Future<FileServiceResponse> _download(
    CacheObject cacheObject,
    Map<String, String>? authHeaders,
    DownloadCancellationToken cancellation,
  ) async {
    final headers = <String, String>{};

    final etag = cacheObject.eTag;

    // Adding `if-none-match` header on web causes a CORS error.
    if (etag != null && !kIsWeb) {
      headers[HttpHeaders.ifNoneMatchHeader] = etag;
    }

    if (authHeaders != null) {
      headers.addAll(authHeaders);
    }

    cancellation.throwIfCancelled();
    final transport = fileFetcher;
    final pending = transport is CancellableFileService
        ? (transport as CancellableFileService).getWithCancellation(
            cacheObject.url,
            headers: headers,
            cancellationToken: cancellation,
          )
        : transport.get(cacheObject.url, headers: headers);
    final result = Completer<FileServiceResponse>();
    final removeListener = cancellation.onCancel(() {
      if (!result.isCompleted) {
        result.completeError(const DownloadCancelledException());
      }
    });
    pending.then(
      (response) {
        if (result.isCompleted) {
          // A legacy service cannot abort before headers. Release a late body
          // rather than leaving an unobserved response alive after cancellation.
          unawaited(
            response.content.listen((_) {}, onError: (Object _) {}).cancel(),
          );
        } else {
          result.complete(response);
        }
      },
      onError: (Object error, StackTrace stack) {
        if (!result.isCompleted) result.completeError(error, stack);
      },
    );
    try {
      return await result.future;
    } finally {
      removeListener();
    }
  }

  Stream<FileResponse> _manageResponse(
    CacheObject cacheObject,
    FileServiceResponse response,
    DownloadCancellationToken cancellation,
  ) async* {
    cancellation.throwIfCancelled();
    final hasNewFile = statusCodesNewFile.contains(response.statusCode);
    final keepOldFile = statusCodesFileNotChanged.contains(response.statusCode);
    if (!hasNewFile && !keepOldFile) {
      throw HttpExceptionWithStatus(
        response.statusCode,
        'Invalid statusCode: ${response.statusCode}',
        uri: Uri.parse(cacheObject.url),
      );
    }

    final oldCacheObject = cacheObject;
    var newCacheObject = _setDataFromHeaders(cacheObject, response);
    if (statusCodesNewFile.contains(response.statusCode)) {
      var savedBytes = 0;
      await for (final progress in _saveFile(
        newCacheObject,
        response,
        cancellation,
      )) {
        savedBytes = progress;
        yield DownloadProgress(
          cacheObject.url,
          response.contentLength,
          progress,
        );
      }
      newCacheObject = newCacheObject.copyWith(length: savedBytes);
    }

    try {
      cancellation.throwIfCancelled();
      await _store.putFile(newCacheObject);
    } catch (e) {
      if (statusCodesNewFile.contains(response.statusCode)) {
        await _removeOldFile(newCacheObject.relativePath);
      }
      rethrow;
    }
    if (newCacheObject.relativePath != oldCacheObject.relativePath) {
      await _removeOldFile(oldCacheObject.relativePath);
    }

    final file = await _store.fileSystem.createFile(
      newCacheObject.relativePath,
    );
    yield FileInfo(
      file,
      FileSource.Online,
      newCacheObject.validTill,
      newCacheObject.url,
      statusCode: response.statusCode,
    );
  }

  CacheObject _setDataFromHeaders(
    CacheObject cacheObject,
    FileServiceResponse response,
  ) {
    final fileExtension = response.fileExtension;
    var filePath = cacheObject.relativePath;

    if (!statusCodesFileNotChanged.contains(response.statusCode)) {
      // Store new file on different path
      filePath = '${const Uuid().v1()}$fileExtension';
    }
    return cacheObject.copyWith(
      relativePath: filePath,
      validTill: response.validTill,
      eTag: response.eTag,
    );
  }

  Stream<int> _saveFile(
    CacheObject cacheObject,
    FileServiceResponse response,
    DownloadCancellationToken cancellation,
  ) {
    final receivedBytesResultController = StreamController<int>();
    _saveFileAndPostUpdates(
      receivedBytesResultController,
      cacheObject,
      response,
      cancellation,
    );
    return receivedBytesResultController.stream;
  }

  Future<void> _saveFileAndPostUpdates(
    StreamController<int> receivedBytesResultController,
    CacheObject cacheObject,
    FileServiceResponse response,
    DownloadCancellationToken cancellation,
  ) async {
    Stream<List<int>>? content;
    try {
      content = response.content;
      final file = await _store.fileSystem.createFile(cacheObject.relativePath);
      cancellation.throwIfCancelled();
      var receivedBytes = 0;
      final sink = file.openWrite();
      try {
        await sink.addStream(
          cancellation.bind(content).map((s) {
            receivedBytes += s.length;
            receivedBytesResultController.add(receivedBytes);
            return s;
          }),
        );
        await sink.close();
        cancellation.throwIfCancelled();
      } on Object {
        try {
          await sink.close();
        } on Object {
          // Preserve the original stream/cancellation error, not close's echo.
        }
        rethrow;
      }
    } on Object catch (e, stacktrace) {
      try {
        // Also release a body not yet subscribed when file creation or the
        // cancellation check failed. An already-listened single-subscription
        // stream rejects this attempt; its addStream owner already cancelled it.
        await content?.listen((_) {}, onError: (Object _) {}).cancel();
      } on Object {
        // Preserve the original error if the body was already consumed.
      }
      try {
        await _removeOldFile(cacheObject.relativePath);
      } on Object catch (cleanupError) {
        cacheLogger.log(
          'CacheManager: Partial-file cleanup failed: $cleanupError',
          CacheManagerLogLevel.warning,
        );
      }
      receivedBytesResultController.addError(e, stacktrace);
    } finally {
      await receivedBytesResultController.close();
    }
  }

  Future<void> _removeOldFile(String? relativePath) async {
    if (relativePath == null) return;
    final file = await _store.fileSystem.createFile(relativePath);
    try {
      if (await file.exists()) {
        await file.delete();
      }
    } on FileSystemException {
      // Already deleted (see #184) or not deletable. The cache info no longer
      // points at this path, so there is nothing to recover here.
    }
  }
}

class _DownloadJob {
  _DownloadJob(this.url, this.key, this.headers);

  final String url;
  final String key;
  final Map<String, String>? headers;
  final subject = BehaviorSubject<FileResponse>();
  final cancellation = DownloadCancellationToken();
  int listeners = 0;
  bool scheduled = false;
  bool finished = false;
}

class HttpExceptionWithStatus extends HttpException {
  const HttpExceptionWithStatus(this.statusCode, super.message, {super.uri});

  final int statusCode;
}
