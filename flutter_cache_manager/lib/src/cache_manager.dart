import 'dart:async';
import 'dart:typed_data';

import 'package:file/file.dart';
import 'package:flutter/foundation.dart' show visibleForTesting;
import 'package:flutter_cache_manager/flutter_cache_manager.dart';
import 'package:flutter_cache_manager/src/cache_store.dart';
import 'package:flutter_cache_manager/src/web/web_helper.dart';
import 'package:uuid/uuid.dart';

///Flutter Cache Manager
///Copyright (c) 2019 Rene Floor
///Released under MIT License.

/// Basic cache manager implementation, which should be used as a single
/// instance.
class CacheManager implements BaseCacheManager {
  static CacheManagerLogLevel logLevel = CacheManagerLogLevel.none;

  /// Creates a new instance of a cache manager. This can be used to retrieve
  /// files from the cache or download them online. The http headers are used
  /// for the maximum age of the files. The BaseCacheManager should only be
  /// used in singleton patterns.
  ///
  /// The [_cacheKey] is used for the sqlite database file and should be unique.
  /// Files are removed when they haven't been used for longer than [stalePeriod]
  /// or when this cache has grown too big. When the cache is larger than [maxNrOfCacheObjects]
  /// files the files that haven't been used longest will be removed.
  /// The [fileService] can be used to customize how files are downloaded. For example
  /// to edit the urls, add headers or use a proxy. You can also choose to supply
  /// a CacheStore or WebHelper directly if you want more customization.
  CacheManager(Config config) : _config = config, _store = CacheStore(config) {
    _webHelper = WebHelper(_store, config.fileService);
  }

  @visibleForTesting
  CacheManager.custom(
    Config config, {
    CacheStore? cacheStore,
    WebHelper? webHelper,
  }) : _config = config,
       _store = cacheStore ?? CacheStore(config) {
    _webHelper = webHelper ?? WebHelper(_store, config.fileService);
  }

  final Config _config;

  /// Get the config
  Config get config => _config;

  /// Store helper for cached files
  final CacheStore _store;

  /// Get the underlying store helper
  CacheStore get store => _store;

  /// WebHelper to download and store files
  late final WebHelper _webHelper;

  /// Get the underlying web helper
  WebHelper get webHelper => _webHelper;

  /// Get the file from the cache and/or online, depending on availability and age.
  /// Downloaded form [url], [headers] can be used for example for authentication.
  /// When a file is cached and up to date it is return directly, when the cached
  /// file is too old the file is downloaded and returned after download.
  /// When a cached file is not available the newly downloaded file is returned.
  @override
  Future<File> getSingleFile(
    String url, {
    String? key,
    Map<String, String>? headers,
  }) async {
    key ??= url;
    final cacheFile = await getFileFromCache(key);
    if (cacheFile != null && cacheFile.validTill.isAfter(DateTime.now())) {
      return cacheFile.file;
    }
    return (await downloadFile(url, key: key, authHeaders: headers)).file;
  }

  /// Get the file from the cache and/or online, depending on availability and age.
  /// Downloaded form [url], [headers] can be used for example for authentication.
  /// The files are returned as stream. First the cached file if available, when the
  /// cached file is too old the newly downloaded file is returned afterwards.
  @override
  @Deprecated('Prefer to use the new getFileStream method')
  Stream<FileInfo> getFile(
    String url, {
    String? key,
    Map<String, String>? headers,
  }) {
    return getFileStream(
      url,
      key: key,
      withProgress: false,
    ).where((r) => r is FileInfo).cast<FileInfo>();
  }

  /// Get the file from the cache and/or online, depending on availability and age.
  /// Downloaded form [url], [headers] can be used for example for authentication.
  /// The files are returned as stream. First the cached file if available, when the
  /// cached file is too old the newly downloaded file is returned afterwards.
  ///
  /// The [FileResponse] is either a [FileInfo] object for fully downloaded files
  /// or a [DownloadProgress] object for when a file is being downloaded.
  /// The [DownloadProgress] objects are only dispatched when [withProgress] is
  /// set on true and the file is not available in the cache. When the file is
  /// returned from the cache there will be no progress given, although the file
  /// might be outdated and a new file is being downloaded in the background.
  @override
  Stream<FileResponse> getFileStream(
    String url, {
    String? key,
    Map<String, String>? headers,
    bool withProgress = false,
  }) {
    final state = _FileStreamState();
    final controller = StreamController<FileResponse>(onCancel: state.cancel);
    unawaited(
      _pushFileToStream(controller, url, key, headers, withProgress, state),
    );
    return controller.stream;
  }

  Future<void> _pushFileToStream(
    StreamController<FileResponse> controller,
    String url,
    String? key,
    Map<String, String>? headers,
    bool withProgress,
    _FileStreamState state,
  ) async {
    key ??= url;
    FileInfo? cacheFile;
    try {
      cacheFile = await getFileFromCache(key);
    } on Object catch (e) {
      cacheLogger.log(
        'CacheManager: Failed to load cached file for $url with error:\n$e',
        CacheManagerLogLevel.debug,
      );
    }
    if (cacheFile != null) {
      if (state.cancelled) return;
      controller.add(cacheFile);
      withProgress = false;
    }
    if (state.cancelled) return;
    if (cacheFile == null || cacheFile.validTill.isBefore(DateTime.now())) {
      try {
        final done = state.downloadDone = Completer<void>();
        state.subscription = _webHelper
            .downloadFile(url, key: key, authHeaders: headers)
            .listen(
              (response) {
                if (response is DownloadProgress && withProgress) {
                  controller.add(response);
                }
                if (response is FileInfo) {
                  controller.add(response);
                }
              },
              onError: (Object error, StackTrace stack) {
                if (!done.isCompleted) done.completeError(error, stack);
              },
              onDone: () {
                if (!done.isCompleted) done.complete();
              },
              cancelOnError: true,
            );
        if (state.cancelled) await state.cancel();
        await done.future;
      } on Object catch (e) {
        cacheLogger.log(
          'CacheManager: Failed to download file from $url with error:\n$e',
          CacheManagerLogLevel.debug,
        );
        if (cacheFile == null && !state.cancelled && controller.hasListener) {
          controller.addError(e);
        }

        if (cacheFile != null &&
            e is HttpExceptionWithStatus &&
            e.statusCode == 404) {
          await removeFile(key);
          if (!state.cancelled && controller.hasListener) {
            controller.addError(e);
          }
        }
      }
    }
    unawaited(controller.close());
  }

  ///Download the file and add to cache
  @override
  Future<FileInfo> downloadFile(
    String url, {
    String? key,
    Map<String, String>? authHeaders,
    bool force = false,
  }) async {
    key ??= url;
    final fileResponse = await _webHelper
        .downloadFile(
          url,
          key: key,
          authHeaders: authHeaders,
          ignoreMemCache: force,
        )
        .firstWhere((r) => r is FileInfo);
    return fileResponse as FileInfo;
  }

  /// Get the file from the cache.
  /// Specify [ignoreMemCache] to force a re-read from the database
  @override
  Future<FileInfo?> getFileFromCache(
    String key, {
    bool ignoreMemCache = false,
  }) => _store.getFile(key, ignoreMemCache: ignoreMemCache);

  ///Returns the file from memory if it has already been fetched
  @override
  Future<FileInfo?> getFileFromMemory(String key) =>
      _store.getFileFromMemory(key);

  /// Put a file in the cache. It is recommended to specify the [eTag] and the
  /// [maxAge]. When [maxAge] is passed and the eTag is not set the file will
  /// always be downloaded again. The [fileExtension] should be without a dot,
  /// for example "jpg". When cache info is available for the url that path
  /// is re-used.
  /// The returned [File] is saved on disk.
  @override
  Future<File> putFile(
    String url,
    Uint8List fileBytes, {
    String? key,
    String? eTag,
    Duration maxAge = const Duration(days: 30),
    String fileExtension = 'file',
  }) async {
    key ??= url;
    var cacheObject = await _store.retrieveCacheData(key);
    cacheObject ??= CacheObject(
      url,
      key: key,
      relativePath: '${const Uuid().v1()}.$fileExtension',
      validTill: DateTime.now().add(maxAge),
    );

    cacheObject = cacheObject.copyWith(
      validTill: DateTime.now().add(maxAge),
      eTag: eTag,
    );

    final file = await _config.fileSystem.createFile(cacheObject.relativePath);
    await file.writeAsBytes(fileBytes);
    await _store.putFile(cacheObject);
    return file;
  }

  /// Put a byte stream in the cache. When using an existing file you can use
  /// file.openRead(). It is recommended to specify  the [eTag] and the
  /// [maxAge]. When [maxAge] is passed and the eTag is not set the file will
  /// always be downloaded again. The [fileExtension] should be without a dot,
  /// for example "jpg". When cache info is available for the url that path
  /// is re-used.
  /// The returned [File] is saved on disk.
  @override
  Future<File> putFileStream(
    String url,
    Stream<List<int>> source, {
    String? key,
    String? eTag,
    Duration maxAge = const Duration(days: 30),
    String fileExtension = 'file',
  }) async {
    key ??= url;
    var cacheObject = await _store.retrieveCacheData(key);
    cacheObject ??= CacheObject(
      url,
      key: key,
      relativePath:
          '${const Uuid().v1()}'
          '.$fileExtension',
      validTill: DateTime.now().add(maxAge),
    );

    cacheObject = cacheObject.copyWith(
      validTill: DateTime.now().add(maxAge),
      eTag: eTag,
    );

    final file = await _config.fileSystem.createFile(cacheObject.relativePath);

    // Always copy file
    final sink = file.openWrite();
    await source
        // this map is need to map UInt8List to List<int>
        .map((event) => event)
        .pipe(sink);

    await _store.putFile(cacheObject);
    return file;
  }

  /// Remove a file from the cache
  @override
  Future<void> removeFile(String key) async {
    final cacheObject = await _store.retrieveCacheData(key);
    if (cacheObject?.id != null) {
      await _store.removeCachedFile(cacheObject!);
    }
  }

  /// Removes all files from the cache
  @override
  Future<void> emptyCache() => _store.emptyCache();

  /// Closes the cache database
  @override
  Future<void> dispose() async {
    await _config.repo.close();
  }
}

/// Additive cancellation APIs: existing [BaseCacheManager] implementations and
/// overrides do not acquire new parameters or abstract methods.
extension CancellableCacheManager on BaseCacheManager {
  /// Like [BaseCacheManager.getFileStream], with cancellation for this caller.
  ///
  /// [CacheManager] releases its shared download when the subscription ends;
  /// it aborts the transport only after the last caller leaves. Custom managers
  /// retain responsibility for responding to subscription cancellation.
  Stream<FileResponse> getFileStreamWithCancellation(
    String url, {
    required DownloadCancellationToken cancellationToken,
    String? key,
    Map<String, String>? headers,
    bool withProgress = false,
  }) async* {
    cancellationToken.throwIfCancelled();
    yield* cancellationToken.bind(
      getFileStream(
        url,
        key: key,
        headers: headers,
        withProgress: withProgress,
      ),
    );
  }

  /// Returns the fresh file, cancelling only this caller's wait when requested.
  Future<File> getSingleFileWithCancellation(
    String url, {
    required DownloadCancellationToken cancellationToken,
    String? key,
    Map<String, String>? headers,
  }) async {
    cancellationToken.throwIfCancelled();
    final cached = await cancellationToken
        .bind(Stream.fromFuture(getFileFromCache(key ?? url)))
        .first;
    if (cached != null && cached.validTill.isAfter(DateTime.now())) {
      return cached.file;
    }
    cancellationToken.throwIfCancelled();
    final manager = this;
    if (manager is CacheManager) {
      return (await manager.downloadFileWithCancellation(
        url,
        cancellationToken: cancellationToken,
        key: key,
        authHeaders: headers,
      )).file;
    }
    final response = await cancellationToken
        .bind(
          Stream.fromFuture(downloadFile(url, key: key, authHeaders: headers)),
        )
        .first;
    return response.file;
  }
}

class _FileStreamState {
  bool cancelled = false;
  StreamSubscription<FileResponse>? subscription;
  Completer<void>? downloadDone;

  Future<void> cancel() async {
    cancelled = true;
    await subscription?.cancel();
    final done = downloadDone;
    if (done != null && !done.isCompleted) done.complete();
  }
}

/// Direct-download cancellation without changing [CacheManager.downloadFile].
extension CancellableDirectDownload on CacheManager {
  /// Downloads a file while retaining other callers' ownership of a shared URL.
  Future<FileInfo> downloadFileWithCancellation(
    String url, {
    required DownloadCancellationToken cancellationToken,
    String? key,
    Map<String, String>? authHeaders,
    bool force = false,
  }) async {
    cancellationToken.throwIfCancelled();
    final response = await cancellationToken
        .bind(
          webHelper.downloadFile(
            url,
            key: key,
            authHeaders: authHeaders,
            ignoreMemCache: force,
          ),
        )
        .firstWhere((response) => response is FileInfo);
    return response as FileInfo;
  }
}
