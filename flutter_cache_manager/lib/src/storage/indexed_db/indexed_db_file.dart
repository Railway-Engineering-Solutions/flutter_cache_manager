part of 'indexed_db_cache.dart';

class _IndexedDbFile implements files.File {
  _IndexedDbFile(this.cache, this.name);
  final IndexedDbCache cache;
  final String name;

  Future<Map<String, Object?>?> _metadata() =>
      cache._run([IndexedDbDatabase.files], false, (transaction) async {
        final value = await transaction
            .objectStore(IndexedDbDatabase.files)
            .getObject(name);
        return value == null ? null : Map<String, Object?>.from(value as Map);
      });

  @override
  String get path =>
      '/indexeddb/${Uri.encodeComponent(cache.databaseName)}/$name';
  @override
  String get basename => paths.posix.basename(name);
  @override
  String get dirname => paths.posix.dirname(path);
  @override
  Uri get uri => Uri(scheme: 'indexeddb', path: path);
  @override
  bool get isAbsolute => true;
  @override
  files.File get absolute => this;

  @override
  Future<bool> exists() async => await _metadata() != null;

  @override
  Future<int> length() async {
    final metadata = await _metadata();
    if (metadata == null) {
      throw io.FileSystemException('File does not exist', path);
    }
    return metadata['length'] as int;
  }

  @override
  Stream<List<int>> openRead([int? start, int? end]) async* {
    final from = start ?? 0;
    if (from < 0 || (end != null && end < from)) {
      throw RangeError('Invalid file read range');
    }
    final metadata = await _metadata();
    if (metadata == null) {
      throw io.FileSystemException('File does not exist', path);
    }
    final length = metadata['length'] as int;
    final until = end == null || end > length ? length : end;
    final generation = metadata['generation'] as String;
    for (var offset = from; offset < until;) {
      final index = offset ~/ _IndexedDbWriter.chunkSize;
      final value = await cache._run(
        [IndexedDbDatabase.chunks],
        false,
        (transaction) => transaction
            .objectStore(IndexedDbDatabase.chunks)
            .getObject('$generation:$index'),
      );
      if (value is! Uint8List) {
        throw io.FileSystemException(
          'File changed or its chunk is missing',
          path,
        );
      }
      final chunkStart = offset % _IndexedDbWriter.chunkSize;
      final count = (until - offset).clamp(0, value.length - chunkStart);
      if (count == 0) {
        throw io.FileSystemException('Truncated file chunk', path);
      }
      yield Uint8List.sublistView(value, chunkStart, chunkStart + count);
      offset += count;
    }
  }

  @override
  Future<Uint8List> readAsBytes() async {
    final bytes = BytesBuilder(copy: false);
    await for (final chunk in openRead()) {
      bytes.add(chunk);
    }
    return bytes.takeBytes();
  }

  @override
  Future<String> readAsString({Encoding encoding = utf8}) async =>
      encoding.decode(await readAsBytes());
  @override
  Future<List<String>> readAsLines({Encoding encoding = utf8}) async =>
      const LineSplitter().convert(await readAsString(encoding: encoding));

  @override
  io.IOSink openWrite({
    io.FileMode mode = io.FileMode.write,
    Encoding encoding = utf8,
  }) {
    final append =
        mode == io.FileMode.append || mode == io.FileMode.writeOnlyAppend;
    if (!append && mode != io.FileMode.write && mode != io.FileMode.writeOnly) {
      throw ArgumentError.value(mode, 'mode');
    }
    if (cache._closed || cache._closing) {
      throw StateError('IndexedDB cache is closed.');
    }
    return _IndexedDbSink(_IndexedDbWriter(this, append: append), encoding);
  }

  @override
  Future<files.File> writeAsBytes(
    List<int> bytes, {
    io.FileMode mode = io.FileMode.write,
    bool flush = false,
  }) async {
    final sink = openWrite(mode: mode);
    try {
      await sink.addStream(Stream.value(bytes));
      await sink.close();
      return this;
    } on Object {
      try {
        await sink.close();
      } on Object {
        /* Preserve original failure. */
      }
      rethrow;
    }
  }

  @override
  Future<files.File> writeAsString(
    String contents, {
    io.FileMode mode = io.FileMode.write,
    Encoding encoding = utf8,
    bool flush = false,
  }) => writeAsBytes(encoding.encode(contents), mode: mode, flush: flush);

  @override
  Future<files.File> create({
    bool recursive = false,
    bool exclusive = false,
  }) async {
    await cache._run(IndexedDbDatabase.stores, true, (transaction) async {
      final store = transaction.objectStore(IndexedDbDatabase.files);
      final existing = await store.getObject(name);
      if (existing != null) {
        if (exclusive) {
          throw io.FileSystemException('File already exists', path);
        }
        return;
      }
      await store.put({
        'path': name,
        'generation': const Uuid().v4(),
        'length': 0,
      });
    }, protectedPaths: {name});
    return this;
  }

  @override
  Future<files.FileSystemEntity> delete({bool recursive = false}) async {
    await cache._run(IndexedDbDatabase.stores, true, (transaction) async {
      if (await transaction
              .objectStore(IndexedDbDatabase.files)
              .getObject(name) ==
          null) {
        throw io.FileSystemException('File does not exist', path);
      }
      await IndexedDbDatabase.removeFile(transaction, name);
    });
    return this;
  }

  @override
  Future<files.File> copy(String newPath) async {
    final destination = await cache.createFile(newPath);
    await openRead().pipe(destination.openWrite());
    return destination;
  }

  @override
  Future<files.File> rename(String newPath) async {
    final target = cache._relativeName(newPath);
    if (target == name) return this;
    await cache._run(IndexedDbDatabase.stores, true, (transaction) async {
      final store = transaction.objectStore(IndexedDbDatabase.files);
      final value = await store.getObject(name);
      if (value == null) {
        throw io.FileSystemException('File does not exist', path);
      }
      await IndexedDbDatabase.removeFile(transaction, target);
      await store.put({
        ...Map<String, Object?>.from(value as Map),
        'path': target,
      });
      await store.delete(name);
    }, protectedPaths: {name, target});
    return cache.createFile(target);
  }

  @override
  Future<String> resolveSymbolicLinks() async => path;

  // IndexedDB cannot provide synchronous disk IO, native descriptors, directory
  // traversal or file watchers. Unsupported File members fail explicitly.
  @override
  dynamic noSuchMethod(Invocation invocation) => throw UnsupportedError(
    '${invocation.memberName} is not supported by IndexedDB files',
  );
}
