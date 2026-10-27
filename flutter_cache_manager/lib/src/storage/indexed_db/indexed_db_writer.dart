part of 'indexed_db_cache.dart';

class _IndexedDbWriter {
  _IndexedDbWriter(this.file, {required this.append}) {
    file.cache._writers.add(this);
  }
  static const chunkSize = 64 * 1024;
  final _IndexedDbFile file;
  final bool append;
  final generation = const Uuid().v4();
  final buffer = Uint8List(chunkSize);
  Future<void>? _starting;
  Future<void>? _aborting;
  Future<void> Function()? cancelSource;
  var _used = 0;
  var _index = 0;
  var _dirty = false;
  var _aborted = false;
  var _committed = false;
  int get length => _index * chunkSize + _used;

  Future<void> _start() => _starting ??= () async {
    _checkActive();
    await file.cache._run([IndexedDbDatabase.staging], true, (
      transaction,
    ) async {
      _checkActive();
      await transaction.objectStore(IndexedDbDatabase.staging).put({
        'generation': generation,
        'path': file.name,
        'length': 0,
        'lastWrite': clock.now().millisecondsSinceEpoch,
      });
    }, protectedPaths: {file.name});
    if (append && await file.exists()) {
      await for (final bytes in file.openRead()) {
        await _write(bytes);
      }
    }
  }();

  void _checkActive() {
    if (_aborted || _committed) throw StateError('IndexedDB writer is closed.');
  }

  Future<void> add(List<int> bytes) async {
    await _start();
    await _write(bytes);
  }

  Future<void> _write(List<int> bytes) async {
    _checkActive();
    for (var offset = 0; offset < bytes.length;) {
      final count = (bytes.length - offset).clamp(0, chunkSize - _used);
      buffer.setRange(_used, _used + count, bytes, offset);
      _used += count;
      offset += count;
      _dirty = true;
      if (_used == chunkSize) {
        await _persist();
        _index++;
        _used = 0;
      }
    }
  }

  Future<void> _persist() async {
    _checkActive();
    if (!_dirty) return;
    final bytes = Uint8List.fromList(Uint8List.sublistView(buffer, 0, _used));
    await file.cache._run(
      [IndexedDbDatabase.staging, IndexedDbDatabase.chunks],
      true,
      (transaction) async {
        _checkActive();
        final stages = transaction.objectStore(IndexedDbDatabase.staging);
        if (await stages.getObject(generation) == null) {
          throw io.FileSystemException('Staged write expired', file.path);
        }
        await transaction
            .objectStore(IndexedDbDatabase.chunks)
            .put(bytes, '$generation:$_index');
        await stages.put({
          'generation': generation,
          'path': file.name,
          'length': length,
          'lastWrite': clock.now().millisecondsSinceEpoch,
        });
      },
      protectedPaths: {file.name},
    );
    _dirty = false;
  }

  Future<void> flush() async {
    await _start();
    await _persist();
  }

  Future<void> commit() async {
    await flush();
    await file.cache._run(IndexedDbDatabase.stores, true, (transaction) async {
      _checkActive();
      final stages = transaction.objectStore(IndexedDbDatabase.staging);
      final stage = await stages.getObject(generation);
      final count = await transaction
          .objectStore(IndexedDbDatabase.chunks)
          .count(IndexedDbDatabase.generationRange(generation));
      if (stage == null ||
          (stage as Map)['length'] != length ||
          count != (length / chunkSize).ceil()) {
        throw io.FileSystemException(
          'Staged write is incomplete or expired',
          file.path,
        );
      }
      await IndexedDbDatabase.removeFile(transaction, file.name);
      await transaction.objectStore(IndexedDbDatabase.files).put({
        'path': file.name,
        'generation': generation,
        'length': length,
      });
      await stages.delete(generation);
    }, protectedPaths: {file.name});
    _committed = true;
    file.cache._writers.remove(this);
  }

  Future<void> abort() => _aborting ??= _abort();

  Future<void> _abort() async {
    if (_committed) return;
    _aborted = true;
    try {
      await cancelSource?.call();
      if (!file.cache._database.hasConnection) return;
      await file.cache._database.run(
        [IndexedDbDatabase.staging, IndexedDbDatabase.chunks],
        true,
        (transaction) async {
          final stages = transaction.objectStore(IndexedDbDatabase.staging);
          // A commit already in flight may have completed before this cleanup.
          // Its missing staging row means those chunks are now committed data.
          if (await stages.getObject(generation) == null) return;
          await transaction
              .objectStore(IndexedDbDatabase.chunks)
              .delete(IndexedDbDatabase.generationRange(generation));
          await stages.delete(generation);
        },
        protectedPaths: {file.name},
      );
    } finally {
      file.cache._writers.remove(this);
    }
  }
}

class _IndexedDbSink implements io.IOSink {
  _IndexedDbSink(this.writer, this.encoding) {
    _done.future.ignore();
  }
  final _IndexedDbWriter writer;
  final _done = Completer<void>();
  Future<void> _pending = Future.value();
  bool _closed = false;
  bool _addingStream = false;
  Object? _failure;
  StackTrace? _failureStack;
  @override
  Encoding encoding;

  void _checkWritable() {
    if (_closed || _addingStream) {
      throw StateError('Sink is closed or adding a stream.');
    }
  }

  void _enqueue(List<int> bytes) {
    _pending = _pending
        .then((_) async {
          if (_failure != null) {
            Error.throwWithStackTrace(_failure!, _failureStack!);
          }
          await writer.add(bytes);
        })
        .catchError((Object error, StackTrace stack) {
          _failure ??= error;
          _failureStack ??= stack;
        });
  }

  @override
  void add(List<int> bytes) {
    _checkWritable();
    // Retain byte buffers, not an amplified List<int>. For bounded producer
    // backpressure use addStream (the package download/pipe path).
    _enqueue(Uint8List.fromList(bytes));
  }

  @override
  Future<void> addStream(Stream<List<int>> stream) async {
    _checkWritable();
    _addingStream = true;
    final iterator = StreamIterator(stream);
    writer.cancelSource = iterator.cancel;
    try {
      await _pending;
      if (_failure != null) {
        Error.throwWithStackTrace(_failure!, _failureStack!);
      }
      while (await iterator.moveNext()) {
        await writer.add(iterator.current);
      }
      writer._checkActive();
    } on Object catch (error, stack) {
      _failure ??= error;
      _failureStack ??= stack;
      await writer.abort();
      rethrow;
    } finally {
      await iterator.cancel();
      writer.cancelSource = null;
      _addingStream = false;
    }
  }

  @override
  Future<void> flush() async {
    _checkWritable();
    await _pending;
    if (_failure != null) Error.throwWithStackTrace(_failure!, _failureStack!);
    await writer.flush();
  }

  @override
  Future<void> close() {
    if (_closed) return _done.future;
    if (_addingStream) {
      return Future.error(StateError('Cannot close while adding a stream.'));
    }
    _closed = true;
    unawaited(() async {
      try {
        await _pending;
        if (_failure != null) {
          Error.throwWithStackTrace(_failure!, _failureStack!);
        }
        await writer.commit();
        _done.complete();
      } on Object catch (error, stack) {
        try {
          await writer.abort();
        } on Object {
          /* Preserve the original failure. */
        }
        _done.completeError(error, stack);
      }
    }());
    return _done.future;
  }

  @override
  Future<void> get done => _done.future;
  @override
  void addError(Object error, [StackTrace? stackTrace]) {
    _checkWritable();
    _failure ??= error;
    _failureStack ??= stackTrace ?? StackTrace.current;
    unawaited(close());
  }

  @override
  void write(Object? object) => add(encoding.encode(object.toString()));
  @override
  void writeAll(Iterable objects, [String separator = '']) =>
      write(objects.join(separator));
  @override
  void writeln([Object? object = '']) => write('$object\n');
  @override
  void writeCharCode(int charCode) => write(String.fromCharCode(charCode));
}
