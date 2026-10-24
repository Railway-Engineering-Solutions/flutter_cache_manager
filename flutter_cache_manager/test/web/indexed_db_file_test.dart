@TestOn('browser')
library;

import 'dart:typed_data';

import 'package:flutter_cache_manager/src/storage/file_system/indexed_db_file.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test(
    'openWrite streams a large file into IndexedDB and reads it back',
    () async {
      final file = IndexedDbFile('large.file', 'indexed_db_file_test');
      final sink = file.openWrite();
      const chunkSize = 64 * 1024;
      const totalBytes = 212 * 1024 * 1024;
      final chunk = Uint8List(chunkSize);
      for (var i = 0; i < chunkSize; i++) {
        chunk[i] = i & 0xff;
      }
      for (var written = 0; written < totalBytes; written += chunkSize) {
        sink.add(chunk);
      }
      await sink.close();

      final bytes = await file.readAsBytes();
      expect(bytes.length, totalBytes);
      expect(bytes[chunkSize + 7], 7);
      expect(bytes[totalBytes - 1], 0xff);
      await file.delete();
    },
    timeout: const Timeout(Duration(minutes: 2)),
  );

  test('write and writeAll encode text into the buffered bytes', () async {
    final file = IndexedDbFile('text.file', 'indexed_db_file_test');
    final sink = file.openWrite()
      ..write('ab')
      ..writeAll(['c', 'd'], '-')
      ..add([0x21]);
    await sink.close();

    expect(String.fromCharCodes(await file.readAsBytes()), 'abc-d!');
    await file.delete();
  });
}
