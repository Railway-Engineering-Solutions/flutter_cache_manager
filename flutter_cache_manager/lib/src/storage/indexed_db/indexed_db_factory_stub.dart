import 'package:idb_shim/idb.dart';

IdbFactory persistentIndexedDbFactory() => throw UnsupportedError(
  'IndexedDB persistence requires a supported browser.',
);
