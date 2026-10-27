import 'package:idb_shim/idb.dart' show IdbFactory;
import 'package:idb_shim/idb_client_native.dart' show idbFactoryNative;

IdbFactory persistentIndexedDbFactory() => idbFactoryNative;
