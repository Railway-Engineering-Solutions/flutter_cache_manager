import 'package:dio/dio.dart' as dio;

class QueueItem {
  final String url;
  final String key;
  final Map<String, String>? headers;
  final dio.CancelToken? cancelToken;

  const QueueItem(this.url, this.key, this.headers, {this.cancelToken});
}
