import 'dart:io';

import 'package:dio/dio.dart' as dio;
import 'package:flutter_cache_manager/flutter_cache_manager.dart';
import 'package:flutter_cache_manager/src/web/mime_converter.dart';

class DioHttpFileService extends FileService {
  final dio.Dio _dio;

  DioHttpFileService(dio.Dio dio) : _dio = dio;

  @override
  Future<FileServiceResponse> get(String url,
      {Map<String, String>? headers, dio.CancelToken? cancelToken}) async {
    final options = dio.Options(
      headers: headers,
      responseType: dio.ResponseType.stream,
    );

    final response = await _dio.get<dio.ResponseBody>(
      url,
      cancelToken: cancelToken,
      options: options,
    );
    return DioGetResponse(response);
  }
}

class DioGetResponse implements FileServiceResponse {
  final dio.Response<dio.ResponseBody> _response;
  final DateTime _receivedTime = DateTime.now();

  DioGetResponse(this._response);

  @override
  int get statusCode => _response.statusCode ?? 500;

  String? _header(String name) {
    return _response.headers.value(name);
  }

  @override
  Stream<List<int>> get content => _response.data!.stream;

  @override
  int? get contentLength {
    final length = _header(HttpHeaders.contentLengthHeader);
    return length != null ? int.tryParse(length) : null;
  }

  @override
  DateTime get validTill {
    // Without a cache-control header we keep the file for a week
    var ageDuration = const Duration(days: 7);
    final controlHeader = _header(HttpHeaders.cacheControlHeader);
    if (controlHeader != null) {
      final controlSettings = controlHeader.split(',');
      for (final setting in controlSettings) {
        final sanitizedSetting = setting.trim().toLowerCase();
        if (sanitizedSetting == 'no-cache') {
          ageDuration = Duration.zero;
        }
        if (sanitizedSetting.startsWith('max-age=')) {
          final validSeconds =
              int.tryParse(sanitizedSetting.split('=')[1]) ?? 0;
          if (validSeconds > 0) {
            ageDuration = Duration(seconds: validSeconds);
          }
        }
      }
    }

    return _receivedTime.add(ageDuration);
  }

  @override
  String? get eTag => _header(HttpHeaders.etagHeader);

  @override
  String get fileExtension {
    var fileExtension = '';
    final contentTypeHeader = _header(HttpHeaders.contentTypeHeader);
    if (contentTypeHeader != null) {
      final contentType = ContentType.parse(contentTypeHeader);
      fileExtension = contentType.fileExtension;
    }
    return fileExtension;
  }
}
