import 'dart:async';

/// Cancellation of one caller's download, not of other callers sharing its URL.
class DownloadCancellationToken {
  final _cancelled = Completer<void>();
  final _listeners = <void Function()>{};

  /// Whether [cancel] has been called. Cancellation cannot be reset.
  bool get isCancelled => _cancelled.isCompleted;

  /// Completes when this token is cancelled.
  Future<void> get whenCancelled => _cancelled.future;

  /// Cancels this caller. Repeated calls have no effect.
  void cancel() {
    if (isCancelled) return;
    _cancelled.complete();
    for (final listener in _listeners.toList()) {
      listener();
    }
    _listeners.clear();
  }

  /// Throws [DownloadCancelledException] if this token has been cancelled.
  void throwIfCancelled() {
    if (isCancelled) throw const DownloadCancelledException();
  }

  /// Registers a callback and returns a function that removes it.
  ///
  /// An already-cancelled token invokes the callback immediately.
  void Function() onCancel(void Function() listener) {
    if (isCancelled) {
      listener();
    } else {
      _listeners.add(listener);
    }
    return () => _listeners.remove(listener);
  }

  /// Stops this subscription on cancellation, releasing its shared-download
  /// lease. Other subscriptions to [source] are unaffected.
  Stream<T> bind<T>(Stream<T> source) => Stream<T>.multi((controller) {
    if (isCancelled) {
      controller.addErrorSync(const DownloadCancelledException());
      controller.closeSync();
      return;
    }
    StreamSubscription<T>? subscription;
    var finished = false;
    void Function() removeListener = () {};
    void finish() {
      finished = true;
      removeListener();
    }

    removeListener = onCancel(() {
      if (finished) return;
      finish();
      controller.addErrorSync(const DownloadCancelledException());
      controller.closeSync();
      unawaited(subscription?.cancel());
    });
    subscription = source.listen(
      (value) {
        if (!finished) controller.addSync(value);
      },
      onError: (Object error, StackTrace stack) {
        if (!finished) controller.addErrorSync(error, stack);
      },
      onDone: () {
        if (finished) return;
        finish();
        controller.closeSync();
      },
    );
    if (finished) unawaited(subscription.cancel());
    controller.onPause = subscription.pause;
    controller.onResume = subscription.resume;
    controller.onCancel = () async {
      finish();
      await subscription?.cancel();
    };
  }, isBroadcast: source.isBroadcast);
}

/// A caller explicitly cancelled its download subscription.
class DownloadCancelledException implements Exception {
  const DownloadCancelledException();

  @override
  String toString() => 'Download cancelled';
}
