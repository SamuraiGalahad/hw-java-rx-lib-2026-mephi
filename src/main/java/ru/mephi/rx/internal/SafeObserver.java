package ru.mephi.rx.internal;

import ru.mephi.rx.Disposable;
import ru.mephi.rx.Observer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Обёртка над {@link Observer}, которая прекращает доставку событий после dispose.
 */
public final class SafeObserver<T> implements Observer<T>, Disposable {

    private final Observer<T> downstream;
    private final AtomicBoolean disposed = new AtomicBoolean();

    public SafeObserver(Observer<T> downstream) {
        this.downstream = downstream;
    }

    @Override
    public void onNext(T item) {
        if (!isDisposed()) {
            downstream.onNext(item);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!isDisposed()) {
            dispose();
            downstream.onError(t);
        }
    }

    @Override
    public void onComplete() {
        if (!isDisposed()) {
            dispose();
            downstream.onComplete();
        }
    }

    @Override
    public void dispose() {
        disposed.set(true);
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
