package ru.mephi.rx.internal;

import ru.mephi.rx.Disposable;

import java.util.concurrent.atomic.AtomicBoolean;

public final class CompositeDisposable implements Disposable {

    private final AtomicBoolean disposed = new AtomicBoolean();
    private final Disposable first;
    private final Disposable second;

    public CompositeDisposable(Disposable first, Disposable second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            first.dispose();
            second.dispose();
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
