package ru.mephi.rx.internal;

import ru.mephi.rx.Disposable;

import java.util.concurrent.atomic.AtomicBoolean;

public final class DisposableHelper {

    private DisposableHelper() {
    }

    public static Disposable empty() {
        return new EmptyDisposable();
    }

    public static Disposable fromRunnable(Runnable onDispose) {
        return new RunnableDisposable(onDispose);
    }

    public static Disposable composite(Disposable first, Disposable second) {
        return new CompositeRunnableDisposable(first, second);
    }

    private static final class EmptyDisposable extends AtomicBoolean implements Disposable {

        @Override
        public void dispose() {
            set(true);
        }

        @Override
        public boolean isDisposed() {
            return get();
        }
    }

    private static final class CompositeRunnableDisposable extends AtomicBoolean implements Disposable {

        private final Disposable first;
        private final Disposable second;

        private CompositeRunnableDisposable(Disposable first, Disposable second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public void dispose() {
            if (compareAndSet(false, true)) {
                first.dispose();
                second.dispose();
            }
        }

        @Override
        public boolean isDisposed() {
            return get();
        }
    }

    private static final class RunnableDisposable extends AtomicBoolean implements Disposable {

        private final Runnable onDispose;

        private RunnableDisposable(Runnable onDispose) {
            this.onDispose = onDispose;
        }

        @Override
        public void dispose() {
            if (compareAndSet(false, true)) {
                onDispose.run();
            }
        }

        @Override
        public boolean isDisposed() {
            return get();
        }
    }
}
