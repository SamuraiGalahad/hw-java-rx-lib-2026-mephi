package ru.mephi.rx.operators;

import ru.mephi.rx.Disposable;
import ru.mephi.rx.Function;
import ru.mephi.rx.Observable;
import ru.mephi.rx.Observer;
import ru.mephi.rx.internal.DisposableHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class ObservableFlatMap<T, R> extends Observable<R> {

    private final Observable<T> source;
    private final Function<T, Observable<R>> mapper;

    public ObservableFlatMap(Observable<T> source, Function<T, Observable<R>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected Disposable subscribeActual(Observer<R> observer) throws Exception {
        FlatMapObserver<T, R> flatMapObserver = new FlatMapObserver<>(observer, mapper);
        Disposable upstream = source.subscribe(flatMapObserver);
        return DisposableHelper.composite(flatMapObserver, upstream);
    }

    private static final class FlatMapObserver<T, R> implements Observer<T>, Disposable {

        private final Observer<R> downstream;
        private final Function<T, Observable<R>> mapper;
        private final AtomicBoolean disposed = new AtomicBoolean();
        private final AtomicInteger activeSources = new AtomicInteger(1);
        private final List<Disposable> innerDisposables = new ArrayList<>();
        private volatile boolean sourceCompleted;

        private FlatMapObserver(Observer<R> downstream, Function<T, Observable<R>> mapper) {
            this.downstream = downstream;
            this.mapper = mapper;
        }

        @Override
        public void onNext(T item) {
            if (isDisposed()) {
                return;
            }

            Observable<R> inner;
            try {
                inner = mapper.apply(item);
                if (inner == null) {
                    onError(new NullPointerException("flatMap mapper returned null Observable"));
                    return;
                }
            } catch (Throwable t) {
                onError(t);
                return;
            }

            activeSources.incrementAndGet();
            Disposable innerDisposable = inner.subscribe(new Observer<R>() {
                @Override
                public void onNext(R value) {
                    if (!isDisposed()) {
                        downstream.onNext(value);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    FlatMapObserver.this.onError(t);
                }

                @Override
                public void onComplete() {
                    completeInner();
                }
            });

            synchronized (innerDisposables) {
                if (!isDisposed()) {
                    innerDisposables.add(innerDisposable);
                } else {
                    innerDisposable.dispose();
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            if (disposed.compareAndSet(false, true)) {
                disposeInners();
                downstream.onError(t);
            }
        }

        @Override
        public void onComplete() {
            sourceCompleted = true;
            completeInner();
        }

        private void completeInner() {
            if (activeSources.decrementAndGet() == 0 && sourceCompleted && !isDisposed()) {
                downstream.onComplete();
            }
        }

        @Override
        public void dispose() {
            if (disposed.compareAndSet(false, true)) {
                disposeInners();
            }
        }

        private void disposeInners() {
            synchronized (innerDisposables) {
                for (Disposable disposable : innerDisposables) {
                    disposable.dispose();
                }
                innerDisposables.clear();
            }
        }

        @Override
        public boolean isDisposed() {
            return disposed.get();
        }
    }
}
