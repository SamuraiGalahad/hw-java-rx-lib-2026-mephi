package ru.mephi.rx.operators;

import ru.mephi.rx.Disposable;
import ru.mephi.rx.Observable;
import ru.mephi.rx.Observer;
import ru.mephi.rx.internal.DisposableHelper;
import ru.mephi.rx.scheduler.Scheduler;

public final class ObservableObserveOn<T> extends Observable<T> {

    private final Observable<T> source;
    private final Scheduler scheduler;

    public ObservableObserveOn(Observable<T> source, Scheduler scheduler) {
        this.source = source;
        this.scheduler = scheduler;
    }

    @Override
    protected Disposable subscribeActual(Observer<T> observer) throws Exception {
        ObserveOnObserver<T> observeOnObserver = new ObserveOnObserver<>(observer, scheduler);
        Disposable upstream = source.subscribe(observeOnObserver);
        return DisposableHelper.composite(observeOnObserver, upstream);
    }

    private static final class ObserveOnObserver<T> implements Observer<T>, Disposable {

        private final Observer<T> downstream;
        private final Scheduler scheduler;
        private volatile boolean disposed;

        private ObserveOnObserver(Observer<T> downstream, Scheduler scheduler) {
            this.downstream = downstream;
            this.scheduler = scheduler;
        }

        @Override
        public void onNext(T item) {
            if (disposed) {
                return;
            }
            scheduler.execute(() -> {
                if (!disposed) {
                    downstream.onNext(item);
                }
            });
        }

        @Override
        public void onError(Throwable t) {
            if (disposed) {
                return;
            }
            scheduler.execute(() -> {
                if (!disposed) {
                    dispose();
                    downstream.onError(t);
                }
            });
        }

        @Override
        public void onComplete() {
            if (disposed) {
                return;
            }
            scheduler.execute(() -> {
                if (!disposed) {
                    dispose();
                    downstream.onComplete();
                }
            });
        }

        @Override
        public void dispose() {
            disposed = true;
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }
    }
}
