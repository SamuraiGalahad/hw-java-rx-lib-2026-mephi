package ru.mephi.rx.operators;

import ru.mephi.rx.Disposable;
import ru.mephi.rx.Observable;
import ru.mephi.rx.Observer;
import ru.mephi.rx.internal.DisposableHelper;
import ru.mephi.rx.scheduler.Scheduler;

public final class ObservableSubscribeOn<T> extends Observable<T> {

    private final Observable<T> source;
    private final Scheduler scheduler;

    public ObservableSubscribeOn(Observable<T> source, Scheduler scheduler) {
        this.source = source;
        this.scheduler = scheduler;
    }

    @Override
    protected Disposable subscribeActual(Observer<T> observer) throws Exception {
        SubscribeOnWorker<T> worker = new SubscribeOnWorker<>(source, observer);
        scheduler.execute(worker);
        return worker;
    }

    private static final class SubscribeOnWorker<T> implements Runnable, Disposable {

        private final Observable<T> source;
        private final Observer<T> observer;
        private volatile boolean disposed;
        private volatile Disposable upstream = DisposableHelper.empty();

        private SubscribeOnWorker(Observable<T> source, Observer<T> observer) {
            this.source = source;
            this.observer = observer;
        }

        @Override
        public void run() {
            if (disposed) {
                return;
            }
            try {
                upstream = source.subscribe(observer);
            } catch (Throwable t) {
                if (!disposed) {
                    observer.onError(t);
                }
            }
        }

        @Override
        public void dispose() {
            disposed = true;
            upstream.dispose();
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }
    }
}
