package ru.mephi.rx.testsupport;

import ru.mephi.rx.Disposable;
import ru.mephi.rx.Observer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class TestObserver<T> implements Observer<T> {

    private final List<T> values = new ArrayList<>();
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final CountDownLatch completed = new CountDownLatch(1);
    private volatile boolean finished;

    @Override
    public void onNext(T item) {
        values.add(item);
    }

    @Override
    public void onError(Throwable t) {
        error.set(t);
        finished = true;
        completed.countDown();
    }

    @Override
    public void onComplete() {
        finished = true;
        completed.countDown();
    }

    public List<T> values() {
        return values;
    }

    public Throwable error() {
        return error.get();
    }

    public boolean awaitDone(long timeout, TimeUnit unit) throws InterruptedException {
        return completed.await(timeout, unit);
    }

    public boolean isFinished() {
        return finished;
    }

    public static <T> TestObserver<T> create() {
        return new TestObserver<>();
    }

    public static <T> Disposable subscribeAndAwait(ru.mephi.rx.Observable<T> observable, long timeoutMs)
            throws InterruptedException {
        TestObserver<T> observer = create();
        Disposable disposable = observable.subscribe(observer);
        observer.awaitDone(timeoutMs, TimeUnit.MILLISECONDS);
        return disposable;
    }
}
