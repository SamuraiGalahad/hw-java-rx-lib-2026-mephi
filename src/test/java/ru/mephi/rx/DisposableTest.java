package ru.mephi.rx;

import org.junit.jupiter.api.Test;
import ru.mephi.rx.internal.SafeObserver;
import ru.mephi.rx.testsupport.TestObserver;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisposableTest {

    @Test
    void disposeMarksSubscriptionAsDisposed() {
        TestObserver<Integer> observer = TestObserver.create();
        Disposable disposable = Observable.just(1).subscribe(observer);

        disposable.dispose();

        assertTrue(disposable.isDisposed());
    }

    @Test
    void safeObserverStopsEventsAfterDispose() {
        List<Integer> values = new ArrayList<>();
        TestObserver<Integer> downstream = TestObserver.create();
        SafeObserver<Integer> safeObserver = new SafeObserver<>(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                values.add(item);
                downstream.onNext(item);
            }

            @Override
            public void onError(Throwable t) {
                downstream.onError(t);
            }

            @Override
            public void onComplete() {
                downstream.onComplete();
            }
        });

        safeObserver.onNext(1);
        safeObserver.dispose();
        safeObserver.onNext(2);
        safeObserver.onComplete();

        assertTrue(safeObserver.isDisposed());
        assertEquals(List.of(1), values);
    }

    @Test
    void disposeReturnedFromSubscribeCanBeCalledSafelyMultipleTimes() {
        TestObserver<Integer> observer = TestObserver.create();
        Disposable disposable = Observable.just(1, 2, 3).subscribe(observer);

        disposable.dispose();
        disposable.dispose();

        assertTrue(disposable.isDisposed());
    }
}
