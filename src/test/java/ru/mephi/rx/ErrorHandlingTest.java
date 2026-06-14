package ru.mephi.rx;

import org.junit.jupiter.api.Test;
import ru.mephi.rx.testsupport.TestObserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ErrorHandlingTest {

    @Test
    void sourceErrorIsDeliveredToObserver() throws InterruptedException {
        TestObserver<Integer> observer = TestObserver.create();
        IllegalStateException error = new IllegalStateException("source failed");

        Observable.<Integer>create(source -> {
                    source.onNext(1);
                    source.onError(error);
                    source.onNext(2);
                })
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(1, observer.values().size());
        assertEquals(error, observer.error());
    }

    @Test
    void subscribeActualExceptionIsDeliveredAsError() throws InterruptedException {
        TestObserver<Integer> observer = TestObserver.create();

        Observable.<Integer>create(source -> {
                    throw new RuntimeException("subscribe failed");
                })
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertNotNull(observer.error());
        assertEquals("subscribe failed", observer.error().getMessage());
    }

    @Test
    void errorInFlatMapMapperIsDelivered() throws InterruptedException {
        TestObserver<String> observer = TestObserver.create();

        Function<String, Observable<String>> failingMapper = item -> {
            throw new RuntimeException("mapper failed");
        };

        Observable.<String>just("value")
                .flatMap(failingMapper)
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertNotNull(observer.error());
    }
}
