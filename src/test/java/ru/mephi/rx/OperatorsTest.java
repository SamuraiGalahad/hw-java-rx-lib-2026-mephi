package ru.mephi.rx;

import org.junit.jupiter.api.Test;
import ru.mephi.rx.testsupport.TestObserver;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperatorsTest {

    @Test
    void mapTransformsValues() throws InterruptedException {
        TestObserver<Integer> observer = TestObserver.create();
        Observable.just(1, 2, 3)
                .map(value -> value * 2)
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(Arrays.asList(2, 4, 6), observer.values());
    }

    @Test
    void mapPropagatesMapperError() throws InterruptedException {
        TestObserver<Integer> observer = TestObserver.create();
        Observable.just(1, 2)
                .map(value -> {
                    if (value == 2) {
                        throw new IllegalArgumentException("bad value");
                    }
                    return value;
                })
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(Arrays.asList(1), observer.values());
        assertNotNull(observer.error());
    }

    @Test
    void filterKeepsMatchingValues() throws InterruptedException {
        TestObserver<Integer> observer = TestObserver.create();
        Observable.just(1, 2, 3, 4)
                .filter(value -> value % 2 == 0)
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(Arrays.asList(2, 4), observer.values());
    }

    @Test
    void filterPropagatesPredicateError() throws InterruptedException {
        TestObserver<Integer> observer = TestObserver.create();
        Observable.just(1, 2)
                .filter(value -> {
                    if (value == 2) {
                        throw new IllegalStateException("predicate failed");
                    }
                    return true;
                })
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(Arrays.asList(1), observer.values());
        assertNotNull(observer.error());
    }

    @Test
    void mapAndFilterCanBeChained() throws InterruptedException {
        TestObserver<String> observer = TestObserver.create();
        Observable.just(1, 2, 3, 4, 5)
                .filter(value -> value > 2)
                .map(value -> "n=" + value)
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(Arrays.asList("n=3", "n=4", "n=5"), observer.values());
    }
}
