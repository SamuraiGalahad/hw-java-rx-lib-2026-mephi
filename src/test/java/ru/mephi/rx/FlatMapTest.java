package ru.mephi.rx;

import org.junit.jupiter.api.Test;
import ru.mephi.rx.testsupport.TestObserver;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlatMapTest {

    @Test
    void flatMapMergesInnerStreams() throws InterruptedException {
        TestObserver<String> observer = TestObserver.create();
        Observable.just("x", "y")
                .flatMap(letter -> Observable.just(letter + "1", letter + "2"))
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(4, observer.values().size());
        assertTrue(observer.values().containsAll(Arrays.asList("x1", "x2", "y1", "y2")));
    }

    @Test
    void flatMapHandlesEmptyInnerStreams() throws InterruptedException {
        TestObserver<Integer> observer = TestObserver.create();
        Observable.just(1, 2, 3)
                .flatMap(value -> value % 2 == 0
                        ? Observable.just(value)
                        : Observable.<Integer>empty())
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(Arrays.asList(2), observer.values());
    }

    @Test
    void flatMapPropagatesInnerError() throws InterruptedException {
        TestObserver<Integer> observer = TestObserver.create();
        Observable.<Integer>just(1)
                .flatMap(value -> Observable.<Integer>error(new RuntimeException("inner failure")))
                .subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertNotNull(observer.error());
    }
}
