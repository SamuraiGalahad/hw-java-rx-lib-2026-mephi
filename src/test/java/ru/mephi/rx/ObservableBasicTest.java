package ru.mephi.rx;

import org.junit.jupiter.api.Test;
import ru.mephi.rx.testsupport.TestObserver;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObservableBasicTest {

    @Test
    void createEmitsValuesAndCompletes() throws InterruptedException {
        TestObserver<Integer> observer = TestObserver.create();
        Observable.<Integer>create(source -> {
            source.onNext(1);
            source.onNext(2);
            source.onComplete();
        }).subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(Arrays.asList(1, 2), observer.values());
        assertNull(observer.error());
    }

    @Test
    void justEmitsAllValues() throws InterruptedException {
        TestObserver<String> observer = TestObserver.create();
        Observable.just("a", "b", "c").subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(Arrays.asList("a", "b", "c"), observer.values());
    }

    @Test
    void emptyCompletesWithoutValues() throws InterruptedException {
        TestObserver<Object> observer = TestObserver.create();
        Observable.empty().subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertTrue(observer.values().isEmpty());
        assertNull(observer.error());
    }

    @Test
    void errorTerminatesStream() throws InterruptedException {
        TestObserver<Integer> observer = TestObserver.create();
        RuntimeException failure = new RuntimeException("failed");
        Observable.<Integer>error(failure).subscribe(observer);

        assertTrue(observer.awaitDone(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertTrue(observer.values().isEmpty());
        assertEquals(failure, observer.error());
    }
}
