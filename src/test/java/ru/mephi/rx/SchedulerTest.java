package ru.mephi.rx;

import org.junit.jupiter.api.Test;
import ru.mephi.rx.scheduler.ComputationScheduler;
import ru.mephi.rx.scheduler.IOThreadScheduler;
import ru.mephi.rx.scheduler.Scheduler;
import ru.mephi.rx.scheduler.Schedulers;
import ru.mephi.rx.scheduler.SingleThreadScheduler;
import ru.mephi.rx.testsupport.TestObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerTest {

    @Test
    void ioSchedulerExecutesTasks() throws InterruptedException {
        assertSchedulerRunsAsync(IOThreadScheduler.getInstance());
    }

    @Test
    void computationSchedulerExecutesTasks() throws InterruptedException {
        assertSchedulerRunsAsync(ComputationScheduler.getInstance());
    }

    @Test
    void singleThreadSchedulerExecutesTasks() throws InterruptedException {
        assertSchedulerRunsAsync(SingleThreadScheduler.getInstance());
    }

    @Test
    void subscribeOnRunsSourceOnSchedulerThread() throws InterruptedException {
        AtomicReference<String> sourceThread = new AtomicReference<>();
        TestObserver<String> observer = TestObserver.create();

        Observable.<String>create(source -> {
                    sourceThread.set(Thread.currentThread().getName());
                    source.onNext("ok");
                    source.onComplete();
                })
                .subscribeOn(Schedulers.io())
                .subscribe(observer);

        assertTrue(observer.awaitDone(2000, TimeUnit.MILLISECONDS));
        assertNotNull(sourceThread.get());
        assertNotEquals(Thread.currentThread().getName(), sourceThread.get());
    }

    @Test
    void observeOnDeliversEventsOnSchedulerThread() throws InterruptedException {
        List<String> deliveryThreads = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        Observable.just("a", "b")
                .observeOn(Schedulers.single())
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        deliveryThreads.add(Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
        assertEquals(2, deliveryThreads.size());
        String firstThread = deliveryThreads.get(0);
        assertEquals(firstThread, deliveryThreads.get(1));
        assertNotEquals(Thread.currentThread().getName(), firstThread);
    }

    @Test
    void computationSchedulerUsesMultipleThreads() throws InterruptedException {
        Scheduler scheduler = Schedulers.computation();
        int taskCount = 8;
        CountDownLatch latch = new CountDownLatch(taskCount);
        List<String> threads = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < taskCount; i++) {
            scheduler.execute(() -> {
                threads.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }

        assertTrue(latch.await(3000, TimeUnit.MILLISECONDS));
        assertTrue(threads.stream().distinct().count() > 1);
    }

    private void assertSchedulerRunsAsync(Scheduler scheduler) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        scheduler.execute(() -> {
            threadName.set(Thread.currentThread().getName());
            latch.countDown();
        });

        assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
        assertNotNull(threadName.get());
    }
}
