package ru.mephi.rx.scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Планировщик для I/O-операций. Использует пул потоков без ограничения размера.
 */
public final class IOThreadScheduler implements Scheduler {

    private static final IOThreadScheduler INSTANCE = new IOThreadScheduler();
    private final ExecutorService executor = Executors.newCachedThreadPool(new SchedulerThreadFactory("rx-io-"));

    private IOThreadScheduler() {
    }

    public static IOThreadScheduler getInstance() {
        return INSTANCE;
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }
}
