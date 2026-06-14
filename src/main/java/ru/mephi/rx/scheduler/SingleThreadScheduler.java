package ru.mephi.rx.scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Планировщик с единственным рабочим потоком. Гарантирует последовательное выполнение.
 */
public final class SingleThreadScheduler implements Scheduler {

    private static final SingleThreadScheduler INSTANCE = new SingleThreadScheduler();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(new SchedulerThreadFactory("rx-single-"));

    private SingleThreadScheduler() {
    }

    public static SingleThreadScheduler getInstance() {
        return INSTANCE;
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }
}
