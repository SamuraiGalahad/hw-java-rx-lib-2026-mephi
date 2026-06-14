package ru.mephi.rx.scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Планировщик для вычислительных задач. Использует фиксированный пул потоков.
 */
public final class ComputationScheduler implements Scheduler {

    private static final ComputationScheduler INSTANCE = new ComputationScheduler();
    private final ExecutorService executor = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            new SchedulerThreadFactory("rx-computation-")
    );

    private ComputationScheduler() {
    }

    public static ComputationScheduler getInstance() {
        return INSTANCE;
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }
}
