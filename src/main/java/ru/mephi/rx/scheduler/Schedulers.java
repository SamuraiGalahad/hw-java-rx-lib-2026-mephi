package ru.mephi.rx.scheduler;

/**
 * Фабрика готовых планировщиков.
 */
public final class Schedulers {

    private Schedulers() {
    }

    public static Scheduler io() {
        return IOThreadScheduler.getInstance();
    }

    public static Scheduler computation() {
        return ComputationScheduler.getInstance();
    }

    public static Scheduler single() {
        return SingleThreadScheduler.getInstance();
    }
}
