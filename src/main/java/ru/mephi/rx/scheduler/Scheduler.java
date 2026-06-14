package ru.mephi.rx.scheduler;

/**
 * Планировщик выполнения задач в отдельном потоке.
 */
public interface Scheduler {

    void execute(Runnable task);
}
