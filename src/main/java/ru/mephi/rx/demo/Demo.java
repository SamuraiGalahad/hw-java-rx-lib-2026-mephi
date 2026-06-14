package ru.mephi.rx.demo;

import ru.mephi.rx.Observable;
import ru.mephi.rx.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;

/**
 * Демонстрация основных возможностей библиотеки.
 */
public final class Demo {

    public static void main(String[] args) throws InterruptedException {
        demoBasicOperators();
        demoFlatMap();
        demoSchedulers();
        demoErrorHandling();
        demoDispose();
        System.out.println("=== done ===");
    }

    private static void demoBasicOperators() throws InterruptedException {
        System.out.println("=== map + filter ===");
        List<Integer> result = new ArrayList<>();
        Object lock = new Object();

        Observable.just(1, 2, 3, 4, 5)
                .filter(value -> value % 2 == 0)
                .map(value -> value * 10)
                .subscribe(new CollectingObserver<>(result, lock));

        waitFor(lock);
        System.out.println("Result: " + result);
    }

    private static void demoFlatMap() throws InterruptedException {
        System.out.println("=== flatMap ===");
        List<String> result = new ArrayList<>();
        Object lock = new Object();

        Observable.just("a", "b")
                .flatMap(letter -> Observable.just(letter + "1", letter + "2"))
                .subscribe(new CollectingObserver<>(result, lock));

        waitFor(lock);
        System.out.println("Result: " + result);
    }

    private static void demoSchedulers() throws InterruptedException {
        System.out.println("=== subscribeOn + observeOn ===");
        List<String> threads = new ArrayList<>();
        Object lock = new Object();

        Observable.<String>create(observer -> {
                    threads.add("source:" + Thread.currentThread().getName());
                    observer.onNext("value");
                    observer.onComplete();
                })
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.single())
                .subscribe(new ru.mephi.rx.Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        threads.add("observer:" + Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        synchronized (lock) {
                            lock.notify();
                        }
                    }

                    @Override
                    public void onComplete() {
                        synchronized (lock) {
                            lock.notify();
                        }
                    }
                });

        waitFor(lock);
        System.out.println("Threads: " + threads);
    }

    private static void demoErrorHandling() throws InterruptedException {
        System.out.println("=== error handling ===");
        List<String> events = new ArrayList<>();
        Object lock = new Object();

        Observable.<Integer>create(observer -> {
                    observer.onNext(1);
                    observer.onError(new IllegalStateException("boom"));
                    observer.onNext(2);
                })
                .subscribe(new ru.mephi.rx.Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        events.add("next:" + item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        events.add("error:" + t.getMessage());
                        synchronized (lock) {
                            lock.notify();
                        }
                    }

                    @Override
                    public void onComplete() {
                        events.add("complete");
                        synchronized (lock) {
                            lock.notify();
                        }
                    }
                });

        waitFor(lock);
        System.out.println("Events: " + events);
    }

    private static void demoDispose() throws InterruptedException {
        System.out.println("=== dispose ===");
        List<Integer> result = new ArrayList<>();
        Object lock = new Object();

        ru.mephi.rx.Disposable disposable = Observable.<Integer>create(observer -> {
                    for (int i = 1; i <= 5; i++) {
                        observer.onNext(i);
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    observer.onComplete();
                })
                .subscribeOn(Schedulers.io())
                .subscribe(new CollectingObserver<>(result, lock));

        Thread.sleep(80);
        disposable.dispose();

        synchronized (lock) {
            lock.wait(500);
        }
        System.out.println("Received before dispose: " + result);
    }

    private static void waitFor(Object lock) throws InterruptedException {
        synchronized (lock) {
            lock.wait(3000);
        }
    }

    private static final class CollectingObserver<T> implements ru.mephi.rx.Observer<T> {

        private final List<T> target;
        private final Object lock;

        private CollectingObserver(List<T> target, Object lock) {
            this.target = target;
            this.lock = lock;
        }

        @Override
        public void onNext(T item) {
            target.add(item);
        }

        @Override
        public void onError(Throwable t) {
            synchronized (lock) {
                lock.notify();
            }
        }

        @Override
        public void onComplete() {
            synchronized (lock) {
                lock.notify();
            }
        }
    }
}
