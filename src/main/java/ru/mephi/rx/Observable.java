package ru.mephi.rx;

import ru.mephi.rx.internal.CompositeDisposable;
import ru.mephi.rx.internal.SafeObserver;
import ru.mephi.rx.operators.ObservableCreate;
import ru.mephi.rx.operators.ObservableFilter;
import ru.mephi.rx.operators.ObservableFlatMap;
import ru.mephi.rx.operators.ObservableMap;
import ru.mephi.rx.operators.ObservableObserveOn;
import ru.mephi.rx.operators.ObservableSubscribeOn;
import ru.mephi.rx.scheduler.Scheduler;

/**
 * Реактивный поток данных, поддерживающий подписку и цепочку операторов.
 *
 * @param <T> тип элементов потока
 */
public abstract class Observable<T> {

    public final Disposable subscribe(Observer<T> observer) {
        SafeObserver<T> safeObserver = new SafeObserver<>(observer);
        try {
            Disposable upstream = subscribeActual(safeObserver);
            return new CompositeDisposable(safeObserver, upstream);
        } catch (Throwable t) {
            if (!safeObserver.isDisposed()) {
                safeObserver.onError(t);
            }
            return safeObserver;
        }
    }

    protected abstract Disposable subscribeActual(Observer<T> observer) throws Exception;

    public static <T> Observable<T> create(ObservableOnSubscribe<T> source) {
        return new ObservableCreate<>(source);
    }

    public static <T> Observable<T> just(T... items) {
        return create(observer -> {
            for (T item : items) {
                observer.onNext(item);
            }
            observer.onComplete();
        });
    }

    public static <T> Observable<T> error(Throwable error) {
        return create(observer -> observer.onError(error));
    }

    public static <T> Observable<T> empty() {
        return create(Observer::onComplete);
    }

    public <R> Observable<R> map(Function<T, R> mapper) {
        return new ObservableMap<>(this, mapper);
    }

    public Observable<T> filter(Predicate<T> predicate) {
        return new ObservableFilter<>(this, predicate);
    }

    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        return new ObservableFlatMap<>(this, mapper);
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new ObservableSubscribeOn<>(this, scheduler);
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return new ObservableObserveOn<>(this, scheduler);
    }
}
