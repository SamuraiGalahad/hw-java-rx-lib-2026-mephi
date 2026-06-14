package ru.mephi.rx.operators;

import ru.mephi.rx.Disposable;
import ru.mephi.rx.Observable;
import ru.mephi.rx.Observer;
import ru.mephi.rx.Predicate;

public final class ObservableFilter<T> extends Observable<T> {

    private final Observable<T> source;
    private final Predicate<T> predicate;

    public ObservableFilter(Observable<T> source, Predicate<T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    protected Disposable subscribeActual(Observer<T> observer) throws Exception {
        return source.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                try {
                    if (predicate.test(item)) {
                        observer.onNext(item);
                    }
                } catch (Throwable t) {
                    observer.onError(t);
                }
            }

            @Override
            public void onError(Throwable t) {
                observer.onError(t);
            }

            @Override
            public void onComplete() {
                observer.onComplete();
            }
        });
    }
}
