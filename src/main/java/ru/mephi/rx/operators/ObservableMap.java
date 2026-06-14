package ru.mephi.rx.operators;

import ru.mephi.rx.Disposable;
import ru.mephi.rx.Function;
import ru.mephi.rx.Observable;
import ru.mephi.rx.Observer;

public final class ObservableMap<T, R> extends Observable<R> {

    private final Observable<T> source;
    private final Function<T, R> mapper;

    public ObservableMap(Observable<T> source, Function<T, R> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected Disposable subscribeActual(Observer<R> observer) throws Exception {
        return source.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                try {
                    observer.onNext(mapper.apply(item));
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
