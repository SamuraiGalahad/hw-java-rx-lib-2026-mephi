package ru.mephi.rx.operators;

import ru.mephi.rx.Disposable;
import ru.mephi.rx.Observable;
import ru.mephi.rx.ObservableOnSubscribe;
import ru.mephi.rx.Observer;
import ru.mephi.rx.internal.DisposableHelper;

public final class ObservableCreate<T> extends Observable<T> {

    private final ObservableOnSubscribe<T> source;

    public ObservableCreate(ObservableOnSubscribe<T> source) {
        this.source = source;
    }

    @Override
    protected Disposable subscribeActual(Observer<T> observer) throws Exception {
        source.subscribe(observer);
        return DisposableHelper.empty();
    }
}
