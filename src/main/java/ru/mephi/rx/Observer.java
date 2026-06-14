package ru.mephi.rx;

/**
 * Наблюдатель реактивного потока данных.
 *
 * @param <T> тип элементов потока
 */
public interface Observer<T> {

    void onNext(T item);

    void onError(Throwable t);

    void onComplete();
}
