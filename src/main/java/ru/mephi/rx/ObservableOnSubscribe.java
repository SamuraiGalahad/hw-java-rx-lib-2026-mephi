package ru.mephi.rx;

/**
 * Источник данных для создания {@link Observable}.
 *
 * @param <T> тип элементов потока
 */
@FunctionalInterface
public interface ObservableOnSubscribe<T> {

    void subscribe(Observer<T> observer) throws Exception;
}
