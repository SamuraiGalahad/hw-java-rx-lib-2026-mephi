package ru.mephi.rx;

/**
 * Функция преобразования значения.
 *
 * @param <T> тип входного значения
 * @param <R> тип результата
 */
@FunctionalInterface
public interface Function<T, R> {

    R apply(T t) throws Exception;
}
