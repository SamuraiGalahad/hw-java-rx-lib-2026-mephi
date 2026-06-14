package ru.mephi.rx;

/**
 * Предикат для фильтрации элементов потока.
 *
 * @param <T> тип проверяемого значения
 */
@FunctionalInterface
public interface Predicate<T> {

    boolean test(T t) throws Exception;
}
