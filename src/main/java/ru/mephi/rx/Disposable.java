package ru.mephi.rx;

/**
 * Представляет подписку, которую можно отменить.
 */
public interface Disposable {

    void dispose();

    boolean isDisposed();
}
