# Mini RxJava

Я реализовал свой мини RxJava для проекта. Ниже находится отчет и подробно описано, как запускать.

## Запуск

```bash
mvn test
mvn exec:java -Dexec.mainClass="ru.mephi.rx.demo.Demo"
```

Демо показывает map/filter, flatMap, schedulers, ошибки и dispose. После всех примеров печатает === done ===.

## Структура

```
src/main/java/ru/mephi/rx/
├── Observer, Observable, Disposable
├── operators/     (map, filter, flatMap, subscribeOn, observeOn)
├── scheduler/     (IO, Computation, SingleThread)
├── internal/      (SafeObserver, DisposableHelper)
└── demo/Demo.java

src/test/java/ru/mephi/rx/
├── ObservableBasicTest
├── OperatorsTest
├── FlatMapTest
├── SchedulerTest
├── DisposableTest
└── ErrorHandlingTest
```

## Как устроено

Всё крутится вокруг пары Observable + Observer. Observable это источник событий. Observer тот, кто их принимает: onNext, onError, onComplete.

Подписка:

```java
Disposable d = observable.subscribe(observer);
```

create() это главный способ создать поток. Дальше по коду определяешь, что и когда отправлять:

```java
Observable.<Integer>create(observer -> {
    observer.onNext(1);
    observer.onNext(2);
    observer.onComplete();
});
```

Есть ещё just(), empty(), error(). При subscribe() внешний observer оборачивается в SafeObserver. Он не даёт слать события после dispose() и ловит исключения при старте подписки, чтобы они ушли в onError, а не в stderr. Каждый оператор это отдельный класс-наследник Observable. Он подписывается на предыдущий поток и передаёт результат дальше. Примерно как в настоящей либе RxJava.

### Операторы

map преобразует каждый элемент. Упал mapper, срабатывает onError, поток стопается.

filter пропускает только то, что прошло предикат.

flatMap на каждый элемент создаёт новый Observable и сливает их в один поток. onComplete приходит только когда завершился исходный поток и все внутренние.

subscribeOn переносит подписку на источник в поток планировщика. Удобно, если источник тяжёлый (сеть, файл).

observeOn переносит сами события (onNext, onError, onComplete) в поток планировщика.

subscribeOn отвечает за то, где запускается источник.
observeOn отвечает за то, где обрабатываются события.

Частый вариант: данные грузим в io(), а обрабатываем в single().

## Schedulers

Интерфейс один:

```java
public interface Scheduler {
    void execute(Runnable task);
}
```

Три реализации:

IOThreadScheduler на CachedThreadPool, для блокирующего I/O (сеть, диск).

ComputationScheduler на FixedThreadPool, для вычислений.

SingleThreadScheduler на одном потоке, всё выполняется строго по очереди.

ComputationScheduler берёт max(2, availableProcessors()) потоков. Потоки создаются как daemon через SchedulerThreadFactory. 

io: потоков сколько угодно, норм для сети и диска. Для CPU-задач не надо, раздует пул.

computation: фиксированный пул под ядра, для коротких вычислений.

single: всё строго по очереди в одном потоке.

Доступ через Schedulers.io(), Schedulers.computation(), Schedulers.single().

## Ошибки

Если что-то падает, уходит в onError:

1. исключение в create при подписке
2. ошибка в map / filter / flatMap
3. ошибка во внутреннем потоке flatMap

После onError новых onNext уже не будет. Один терминальный сигнал: либо ошибка, либо complete.

## Disposable

subscribe() возвращает Disposable. Вызвал dispose(), подписка отменена, новые события не доходят.

Для составных операторов (flatMap, observeOn) отмена пробрасывается и наверх.

Это учебная версия: dispose() не останавливает сам источник в create(), если он уже крутится в цикле. Он просто перестаёт слать данные observer'у.

## Тесты

24 теста на JUnit 5. Запуск: mvn test.

Что проверяется:

1. ObservableBasicTest: create, just, empty, error
2. OperatorsTest: map, filter, цепочки, ошибки в mapper/predicate
3. FlatMapTest: слияние потоков, пустые inner, ошибки
4. SchedulerTest: все три планировщика, subscribeOn, observeOn, параллельность
5. DisposableTest: dispose и SafeObserver
6. ErrorHandlingTest: ошибки в источнике и операторах

## Примеры

Простая подписка:

```java
Observable.just(1, 2, 3).subscribe(new Observer<Integer>() {
    public void onNext(Integer item) { System.out.println(item); }
    public void onError(Throwable t) { t.printStackTrace(); }
    public void onComplete() { System.out.println("done"); }
});
```

map + filter:

```java
Observable.just(1, 2, 3, 4, 5)
    .filter(n -> n % 2 == 0)
    .map(n -> n * 10)
    .subscribe(observer);
```

flatMap:

```java
Observable.just("a", "b")
    .flatMap(l -> Observable.just(l + "1", l + "2"))
    .subscribe(observer);
```

schedulers:

```java
Observable.<String>create(source -> {
        source.onNext(loadFromNetwork());
        source.onComplete();
    })
    .subscribeOn(Schedulers.io())
    .observeOn(Schedulers.single())
    .subscribe(observer);
```

Отмена:

```java
Disposable d = observable.subscribe(observer);
d.dispose();
```

Живой пример со всеми сценариями: ru.mephi.rx.demo.Demo.