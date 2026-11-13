# Сверка заявки по клиентскому UUID в T‑Invest Java SDK 1.51

13 ноября 2026 года. После потери ответа на `PostOrder` Orbita может запросить состояние по сохранённым счёту повторения и UUID, даже если биржевой номер остался неизвестен. Для этого контракт принимает `GetOrderStateRequest.order_id_type = ORDER_ID_TYPE_REQUEST`. Это операция чтения. Реальный брокерский API в исследовании не вызывался. [Контракт GetOrderState](https://developer.tbank.ru/invest/api/orders-service-get-order-state) · [Описание поиска по ключу](https://developer.tbank.ru/invest/services/orders/async)

## Идентификаторы и сроки

| Поле | Что содержит |
| --- | --- |
| `PostOrderRequest.order_id` | Выбранный клиентом ключ идемпотентности. Контракт 1.51 указывает формат UID и предел 36 символов. Для намерения Orbita подходит каноническая строка UUID с дефисами. |
| `PostOrderResponse.order_id`, `OrderState.order_id` | Биржевой номер. Если он известен, применяется `ORDER_ID_TYPE_EXCHANGE = 1`. |
| `PostOrderResponse.order_request_id`, `OrderState.order_request_id` | Клиентский ключ. Для поиска по нему используется `ORDER_ID_TYPE_REQUEST = 2`. В обоих вариантах само значение передаётся в `GetOrderStateRequest.order_id`. |
| `OrderStage.trade_id` | Номер конкретной сделки. Одно поручение может иметь несколько сделок. |

Сверено с таблицами Т‑Банка и с `orders.proto` непосредственно из `ru.tinkoff.piapi:java-sdk-grpc-contract:1.51`. SHA‑256 проверенного JAR — `766e19f0309568bacec486aefca705ebe1a0dc66973dc18ee192a1910a3f3ad3`. [Схема сервиса заявок](https://developer.tbank.ru/invest/services/orders/methods) · [Контракт версии 1.51](https://repo.maven.apache.org/maven2/ru/tinkoff/piapi/java-sdk-grpc-contract/1.51/java-sdk-grpc-contract-1.51.jar)

Банк проверяет уникальность пары из счёта и ключа в пределах последнего месяца. FAQ отдельно оговаривает сохранность ключа. У самого `GetOrderState` другой горизонт — сведения о поручениях старше одного дня могут оказаться недоступными. Два раздела документации по-разному описывают ответ на повторный `PostOrder` с тем же ключом. FAQ допускает возврат состояния существующего поручения, обзор указывает ошибку `30057`. Поэтому этот вызов нельзя использовать как проверку существования поручения. [FAQ сервиса](https://developer.tbank.ru/invest/services/orders/faq_orders) · [Время жизни ключа](https://developer.tbank.ru/invest/intro/developer/sla) · [Описание сервиса](https://developer.tbank.ru/invest/services/orders/head-orders)

## Чтение в Java

Фрагмент для уже созданной `ServiceStubFactory factory`. `accountId` и `clientUuid` берутся из сохранённого намерения. priceType задаётся по сохранённой единице цены. Форма вызова скомпилирована javac 25 на фактическом classpath SDK 1.51. [GetOrderState и PriceType](https://developer.tbank.ru/invest/api/orders-service-get-order-state) · [Исходники Java SDK Core 1.51](https://repo.maven.apache.org/maven2/ru/tinkoff/piapi/java-sdk-core/1.51/java-sdk-core-1.51-sources.jar)

```java
var orders = factory.newSyncService(OrdersServiceGrpc::newBlockingStub);
var request = GetOrderStateRequest.newBuilder()
    .setAccountId(accountId)
    .setOrderId(clientUuid.toString())
    .setOrderIdType(OrderIdType.ORDER_ID_TYPE_REQUEST)
    .setPriceType(priceType)
    .build();
OrderState state = orders.callSyncMethod(stub -> stub.getOrderState(request));
```

После неопределённого результата выполняется только чтение. С ответом сохраняют счёт из фактически отправленного запроса — в `OrderState` 1.51 отсутствует поле `account_id`. Для идентификации сверяют возвращённый `order_request_id`, биржевой номер (если уже известен), `instrument_uid`, направление, тип заявки, запрошенные лоты и, для лимитного поручения, исходную цену за инструмент и её единицу. Статус, `lots_executed` и детали `stages` описывают результат уже идентифицированной заявки. В статусе отмены также учитывают исполнения до отмены. Пустой ответ, ошибка чтения, пропущенное обязательное доказательство или противоречие оставляют намерение на сверке. Их нельзя использовать как основание для новой отправки. [Контракт заявок](https://developer.tbank.ru/invest/services/orders/methods) · [Протокол Orbita](https://github.com/prmAim/orbita/issues/11#issuecomment-6725242827) · [Предыдущая локальная проверка обычного и resilience-вызова](https://github.com/prmAim/orbita/issues/17#issuecomment-6859565148)

## Цена, количество и история

| Источник | Количество | Цена и время |
| --- | --- | --- |
| `PostOrderRequest` и `OrderState` | Заявляемое `quantity`, возвращаемые `lots_requested` и `lots_executed` измеряются в лотах. | Заявленная лимитная цена относится к одному инструменту. Для проверки исходной цены в состоянии используют `initial_security_price`. Время `order_date` относится к выставлению. |
| `OrderState.stages[]` | `OrderStage.quantity` измеряется в лотах. | `price` относится к одному инструменту, `execution_time` — к данной сделке. Проверка наличия времени в Java — `stage.hasExecutionTime()`. |
| `TradesStream → OrderTrades.trades[]` | `OrderTrade.quantity` измеряется в штуках. | `price` относится к одному инструменту, `date_time` — к исполнению. Внешнее `created_at` обозначает создание сообщения. |
| История операций | `OperationTrade.quantity` и `OperationItemTrade.quantity` измеряются в единицах инструмента. | В курсорной истории поля отдельной сделки называются `num`, `date`, `price`. |

Например, при лотности 10 одна стадия на 2 лота сопоставляется с 20 штуками. Количество и цену сверяют с учётом конкретного типа сообщения. В 1.51 комментарий к `PostOrderResponse.executed_order_price` описывает среднюю цену инструмента, а к одноимённому полю `OrderState` — агрегат с учётом количества лотов. Для цены отдельного исполнения используют поле стадии. Денежные значения сопоставляют с учётом валюты, `units` и `nano`. Для облигаций и фьючерсов сохраняют различие между пунктами и валютой. [Схема заявок](https://developer.tbank.ru/invest/services/orders/methods) · [Схема операций](https://developer.tbank.ru/invest/services/operations/methods) · [Контракт SDK 1.51](https://repo.maven.apache.org/maven2/ru/tinkoff/piapi/java-sdk-grpc-contract/1.51/java-sdk-grpc-contract-1.51.jar)

Т‑Банк предупреждает, что ID операции может измениться при повторной выгрузке, а номер сделки в истории операций может отличаться от номера в сервисе заявок. Банк предлагает по ID операции запросить стадии через `GetOrderState`. В Orbita это дополнительная проверка кандидата в пределах доступной истории. Сходства инструмента, направления, времени, цены и количества недостаточно для однозначной привязки при нескольких похожих сделках. Если связующее подтверждение получить не удалось, результат остаётся неопределённым. Доступ конкретного счёта и фактическую полноту данных при восстановлении соединения предстоит проверить отдельно. [Особенности истории операций](https://developer.tbank.ru/invest/services/operations/operations_problems) · [Ограничения глубины истории заявок](https://developer.tbank.ru/invest/services/orders/head-orders) · [Задача живой проверки](https://github.com/prmAim/orbita/issues/6)

Для учёта отдельных исполнений Orbita сохраняет область идентификатора (счёт повторения, биржевой номер заявки, trade_id). Повторное чтение той же стадии с теми же параметрами не увеличивает позицию. Противоречие по одному ключу требует сверки. Для полного согласованного снимка сумма quantity уникальных стадий должна совпасть с lots_executed; для статуса FILL сверяют также равенство с lots_requested. Несходящиеся детали и положительный общий объём сохраняются как наблюдение, разницу не превращают в придуманную сделку. PARTIALLYFILL фиксирует известный объём. Например, при отмене заявки на 5 лотов с фактически исполненными 2 учитываются эти 2 лота, а для освобождения резерва остатка требуется подтверждение прекращения заявки. В OrderStateStream частичная заявка может сохранить частичный статус после отмены. Это правила учёта Orbita и документированная особенность стрима. [Состояния в стриме](https://developer.tbank.ru/invest/services/orders/orders_state_stream) · [Единицы цены по классу инструмента](https://developer.tbank.ru/invest/intro/developer/table_order_currency)

При тайм-ауте, NOT_FOUND или ранее полученном 30057 сохранить барьер отправки, резерв и состояние UNKNOWN до положительной сверки. 30057 сообщает о дубликате при недоступном отчёте и не содержит фактического объёма. Проверка реального адаптера требует ранее известного клиентского UUID именно заявки на счёте повторения; проверка потока исходного счёта в задаче №6 решает другую задачу. [Ошибки API](https://developer.tbank.ru/invest/intro/developer/error-codes/errors) · [Протокол восстановления](https://github.com/prmAim/orbita/issues/11#issuecomment-6725242827)
