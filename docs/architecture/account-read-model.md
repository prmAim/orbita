# Постоянная модель чтения счёта v0.149 (#27)

`ru.orbita.account` хранит наблюдения и отдельные проекции для веб-панели.
Это чтение счёта повторения: модуль не создаёт source_execution, copy_intent,
broker_fill, ручные корректировки или торговые команды. Защищённые GET не
обращаются к брокеру. Контракты банковских полей — [account-read.md](account-read.md).

## Схема и транзакции

Миграция V16 добавляет три таблицы:

- `account_read_document`: ключ `(account_id,kind,id)`, точный JSONB исходного
  контракта или HTTP-проекции, индексируемые поля поиска/фильтров/сортировки,
  время фактического наблюдения и признак текущей строки. Исходные виды:
  SECURITY, CASH, BANK_ORDER, BANK_FILL. Проекции: ASSET, ORDER, OPERATION, FILL.
- `account_read_freshness`: ключ `(account_id,section)`, последняя попытка,
  последнее успешное чтение, период и полнота отдельного источника.
- `account_read_revision`: ключ `account_id`, отпечаток долговечных локальных
  фактов, от которых зависит проекция.

Используются естественные ключи брокера/валюты/инструмента; вложенные составные
идентификаторы кодируются с длинами частей и Base64url, без неоднозначных
разделителей. Числа JSONB читаются как BigDecimal; HTTP передаёт десятичные
значения строками. PostgreSQL numeric используется для числовой сортировки.
Индексы обслуживают account/kind/current/time/id, инструмент и происхождение/
статус/направление; поиск по тикеру/названию выполняется буквальным подстрочным
поиском, без интерпретации `%` и `_` как шаблона.

`AccountReadService.refresh()` снимает все SDK-наблюдения вне транзакции БД.
Импорт и проекции фиксируются одной короткой транзакцией REPEATABLE READ;
отдельная advisory-блокировка `orbita-account-read:<account>` сериализует только
эту модель, не торговый координатор. Повторяются максимум три безопасные
транзакции при конфликте сериализации/блокировки; повтор не вызывает новый SDK
запрос и не выполняет внешний эффект. Обновление страницы читает согласованные
count/строки под той же границей. При неизменном отпечатке локальных фактов GET
не переписывает проекции. При изменении состояния инструмента или намерения
перестроение доступно сразу, в том числе без брокера. Отпечаток требует чтения
локальных таблиц; это осознанная цена изоляции без триггеров/изменения торговых
писателей. Внешние денежные обновления не перестраивают историю.

SQL применяет поиск, фильтры, сортировку и count ко всей проекции до LIMIT/OFFSET.
Завершающий ключ сортировки — устойчивый ID. Параметры связаны, сортировка
выбирается из закрытого списка; размер страницы только 25/50/100. Коллекции
возвращают текущую дату/полноту каждого источника и полный filtered total.

## Сохранение фактов и неопределённости

Полный пустой портфель скрывает текущую бумагу, сохраняя историю. Частичный/
ошибочный ответ этого не делает. Заявки хранятся независимо от текущего активного
списка; ранее известные незавершённые ID передаются на отдельное чтение.
Ошибки не удаляют заявки/исполнения. Неполный ответ объединяется с ранее
подтверждёнными исполнениями; меньшее новое исполненное количество не уменьшает
ранее доказанное, но делает текущую среднюю неподтверждённой с причиной.

ORBITA подтверждается долговечным намерением по requestId или brokerOrderId.
MANUAL требует точного совпадения всех исполнений с `mirror_trade_receipt`:
ключ счёт/сделка, инструмент, направление, количество и банковское время.
Отсутствие локальной записи означает UNKNOWN. Обнаруженные ручные сведения не
запускают ManualReconciliationService и не меняют позиции торговли.

Намерения SEND_STARTED/UNKNOWN/CANCEL_STARTED видны без банковского ID под
устойчивым `intent:<UUID>`. Отправка/отмена подписана как локальная попытка,
если банковское время неизвестно. Чистое несохранённое у брокера WAITING,
теневое SHADOW, HALTED и подтверждённые FINAL/RETRIED не являются активными
банковскими заявками; план/результат остаётся в «Операциях».

Группа ORBITA основана на сохранённом исходном исполнении и зарегистрированных
намерениях. При HALT-повторе целевой объём считается по корневому намерению,
не повторно по каждому retry. Средняя взвешивается уникальными реальными
исполнениями, не средними заявок; исходная цена не становится новой колонкой.
Внешние записи не получают фиктивный коэффициент, исходное исполнение и цель.
Непривязанные исполнения Operations API сохраняются как UNLINKED_EVIDENCE,
исключаются из сгруппированных итогов и не объединяются с Orders API по
совпадению цены/времени. При точном совпадении ID и всех полей с каноническим
исполнением повторное представление не создаётся.

В активах выбирается заявка последней известной фактической сделки, включая
продажу. Полнота списка старых заявок и полнота прочитанного периода исполнений
разделены. Подтверждённая средняя возможна, когда последняя сделка находится в
полностью прочитанном периоде исполнений и однозначно связана с заявкой. Более
новое непривязанное исполнение, несколько заявок в один последний момент или
неполнота исполненной части дают UNCONFIRMED; старая средняя не подставляется.
Ручной/повторяемый остаток разделяется только при квалифицированном совпадении
сохранённой композиции позиции и текущего банковского остатка.

## Деньги и жизненный цикл

`AccountReadLifecycle` включён при broker.enabled=true, account-read.enabled=true
(по умолчанию) и отключён в admin reset. После готовности приложения он открывает
отдельный денежный stream и раз в 30 секунд читает счёт; интервал настраивается
через `orbita.account-read.refresh-ms`. Он не затрагивает исходный trade stream.
Историческое окно текущего импорта — последние 24 часа, опубликовано в freshness;
сохранённые более старые факты остаются доступны. Ограничения SDK текущего дня
публикуются явно, без обещания полной старой истории реального счёта.

Денежные изменения применяются по валюте/банковскому времени, без удаления
пропущенных валют. Старые/повторные события не освежают сумму. Неоднозначный
GetPositions.money сохраняется как reportedMoney и не заменяет подтверждённые
stream total/available. Такой опрос не меняет время подтверждения этих сумм и
не обновляет их lastSuccessAt. Свежесть учитывает и банковское время, и время
получения; устаревшая валюта помечается STALE, даже если сырой опрос работает.

Ошибка callback сохранения логируется с исходной причиной и закрывает stream.
Следующий цикл сначала подтверждает восстановление записи явного пробела,
затем создаёт новую подписку; ошибка не трактуется как пустой банковский счёт.
Публичный AutoCloseable lifecycle закрывает и дожидается callback через SDK feed;
после close повторные плановые циклы не подписываются. Это также явная граница
остановки импорта перед фиксацией/очисткой синтетического предпросмотра.

Без gateway сохранённые строки не обнуляются. Явно заданный mirror account
выбирает его раздел. Если конфигурация очищена для offline-предпросмотра,
допустим ровно один сохранённый счёт; несколько дают явный конфликт, а не
скрытый выбор или пустой результат. Пароль/сессии живут в прежней подсистеме;
новые маршруты используют тот же OWNER и механизм отзыва сессий.

## HTTP и проверки

Контракт ниже реализован в AccountReadController; точные query-ID получают
запись вне текущей страницы/фильтров, неизвестный ID даёт 404. Journal link
паузы использует существующее полное событие с точным совпадением причины.

AccountReadIT проверяет SDK/TLS → приложение → настоящий PostgreSQL → обычный
HTTP владельца: 123 строки, поиск/фильтры/стабильные страницы, точные IDs,
106→107, меньший неполный ответ, сохранение после сбоя, последнюю продажу,
непривязанную новую сделку, пустой портфель, валютный stream, сохранение времени
подтверждения после сырого опроса, восстановление callback после ошибки БД,
новый offline-контекст приложения без gateway/настроенных счетов, 401/400/404 и
отзыв сессии. AccountLocalReadIT проверяет публичный вход исполнения/координатора
с настоящим PostgreSQL: неизвестное собственное намерение, окончательный
результат, точную ручную классификацию и повторную заявку без двойной цели/сделки.
Торговые фикстуры прямым SQL не загружаются; SQL служит очистке изолированной
тестовой БД и внесению/снятию проверяемой ошибки сохранения.

Живой брокер/Telegram и GUI-приёмка NOT_RUN в #27. Полный общий прогон и
независимое review выполняются после всей серии #26–33.

---

# v0.149 HTTP read contract (#27)

Owner session and existing revocation apply to every endpoint. GET only. Decimal numbers are exact decimal strings or null; dates are ISO UTC instants, UI labels/converts Moscow time. IDs are opaque Unicode-safe strings; preserve them exactly and URL-encode query parameters.

Endpoints: GET `/api/account/assets`, `/api/account/orders`, `/api/account/operations`.
All return `{items,total,page,size,sort,direction,freshness,cash}`. `cash` is populated for assets (otherwise []). `total` covers the whole filtered set. page is zero-based, size only25/50/100. Out-of-range page returns empty items with actual total.

Query: `q` (ticker/name), `instrumentId`, `side` BUY/SELL, `status`, `origin` ORBITA/MANUAL/UNKNOWN, `state` PAUSED/WAITING/READY, `attention` boolean, `from`/`to` ISO instants (inclusive), `sort` time/instrument/side/status/origin/lots/executedLots/executionPrice, `direction` asc/desc, `page`, `size`. Unsupported parameters/values produce400. Stable ID is always final sorting tiebreaker. Operations time is source execution time for ORBITA groups; order creation time for standalone broker orders.

Asset item: `{id,instrumentId,ticker,name,state,reason,journalId,copiedLots,manualLots,brokerLots,lastExecutionAt,lastExecutionId,lastOrderId,executionPrice,updatedAt,complete}`.

Order item: `{id,brokerOrderId,requestId,instrumentId,ticker,name,side,lots,executedLots,remainingLots,type,createdAt,timeSemantics,limitPrice,status,origin,cancellationPending,sourceExecutionIds,fills,executionPrice,updatedAt,complete,reason}`. Unsent WAITING/SHADOW intentions are not active rows; their planned result remains in Operations. Unknown app intents have stable `intent:<UUID>` IDs even without bank ID. External IDs begin `order:`. Limits remain separate from execution price.

Operation item: `{id,sourceExecutionId,instrumentId,ticker,name,side,time,timeSemantics,sourceLots,coefficient,targetLots,executedLots,skippedLots,result,reason,origin,orderIds,fills,lastExecutionAt,executionPrice,complete}`. No invented source/coefficient for external rows. ORBITA grouping uses durable source execution; fills unique across linked orders. Source price is not another primary column.

Fill item: `{id,orderId,tradeId,instrumentId,side,lots,price,executedAt,commission,provenance,complete}`. Execution IDs are opaque composite encodings to avoid delimiter collisions. Unlinked operation-service fills stay explicitly unlinked and are not combined with order-service fills without evidence.

Price: `{value,currency,unit,status,reason}`; status CONFIRMED / NO_EXECUTIONS / UNCONFIRMED. Nullable value never means0. Price applies to actual unique executed portion only. Assets use the order containing the latest known actual execution, including sale/manual. Unknown latest identity/coverage stays UNCONFIRMED.

Cash: `{currency,total,available,blocked,reportedMoney,bankAt,updatedAt,source,limitation,freshness}`. Currency stream updates merge without deleting omitted rows; GetPositions reportedMoney is not guessed as free/total.

`freshness` maps assets/orders/history/executions/cash to `{status,lastAttemptAt,lastSuccessAt,from,through,complete,detail}`. Status NEVER/FRESH/PARTIAL/ERROR/STALE. Partial/error import retains known rows and last successful time.

Exact retrieval independent of filters/pages: GET `/api/account/order?id=<opaqueOrderId>`, `/api/account/operation?id=<opaqueOperationId>`, `/api/account/fill?id=<opaqueFillId>`, `/api/account/journal?id=<numericId>`. Unknown exact ID404. Journal returns existing `{id,occurredAt,category,instrument,detail}`. Exact fill includes orderId for navigation.

This file is the agreed implementation contract for frontend integration. Verified by AccountReadIT and AccountLocalReadIT through real PostgreSQL and the declared application seams; live broker availability remains separate.
