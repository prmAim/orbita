# Ф-04 / A2: долговечные доказательства заявки

Дата: 17 ноября 2026. Задача #36, спецификация #34,
[§§3.2–3.4](../specifications/f04-r52-rejection-classification.md).

Статус: **локальная реализация #36 завершена** в ветке
`codex/f04-a2-durable-order-evidence`, код проверен на `901a945`.
Полный `spotless:check verify` — **PASS: 134 unit + 174 integration = 308**,
failures/errors/skipped = 0. В исходном локальном результате добавлены 22
проверки: 5 TLS и 17 PostgreSQL. При публикации PR #43 добавлены ещё две
PostgreSQL-регрессии позднего исполнения FINAL; исправление `97cdcb4`
прошло целевой набор 66/66 и повторное pre-merge ревью 0/0/0.
Полный CI обновлённого PR проверяется отдельно от исходных 308 тестов.
После запуска
Docker владельцем свежий preflight подтвердил Engine 29.7.2. Первый независимый
раунд GPT-6 Astra: Standards — 0 замечаний, Spec — 3 замечания. Для всех трёх
добавлены падающие регрессии и исправления. Повторное ревью `7e252ea`:
Standards — 0, Spec — 0 открытых замечаний. После исправлений полного прогона
финальное ревью `901a945` также дало 0/0. Повторный штатный verify завершён.
Положительные BUY/HALTED/SELL
предикаты, реальные счета, Telegram и живая приёмка — вне этого этапа.

## Изменение

OrderStateStream и mirror TradesStream дополняют существующий источник сделок.
Типизированный receipt в существующем журнале подтверждается отдельным commit
до решения; маркер применения записывается атомарно с состоянием и уведомлением.
Replay восстанавливает неприменённые receipts после ошибки/рестарта. Схема 17
не менялась. Локальный номер журнала фиксирует порядок записи, а не порядок
событий брокера. Внутренний order-id, trade-order-id и UUID не взаимозаменяются.

Конечный результат не открывается повторным UNKNOWN/активным кадром. Дубли не
создают второй окончательный пропуск/уведомление. Поздние уникальные сделки
сохраняют исполненный объём и исправляют оставшийся пропуск. Противоречие своей
попытки сохраняет защитную паузу и требует независимой сверки. Неизвестное
размещение не создаёт фиктивный brokerOrderId или нулевую комиссию.

## Локальные результаты

| Проверка | Статус / доказательство |
| --- | --- |
| Ранняя совместимость набора | PASS: `check_demo_schema.py`, схема 17 |
| JDK / cached Maven | PASS: JDK 25.0.1, Maven 3.10.0, offline repository |
| Docker Engine | Первый preflight: `ENGINE_UNAVAILABLE`; после запуска владельцем свежий preflight PASS, Engine 29.7.2 |
| Первый SDK/TLS запуск sandbox | BLOCKED: loopback `Permission denied`, 23 errors; не ошибка брокерского предиката |
| A1 SDK unit/TLS после разрешённого loopback | PASS 27/27, `a2-focused-tls.log` |
| TDD malformed quotation | RED 1/1: событие терялось до receipt callback; GREEN 1/1 после сохранения unqualified price, `a2-mirror-red.log`, `a2-mirror-green.log` |
| SDK/TLS регрессии | PASS 54/54: OrderSdkEvidenceTest, OrderSdkTlsTest, MirrorOrderFeedTlsTest, TInvestBrokerGatewayTest; `a2-focused-sdk-regressions.log` |
| Завершающий TLS E01/REQUEST/stream | PASS 3/3, `a2-final-focused-tls.log`; повторная компиляция всех 39 test source files PASS |
| Первый PostgreSQL запуск | CopyLifecycle 47/47 PASS; новые A2 фикстуры ошибочно упёрлись в firstAcceptance=1 lot и исправлены через публичный синтетический setup допуска |
| Astra регрессии RED | 16 сценариев: 5 ожидаемых failures (3 terminal quantity rows, timer, independent reconciliation), 0 errors; genuine restart PASS; `a2-astra-findings-red.log` |
| PostgreSQL GREEN | PASS 63/63: CopyLifecycleIT 47 + OrderEvidenceIT 15 + OrderEvidenceRestartIT 1; `a2-astra-findings-green.log` |
| Незавершённый собственный receipt / readiness | Дополнительный RED 1 failure: лотность 0 не останавливала восстановление; исправлено fail-closed. Итоговый A2 GREEN 17/17 (OrderEvidenceIT 16 + genuine restart 1), `a2-pending-readiness-red.log`, `a2-pending-readiness-green.log` |
| Независимое ревью GPT-6 Astra | Раунд 1 (`5d84f52`): Standards 0, Spec 3. Повторное ревью (`7e252ea`) и финальная дельта (`901a945`): Standards 0, Spec 0 открытых замечаний |
| Первый полный backend verify | 132 unit PASS; 174 integration, 2 failures, 0 errors/skips. Старая потоковая фикстура оставляла mirror-барьер закрытым; общий имитатор изменил число fill-записей для AccountLocalReadIT. Все 17 новых A2 IT прошли. `a2-full-verify.log` |
| Регрессии после полного прогона | Shutdown race воспроизведён RED 1: intentional close ошибочно сообщал persistence failure. Добавлены guards закрытия; подтверждены отдельные source/mirror ACK; агрегированный fill возвращён по умолчанию. GREEN 43/43: BrokerConnectivity 12, Mirror TLS 5, AccountLocalRead 10, A2 OrderEvidence 16. `a2-shutdown-red.log`, `a2-full-suite-findings-green.log` |
| Повторный полный backend verify | PASS: 134 unit + 174 integration = 308; failures/errors/skipped = 0, `spotless:check` PASS. Проверен код `901a945`, `a2-full-verify-final.log`, 7:58 |
| Frontend | `npm run typecheck` и `npm run build` — PASS |
| Публикация | [PR #43](https://github.com/prmAim/orbita/pull/43), ветка отправлена. Первый Java/TypeScript и Python CI на `d1c554b` PASS; CI исправленного коммита проверяется отдельно. Merge и закрытие #36 ожидают успешного CI |
| Pre-merge исправление позднего FINAL fill | RED 2/2 → GREEN 66/66 (CopyLifecycle 47, OrderEvidence 18, restart 1); `a2-late-partial-premerge-red-confirmed.log`, `a2-late-partial-premerge-green.log`. Коммит `97cdcb4` |
| Живая приёмка / реальные API / Telegram | NOT_RUN |

Журналы находятся в игнорируемом `.scratch/r52-research/`, сборка —
`.scratch/f04-a2-build/`; машинные пути и credentials в отслеживаемые файлы
не добавлялись. Maven запускался offline с отдельным build directory, отбором
`-Dtest=...` и штатным `spotless:apply test`; затем выполнен полный
`spotless:check verify`. Повтор полного набора потребовался после двух
подтверждённых ошибок первого прогона; до повтора выполнены целевые регрессии
и независимое ревью. Python-инструменты не менялись и их набор не повторялся.

## Независимое ревью

### Standards

Повторное независимое ревью GPT-6 Astra `5d84f52…7e252ea`: **0 замечаний**.
Исправления соответствуют документированным границам: stream не задаёт
минутный таймер и не заменяет независимую сверку; несогласованные статус и
объёмы сохраняют защиту; неподтверждённая лотность собственного receipt
останавливает восстановление готовности. Существенных нарушений стандартов
или эвристических smells в дельте не обнаружено.

### Spec

Повторное независимое ревью GPT-6 Astra `7e252ea`: **0 открытых замечаний**.
Все три прежних замечания исправлены:

- `validMirror` проверяет соответствие статуса объёмам: terminal с активным
  остатком и FILLED без полного исполнения уходят в противоречие/сверку.
- Stream NEW/PARTIAL не устанавливает `sentAt`; достоверный unary может
  задать минутный таймер.
- Stream-terminal не устанавливает `acceptanceOrderReconciled` и создаёт
  долговечный `ORDER_EVIDENCE_RECHECK`, включая FINAL в независимую сверку.

Дополнительный случай readiness исправлен: отсутствие gateway или
неподтверждённая лотность собственного связанного наблюдения прерывает replay;
receipt остаётся необработанным и барьер готовности не открывается.
Положительные BUY/HALTED/SELL-пути не подключены. Оба ревью выполнялись
read-only; результаты запусков тестов получены основным исполнителем.

Итог по осям: Standards — 0; Spec — 0 открытых замечаний.

Финальная дельта `7e252ea…901a945` независимо проверена теми же двумя осями:
**Standards — 0; Spec — 0 новых и открытых замечаний**. Guards намеренного
закрытия сохраняют обработку ошибок работающего потока. Фикстуры раздельно
подтверждают source/mirror-подписки; общий симулятор сохраняет агрегированные
fills, а детализация включается только для A2. Три прежних исправления
сохранены; расширения объёма A2 не обнаружено.

## Pre-merge ревью PR #43

Три независимых проверки GPT-6 Astra, база `9451ae2`, исходный head `d1c554b`,
повторная проверка исправления `97cdcb4`. Ревью выполнялись read-only.

### Security

**0 замечаний.** Изменения позднего исполнения атомарны с учётом исполнения
и маркером применения под существующими блокировками. Проверка `delta > 0`
предотвращает повторную инвалидизацию при дубле. Нового обхода разрешения
владельца не выявлено.

### Business Logic

Первый раунд: **1 P2** — поздний partial fill после FINAL не назначал
независимую сверку и не обновлял распределение восстановления. Исправлено
в `97cdcb4`, две регрессии подтвердили RED→GREEN. При увеличении исполнения
FINAL атомарно обновляются объёмы, назначается сверка, сбрасываются
подтверждения приёмки (включая прежнюю комиссию разовой попытки) и обновляется
распределение. Дубли с `delta=0` не повторяют эти действия.
Повторное ревью: **0 открытых замечаний**.

### Duplication + Dead Code

**0 замечаний.** Новые hooks, параметры и флаг фикстуры имеют вызывающие
стороны; флаг сбрасывается между сценариями. Повторная запись требования
сверки для одного наблюдения предотвращена явным условием.

Итог: Security — 0; Business Logic — 0 открытых; Duplication + Dead Code — 0.

## Соответствие сценариям

| ID | Исходник / наблюдаемое поведение | Выполнение |
| --- | --- | --- |
| A03 | TLS `acceptedPostWithLostReplyUsesOriginalRequestUuidForEveryLookup`: один принятый PostOrder, два not-found, затем terminal, все lookup — прежний счёт/UUID REQUEST. `OrderEvidenceIT.lostPostResponse...` проверяет циклы и резерв. `OrderEvidenceRestartIT` закрывает контекст и создаёт новые сервисы над той же PostgreSQL и тем же внешним имитатором | TLS и IT PASS |
| A04 | `lostPostResponse...`: независимый конечный результат, сохранённая защитная пауза, использованный разовый допуск не возвращается | IT PASS |
| A06 | `correlationRequiresCompleteConsistentAttemptIdentity` (instrument/side/volume/missing UUID), `foreignAndUnlinkedEvents...`: чужое не применяется, своё противоречие сохраняет fill и резерв, ставит паузу | IT PASS |
| A07 | `partialAfterCancelAndLateUniqueTrade...`: PARTIALLY_FILL не terminal; unary CANCELLED; поздние 10 штук = 1 лот; дубль не удваивает исполнение и итоговое уведомление | IT PASS |
| A08 | `duplicateTerminalAndLateUnknown...`, `lateCauseAndStateBeforeUnary...`, `receiptFailure...`: дубль, stream до возврата PostOrder, поздний cause, PostgreSQL failure до receipt и перед marker. `OrderEvidenceRestartIT`: committed receipt, откат решения, новый application context | IT PASS |
| E01 | TLS `earlyFailureWithoutOrderIdAndNotFoundDoesNotInventOrderOrCommission`: UNKNOWN, нет ID, нет qualified/нулевой комиссии. UNKNOWN/reserve после потери ответа проверяется через публичный торговый сервис | TLS и IT PASS |

Регрессии первого ревью отдельно подтверждают согласованность terminal-статуса
с остатком (FILLED/CANCELLED/REJECTED), сохранение достоверной минутной границы
при stream-NEW до PostOrder и обязательный unary после stream-terminal с R41.
Разовый допуск в новых multi-lot фикстурах настраивается публичными сервисами
на синтетической БД; это не живая приёмка брокерского счёта.

Ошибки записи моделируются PostgreSQL trigger
на отдельной синтетической БД, результат читается через публичные сервисы/журнал.
Рестарт — закрытие и создание нового Spring application context; это не имитация
повторным вызовом replay на прежнем объекте и не доказательство аварийной
устойчивости файловой системы целевого ПК.

## Границы

Ветка создана от актуальной `main` / `9451ae2`. #35 закрыта после PR #42
и успешного CI в `main`; это отдельное доказательство, описанное в
[отчёте A1](f04-a1-sdk-evidence.md), и не CI этой ветки. #36 реализована и
закоммичена локально; следующие #37/#38 не начаты. Ф-04, #20 и #34 целиком
не закрыты; #40/#41 сохраняют зависимость от уточнений API #39.

Локальный TLS-имитатор подтверждает обработку заданных SDK 1.51 сообщений,
но не контракт реального сервера. Причины не реконструируются из рынка/текста
ошибки. HALTED/SELL и положительный BUY остаются отложенными. Дополнительный
комиссионный резерв требует независимого доказательства; причина отказа его
не обнуляет. JSON-фильтрация журнала не имеет отдельного индекса; replay имеет
фиксированную верхнюю границу и страницы по 200 записей, несвязанные сделки
сохраняются до достаточной связи и не блокируют следующие страницы.
