# Ф-04 / A2: долговечные доказательства заявки

Дата: 17 ноября 2026. Задача #36, спецификация #34,
[§§3.2–3.4](../specifications/f04-r52-rejection-classification.md).

Статус: локальная реализация прошла SDK/TLS и PostgreSQL-регрессии. После запуска
Docker владельцем свежий preflight подтвердил Engine 29.7.2. Первый независимый
раунд GPT-6 Astra: Standards — 0 замечаний, Spec — 3 замечания. Для всех трёх
добавлены падающие регрессии и исправления; повторное ревью ещё не выполнено.
Задача не объявляется принятой или готовой к эксплуатации до повторного
независимого ревью и полного штатного verify. Положительные BUY/HALTED/SELL
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
| Независимое ревью GPT-6 Astra | Раунд 1: Standards 0, Spec 3; исправления прошли RED→GREEN, повторное ревью NOT_RUN |
| Полный backend verify / CI / живая приёмка | NOT_RUN |

Журналы находятся в игнорируемом `.scratch/r52-research/`, сборка —
`.scratch/f04-a2-build/`; машинные пути и credentials в отслеживаемые файлы
не добавлялись. Maven запускался offline с отдельным build directory, отбором
`-Dtest=...` и штатным `spotless:apply test`; полный verify выполняется отдельно.

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

Локальный TLS-имитатор подтверждает обработку заданных SDK 1.51 сообщений,
но не контракт реального сервера. Причины не реконструируются из рынка/текста
ошибки. HALTED/SELL и положительный BUY остаются отложенными. Дополнительный
комиссионный резерв требует независимого доказательства; причина отказа его
не обнуляет. JSON-фильтрация журнала не имеет отдельного индекса; replay имеет
фиксированную верхнюю границу и страницы по 200 записей, несвязанные сделки
сохраняются до достаточной связи и не блокируют следующие страницы.
