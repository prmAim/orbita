# Диагностика разработки и ранняя проверка схемы

Локальная проверка 17 ноября 2026 года, Windows, ветка
`codex/dev-preflight-schema-check` от main
`d57081dd90de47a45fbb4dce328dcf2edd0b37df`.
Процедура повторения — [проверки разработки](../development/checks.md).

## Область

Добавлены stdlib CLI диагностики JDK/Wrapper/кэша/Docker и проверки
явно квалифицированных схем временного набора. Оба CI workflow проверяют
схему сразу после checkout, до установки проектных зависимостей и сборок.
Изменений Java, торговой логики, миграций, runtime guards Generate/Remove,
frontend и фикстур `source(...)` нет. Runtime safety и прежние пары
manifest 1/schema 15 и manifest 2/schema 16,17 сохранены.

Код и тесты: GPT-6.1 Sol. Ревью Standards и Spec: GPT-6 Astra, финальные
результаты — 0 замечаний по каждой оси. Исправлены замечания о потомках
зависшего launcher, старом guard в inline-комментарии PowerShell и точном
hash URL зеркала Wrapper с завершающим slash. Дополнительно исключены
нечисловые/бесконечные значения таймаута.

## Автоматические проверки

Все команды выполнялись из корня репозитория. Для временных файлов тестов
в ограниченном Windows-контексте предварительно выполнено:

```powershell
$env:TEMP=(Resolve-Path .scratch/dev-preflight).Path
$env:TMP=$env:TEMP
```

| Команда | Результат |
| --- | --- |
| `.venv/Scripts/python.exe -m unittest discover -s tests -p test_dev_checks.py -v` | PASS: 19 тестов, 12.748 с, restricted |
| `.venv/Scripts/python.exe -m unittest discover -s tests -v` | Первый restricted-прогон: 41 PASS, 1 ERROR, 13.320 с; прежний local-proxy test получил sandbox WinError 10013 при соединении с собственным loopback HTTP-имитатором |
| Та же полная команда после finite-timeout regression, вне sandbox | PASS: 43 теста, 11.088 с; только имитаторы, без реальных внешних сервисов |
| `.venv/Scripts/python.exe -m ruff check src tests tools` | PASS |
| `.venv/Scripts/python.exe -m ruff format --check src tests tools` | PASS: 27 файлов |
| `.venv/Scripts/python.exe tools/check_demo_schema.py` | PASS: schema 17, Generate 17, cleanup 1:15; 2:16,17 |
| `git diff --check` | PASS |

19 целевых тестов запускают реальные CLI и проверяют ненулевые exit codes:
V18 без квалификации, отсутствующие/испорченные квалификации и миграции,
изменённые/удалённые/дублированные guards Java/PowerShell/SQL, подмену guard
комментариями, старые пары, точный кэш зеркала Wrapper, отсутствие JDK/Maven/
Docker, неверный JDK, отказ Docker и недоступный Engine. Проверены конечный
таймаут launcher, завершение его потомка, отказ назначения Windows job до
запуска инструмента и отклонение `nan`, `inf`, 0, -1, 61.
До finite-timeout regression целевой прогон содержал 18 успешных тестов.

Штатный `uv run --offline --no-sync` в restricted-контексте не смог записать
lock/interpreter-cache uv. Поэтому использован уже установленный interpreter
`.venv/Scripts/python.exe`; переустановка или загрузки не выполнялись.
Его restricted relocation warning не повлиял на успешные проверки.
Первоначальная попытка cleanup собственного probe через taskkill также
получила OS authentication error; итоговая реализация использует частный
Windows Job Object, без изменения учётных данных или прав доступа.

## Read-only проверка установленного окружения

Точная команда этого локального прогона (пути — параметры, не defaults):

```powershell
.venv/Scripts/python.exe tools/check_dev_environment.py --java-home 'C:/Program Files/Java/jdk-25' --maven-repository 'C:/Git/orbita/.scratch/sdk151-probe/m2' --build-directory 'C:/Git/orbita/.scratch/dev-preflight/backend' --timeout 5
```

- Restricted: exit 1; JDK 25.0.1, Wrapper 3.3.4 и cached Maven 3.10.0
  подтверждены; Docker — `ACCESS_DENIED`, команда сборки не выдана.
- Та же read-only команда вне sandbox: exit 0; Docker Engine 29.7.2,
  выдана команда `--offline ... spotless:check verify` с явными JAVA_HOME,
  repository и build directory. Maven не запускался.
- `Test-Path .scratch/dev-preflight/backend` — False после проверок:
  выбранный каталог сборки диагностикой не создан.

## Границы доказательств

NOT_RUN: Linux-исполнение адаптера процессов, remote CI, новая сборка
backend/frontend, PostgreSQL/Testcontainers, Standard/Large, реальные
Generate/Remove и живая приёмка брокера/Telegram. Сборка приложения не
повторялась: изменены developer tooling, тесты, документация и CI, runtime
код и SQL не изменены. Полнота Maven dependency cache не квалифицирована.
Локальный PASS не означает remote CI PASS или готовность торговой приёмки.
