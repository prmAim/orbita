# Приёмка подключения Orbita — 2026-11-08

## Итог

Подключены LangGraph, LangChain, Pydantic, OpenAI Responses API и локальный
Langfuse SDK. Это проверенная техническая основа для разработки; бизнес-сценарии
и готовность к промышленной эксплуатации не заявляются.

- Проект: `C:\Git\orbita`, отдельная `.venv`, ветка `codex/ai-foundation`.
- Python 3.13.18, uv 0.13.7. Все версии зависимостей совпадают с проверенным
  набором Fortune; обновления пакетов не выполнялись.
- OpenAI: существующий ключ setupAIv2/Fortune постоянно используется по явному
  решению владельца. Локальная копия находится в `.env` Orbita. Для работы
  приложение не читает другие репозитории; ротация ключа требует обновления копий.
- Langfuse: отдельный проект `orbita`, отдельная пара ключей, общий локальный
  сервер `http://localhost:3000`, окружение `development`.
- LangSmith Cloud не подключён, трассировка явно отключена.

## Проверки

| Проверка | Результат |
| --- | --- |
| `orbita-check` без сети | PASS |
| `orbita-check --mode local` с записью и чтением трассы | PASS |
| Один `orbita-check --mode live` через Responses API | PASS |
| 17 unittest-тестов, включая Windows proxy и offline-набор | PASS |
| Ruff и форматирование | PASS |
| Сборка sdist и wheel без сети и общего кэша | PASS |
| Установка из автономного набора в новую `.venv` | PASS |
| Ключи Fortune/setupAIv2 не читают трассу Orbita | PASS |
| Проекты Fortune/setupAIv2 доступны с прежними ключами | PASS |
| Секреты отсутствуют в кандидатах Git и сборочных архивах | PASS |
| GitHub Actions, push, новая перезагрузка Windows | NOT_RUN |
| Восстановление Docker и данных Langfuse из backup | NOT_RUN |

Локальная трасса: `0174ed48384758f57ecc25b30c2a13e2`.
Живая трасса: `c213c3de472cd875670ae4378e43e437`.
Модель: `gpt-6-luna`, вход 14, выход 8, всего 22 токена. Автоматических повторов
вызова модели нет; `store=False`, таймаут 30 секунд, максимум 256 выходных токенов.
API-ключи других проектов возвращают пустой результат
при запросе этой трассы; общий администратор сервера всё равно имеет доступ
ко всем проектам. Ключи и содержимое `.env` не выводились.

## Автономный набор

Каталог `.offline/windows-x64-cp313` содержит 66 wheels, Python и uv;
размер около 110.6 MiB. Манифест проверяет SHA-256 для 2825 файлов.
Сборочные зависимости включают Hatchling 1.33.0 и editables 0.6.

Пакеты подготовлены из уже сохранённых wheels Fortune, с `UV_OFFLINE=true`,
`PIP_NO_INDEX=1` и локальным `PIP_FIND_LINKS`. Установка выполнялась Python/uv
из набора, с `--offline --no-cache --no-index --no-python-downloads`, пустым
временным кэшем и заведомо недоступным прокси только в процессе установки.
Проверка совместимости подтвердила 67 установленных пакетов, включая Orbita.

Готовый набор не зависит от кэша или `.venv` Fortune. Бинарные файлы исключены
из Git. Для защиты от потери диска скопируйте набор вместе с исходниками
на резервный носитель; секреты, Docker-образы и базы требуют отдельной копии.
OpenAI по-прежнему требует интернет. Рабочие окружения других проектов не менялись.

## Воспроизведение

```powershell
cd C:\Git\orbita
uv run --offline --no-sync orbita-check
uv run --offline --no-sync orbita-check --mode local
# Отдельный платный вызов, если требуется повторная живая проверка:
uv run --offline --no-sync orbita-check --mode live
```

Полные локальные журналы: `.scratch/offline-prepare.log`,
`.scratch/offline-install.log`, `.scratch/unit-tests.log`,
`.scratch/local-check.log`, `.scratch/live-check.log`.
[Запуск и выключение](local-services.md),
[автономное восстановление](offline-environment.md).

## Основания конфигурации

Основа перенесена из текущего Fortune, включая исправление прямого доступа
HTTPX к локальному API Langfuse. Публичные источники:
[GPT-6 Luna](https://developers.openai.com/api/docs/models/gpt-6-luna),
[headless-инициализация Langfuse](https://langfuse.com/self-hosting/administration/headless-initialization).
Временный контейнер инициализации Orbita удалён; существующие сервисы не
перезапускались. AGENTS.md и инструкции домена/трекера Orbita сохранены.
