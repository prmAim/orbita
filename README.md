# Orbita

Основа AI-приложения: LangGraph, LangChain, Pydantic, OpenAI Responses API
и локальный Langfuse. Бизнес-сценарии пока не реализованы.

## Проверка подключения

Из корня проекта после установки окружения:

```powershell
uv run --offline --no-sync orbita-check
uv run --offline --no-sync orbita-check --mode local
uv run --offline --no-sync orbita-check --mode live
```

- `offline`: граф и проверка данных с fake-моделью; без сети и ключей.
- `local`: fake-модель и сохранение/обратное чтение трассы в локальном Langfuse.
- `live`: один короткий платный запрос к `gpt-6-luna` через Responses API и трасса.

`--offline` ограничивает сеть менеджера uv, а не приложения: для `live` нужен
интернет. `--no-sync` использует готовую `.venv` без пересборки и не проверяет
её соответствие lockfile. После перезагрузки переустановка не требуется.

## Конфигурация

Настройки загружаются из `.env` в текущем каталоге; шаблон — `.env.example`.
Секреты не входят в Git, журналы или отчёты. Приложение — `orbita`, окружение —
`development`, Langfuse — `http://localhost:3000`, отдельный проект `orbita`.
По решению владельца постоянно используется существующий ключ OpenAI из
setupAIv2/Fortune. Его значение сохранено локально в `.env` Orbita: при работе
приложение не читает другие репозитории. При ротации общего ключа обновите
его во всех использующих проектах.

Отдельные ключи Langfuse ограничивают доступ проектом Orbita. Общий администратор
Langfuse имеет доступ ко всем проектам. LangSmith Cloud не подключён;
его SDK остаётся зависимостью LangChain, трассировка LangSmith явно отключена.

## Структура

- `src/orbita/`: настройки, модель, граф, трассировка и CLI.
- `tests/`: проверки графа, конфигурации, маршрута локального API и offline-набора.
- `tools/`: автономная установка, lockfile сборочных зависимостей и офлайн-имитаторы Wayfinder (`simulate_*.py`).
- `docs/operations/`: эксплуатационные инструкции и результаты приёмки.
- `.github/workflows/ci.yml`: проверки без секретов и внешней модели.

Python 3.13.18, uv 0.13.7; точные версии пакетов сохранены в `uv.lock`.
Сборочный backend Hatchling 1.33.0 и зависимости закреплены отдельно.
Для обычной установки с доступом к PyPI: `uv sync --locked`.
Для установки без сети используйте инструкцию ниже.

## Проверки разработки

```powershell
uv run --offline --no-sync python -m unittest discover -s tests -v
uv run --offline --no-sync ruff check src tests tools
uv run --offline --no-sync ruff format --check src tests tools
```

## Документация

- [Локальная проверка исполнения по задаче №6](docs/operations/subscriber-execution-check.md).
- [Термины пропорционального повторения сделок](GLOSSARY.md).
- [Карта Wayfinder: независимое пропорциональное повторение сделок](https://github.com/prmAim/orbita/issues/1).
- [Текущая спецификация повторения сделок](https://github.com/prmAim/orbita/issues/10) — правила, сценарии приёмки и задачи реализации.
- [Технический протокол сверки и хранения v0.1](https://github.com/prmAim/orbita/issues/11#issuecomment-6725242827) — исходный проект. Офлайн-имитаторы в `tools/` воспроизводят отдельные сценарии на синтетических данных. Для первой версии [выбран PostgreSQL](https://github.com/prmAim/orbita/issues/17).
- [Проект протокола комиссии и доступных ресурсов v0.2](https://github.com/prmAim/orbita/issues/3#issuecomment-6725611872) — формулы, резервы и открытые условия проверки API/тарифа.
- [Проектный протокол времени и возраста исполнений v0.1](https://github.com/prmAim/orbita/issues/15#issuecomment-6725904900) — правила времени и сбоев, требования к проверке часов на целевом ПК.
- [Запуск и остановка локальных сервисов](docs/operations/local-services.md).
- [Автономная установка и восстановление](docs/operations/offline-environment.md).
- [Приёмка подключения](docs/operations/acceptance-2026-11-08.md).

Основа адаптирована из Fortune, включая исправление Windows proxy для локального
API Langfuse. Код Orbita независим: изменения других проектов не применяются
автоматически. Установочные файлы `.offline/` исключены из Git — сохраните их
вместе с исходниками и отдельно защищёнными секретами на резервный носитель.
