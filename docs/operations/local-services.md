# Локальные сервисы Orbita

Orbita использует общий Langfuse из `C:\Git\setupAIv2`, отдельный проект
`orbita` и отдельные API-ключи. Само приложение и Python-окружение находятся
в Orbita и не импортируют код других репозиториев.

## После включения ПК

1. Запустите Docker Desktop, дождитесь Engine running.
2. В PowerShell запустите сохранённые образы общего сервера:

```powershell
cd C:\Git\setupAIv2
docker compose --env-file infra/langfuse/.env -f infra/langfuse/compose.yaml up -d --pull never --wait --wait-timeout 240
```

3. После успешной готовности контейнеров проверьте Orbita:

```powershell
cd C:\Git\orbita
uv run --offline --no-sync orbita-check
uv run --offline --no-sync orbita-check --mode local
```

Ожидаются `PASS: offline` и `PASS: local; graph and validated result; trace persisted`.
Откройте <http://localhost:3000/project/orbita/traces> и проверьте новую трассу.
Эти проверки не обращаются к OpenAI и не требуют интернета.

4. Для отдельной проверки реальной модели при работающем интернете:

```powershell
uv run --offline --no-sync orbita-check --mode live
```

Это один платный запрос к `gpt-6-luna` через Responses API, максимум 256
выходных токенов, без автоматических повторов. Флаг uv `--offline` не запрещает
сетевые обращения самого приложения. При ошибке сохраните безопасный вывод CLI.

После перезагрузки не нужны `uv sync`, `prepare` или `restore`, пока `.venv`
сохранена. Для нового окружения используйте [автономное восстановление](offline-environment.md).

## Перед выключением ПК

Дождитесь завершения запросов всех проектов, сохраните файлы. Затем:

```powershell
cd C:\Git\setupAIv2
docker compose --env-file infra/langfuse/.env -f infra/langfuse/compose.yaml stop --timeout 60
```

Это остановит общий Langfuse также для Fortune и setupAIv2. Дождитесь окончания
команды, выйдите из Docker Desktop через Quit Docker Desktop, затем штатно
выключите или перезагрузите Windows. Данные остаются в Docker volumes.
Не используйте `down -v` для обычного выключения.

## Ключи и локальный прокси

По решению владельца существующий OpenAI API-ключ используется постоянно.
Копия сохранена только в `.env` Orbita. При ротации общего ключа нужно обновить
все его локальные копии; приложение не читает `.env` других проектов.
Langfuse-ключи отдельные, относятся только к `orbita`.

Вход в UI: общий администратор `admin@aim-setupaiv2.local`, пароль — поле
`ADMIN_PASSWORD` в `C:\Git\setupAIv2\infra\langfuse\.env`.
Не публикуйте `.env`, полный `docker inspect` или `docker compose config`.

Для чтения локального API Langfuse используется HTTPX-клиент с
`trust_env=False`: запрос не зависит от системного HTTP-прокси Windows.
Настройки Clash и транспорта OpenAI не меняются. Статус Healthy контейнеров
сам по себе не доказывает, что приложение сохранило трассу: для этого нужен
режим `local` с обратным чтением результата.

Проект создан штатной [headless-инициализацией Langfuse](https://langfuse.com/self-hosting/administration/headless-initialization)
в существующей организации AIM Local. Временный контейнер инициализации удалён;
основные сервисы не перезапускались. Общий администратор имеет доступ ко всем
проектам; разделение API-ключей не является изоляцией от администратора сервера.

[Результаты подключения](acceptance-2026-11-08.md).
