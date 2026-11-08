# Issue tracker: GitHub

Задачи и спецификации проекта хранятся в GitHub Issues:
https://github.com/prmAim/orbita/issues

Используй GitHub CLI (`gh`). В операциях с задачами и метками явно
указывай `--repo prmAim/orbita`; для `gh repo view` передавай
`prmAim/orbita` позиционным аргументом.

## Основные операции

- Проверить целевой репозиторий:
  `gh repo view prmAim/orbita --json nameWithOwner,url`
- Создать задачу:
  `gh issue create --repo prmAim/orbita --title "..." --body-file <file>`
- Прочитать задачу и комментарии:
  `gh issue view <number> --repo prmAim/orbita --json number,title,body,labels,comments`
- Получить открытые задачи:
  `gh issue list --repo prmAim/orbita --state open --json number,title,labels,assignees`
  При необходимости добавляй фильтр `--label`.
- Обновить описание:
  `gh issue edit <number> --repo prmAim/orbita --body-file <file>`
- Добавить комментарий:
  `gh issue comment <number> --repo prmAim/orbita --body-file <file>`
- Добавить или удалить метку:
  `gh issue edit <number> --repo prmAim/orbita --add-label "..."`
  или `--remove-label "..."`.
- Закрыть задачу:
  сначала опубликуй объяснение отдельным комментарием,
  затем выполни `gh issue close <number> --repo prmAim/orbita`.
  Для уже закрытой задачи достаточно комментария.

Многострочные описания и комментарии передавай через `--body-file`,
сохраняя реальные переводы строк. Временные файлы размещай в системном
временном каталоге или выделенном игнорируемом каталоге проекта.

Перед первой записью в сессии проверь целевой репозиторий.
При несовпадении с `prmAim/orbita` останови запись и уточни адрес.

## Иерархия и зависимости

Используй нативные связи GitHub:

- Создать подзадачу: добавь `--parent <parent-number>` к созданию задачи.
- Привязать существующую подзадачу:
  `gh issue edit <parent> --repo prmAim/orbita --add-sub-issue <child>`
- Указать блокирующую задачу:
  `gh issue edit <child> --repo prmAim/orbita --add-blocked-by <blocker>`

Перед использованием проверь поддержку флагов через `gh issue --help`
и справку соответствующей подкоманды.

Если нативные связи недоступны, укажи `Part of #<parent>` и
`Blocked by: #<number>` в начале описания дочерней задачи,
а у родителя веди список подзадач со ссылками.

Задача готова к началу только после закрытия всех её блокирующих задач.

## Значение команд навыков

«Publish to the issue tracker» означает создать GitHub Issue.
«Fetch the relevant ticket» означает прочитать соответствующую задачу
и её комментарии.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## Wayfinding

Если используется `/wayfinder`:

- Карта — одна задача с меткой `wayfinder:map` и разделами
  Notes, Decisions-so-far и Fog.
- Билеты — подзадачи карты с метками `wayfinder:<type>`,
  где type — research, prototype, grilling или task.
- Следующая задача — первая по порядку карты открытая подзадача
  без исполнителя и открытых блокирующих задач.
- Перед началом назначь задачу на себя через `--add-assignee "@me"`.
- После решения опубликуй результат, закрой задачу и добавь
  краткий вывод со ссылкой в Decisions-so-far карты.
