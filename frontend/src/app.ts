import { OwnerNavigation, pages } from "./navigation.js";
type Status = {
  mode: string;
  coefficient: string | null;
  ratioNumerator: string | null;
  ratioDenominator: string | null;
  reconciled: boolean;
  permitLimit: string | null;
  permitIssuedAt: string | null;
  recalculationRunning: boolean;
  recalculationPaused: boolean;
  lastError: string | null;
};
type Notification = {
  id: string;
  text: string;
  status: string;
  automaticFailures: number;
  nextAttemptAt: string | null;
  telegramBlockUntil: string | null;
  lastResult: string | null;
};
type Journal = {
  id: number;
  occurredAt: string;
  category: string;
  instrument: string | null;
  detail: string;
};
type Instrument = {
  instrumentId: string;
  paused: boolean;
  waiting: boolean;
  reconciled: boolean;
  reason: string | null;
};
type ShadowCheck = {
  id: string;
  executionId: string;
  algorithmVersion: string;
  side: string;
  checkedAt: string;
  executionMatches: boolean;
  calculationMatches: boolean;
  detail: string;
};
const byId = <T extends HTMLElement>(id: string) => {
  const element = document.getElementById(id);
  if (!element) throw Error(id);
  return element as T;
};
let csrf: { token: string; headerName: string };
let authenticated = false;
let refreshing = false;
let activePage = "assets";
let earliest = 0;
let accessEvents: EventSource | null = null;
let instrumentsSnapshot = "";
let notificationsSnapshot = "";
let journalEntries: Journal[] = [];
let journalSnapshot = "";
let hasOlderJournal = true;
const channel = new BroadcastChannel("orbita-owner-access");
const date = (value: string | null) =>
  value
    ? new Intl.DateTimeFormat("ru-RU", {
        timeZone: "Europe/Moscow",
        dateStyle: "short",
        timeStyle: "medium",
      }).format(new Date(value)) + " МСК"
    : "—";
const modes: Record<string, string> = {
  NOT_STARTED: "Наблюдение не начато",
  SHADOW: "Теневой режим",
  AUTOMATIC: "Автоматическая отправка",
  STOPPED: "Остановлено",
  STOPPING: "Остановка и сверка",
  RESTORED: "Восстановленная копия",
};
const states: Record<string, string> = {
  PENDING: "Ожидает доставки",
  SENDING: "Отправка выполняется",
  DELIVERED: "Доставлено",
  IGNORED: "Очередь восстановленной копии, без отправки",
  WAITING: "Ожидает создания",
  CREATING: "Создание и проверка",
  VERIFIED: "Копия создана и проверена, ожидает переноса",
  WAITING_TRANSFER: "Копия создана и проверена, ожидает переноса",
  TRANSFERRED: "Проверена и перенесена",
  CREATE_FAILED: "Ошибка создания копии",
  EXISTING_COPY_UNCONFIRMED:
    "Месячная копия уже существует, нужна ручная сверка",
  AUTOMATIC: "Автоматическая попытка",
  MANUAL: "Ручная попытка",
};
const stateLabel = (value: string) => states[value] ?? value;
const journalLabels: Record<string, string> = {
  PROCESS_STARTED: "Приложение запущено",
  RECALCULATION_STARTED: "Пересчёт начат",
  RECALCULATION_FAILED: "Пересчёт не выполнен",
  RECALCULATION_ERROR: "Ошибка обязательного пересчёта",
  RECALCULATION_RECOVERED: "Пересчёт восстановлен",
  COEFFICIENT_SAVED: "Коэффициент сохранён",
  MANUAL_STOP: "Ручная остановка",
  BACKUP_RESTORED: "Копия восстановлена",
  PERMIT_ISSUED: "Разовый допуск выдан",
  BOUNDARY_TIME_CONFIRMED: "Время границы наблюдения подтверждено",
  INSTRUMENT_PAUSED: "Инструмент приостановлен",
  POSITION_MISMATCH: "Расхождение позиции",
  SOURCE_RECEIVED: "Получено исходное исполнение",
  SOURCE_ACCEPTED: "Исходное исполнение принято к расчёту",
  SOURCE_SKIPPED: "Исходное исполнение пропущено",
  SHADOW_DECISION: "Теневой расчёт",
  SEND_STARTED: "Отправка заявки начата",
  CANCEL_STARTED: "Запрошена отмена заявки",
  ORDER_RECONCILED: "Результат заявки сверен",
  COMMISSION_RECONCILED: "Комиссия сверена",
  RECOVERY_ALLOCATION: "Связанные остатки уточнены",
  POSITION_RECOVERY: "Последовательность исполнений восстановлена",
  MANUAL_TRADE_RECONCILED: "Ручная сделка учтена после сверки",
  LIVE_ACCEPTED: "Живая приёмка подтверждена",
  SHADOW_CHECK: "Теневая сверка",
  ORDER_UNKNOWN: "Результат заявки неизвестен",
  COPY_SKIPPED: "Повторение пропущено",
  RESOURCE_SKIP: "Недостаточно собственных ресурсов",
  ORDER_FINAL_SKIP: "Остаток окончательно пропущен",
  RESOLVED: "Временная блокировка снята",
};
function journalLabel(category: string) {
  const notice = category.startsWith("NOTIFICATION_");
  const kind = notice ? category.slice(13) : category;
  return (
    (notice ? "Уведомление: " : "") +
    (journalLabels[kind] ?? "событие приложения")
  );
}
function journalDetail(row: Journal) {
  const summaries: Record<string, string> = {
    RECALCULATION_STARTED: "Получение свежих оценок капитала обоих счетов.",
    SOURCE_RECEIVED: "Получено фактическое исполнение на счёте автоследования.",
    SHADOW_DECISION: "Расчёт выполнен в тени. Торговая заявка не отправлялась.",
    SEND_STARTED:
      "Начата попытка отправки рассчитанной заявки. Итог требует ответа и сверки.",
    CANCEL_STARTED:
      "Запрошена отмена остатка заявки. До подтверждения она ещё может исполняться.",
    ORDER_RECONCILED: "Фактический результат заявки сверен с брокером.",
    COMMISSION_RECONCILED: "Фактическая комиссия сверена и учтена.",
    RECOVERY_ALLOCATION:
      "Связанные с повторением остатки уточнены после сверки.",
    POSITION_RECOVERY:
      "Последовательность исполнений и связанный остаток подтверждены.",
    SHADOW_CHECK:
      "Сохранённое исполнение проверено по независимой истории. Подробный результат доступен в разделе управления.",
    LIVE_ACCEPTED:
      "Фактическая сделка сверена с брокером. Автоматическую отправку владелец включает отдельно.",
  };
  let text = summaries[row.category] ?? row.detail;
  if (row.category === "COEFFICIENT_SAVED")
    text = `Новый коэффициент ${row.detail.split(" ")[0]}. Применяется только к новым принятым исполнениям.`;
  if (row.category === "PERMIT_ISSUED")
    text = `Новый рублёвый предел с комиссией: ${row.detail} ₽.`;
  if (row.category === "SOURCE_SKIPPED")
    text = row.detail.substring(row.detail.indexOf(" ") + 1);
  if (row.category === "SOURCE_ACCEPTED") {
    const coefficient = row.detail.match(/coefficient=([^ ]+)/)?.[1];
    const lots = row.detail.match(/increment=([^ ]+)/)?.[1];
    text = `Исполнение принято с коэффициентом ${coefficient ?? "из журнала"}. Новый целый объём: ${lots ?? "см. подробности"} лотов.`;
  }
  const cell = element("td");
  cell.append(element("div", text));
  const detail = element("details");
  detail.append(
    element("summary", "Подробности записи"),
    element("pre", `Запись ${row.id}. ${row.category}\n${row.detail}`),
  );
  cell.append(detail);
  return cell;
}
function message(text: string) {
  byId("message").textContent = text;
}
function lock(text = "Введите пароль для доступа к журналу и управлению.") {
  if (accessEvents) {
    accessEvents.onerror = null;
    accessEvents.close();
    accessEvents = null;
  }
  authenticated = false;
  instrumentsSnapshot = "";
  notificationsSnapshot = "";
  journalEntries = [];
  journalSnapshot = "";
  hasOlderJournal = true;
  earliest = 0;
  byId("workspace").hidden = true;
  byId("login").hidden = false;
  byId("logout").hidden = true;
  for (const id of [
    "journal-rows",
    "notification-list",
    "instruments",
    "backup-list",
    "shadow-checks",
    "migration-proofs",
  ])
    byId(id).replaceChildren();
  byId("coefficient").textContent = "—";
  byId("permit").textContent = "—";
  message(text);
}
channel.onmessage = () => lock("Доступ закрыт в другом окне. Войдите снова.");
async function watchAccess(): Promise<void> {
  if (accessEvents) {
    accessEvents.close();
    accessEvents = null;
  }
  await new Promise<void>((resolve, reject) => {
    const stream = new EventSource("/api/access-events");
    accessEvents = stream;
    const timeout = setTimeout(() => {
      lock("Канал подтверждения доступа недоступен. Повторите вход.");
      reject(Error("Канал доступа недоступен"));
    }, 5000);
    stream.addEventListener("ready", () => {
      clearTimeout(timeout);
      resolve();
    });
    stream.addEventListener("access-revoked", () => {
      clearTimeout(timeout);
      lock("Локальный пароль сброшен. Войдите с новым паролем.");
      channel.postMessage("lock");
      reject(Error("Доступ отозван"));
    });
    stream.onerror = () => {
      clearTimeout(timeout);
      lock("Связь подтверждения доступа потеряна. Журнал скрыт.");
      reject(Error("Канал доступа закрыт"));
    };
  });
}
async function api<T>(
  path: string,
  method = "GET",
  body?: unknown,
): Promise<T> {
  const headers: Record<string, string> = {};
  if (method !== "GET") {
    headers[csrf.headerName] = csrf.token;
    if (body !== undefined) headers["Content-Type"] = "application/json";
  }
  const response = await fetch("/api" + path, {
    method,
    headers,
    credentials: "same-origin",
    cache: "no-store",
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(15000),
  });
  if (response.status === 401) {
    lock("Доступ закрыт. Войдите с действующим паролем.");
    throw Error("Требуется вход");
  }
  if (!response.ok) {
    let text = "Команда не выполнена. Обновите состояние и проверьте журнал.";
    try {
      const error = (await response.json()) as { detail?: string };
      text = error.detail ?? text;
    } catch {}
    throw Error(text);
  }
  if (response.status === 204 || response.headers.get("content-length") === "0")
    return undefined as T;
  const text = await response.text();
  return (text ? JSON.parse(text) : undefined) as T;
}
function element<K extends keyof HTMLElementTagNameMap>(tag: K, text?: string) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  return node;
}
function action(label: string, callback: () => Promise<void>) {
  const button = element("button", label);
  button.onclick = () => void run(button, callback);
  return button;
}
async function run(button: HTMLButtonElement, callback: () => Promise<void>) {
  button.disabled = true;
  try {
    await callback();
    await refresh();
  } catch (error) {
    message(
      error instanceof Error ? error.message : "Не удалось выполнить действие.",
    );
  } finally {
    button.disabled = false;
  }
}
function renderStatus(status: Status) {
  byId("mode").textContent = modes[status.mode] ?? status.mode;
  byId("coefficient").textContent = status.ratioNumerator && status.ratioDenominator
    ? status.ratioDenominator === "1" ? status.ratioNumerator : `${status.ratioNumerator}/${status.ratioDenominator}`
    : status.coefficient ?? "Не установлен";
  byId("coefficient").title = status.coefficient ? `Десятичное отображение: ${status.coefficient}` : "";
  byId("reconciled").textContent = status.reconciled
    ? "Подтверждена"
    : "Требуется";
  byId("permit").textContent = status.permitLimit
    ? status.permitLimit + " ₽ с " + date(status.permitIssuedAt)
    : "Не выдан";
  byId("state-description").textContent = status.recalculationRunning
    ? "Пересчёт выполняется. Повторная команда покажет ту же попытку."
    : status.recalculationPaused
      ? "Пауза обязательного пересчёта."
      : (status.lastError ?? "Состояние подтверждено приложением.");
}
async function refreshJournal(append = false) {
  const rows = await api<Journal[]>(
    "/journal" + (append ? "?before=" + earliest : ""),
  );
  const body = byId("journal-rows");
  if (append || !journalEntries.length) hasOlderJournal = rows.length === 100;
  const merged = new Map(journalEntries.map((row) => [row.id, row]));
  for (const row of rows) merged.set(row.id, row);
  journalEntries = [...merged.values()].sort((a, b) => b.id - a.id);
  const snapshot = JSON.stringify(journalEntries);
  if (snapshot === journalSnapshot) return;
  journalSnapshot = snapshot;
  body.replaceChildren();
  for (const row of journalEntries) {
    const tr = element("tr");
    for (const value of [
      date(row.occurredAt),
      journalLabel(row.category),
      row.instrument ?? "—",
    ])
      tr.append(element("td", value));
    tr.append(journalDetail(row));
    body.append(tr);
    earliest = row.id;
  }
  if (!body.children.length) {
    const row = element("tr");
    const cell = element(
      "td",
      "Записей пока нет. Начните наблюдение после настройки и сверки счетов.",
    );
    cell.colSpan = 4;
    row.append(cell);
    body.append(row);
  }
  byId<HTMLButtonElement>("older-journal").disabled = !hasOlderJournal;
}
async function refreshNotifications() {
  const list = byId("notification-list");
  const events = await api<Notification[]>("/notifications");
  const snapshot = JSON.stringify(events);
  if (snapshot === notificationsSnapshot) return;
  notificationsSnapshot = snapshot;
  list.replaceChildren();
  if (!events.length) list.append(element("p", "Уведомлений пока нет."));
  for (const event of events) {
    const article = element("article");
    article.className = "notification";
    article.append(element("pre", event.text));
    const wait = event.telegramBlockUntil
      ? Math.max(
          0,
          Math.ceil(
            (new Date(event.telegramBlockUntil).getTime() - Date.now()) / 1000,
          ),
        )
      : 0;
    const description = element(
      "p",
      `Статус: ${stateLabel(event.status)}. Подтверждённых автоматических неудач: ${event.automaticFailures}. Следующая попытка: ${date(event.nextAttemptAt)}. ${event.lastResult ?? ""}${wait ? " Ограничение Telegram ещё " + wait + " с. Автоповтор запланирован." : ""}`,
    );
    description.className = "metadata";
    article.append(description);
    if (event.status !== "DELIVERED" && event.status !== "IGNORED")
      article.append(
        action("Повторить доставку сейчас", async () => {
          const remaining = event.telegramBlockUntil
            ? Math.max(
                0,
                Math.ceil(
                  (new Date(event.telegramBlockUntil).getTime() - Date.now()) /
                    1000,
                ),
              )
            : 0;
          if (remaining > 0) {
            message(
              `Ограничение Telegram ещё ${remaining} с. Автоматический повтор запланирован на ${date(event.nextAttemptAt)}.`,
            );
            return;
          }
          await api("/notifications/" + event.id + "/retry", "POST");
          message("Статус доставки обновлён.");
        }),
      );
    article.append(
      action("История попыток", async () => {
        const attempts = await api<
          Array<{
            kind: string;
            startedAt: string;
            result: string;
            detail: string;
            payload: string | null;
          }>
        >("/notifications/" + event.id + "/attempts");
        const history = element(
          "pre",
          attempts
            .map(
              (item) =>
                date(item.startedAt) +
                " " +
                stateLabel(item.kind) +
                " " +
                (item.detail ?? item.result ?? "Ожидание ответа") +
                "\n" +
                (item.payload ?? "Текст старой попытки не был зафиксирован."),
            )
            .join("\n") || "Попыток ещё нет.",
        );
        article.append(history);
      }),
    );
    list.append(article);
  }
}
async function refreshInstruments() {
  const [rows, checks] = await Promise.all([
    api<Instrument[]>("/instruments"),
    api<ShadowCheck[]>("/acceptance/checks"),
  ]);
  const snapshot = JSON.stringify([rows, checks]);
  if (snapshot === instrumentsSnapshot) return;
  instrumentsSnapshot = snapshot;
  const list = byId("instruments");
  list.replaceChildren();
  if (!rows.length)
    list.append(
      element("p", "Инструменты появятся после наблюдения за исполнениями."),
    );
  for (const item of rows) {
    const row = element("div");
    row.className = "instrument";
    row.append(
      element("strong", item.instrumentId),
      element(
        "p",
        (item.paused
          ? "Пауза. "
          : item.waiting
            ? "Ожидание сверки. "
            : "Наблюдение. ") + (item.reason ?? ""),
      ),
    );
    if (item.paused)
      row.append(
        action("Возобновить после сверки", async () => {
          await api(
            "/instruments/" + encodeURIComponent(item.instrumentId) + "/resume",
            "POST",
          );
        }),
      );
    else
      row.append(
        action("Приостановить", async () => {
          await api(
            "/instruments/" + encodeURIComponent(item.instrumentId) + "/pause",
            "POST",
            { text: "Ручная пауза владельца" },
          );
        }),
      );
    list.append(row);
  }
  const shadow = byId("shadow-checks");
  shadow.replaceChildren();
  if (!checks.length)
    shadow.append(
      element(
        "p",
        "Сверка ещё не подтверждена. Нужны сохранённые покупка и продажа и доступная история брокера.",
      ),
    );
  for (const check of checks) {
    const article = element("article");
    article.className = "notification";
    article.append(
      element(
        "h3",
        `${check.side === "BUY" ? "Покупка" : "Продажа"}: ${check.executionMatches && check.calculationMatches ? "подтверждена" : "есть расхождение"}`,
      ),
      element(
        "p",
        `Версия ${check.algorithmVersion}. Исходное исполнение ${check.executionId}. Проверка ${date(check.checkedAt)}.`,
      ),
      element("pre", check.detail),
    );
    shadow.append(article);
  }
}
async function refreshBackups() {
  const rows = await api<
    Array<{
      month: string;
      status: string;
      verifiedAt: string | null;
      transferredAt: string | null;
      lastError: string | null;
    }>
  >("/backups");
  const list = byId("backup-list");
  list.replaceChildren();
  if (!rows.length)
    list.append(
      element(
        "p",
        "Проверенных копий пока нет. Проверьте настройки папок резервирования.",
      ),
    );
  for (const row of rows)
    list.append(
      element(
        "p",
        `${row.month}: ${stateLabel(row.status)}. Проверена: ${date(row.verifiedAt)}. Перенесена: ${date(row.transferredAt)}. ${row.lastError ?? ""}`,
      ),
    );
  const proofs = await api<
    Array<{ id: string; checkedAt: string; detail: string }>
  >("/migration/verifications");
  const listProofs = byId("migration-proofs");
  listProofs.replaceChildren();
  for (const proof of proofs)
    listProofs.append(
      element("p", date(proof.checkedAt) + ". " + proof.detail),
    );
}
async function refresh() {
  if (!authenticated || refreshing) return;
  refreshing = true;
  try {
    renderStatus(await api<Status>("/status"));
    if (activePage === "control") await refreshInstruments();
    else if (activePage === "journal") await refreshJournal();
    else if (activePage === "notifications") await refreshNotifications();
    else if (activePage === "backup") await refreshBackups();
    navigation.restore();
  } catch (error) {
    lock(
      "Связь с приложением потеряна. Журнал скрыт; повторите вход после восстановления связи.",
    );
  } finally {
    refreshing = false;
  }
}
const navigation = new OwnerNavigation(() => activateRoute());
function activateRoute() {
  activePage = navigation.route.page;
  for (const page of document.querySelectorAll<HTMLElement>(".page"))
    page.hidden = page.id !== activePage;
  for (const tab of document.querySelectorAll("[data-page]"))
    tab.removeAttribute("aria-current");
  byId("nav-" + activePage).setAttribute("aria-current", "page");
  const detail = navigation.route.fill ?? navigation.route.order
    ?? navigation.route.operation ?? navigation.route.journal;
  const status = byId("route-status");
  status.hidden = detail === undefined;
  status.textContent = detail === undefined ? ""
    : `Запрошена конкретная запись: ${detail}. Точный переход будет доступен после подключения истории.`;
  void refresh();
}
for (const button of document.querySelectorAll<HTMLButtonElement>("[data-page]"))
  button.onclick = () => {
    const page = button.dataset.page;
    if (pages.includes(page as (typeof pages)[number]))
      navigation.navigate({ page: page as (typeof pages)[number] });
  };
activateRoute();
for (const button of document.querySelectorAll<HTMLButtonElement>(
  "[data-command]",
))
  button.onclick = () =>
    void run(button, async () => {
      await api("/control/" + button.dataset.command, "POST");
      message("Команда обработана. Проверьте фактический режим.");
    });
byId<HTMLFormElement>("permit-form").onsubmit = (event) => {
  event.preventDefault();
  const button = byId("permit-form").querySelector<HTMLButtonElement>(
    "button[type=submit]",
  )!;
  void run(button, async () => {
    await api("/control/permit", "POST", {
      rubLimit: byId<HTMLInputElement>("rub-limit").value,
    });
    byId<HTMLInputElement>("rub-limit").value = "";
    message("Разовый допуск выдан с новым пределом.");
  });
};
byId<HTMLFormElement>("migration-export").onsubmit = (event) => {
  event.preventDefault();
  const button =
    byId("migration-export").querySelector<HTMLButtonElement>("button")!;
  void run(button, async () => {
    const manifest = await api<unknown>("/migration/manifest", "POST", {
      executionId: byId<HTMLInputElement>("migration-execution").value,
    });
    const url = URL.createObjectURL(
      new Blob([JSON.stringify(manifest, null, 2)], {
        type: "application/json",
      }),
    );
    const link = element("a");
    link.href = url;
    link.download =
      "orbita-migration-" + new Date().toISOString().slice(0, 10) + ".json";
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    message("Протокол сохранён. После переноса сравните его на сервере.");
  });
};
byId<HTMLFormElement>("migration-compare").onsubmit = (event) => {
  event.preventDefault();
  const button =
    byId("migration-compare").querySelector<HTMLButtonElement>("button")!;
  void run(button, async () => {
    const file = byId<HTMLInputElement>("migration-file").files?.[0];
    if (!file || file.size > 65536)
      throw Error("Выберите протокол JSON размером до 64 КБ.");
    const manifest = JSON.parse(await file.text()) as unknown;
    const proof = await api<{ detail: string }>(
      "/migration/verify",
      "POST",
      manifest,
    );
    message(proof.detail);
  });
};
byId("older-journal").onclick = () =>
  void run(byId<HTMLButtonElement>("older-journal"), () =>
    refreshJournal(true),
  );
byId("logout").onclick = () =>
  void run(byId<HTMLButtonElement>("logout"), async () => {
    lock("Вы вышли. Режим повторения сохранён.");
    channel.postMessage("lock");
    await api("/logout", "POST");
    csrf = await api("/csrf");
  });
byId<HTMLFormElement>("login-form").onsubmit = (event) => {
  event.preventDefault();
  void (async () => {
    try {
      csrf = await api("/csrf");
      const password = byId<HTMLInputElement>("password");
      const response = await fetch("/api/login", {
        method: "POST",
        headers: {
          [csrf.headerName]: csrf.token,
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: new URLSearchParams({
          username: "owner",
          password: password.value,
        }),
        credentials: "same-origin",
        cache: "no-store",
      });
      password.value = "";
      if (!response.ok) {
        message(
          response.status === 429
            ? "Слишком много попыток. Повторите через 30 секунд."
            : "Пароль не принят.",
        );
        return;
      }
      csrf = await api("/csrf");
      await watchAccess();
      authenticated = true;
      byId("login").hidden = true;
      byId("workspace").hidden = false;
      byId("logout").hidden = false;
      message("");
      await refresh();
    } catch {
      lock("Приложение недоступно. Проверьте запуск Docker.");
    }
  })();
};
document.addEventListener("visibilitychange", () => {
  if (!document.hidden && authenticated) void refresh();
});
void (async () => {
  try {
    csrf = await api("/csrf");
    const status = await api<Status>("/status");
    await watchAccess();
    authenticated = true;
    byId("login").hidden = true;
    byId("workspace").hidden = false;
    byId("logout").hidden = false;
    renderStatus(status);
    await refresh();
  } catch {
    lock();
  }
})();
setInterval(() => {
  if (authenticated) void refresh();
}, 1000);
export {};
