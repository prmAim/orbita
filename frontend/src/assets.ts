import { CollectionView } from "./collection.js";
import { type AccountPage, type Asset, type Read, el, time, label, price, decimal, link, reasonLink, instrument, note } from "./account-ui.js";
export class AssetsView {
  readonly list: CollectionView<Asset>;
  private cash = el("div");
  private readAt = note("Время получения остатков не подтверждено.");
  constructor(host: HTMLElement, read: Read, control: (id: string, paused: boolean) => Promise<void>) {
    const listHost = el("div");
    host.replaceChildren(this.cash, this.readAt, note("Дата — последнее известное фактическое исполнение на счёте повторения, включая ручное, МСК. Цена — по исполненной части заявки этой сделки, а не цена приобретения остатка."), listHost);
    this.list = new CollectionView(listHost, {
      id: "assets", label: "Текущие бумаги", key: row => row.id, freshnessKeys: ["assets", "executions"],
      filters: [
        {
          key: "state", label: "Состояние инструмента", options: [["", "Все"], ["READY", "Наблюдение"], ["PAUSED", "Пауза"], ["WAITING", "Ожидание сверки"]]
        },
        {
          key: "attention", label: "Требуют внимания", options: [["", "Все"], ["true", "Только требующие внимания"]]
        },
      ],
      columns: [
        {
          key: "time", label: "Дата", sort: "time", cell: row => {
            const cell = el("div");
            if (row.lastExecutionAt && row.lastExecutionId)
              cell.append(link(time(row.lastExecutionAt), {
                page: "operations", fill: row.lastExecutionId, ...(row.lastOrderId ? {
                  order: row.lastOrderId
                } : {})
              }));
            else if (row.lastExecutionAt)
              cell.append(el("span", time(row.lastExecutionAt)), note("Связь с конкретным исполнением или заявкой не подтверждена"));
            else
              cell.append(el("span", row.executionPrice.status === "NO_EXECUTIONS" ? "Нет исполнений" : "Не подтверждено"));
            if (!row.complete)
              cell.append(note("История или состав остатка неполные"));
            return cell;
          }
        },
        {
          key: "instrument", label: "Инструмент", sort: "instrument", cell: row => {
            const cell = instrument(row);
            cell.append(note(label(row.state)));
            if (row.reason)
              cell.append(row.journalId ? reasonLink(row.reason, row.journalId) : note(row.reason));
            return cell;
          }
        },
        {
          key: "price", label: "Цена исполнения", sort: "executionPrice", numeric: true, cell: row => price(row.executionPrice)
        },
        {
          key: "copied", label: "Остаток повторения, лоты", numeric: true, cell: row => decimal(row.copiedLots)
        },
        {
          key: "manual", label: "Ручные, лоты", numeric: true, cell: row => decimal(row.manualLots)
        },
        {
          key: "broker", label: "Остаток брокера, лоты", numeric: true, cell: row => decimal(row.brokerLots)
        },
        {
          key: "actions", label: "Действия", cell: row => {
            const cell = el("div");
            cell.className = "row-actions";
            const button = el("button", row.state === "PAUSED" ? "Возобновить после сверки" : "Приостановить");
            const result = note("");
            result.setAttribute("role", "status");
            button.onclick = async () => {
              button.disabled = true;
              try {
                await control(row.instrumentId, row.state === "PAUSED");
              }
              catch (error) {
                result.textContent = error instanceof Error ? error.message : "Команда не выполнена";
              }
              finally {
                button.disabled = false;
              }
            };
            cell.append(button, link("История инструмента", {
              page: "operations", instrument: row.instrumentId
            }), result);
            return cell;
          }
        },
      ],
      load: query => read<AccountPage<Asset>>("/account/assets?" + query),
      applied: result => {
        const page = result as AccountPage<Asset>;
        this.renderCash(page);
        this.readAt.textContent = "Получение остатков от брокера: " + time(page.freshness.assets?.lastSuccessAt ?? null);
      },
    });
  }
  private renderCash(page: AccountPage<Asset>) {
    const table = el("table");
    table.className = "collection-table cash-table";
    table.append(el("caption", "Денежные средства счёта повторения"));
    const head = el("thead"), headings = el("tr");
    for (const title of ["Валюта", "Всего", "Доступно", "Заблокировано"]) {
      const th = el("th", title);
      th.scope = "col";
      headings.append(th);
    }
    head.append(headings);
    table.append(head);
    const body = el("tbody");
    for (const cash of page.cash) {
      const tr = el("tr"), currency = cash.currency.toUpperCase();
      const first = el("td", currency);
      first.dataset.label = "Валюта";
      first.append(note("Время банка: " + time(cash.bankAt)));
      if (cash.freshness !== "FRESH")
        first.append(note(cash.freshness === "STALE" ? "Устаревшие данные" : "Суммы не подтверждены"));
      if (cash.limitation)
        first.append(note(cash.limitation));
      tr.append(first);
      for (const [title, value] of [["Всего", cash.total], ["Доступно", cash.available], ["Заблокировано", cash.blocked]] as const) {
        const td = el("td", decimal(value, currency));
        td.className = "numeric";
        td.dataset.label = title;
        tr.append(td);
      }
      body.append(tr);
    }
    table.append(body);
    this.cash.replaceChildren(table);
    if (!page.cash.length)
      this.cash.append(note("Денежные средства не получены. Это не означает нулевой остаток."));
    this.cash.append(note("Суммы разных валют не складываются и автоматически не конвертируются."));
    const freshness = page.freshness.cash;
    if (!freshness || freshness.status !== "FRESH")
      this.cash.append(note(`Состав денежных средств подтверждён не полностью. Последнее успешное чтение: ${time(freshness?.lastSuccessAt ?? null)}. ${freshness?.detail ?? ""}`));
  }
  refresh() {
    return this.list.refresh();
  }
  clear() {
    this.list.clear();
    this.cash.replaceChildren();
    this.readAt.textContent = "Время получения остатков не подтверждено.";
  }
}
