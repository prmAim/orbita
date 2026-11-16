import { CollectionView } from "./collection.js";
import { type AccountPage, type Operation as OperationData, type Order, type Fill, type Read, el, time, label, price, decimal, link, reasonLink, instrument, orderTime, note } from "./account-ui.js";
import { originOptions, sideOptions, statusOptions, orderStatus, limitPrice } from "./orders.js";
type Operation = OperationData & {
  journalId?: string | number | null;
};
const operationStatuses: Array<[
  string,
  string
]> = [...statusOptions,
  ["FILLED", "Исполнена"], ["CANCELLED", "Отменена"], ["REJECTED", "Отклонена"], ["FINAL", "Завершена"],
  ["RETRIED", "Повторная попытка"], ["WAITING", "Ожидание"], ["SHADOW", "Теневой расчёт"], ["HALTED", "Остановлено"],
  ["RECEIVED", "Получено"], ["ACCEPTED", "Принято к расчёту"], ["SKIPPED", "Пропущено"], ["MIXED", "Несколько результатов"], ["UNLINKED_EVIDENCE", "Без подтверждённой связи с заявкой"]];
const field = (dl: HTMLDListElement, title: string, value: string | Node) => {
  const dd = el("dd");
  dd.append(value);
  dl.append(el("dt", title), dd);
};
function fillDetails(fill: Fill): HTMLElement {
  const article = el("article");
  article.className = "execution-detail";
  article.id = "fill-" + encodeURIComponent(fill.id);
  article.append(el("h4", "Фактическое исполнение"));
  const dl = el("dl");
  field(dl, "Дата исполнения, МСК", link(time(fill.executedAt), {
    page: "operations", fill: fill.id, ...(fill.orderId ? {
      order: fill.orderId
    } : {})
  }));
  field(dl, "Направление", label(fill.side));
  field(dl, "Объём, лоты", decimal(fill.lots));
  field(dl, "Цена исполнения", price(fill.price));
  field(dl, "Комиссия", fill.commission?.status === "CONFIRMED" ? decimal(fill.commission.value, fill.commission.currency?.toUpperCase()) : "Не подтверждено");
  field(dl, "Идентификатор исполнения", fill.tradeId);
  article.append(dl);
  if (!fill.complete)
    article.append(note("Сведения об исполнении неполные"));
  return article;
}
function linkedOrder(row: Order, context: string): HTMLElement {
  const article = el("article");
  article.className = "execution-detail";
  article.append(el("h4", "Связанная заявка"), link(row.id, {
    page: "operations", order: row.id
  }));
  const dl = el("dl");
  field(dl, "Дата создания / отправки, МСК", orderTime(row));
  field(dl, "Происхождение", label(row.origin));
  field(dl, "Направление", label(row.side));
  field(dl, "Объём, лоты", decimal(row.lots));
  field(dl, "Исполнено, лоты", decimal(row.executedLots));
  field(dl, "Осталось, лоты", decimal(row.remainingLots));
  field(dl, "Цена исполнения", price(row.executionPrice));
  field(dl, "Лимитная цена и валюта", limitPrice(row));
  field(dl, "Статус заявки", orderStatus(row));
  article.append(dl);
  for (const [index, anchor] of [...article.querySelectorAll("a")].entries())
    anchor.id = `operation-${encodeURIComponent(context)}-order-${encodeURIComponent(row.id)}-${index}`;
  return article;
}
export class OperationsView {
  readonly list: CollectionView<Operation>;
  private coverage = note("Период полноты истории не подтверждён.");
  private generation = 0;
  constructor(host: HTMLElement, private read: Read) {
    const listHost = el("div");
    host.replaceChildren(note("Период, МСК: время исходного исполнения для групп Орбиты; время создания заявки для самостоятельных заявок. Для непривязанного исполнения используется его фактическое время, явно подписанное в строке. Закрытые позиции остаются в истории."), this.coverage, listHost);
    this.list = new CollectionView(listHost, {
      id: "operations", label: "История заявок и исполнений", key: row => row.id, freshnessKeys: ["history", "executions"],
      filters: [
        {
          key: "from", label: "С начала периода, МСК", type: "datetime-local"
        },
        {
          key: "to", label: "До конца периода, МСК", type: "datetime-local"
        },
        {
          key: "side", label: "Направление", options: sideOptions
        },
        {
          key: "status", label: "Результат", options: operationStatuses
        },
        {
          key: "origin", label: "Происхождение", options: originOptions
        },
      ],
      columns: [
        {
          key: "time", label: "Дата события, МСК", sort: "time", cell: row => {
            const cell = el("div");
            cell.append(link(time(row.time), {
              page: "operations", operation: row.id
            }));
            cell.append(note(row.sourceExecutionId ? "Исходное исполнение" : row.timeSemantics === "Unlinked broker execution time" ? "Непривязанное исполнение брокера" : row.timeSemantics === "Broker order creation time" ? "Создание заявки у брокера" : "Локальная попытка; время брокера не подтверждено"));
            return cell;
          }
        },
        {
          key: "instrument", label: "Инструмент", sort: "instrument", cell: instrument
        },
        {
          key: "side", label: "Покупка/продажа", sort: "side", cell: row => label(row.side)
        },
        {
          key: "source", label: "Исходный объём, лоты", numeric: true, cell: row => row.sourceExecutionId ? decimal(row.sourceLots) : "Не применяется"
        },
        {
          key: "coefficient", label: "Закреплённый коэффициент", numeric: true, cell: row => row.sourceExecutionId ? decimal(row.coefficient) : "Не применяется"
        },
        {
          key: "target", label: "Рассчитано, лоты", sort: "lots", numeric: true, cell: row => row.sourceExecutionId ? decimal(row.targetLots) : "Не применяется"
        },
        {
          key: "executed", label: "Исполнено, лоты", sort: "executedLots", numeric: true, cell: row => decimal(row.executedLots)
        },
        {
          key: "skipped", label: "Пропущено, лоты", numeric: true, cell: row => row.sourceExecutionId ? decimal(row.skippedLots) : "Не применяется"
        },
        {
          key: "result", label: "Результат и причина", sort: "status", cell: row => {
            const cell = el("div", label(row.result));
            if (row.reason)
              cell.append(row.journalId != null ? reasonLink(row.reason, row.journalId) : note(row.reason));
            if (!row.complete)
              cell.append(note("История неполная"));
            return cell;
          }
        },
        {
          key: "last", label: "Последнее исполнение на счёте повторения, МСК", cell: row => row.lastExecutionAt ? time(row.lastExecutionAt) : row.executionPrice.status === "NO_EXECUTIONS" ? "Нет исполнений" : "Не подтверждено"
        },
        {
          key: "price", label: "Цена исполнения", sort: "executionPrice", numeric: true, cell: row => price(row.executionPrice)
        },
        {
          key: "origin", label: "Происхождение", sort: "origin", cell: row => label(row.origin)
        },
        {
          key: "details", label: "Заявки и исполнения", cell: row => this.details(row)
        },
      ],
      load: async (query) => {
        const from = query.get("from"), to = query.get("to");
        // datetime-local contains Moscow wall time, never the browser's local zone.
        if (from)
          query.set("from", new Date(from + "+03:00").toISOString());
        if (to)
          query.set("to", new Date(to + "+03:00").toISOString());
        if (from && to && from > to)
          throw Error("Начало периода позже конца. Исправьте границы в МСК.");
        const page = await read<AccountPage<Operation>>("/account/operations?" + query);
        const history = page.freshness.history, executions = page.freshness.executions;
        this.coverage.textContent = `Прочитанный период заявок: ${time(history?.from ?? null)} — ${time(history?.through ?? null)}. ${history?.complete ? "Полнота подтверждена для указанного периода." : "История заявок неполная."} Период исполнений: ${time(executions?.from ?? null)} — ${time(executions?.through ?? null)}. ${executions?.complete ? "Полнота подтверждена для указанного периода." : "История исполнений неполная."} ${history?.detail ?? ""} ${executions?.detail ?? ""}`;
        return page;
      },
    });
  }
  private details(row: Operation): HTMLDetailsElement {
    const detail = el("details");
    detail.dataset.expansion = row.id;
    detail.append(el("summary", "Показать заявки и исполнения"));
    if (!row.sourceExecutionId)
      detail.append(note("Нет исходного исполнения автоследования"));
    else
      detail.append(note("Исходное исполнение: " + row.sourceExecutionId));
    const orders = el("div");
    detail.append(orders);
    if (!row.orderIds.length)
      orders.append(note("Связанных заявок нет или связь не подтверждена."));
    const fills = el("div");
    fills.append(el("h4", "Фактические исполнения группы"));
    if (!row.fills.length)
      fills.append(note(row.executionPrice.status === "NO_EXECUTIONS" ? "Нет исполнений" : "Данные исполнений не подтверждены"));
    for (const fill of row.fills)
      fills.append(fillDetails(fill));
    detail.append(fills);
    let loaded = false;
    const load = async () => {
      if (loaded || !row.orderIds.length)
        return;
      loaded = true;
      const generation = this.generation;
      orders.replaceChildren(note("Загрузка связанных заявок…"));
      try {
        const result = await Promise.all(row.orderIds.map(id => this.read<Order>("/account/order?" + new URLSearchParams({
          id
        }))));
        if (!detail.isConnected || generation !== this.generation)
          return;
        orders.replaceChildren(...result.map(order => linkedOrder(order, row.id)));
      }
      catch (error) {
        if (!detail.isConnected || generation !== this.generation)
          return;
        loaded = false;
        const retry = el("button", "Повторить чтение связанных заявок");
        retry.onclick = () => void load();
        retry.id = "operation-" + encodeURIComponent(row.id) + "-retry-orders";
        orders.replaceChildren(note(error instanceof Error ? error.message : "Заявки не загружены"), retry);
      }
    };
    detail.ontoggle = () => {
      if (detail.open)
        void load();
    };
    return detail;
  }
  refresh() {
    return this.list.refresh();
  }
  clear() {
    this.generation++;
    this.list.clear();
    this.coverage.textContent = "Период полноты истории не подтверждён.";
  }
}
