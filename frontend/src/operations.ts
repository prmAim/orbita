import { CollectionView } from "./collection.js";
import { type AccountPage, type Operation as OperationData, type Order, type Fill, type Read, el, time, label, price, decimal, link, reasonLink, instrument, orderTime, note } from "./account-ui.js";
import { originOptions, sideOptions, statusOptions, orderStatus, limitPrice } from "./orders.js";
import { routeHref, type OwnerRoute } from "./navigation.js";
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
function fillDetails(fill: Fill, context: string): HTMLElement {
  const article = el("article");
  article.className = "execution-detail";
  article.id = "fill-" + encodeURIComponent(context) + "-" + encodeURIComponent(fill.id);
  article.dataset.exactFill = fill.id;
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
  private instrumentContext = el("div");
  private route: OwnerRoute = {page: "operations"};
  private exactContext = el("div");
  private exactSnapshot = "";
  private target: HTMLElement | null = null;
  constructor(host: HTMLElement, private read: Read) {
    const listHost = el("div");
    this.instrumentContext.id = "operation-instrument-context";
    this.instrumentContext.hidden = true;
    this.exactContext.id = "operation-exact-context";
    this.exactContext.hidden = true;
    host.replaceChildren(note("Период, МСК: время исходного исполнения для групп Орбиты; время создания заявки для самостоятельных заявок. Для непривязанного исполнения используется его фактическое время, явно подписанное в строке. Закрытые позиции остаются в истории."), this.instrumentContext, this.exactContext, this.coverage, listHost);
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
        return read<AccountPage<Operation>>("/account/operations?" + query);
      },
      applied: result => {
        const page = result as AccountPage<Operation>;
        if (this.route.instrument && !this.route.instrumentName && page.items[0])
          this.showInstrument([page.items[0].ticker, page.items[0].name].filter(Boolean).join(" · "));
        const history = page.freshness.history, executions = page.freshness.executions;
        this.coverage.textContent = `Прочитанный период заявок: ${time(history?.from ?? null)} — ${time(history?.through ?? null)}. ${history?.complete ? "Полнота подтверждена для указанного периода." : "История заявок неполная."} Период исполнений: ${time(executions?.from ?? null)} — ${time(executions?.through ?? null)}. ${executions?.complete ? "Полнота подтверждена для указанного периода." : "История исполнений неполная."} ${history?.detail ?? ""} ${executions?.detail ?? ""}`;
      },
    });
  }
  setRoute(route: OwnerRoute) {
    if (routeHref(route) !== routeHref(this.route)) this.generation++;
    this.route = route;
    const exact = Boolean(route.fill || route.order || route.operation);
    this.list.setContext(exact ? routeHref(route) : route.instrument ? "instrument=" + route.instrument : "",
      route.instrument ? {instrumentId: route.instrument} : {});
    this.list.exactMode(exact);
    this.exactContext.hidden = !exact;
    this.coverage.hidden = exact;
    this.target = null;
    this.exactSnapshot = "";
    this.exactContext.replaceChildren();
    this.instrumentContext.hidden = !route.instrument;
    if (route.instrument) this.showInstrument(route.instrumentName ?? "Выбранная бумага");
  }
  private async refreshExact() {
    const generation = this.generation;
    const route = this.route;
    try {
      let fill: (Fill & {operationIds: string[]}) | null = null;
      let order: Order | null = null;
      let operationIds: string[];
      if (route.fill) {
        fill = await this.read<Fill & {operationIds: string[]}>("/account/fill?" + new URLSearchParams({id: route.fill}));
        if (route.order && route.order !== fill.orderId) throw Error("Связь исполнения с указанной заявкой не подтверждена");
        if (route.instrument && route.instrument !== fill.instrumentId) throw Error("Исполнение относится к другой бумаге");
        order = fill.orderId ? await this.read<Order>("/account/order?" + new URLSearchParams({id: fill.orderId})) : null;
        operationIds = fill.operationIds;
      } else if (route.operation) {
        operationIds = [route.operation];
      } else {
        const exactOrder = await this.read<Order & {operationIds: string[]}>("/account/order?" + new URLSearchParams({id: route.order!}));
        order = exactOrder;
        operationIds = exactOrder.operationIds;
      }
      if (generation !== this.generation) return;
      const groups = await Promise.all(operationIds.map(id => this.read<Operation>("/account/operation?" + new URLSearchParams({id}))));
      if (generation !== this.generation) return;
      if (route.operation && groups[0]?.id !== route.operation) throw Error("Запрошенная группа не подтверждена");
      if (fill && groups.some(group => !group.fills.some(item => item.id === fill!.id))) throw Error("Связь исполнения с группой не подтверждена");
      if (!fill && order && groups.some(group => !group.orderIds.includes(order!.id))) throw Error("Связь заявки с группой не подтверждена");
      const snapshot = JSON.stringify({fill, order, operation: route.operation});
      if (snapshot !== this.exactSnapshot) {
        this.exactContext.replaceChildren(el("h3", fill ? "История выбранного исполнения" : order ? "История выбранной заявки" : "История выбранной группы"));
        if (order) {
          const article = linkedOrder(order, "exact:" + (fill?.id ?? order.id));
          article.id = "exact-order-" + encodeURIComponent(order.id);
          article.dataset.exactOrder = order.id;
          if (!fill) { article.dataset.selected = "true"; article.tabIndex = -1; }
          this.exactContext.append(article);
        }
        if (fill) this.exactContext.append(fillDetails(fill, "exact"));
        this.exactSnapshot = snapshot;
      }
      this.list.showExact(groups, groups.map(group => group.id));
      if (fill) {
        const targets = [...document.querySelectorAll<HTMLElement>("#operations-view [data-exact-fill]")].filter(node => node.dataset.exactFill === fill!.id);
        for (const node of targets) { node.dataset.selected = "true"; node.tabIndex = -1; }
        this.target = targets[0] ?? null;
      } else if (order) this.target = this.exactContext.querySelector<HTMLElement>("[data-exact-order]");
      else {
        const row = [...document.querySelectorAll<HTMLElement>("#operations-view tr[data-row-key]")].find(node => node.dataset.rowKey === route.operation);
        if (row) { row.dataset.selected = "true"; row.tabIndex = -1; row.id = "exact-operation-" + encodeURIComponent(route.operation!); }
        this.target = row ?? null;
      }
    } catch (error) {
      if (generation !== this.generation) return;
      this.list.clear();
      this.list.exactMode(true);
      const status = el("p", "Точная запись недоступна или чтение завершилось ошибкой. " + (error instanceof Error ? error.message : "Повторите чтение."));
      status.id = "operation-exact-unavailable"; status.tabIndex = -1; status.setAttribute("role", "alert");
      this.exactContext.replaceChildren(status);
      this.exactSnapshot = "";
      this.target = status;
    }
  }
  reveal() {
    if (!this.target?.isConnected) return;
    this.target.scrollIntoView({block: "center"});
    this.target.focus({preventScroll: true});
  }
  private showInstrument(name: string) {
    const clear = link("Сбросить бумагу", {page: "operations"});
    clear.id = "clear-operation-instrument";
    this.instrumentContext.replaceChildren(note("История бумаги: " + name), clear);
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
      fills.append(fillDetails(fill, row.id));
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
    return this.route.fill || this.route.order || this.route.operation ? this.refreshExact() : this.list.refresh();
  }
  clear() {
    this.generation++;
    this.list.clear();
    this.exactContext.replaceChildren(); this.exactSnapshot = ""; this.target = null;
    this.coverage.textContent = "Период полноты истории не подтверждён.";
  }
}
