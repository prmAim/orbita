import { decimal, executionPrice, type Freshness, type ListPage, type Price } from "./collection.js";
import { routeHref, type OwnerRoute } from "./navigation.js";
export type AccountFreshness = Freshness & {
  from: string | null;
  through: string | null;
  complete: boolean;
};
export type AccountPage<T> = ListPage<T> & {
  freshness: Record<string, AccountFreshness>;
  cash: Cash[];
};
export type Cash = {
  currency: string;
  total: string | null;
  available: string | null;
  blocked: string | null;
  bankAt: string | null;
  updatedAt: string | null;
  limitation: string | null;
  freshness: string;
};
export type Asset = {
  id: string;
  instrumentId: string;
  ticker: string;
  name: string;
  state: string;
  reason: string | null;
  journalId: string | null;
  copiedLots: string | null;
  manualLots: string | null;
  brokerLots: string | null;
  lastExecutionAt: string | null;
  lastExecutionId: string | null;
  lastOrderId: string | null;
  executionPrice: Price;
  updatedAt: string | null;
  complete: boolean;
};
export type Fill = {
  id: string;
  orderId: string | null;
  tradeId: string;
  instrumentId: string;
  side: string | null;
  lots: string | null;
  price: Price;
  executedAt: string | null;
  commission: Price | null;
  provenance: string;
  complete: boolean;
};
export type Order = {
  id: string;
  brokerOrderId: string | null;
  requestId: string | null;
  instrumentId: string;
  ticker: string;
  name: string;
  side: string | null;
  lots: string | null;
  executedLots: string | null;
  remainingLots: string | null;
  type: string;
  createdAt: string | null;
  timeSemantics: string;
  limitPrice: Price | null;
  status: string;
  origin: string;
  cancellationPending: boolean;
  sourceExecutionIds: string[];
  fills: Fill[];
  executionPrice: Price;
  updatedAt: string | null;
  complete: boolean;
  reason: string | null;
};
export type Operation = {
  id: string;
  sourceExecutionId: string | null;
  instrumentId: string;
  ticker: string;
  name: string;
  side: string | null;
  time: string | null;
  timeSemantics: string;
  sourceLots: string | null;
  coefficient: string | null;
  targetLots: string | null;
  executedLots: string | null;
  skippedLots: string | null;
  result: string;
  reason: string | null;
  origin: string;
  orderIds: string[];
  fills: Fill[];
  lastExecutionAt: string | null;
  executionPrice: Price;
  complete: boolean;
};
export type Read = <T>(path: string) => Promise<T>;
export const el = <K extends keyof HTMLElementTagNameMap>(tag: K, text?: string) => {
  const node = document.createElement(tag);
  if (text !== undefined)
    node.textContent = text;
  return node;
};
export const time = (value: string | null) => value ? new Intl.DateTimeFormat("ru-RU", {
  timeZone: "Europe/Moscow", dateStyle: "short", timeStyle: "medium"
}).format(new Date(value)) + " МСК" : "Не подтверждено";
export const labels: Record<string, string> = {
  ACTIVE: "Активна",
  BUY: "Покупка", SELL: "Продажа", ORBITA: "Орбита", MANUAL: "Ручная", UNKNOWN: "Не установлено",
  READY: "Наблюдение", PAUSED: "Пауза инструмента", WAITING: "Ожидание сверки",
  NEW: "Принята брокером", PARTIALLY_FILLED: "Частично исполнена", FILLED: "Исполнена", CANCELLED: "Отменена", REJECTED: "Отклонена",
  SEND_STARTED: "Отправка начата, итог не подтверждён", CANCEL_STARTED: "Запрошена отмена, итог не подтверждён", FINAL: "Завершена", RETRIED: "Повторная попытка", SHADOW: "Теневой расчёт", HALTED: "Остановлено", RECEIVED: "Получено", ACCEPTED: "Принято к расчёту", SKIPPED: "Пропущено", MIXED: "Несколько результатов", UNLINKED_EVIDENCE: "Исполнение без подтверждённой связи с заявкой",
};
export const label = (value: string | null) => value ? labels[value] ?? value : "Не подтверждено";
export function price(value: Price): string {
  return executionPrice({
    ...value, currency: value.currency?.toUpperCase() ?? null, unit: value.unit === "currency per instrument unit" ? "за единицу инструмента" : value.unit
  });
}
export { decimal };
export function link(text: string, route: OwnerRoute): HTMLAnchorElement {
  const anchor = el("a", text);
  anchor.href = routeHref(route);
  return anchor;
}
export function reasonLink(reason: string, journalId: string | number): HTMLAnchorElement {
  const anchor = link(reason.length > 160 ? reason.slice(0, 160) + "…" : reason,
    {page: "journal", journal: String(journalId)});
  anchor.setAttribute("aria-label", "Полная причина в журнале: " + reason);
  return anchor;
}
export function instrument(row: {
  instrumentId: string;
  ticker: string;
  name: string;
}): HTMLElement {
  const node = el("div");
  node.append(link(row.ticker || row.instrumentId, {
    page: "operations", instrument: row.instrumentId,
    instrumentName: [row.ticker, row.name].filter(Boolean).join(" · ")
  }), el("div", row.name));
  return node;
}
export function note(text: string): HTMLElement {
  const node = el("p", text);
  node.className = "metadata";
  return node;
}
export function orderTime(row: Order): HTMLElement {
  const meaning = row.timeSemantics === "Broker order creation time" ? "Создана у брокера" : row.timeSemantics === "Local submission attempt time" ? "Локальная попытка отправки; время брокера не подтверждено" : "Создано поручение; время брокера не подтверждено";
  const node = el("div");
  node.append(link(time(row.createdAt), {
    page: "operations", order: row.id
  }), note(meaning));
  return node;
}
export function completeness(row: {
  complete: boolean;
  reason: string | null;
}): HTMLElement {
  const node = el("div");
  if (!row.complete)
    node.append(note("Сведения неполные"));
  if (row.reason)
    node.append(note(row.reason));
  return node;
}
