import { CollectionView } from "./collection.js";
import { type AccountPage, type Order, type Read, el, label, price, decimal, link, instrument, orderTime, completeness, note } from "./account-ui.js";
export const statusOptions: Array<[
  string,
  string
]> = [
  ["", "Все"], ["NEW", "Принята брокером"], ["ACTIVE", "Активна"], ["PARTIALLY_FILLED", "Частично исполнена"],
  ["UNKNOWN", "Результат не установлен"], ["SEND_STARTED", "Отправка начата"], ["CANCEL_STARTED", "Запрошена отмена"],
];
export const originOptions: Array<[
  string,
  string
]> = [["", "Все"], ["ORBITA", "Орбита"], ["MANUAL", "Ручная"], ["UNKNOWN", "Не установлено"]];
export const sideOptions: Array<[
  string,
  string
]> = [["", "Все"], ["BUY", "Покупка"], ["SELL", "Продажа"]];
export function orderStatus(row: Order): HTMLElement {
  const cell = el("div", row.status === "UNKNOWN" ? "Результат заявки не установлен" : label(row.status));
  if (row.cancellationPending)
    cell.append(note("Отмена запрошена; до подтверждения заявка может исполняться"));
  cell.append(completeness(row));
  return cell;
}
export function limitPrice(row: Order): string {
  if (row.limitPrice) return price(row.limitPrice);
  if (row.type === "ORDER_TYPE_MARKET") return "Рыночная заявка — не применяется";
  if (row.type === "ORDER_TYPE_BESTPRICE") return "По лучшей цене — не применяется";
  return "Не подтверждено";
}
export class OrdersView {
  readonly list: CollectionView<Order>;
  constructor(host: HTMLElement, read: Read) {
    const listHost = el("div");
    host.replaceChildren(note("Одна строка — одна активная заявка счёта повторения. Здесь видны заявки Орбиты, ручные и неустановленного происхождения. Дата — создание у брокера либо явно подписанная локальная попытка, МСК. Просмотр не изменяет и не отменяет заявки."), listHost);
    this.list = new CollectionView(listHost, {
      id: "orders", label: "Активные заявки счёта повторения", key: row => row.id, freshnessKeys: ["orders"],
      filters: [
        {
          key: "side", label: "Направление", options: sideOptions
        },
        {
          key: "status", label: "Статус заявки", options: statusOptions
        },
        {
          key: "origin", label: "Происхождение", options: originOptions
        },
      ],
      columns: [
        {
          key: "time", label: "Дата создания / отправки, МСК", sort: "time", cell: orderTime
        },
        {
          key: "instrument", label: "Инструмент", sort: "instrument", cell: instrument
        },
        {
          key: "side", label: "Покупка/продажа", sort: "side", cell: row => label(row.side)
        },
        {
          key: "lots", label: "Объём, лоты", sort: "lots", numeric: true, cell: row => decimal(row.lots)
        },
        {
          key: "executed", label: "Исполнено, лоты", sort: "executedLots", numeric: true, cell: row => decimal(row.executedLots)
        },
        {
          key: "remaining", label: "Осталось, лоты", numeric: true, cell: row => decimal(row.remainingLots)
        },
        {
          key: "price", label: "Цена исполнения", sort: "executionPrice", numeric: true, cell: row => price(row.executionPrice)
        },
        {
          key: "limit", label: "Лимитная цена и валюта", numeric: true, cell: limitPrice
        },
        {
          key: "status", label: "Статус заявки", sort: "status", cell: orderStatus
        },
        {
          key: "origin", label: "Происхождение", sort: "origin", cell: row => label(row.origin)
        },
        {
          key: "actions", label: "Действия", cell: row => link("История этой заявки", {
            page: "operations", order: row.id
          })
        },
      ],
      load: query => read<AccountPage<Order>>("/account/orders?" + query),
    });
  }
  refresh() {
    return this.list.refresh();
  }
  clear() {
    this.list.clear();
  }
}
