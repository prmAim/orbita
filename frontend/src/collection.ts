export type ListQuery = {
  search: string;
  page: number;
  size: 25 | 50 | 100;
  sort: string;
  direction: "asc" | "desc";
  filters: Record<string, string>;
};
export type Freshness = {
  status: string;
  lastSuccessAt: string | null;
  detail: string | null;
};
export type ListPage<T> = {
  items: T[];
  total: number;
  page: number;
  size: number;
  freshness: Record<string, Freshness>;
};
export type Price = {
  status: string;
  value: string | null;
  currency: string | null;
  unit: string | null;
  reason?: string | null;
};
export type Column<T> = {
  key: string;
  label: string;
  sort?: string;
  numeric?: boolean;
  cell: (row: T) => string | Node;
};
type Filter = {
  key: string;
  label: string;
  options?: Array<[string, string]>;
  type?: "text" | "datetime-local";
};
type Options<T> = {
  id: string;
  label: string;
  columns: Column<T>[];
  filters?: Filter[];
  load: (query: URLSearchParams) => Promise<ListPage<T>>;
  key: (row: T) => string;
  freshnessKeys?: string[];
  applied?: (page: ListPage<T>) => void;
};
const node = <K extends keyof HTMLElementTagNameMap>(tag: K, text?: string) => {
  const result = document.createElement(tag);
  if (text !== undefined)
    result.textContent = text;
  return result;
};
/** No Number conversion, rounding or local averaging of broker decimals. */
export function decimal(value: string | null, suffix?: string | null): string {
  return value === null ? "Не подтверждено" : value + (suffix ? " " + suffix : "");
}
export function executionPrice(price: Price): string {
  if (price.status === "NO_EXECUTIONS")
    return "Нет исполнений";
  if (price.status !== "CONFIRMED" || price.value === null || !price.currency || !price.unit)
    return "Не подтверждено" + (price.reason ? ": " + price.reason : "");
  return `${price.value} ${price.currency} · ${price.unit}`;
}
export function queryParams(query: ListQuery): URLSearchParams {
  const params = new URLSearchParams({
    q: query.search, page: String(query.page), size: String(query.size), direction: query.direction
  });
  if (query.sort)
    params.set("sort", query.sort);
  for (const [key, value] of Object.entries(query.filters))
    if (value)
      params.set(key, value);
  return params;
}
/** Server pages represent filtering and stable sorting of the entire available set. */
export class CollectionView<T> {
  readonly query: ListQuery;
  private readonly status = node("p");
  private readonly content = node("div");
  private readonly total = node("p");
  private readonly previous = node("button", "Предыдущая");
  private readonly next = node("button", "Следующая");
  private readonly search = node("input");
  private readonly size = node("select");
  private readonly selectors = new Map<string, HTMLSelectElement | HTMLInputElement>();
  private generation = 0;
  private snapshot = "";
  private result: ListPage<T> | null = null;
  private expanded: string[] = [];
  private storageKey: string;
  private readonly baseStorageKey: string;
  private readonly defaults: ListQuery;
  private context = "";
  private readonly controls: HTMLFormElement;
  private readonly pagination: HTMLDivElement;
  private exactInitialized = false;
  constructor(private host: HTMLElement, private options: Options<T>) {
    this.storageKey = "orbita-list-v1-" + options.id;
    this.baseStorageKey = this.storageKey;
    this.query = {
      search: "", page: 0, size: 25, sort: options.columns.find(c => c.sort)?.sort ?? "", direction: "asc", filters: {}
    };
    this.defaults = structuredClone(this.query);
    this.restoreSaved();
    const controls = node("form");
    this.controls = controls;
    controls.className = "list-controls";
    controls.setAttribute("role", "search");
    this.search.id = options.id + "-search";
    this.search.type = "search";
    this.search.value = this.query.search;
    this.label(controls, "Тикер или название", this.search);
    const submit = node("button", "Найти");
    submit.type = "submit";
    controls.append(submit);
    controls.onsubmit = e => {
      e.preventDefault();
      this.query.search = this.search.value;
      this.change(true);
    };
    for (const filter of options.filters ?? []) {
      const select = filter.options ? node("select") : node("input");
      if (select instanceof HTMLInputElement) {
        select.type = filter.type ?? "text";
        if (select.type === "datetime-local") select.step = "1";
      }
      select.id = options.id + "-filter-" + filter.key;
      for (const [value, text] of filter.options ?? []) {
        const option = node("option", text);
        option.value = value;
        select.append(option);
      }
      select.value = this.query.filters[filter.key] ?? "";
      select.onchange = () => {
        this.query.filters[filter.key] = select.value;
        this.change(true);
      };
      this.selectors.set(filter.key, select);
      this.label(controls, filter.label, select);
    }
    this.status.className = "list-status";
    this.status.setAttribute("role", "status");
    this.status.setAttribute("aria-live", "polite");
    this.content.className = "collection-content";
    this.total.id = options.id + "-total";
    this.total.setAttribute("aria-live", "polite");
    const pagination = node("div");
    this.pagination = pagination;
    pagination.className = "pagination";
    pagination.setAttribute("role", "group");
    pagination.setAttribute("aria-label", options.label + ": страницы");
    this.size.id = options.id + "-size";
    for (const size of [25, 50, 100]) {
      const option = node("option", String(size));
      option.value = String(size);
      this.size.append(option);
    }
    this.size.value = String(this.query.size);
    this.size.onchange = () => {
      this.query.size = Number(this.size.value) as ListQuery["size"];
      this.change(true);
    };
    this.label(pagination, "Строк на странице", this.size);
    this.previous.id = options.id + "-previous";
    this.next.id = options.id + "-next";
    this.previous.onclick = () => {
      if (this.query.page > 0) {
        this.query.page--;
        this.change();
      }
    };
    this.next.onclick = () => {
      if (this.result && (this.query.page + 1) * this.query.size < this.result.total) {
        this.query.page++;
        this.change();
      }
    };
    pagination.append(this.previous, this.next, this.total);
    this.host.replaceChildren(controls, this.status, this.content, pagination);
    this.content.addEventListener("toggle", () => this.save(), true);
    this.updatePagination();
  }
  private label(parent: HTMLElement, text: string, input: HTMLElement) {
    const wrapper = node("div");
    const label = node("label", text);
    label.htmlFor = input.id;
    wrapper.append(label, input);
    parent.append(wrapper);
  }
  private restoreSaved() {
    try {
      const saved = JSON.parse(sessionStorage.getItem(this.storageKey) ?? "null");
      if (saved && typeof saved.query?.search === "string"
        && Number.isSafeInteger(saved.query.page) && saved.query.page >= 0
        && [25, 50, 100].includes(saved.query.size)
        && ["asc", "desc"].includes(saved.query.direction)
        && typeof saved.query.sort === "string"
        && saved.query.filters && typeof saved.query.filters === "object"
        && Object.values(saved.query.filters).every(value => typeof value === "string")) {
        Object.assign(this.query, saved.query);
        this.expanded = Array.isArray(saved.expanded) ? saved.expanded.filter((v: unknown) => typeof v === "string") : [];
      }
    } catch { /* Invalid or unavailable storage starts a fresh context. */ }
  }
  setContext(context: string, filters: Record<string, string> = {}) {
    if (this.context === context) return;
    this.save();
    this.context = context;
    this.storageKey = this.baseStorageKey + (context ? ":context:" + context : "");
    Object.assign(this.query, structuredClone(this.defaults));
    this.expanded = [];
    this.restoreSaved();
    Object.assign(this.query.filters, filters);
    this.search.value = this.query.search;
    this.size.value = String(this.query.size);
    for (const [key, selector] of this.selectors) selector.value = this.query.filters[key] ?? "";
    this.clear();
  }
  exactMode(active: boolean) {
    this.controls.hidden = active;
    this.pagination.hidden = active;
  }
  showExact(items: T[], expandedKeys: string[]) {
    this.exactMode(true);
    this.host.setAttribute("aria-busy", "false");
    this.status.dataset.state = "ready";
    this.status.textContent = "Точная запись: прежние поиск, период и страницы списка здесь не применяются.";
    const snapshot = "exact:" + JSON.stringify(items);
    if (snapshot === this.snapshot) return;
    this.save();
    if (!this.exactInitialized) this.expanded = [...new Set([...this.expanded, ...expandedKeys])];
    this.exactInitialized = true;
    if (items.length) this.render(items);
    else this.content.replaceChildren(node("p", "Связанная группа не подтверждена или недоступна в сохранённой истории."));
    this.snapshot = snapshot;
  }
  private change(reset = false) {
    if (reset)
      this.query.page = 0;
    this.save();
    void this.refresh();
  }
  private save() {
    const expanded = new Set(this.expanded);
    for (const detail of this.content.querySelectorAll<HTMLDetailsElement>("details[data-expansion]")) {
      if (detail.open)
        expanded.add(detail.dataset.expansion!);
      else
        expanded.delete(detail.dataset.expansion!);
    }
    this.expanded = [...expanded];
    try {
      sessionStorage.setItem(this.storageKey, JSON.stringify({
        query: this.query, expanded: this.expanded
      }));
    }
    catch { /* Optional persistence. */
    }
  }
  setFilters(filters: Record<string, string>) {
    Object.assign(this.query.filters, filters);
    for (const [key, selector] of this.selectors)
      selector.value = this.query.filters[key] ?? "";
    this.change(true);
  }
  async refresh() {
    const generation = ++this.generation;
    this.host.setAttribute("aria-busy", "true");
    if (!this.result) {
      this.status.textContent = "Загрузка…";
      this.status.dataset.state = "loading";
    }
    try {
      const result = await this.options.load(queryParams(this.query));
      if (generation !== this.generation)
        return;
      this.result = result;
      this.options.applied?.(result);
      const entries = this.freshnessEntries();
      const stale = !entries.length || entries.some(([, value]) => value.status !== "FRESH");
      this.status.textContent = stale
        ? `Данные не подтверждены свежим чтением. Последнее успешное чтение: ${this.lastSuccessfulRead()}. ${entries.map(([, value]) => value.detail ?? "").filter(Boolean).join("; ")}`
        : "";
      this.status.dataset.state = stale ? "stale" : "ready";
      const snapshot = JSON.stringify(result.items);
      if (snapshot !== this.snapshot) {
        this.save();
        const focus = document.activeElement instanceof HTMLElement ? document.activeElement.id : "";
        const scroll = window.scrollY;
        this.render(result.items);
        this.snapshot = snapshot;
        if (focus)
          document.getElementById(focus)?.focus({
            preventScroll: true
          });
        window.scrollTo(0, scroll);
      }
      this.updatePagination();
    }
    catch (error) {
      if (generation !== this.generation)
        return;
      this.status.dataset.state = this.result ? "stale" : "error";
      const retained = this.result
        ? `Показаны последние полученные данные. Последнее успешное чтение: ${this.lastSuccessfulRead()}. `
        : "Данные не загружены. ";
      this.status.textContent = retained + (error instanceof Error ? error.message : "Ошибка чтения.");
      const retry = node("button", "Повторить чтение");
      retry.onclick = () => void this.refresh();
      this.status.append(" ", retry);
    }
    finally {
      if (generation === this.generation)
        this.host.setAttribute("aria-busy", "false");
    }
  }
  private freshnessEntries() {
    return Object.entries(this.result?.freshness ?? {}).filter(([key]) =>
      !this.options.freshnessKeys || this.options.freshnessKeys.includes(key));
  }
  private lastSuccessfulRead() {
    const times = this.freshnessEntries().map(([, value]) => value.lastSuccessAt)
      .filter((value): value is string => value !== null);
    const format = new Intl.DateTimeFormat("ru-RU", {
      timeZone: "Europe/Moscow", dateStyle: "short", timeStyle: "medium",
    });
    return times.length ? times.map(value => format.format(new Date(value)) + " МСК").join("; ") : "не было";
  }
  private updatePagination() {
    const total = this.result?.total ?? 0;
    const pages = Math.max(1, Math.ceil(total / this.query.size));
    this.total.textContent = this.result ? `Всего: ${total}. Страница ${this.query.page + 1} из ${pages}.` : "Общее число записей ещё не получено.";
    this.previous.disabled = this.query.page === 0;
    this.next.disabled = !this.result || (this.query.page + 1) * this.query.size >= total;
  }
  private render(items: T[]) {
    const table = node("table");
    table.className = "collection-table";
    const caption = node("caption", this.options.label);
    table.append(caption);
    const head = node("thead"), headings = node("tr");
    for (const column of this.options.columns) {
      const th = node("th");
      th.scope = "col";
      if (column.sort) {
        const selected = this.query.sort === column.sort;
        th.setAttribute("aria-sort", selected ? this.query.direction === "asc" ? "ascending" : "descending" : "none");
        const sort = node("button", column.label + (selected ? this.query.direction === "asc" ? " ↑" : " ↓" : ""));
        sort.id = this.options.id + "-sort-" + column.key;
        sort.setAttribute("aria-label", `Сортировать: ${column.label}, ${selected && this.query.direction === "asc" ? "по убыванию" : "по возрастанию"}`);
        sort.onclick = () => {
          this.query.direction = selected && this.query.direction === "asc" ? "desc" : "asc";
          this.query.sort = column.sort!;
          this.snapshot = "";
          this.change(true);
        };
        th.append(sort);
      }
      else
        th.textContent = column.label;
      headings.append(th);
    }
    head.append(headings);
    table.append(head);
    const body = node("tbody");
    for (const item of items) {
      const tr = node("tr");
      const key = this.options.key(item);
      tr.dataset.rowKey = key;
      for (const column of this.options.columns) {
        const td = node("td");
        td.dataset.label = column.label;
        if (column.numeric)
          td.className = "numeric";
        td.append(column.cell(item));
        // Screen renderers may supply explicit IDs for exact record focus targets.
        for (const [i, control] of [...td.querySelectorAll<HTMLElement>("a,button,input,summary,select")].entries())
          if (!control.id)
            control.id = `${this.options.id}-${encodeURIComponent(key)}-${column.key}-${i}`;
        tr.append(td);
      }
      body.append(tr);
    }
    table.append(body);
    if (!items.length) {
      const empty = node("p", this.result?.total ? "На этой странице записей нет. Выберите предыдущую страницу." : "По заданным условиям записей нет.");
      empty.className = "list-empty";
      this.content.replaceChildren(table, empty);
    }
    else
      this.content.replaceChildren(table);
    for (const detail of this.content.querySelectorAll<HTMLDetailsElement>("details[data-expansion]"))
      detail.open = this.expanded.includes(detail.dataset.expansion!);
  }
  clear() {
    this.exactInitialized = false;
    ++this.generation;
    this.result = null;
    this.snapshot = "";
    this.content.replaceChildren();
    this.status.textContent = "";
    this.host.setAttribute("aria-busy", "false");
    this.updatePagination();
  }
}
