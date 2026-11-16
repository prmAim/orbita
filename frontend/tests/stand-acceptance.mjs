import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { join } from "node:path";

const execute = promisify(execFile);
const origin = "http://127.0.0.1:8086";
const usage = "node frontend/tests/stand-acceptance.mjs --session NAME --scenario Standard|Large [--cli /path/to/native/agent-browser]";
const options = { scenario: "Standard" };
for (let index = 2; index < process.argv.length; index++) {
  const name = process.argv[index];
  if (name === "--help") { console.log(usage); process.exit(0); }
  if (!["--session", "--scenario", "--cli"].includes(name) || !process.argv[index + 1]) throw Error(usage);
  options[name.slice(2)] = process.argv[++index];
}
if (!options.session || options.session === "default" || !/^[\p{L}\p{N}_-]+$/u.test(options.session)) throw Error("Use an explicit, already authorized named browser session. " + usage);
if (!["Standard", "Large"].includes(options.scenario)) throw Error(usage);
const cli = options.cli ?? (process.platform === "win32"
  ? join(process.env.APPDATA ?? "", "npm", "node_modules", "agent-browser", "bin", "agent-browser-win32-x64.exe")
  : "agent-browser");

async function command(...args) {
  let output;
  try {
    output = await execute(cli, ["--session", options.session, "--json", ...args], { timeout: 120000, maxBuffer: 1024 * 1024, windowsHide: true });
  } catch {
    throw Error(`agent-browser ${args[0]} failed; verify the named session and native CLI path (--cli). No credentials are read by this runner.`);
  }
  let result;
  try { result = JSON.parse(output.stdout.trim()); } catch { throw Error("agent-browser did not return its documented JSON envelope"); }
  if (!result.success) throw Error(`agent-browser ${args[0]} failed: ${result.error ?? "unknown error"}`);
  return result.data;
}
async function evaluate(fn, argument) {
  const source = `(${fn.toString()})(${JSON.stringify(argument)})`;
  return (await command("eval", "-b", Buffer.from(source, "utf8").toString("base64"))).result;
}

// All browser actions below are ordinary navigation, search, filters and pagination.
// There is no fetch/API replay, password, cookie/storage read, trading command or mock.
async function mainChecks({ expectedTotal, origin }) {
  const checks = [];
  const require = (condition, message) => { if (!condition) throw Error(message); };
  const pass = name => checks.push(name);
  const text = node => node?.textContent?.replace(/\s+/gu, " ").trim() ?? "";
  const rows = page => [...document.querySelectorAll(`#${page}-view .collection-content tbody > tr`)];
  const cell = (row, label) => [...row.children].find(node => node.dataset.label === label);
  const wait = async (condition, message) => {
    const end = Date.now() + 15000;
    while (!condition()) {
      require(!document.querySelector("#workspace").hidden, "Owner session closed; authorize separately and rerun");
      if (Date.now() > end) throw Error(message);
      await new Promise(resolve => setTimeout(resolve, 25));
    }
  };
  const settled = page => document.querySelector(`#${page}-view [aria-busy]`)?.getAttribute("aria-busy") === "false";
  const metadata = page => {
    const match = text(document.querySelector(`#${page}-total`)).match(/^Всего: (\d+)\. Страница (\d+) из (\d+)\.$/u);
    return match ? { total: Number(match[1]), page: Number(match[2]), pages: Number(match[3]) } : null;
  };
  const click = node => { require(node && !node.disabled, "Expected an enabled ordinary UI control"); node.focus(); node.click(); };
  const navigate = async page => {
    click(document.querySelector(`#nav-${page}`));
    await wait(() => !document.querySelector(`#${page}`).hidden && document.querySelector(`#nav-${page}`).getAttribute("aria-current") === "page", `Section ${page} did not open`);
    if (["assets", "orders", "operations"].includes(page))
      await wait(() => settled(page) && !document.querySelector(`#${page}-search`).closest("form").hidden, `Ordinary ${page} list did not finish reading`);
  };
  const set = async (id, value, page) => {
    const input = document.getElementById(id); require(input, `Missing labelled control ${id}`);
    input.value = value; input.dispatchEvent(new Event("change", { bubbles: true }));
    await wait(() => settled(page), `${page} filter did not finish`);
  };
  const search = async (page, value) => {
    const input = document.querySelector(`#${page}-search`); input.value = value; input.closest("form").requestSubmit();
    await wait(() => settled(page), `${page} search did not finish`);
  };
  const reset = async page => {
    await navigate(page);
    for (const input of document.querySelectorAll(`#${page}-view [id^="${page}-filter-"]`)) await set(input.id, "", page);
    await search(page, ""); await set(`${page}-size`, "25", page);
  };
  const priceHeader = page => {
    const headings = [...document.querySelectorAll(`#${page}-view .collection-content th`)];
    require(headings.filter(node => text(node).startsWith("Цена исполнения")).length === 1, `${page}: expected exactly one execution-price column`);
    require(!headings.some(node => /цена источника|цена исходной/iu.test(text(node))), `${page}: unexpected source-price column`);
  };
  require(location.origin === origin, "Use only the intended loopback stand");
  require(document.querySelector("#login").hidden && !document.querySelector("#workspace").hidden, "Log in separately before running acceptance");
  require(JSON.stringify([...document.querySelectorAll("nav [data-page]")].map(text)) === JSON.stringify(["Активы", "Инструменты", "Операции", "Журнал", "Уведомления", "Управление", "Копии"]), "Seven-section order differs from R128");
  pass("seven-sections");

  await reset("assets");
  await wait(() => metadata("assets")?.total === 15 && rows("assets").length === 15, "Expected Standard/Large 15 persisted assets");
  require(rows("assets").every(row => text(cell(row, "Инструмент")).includes("[ТЕСТ]")), "The selected stand is not the synthetic preview");
  priceHeader("assets");
  const sber = () => rows("assets").find(row => text(cell(row, "Инструмент").querySelector("a")) === "[ТЕСТ] SBER");
  require(sber(), "Synthetic SBER absent");
  for (const [label, value] of [["Остаток повторения, лоты", "2"], ["Ручные, лоты", "4"], ["Остаток брокера, лоты", "6"]]) require(text(cell(sber(), label)) === value, `SBER ${label}: expected ${value}`);
  require(text(cell(sber(), "Цена исполнения")) === "107 RUB · за единицу инструмента", "SBER latest manual-sale order mean must be 107");
  const cashRows = [...document.querySelectorAll("#assets-view .cash-table tbody tr")];
  require(cashRows.length === 2, "Expected two separate cash currencies");
  for (const [currency, expected] of [["RUB", ["954450 RUB", "954350 RUB", "100 RUB"]], ["USD", ["5 USD", "4 USD", "1 USD"]]]) {
    const row = cashRows.find(node => text(node.children[0]).startsWith(currency));
    require(row && JSON.stringify([...row.children].slice(1).map(text)) === JSON.stringify(expected), `${currency} cash differs from the independent scenario literals`);
  }
  require(![...document.querySelectorAll(".cash-table th")].some(node => /Цена/u.test(text(node))), "Cash must not gain a price column");
  pass("assets-15-sber-2-4-6-107-cash-two-currencies");

  await reset("orders");
  await wait(() => metadata("orders")?.total === 4 && rows("orders").length === 4, "Expected four active orders");
  priceHeader("orders");
  const origins = rows("orders").map(row => text(cell(row, "Происхождение")));
  require(origins.filter(value => value === "Орбита").length === 2 && origins.filter(value => value === "Ручная").length === 1 && origins.filter(value => value === "Не установлено").length === 1, "Active order provenance differs from 2/1/1");
  const statusText = rows("orders").map(row => text(cell(row, "Статус заявки"))).join(" ");
  for (const status of ["Результат заявки не установлен", "Отмена запрошена", "Частично исполнена", "Принята брокером"]) require(statusText.includes(status), `Active lifecycle state missing: ${status}`);
  for (const row of rows("orders")) if (text(cell(row, "Происхождение")) !== "Орбита") require(!row.querySelector("button,input,select"), "External/unknown order exposes a mutation control");
  require(rows("orders").every(row => text(cell(row, "Инструмент")).includes("SBER")), "Expected multiple active orders of the same paper");
  pass("active-four-provenance-and-uncertainty-view-only");

  await reset("operations"); priceHeader("operations");
  await wait(() => metadata("operations")?.total === expectedTotal, `Expected ${expectedTotal} persisted operations`);
  const sort = document.querySelector("#operations-sort-time");
  if (sort.closest("th").getAttribute("aria-sort") !== "ascending") click(sort);
  await wait(() => settled("operations") && document.querySelector("#operations-sort-time").closest("th").getAttribute("aria-sort") === "ascending", "Ascending server sort absent");
  const collect = async total => {
    const result = [], pages = metadata("operations").pages;
    for (let page = 1; page <= pages; page++) {
      await wait(() => settled("operations") && metadata("operations")?.page === page, `Operations page ${page} absent`);
      for (const row of rows("operations")) result.push({ id: row.dataset.rowKey, time: text(cell(row, "Дата события, МСК").querySelector("a")), paper: text(cell(row, "Инструмент")), origin: text(cell(row, "Происхождение")) });
      if (page < pages) { click(document.querySelector("#operations-next")); await wait(() => metadata("operations")?.page === page + 1 && settled("operations"), "Next page was not applied"); }
    }
    require(result.length === total && new Set(result.map(row => row.id)).size === total, "Pagination lost or duplicated stable operations IDs");
    require(document.querySelector("#operations-next").disabled, "Last-page next button should be disabled");
    return result;
  };
  const instant = value => {
    const m = value.match(/(\d{2})\.(\d{2})\.(\d{2,4}),?\s+(\d{2}):(\d{2}):(\d{2})/u);
    if (!m) return null;
    const year = Number(m[3]) < 100 ? 2000 + Number(m[3]) : Number(m[3]);
    return Date.UTC(year, Number(m[2]) - 1, Number(m[1]), Number(m[4]), Number(m[5]), Number(m[6]));
  };
  const ordered = (values, direction) => {
    let previous = null, sawNull = false;
    for (const row of values) { const current = instant(row.time); if (current === null) { sawNull = true; continue; } require(!sawNull, "Unconfirmed times are not last"); if (previous !== null) require(direction === "asc" ? current >= previous : current <= previous, "Server time ordering is not global"); previous = current; }
  };
  let baseline;
  for (const size of [25, 50, 100]) {
    await set("operations-size", String(size), "operations");
    const all = await collect(expectedTotal); ordered(all, "asc");
    if (baseline) require(JSON.stringify(all.map(row => row.id)) === JSON.stringify(baseline.map(row => row.id)), "Changing page size changed stable result order");
    else baseline = all;
  }
  click(document.querySelector("#operations-sort-time"));
  await wait(() => settled("operations") && document.querySelector("#operations-sort-time").closest("th").getAttribute("aria-sort") === "descending", "Descending sort absent");
  const descending = await collect(expectedTotal); ordered(descending, "desc");
  require(new Set(descending.map(row => row.id)).size === baseline.length, "Sort changed result identity");
  await search("operations", "SBER");
  const paperTotal = metadata("operations").total;
  require(paperTotal > 0 && paperTotal < expectedTotal, "Ticker search did not filter the whole available set");
  require((await collect(paperTotal)).every(row => row.paper.includes("SBER")), "Ticker search contains another paper");
  await set("operations-filter-origin", "MANUAL", "operations");
  const manualTotal = metadata("operations").total;
  require(manualTotal > 0 && manualTotal < paperTotal, "Origin filter was not applied globally");
  require((await collect(manualTotal)).every(row => row.origin === "Ручная"), "Manual filter returned another provenance");
  for (const row of rows("operations")) for (const field of ["Исходный объём, лоты", "Закреплённый коэффициент", "Рассчитано, лоты"]) require(text(cell(row, field)) === "Не применяется", "Manual row invents source or repeat coefficient");
  pass("operations-global-search-sort-pages-25-50-100-unique-ids");

  await reset("operations");
  await set("operations-filter-from", "2099-01-01T00:00", "operations");
  await wait(() => metadata("operations")?.total === 0, "Future period must give an honest empty result");
  await navigate("assets");
  const dateLink = cell(sber(), "Дата").querySelector("a"); require(dateLink, "Latest SBER execution has no exact link");
  const targetRoute = new URLSearchParams(dateLink.hash.split("?")[1]);
  dateLink.focus(); const sourceView = { scroll: scrollY, focus: dateLink.id }; dateLink.click();
  await wait(() => [...document.querySelectorAll("#operation-exact-context [data-exact-fill]")].some(node => node.dataset.exactFill === targetRoute.get("fill")), "Exact fill was constrained by previous period/page");
  const selected = [...document.querySelectorAll("#operation-exact-context [data-exact-fill]")].find(node => node.dataset.exactFill === targetRoute.get("fill"));
  const order = document.querySelector("#operation-exact-context [data-exact-order]");
  require(order?.dataset.exactOrder === targetRoute.get("order") && text(order).includes("107 RUB") && text(order).includes("Ручная"), "Asset date/order average/provenance context mismatch");
  require(text(selected).includes("112 RUB"), "Expected latest actual fill112, separate from order mean107");
  require(document.querySelector("#operations-view details[open]"), "Exact linked group not expanded");
  require(!document.querySelector("#operations-view .collection-content thead button"), "Exact headings still invoke generic sorting");
  await wait(() => document.activeElement.id === selected.id && selected.getBoundingClientRect().top < innerHeight && selected.getBoundingClientRect().bottom > 0, "Exact fill not focused and visible");
  history.back();
  await wait(() => !document.querySelector("#assets").hidden && document.activeElement.id === sourceView.focus && Math.abs(scrollY - sourceView.scroll) <= 2, "Native Back lost asset focus/position");
  await navigate("operations");
  require(document.querySelector("#operations-filter-from").value === "2099-01-01T00:00" && metadata("operations").total === 0, "Deep link overwrote previous list period");
  await navigate("assets");
  const reason = [...sber().querySelectorAll("a")].find(node => node.hash.startsWith("#journal?")); require(reason, "SBER pause has no exact journal link");
  const journalId = new URLSearchParams(reason.hash.split("?")[1]).get("journal");
  const fullReason = reason.getAttribute("aria-label")?.replace(/^Полная причина в журнале: /u, ""); click(reason);
  await wait(() => document.querySelector("#journal-exact [data-journal-id]")?.dataset.journalId === journalId, "Pause did not open its exact journal entry");
  require(text(document.querySelector("#journal-exact pre")) === fullReason?.replace(/\s+/gu, " ").trim(), "Exact journal omitted the full pause reason");
  pass("exact-manual-fill-112-order-107-period-independent-back-journal");

  for (const page of ["notifications", "control", "backup"]) { await navigate(page); require(![...document.querySelectorAll(`#${page} th`)].some(node => text(node).includes("Цена исполнения")), "Extra execution price in a non-account section"); }
  await reset("operations"); await search("operations", "Синтетическая бумага");
  await wait(() => metadata("operations")?.total === expectedTotal, "Reload setup search does not match all synthetic papers");
  await set("operations-size", "25", "operations"); click(document.querySelector("#operations-next"));
  await wait(() => settled("operations") && metadata("operations")?.page === 2, "Reload setup page2 absent");
  const detail = rows("operations")[0].querySelector("details"); if (!detail.open) click(detail.querySelector("summary"));
  await wait(() => !text(detail).includes("Загрузка связанных заявок"), "Related order disclosure still loading");
  const focus = detail.querySelector("summary"); focus.focus({ preventScroll: true }); window.scrollTo(0, 500);
  await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  const sorted = document.querySelector('#operations-view th[aria-sort="ascending"], #operations-view th[aria-sort="descending"]');
  const reloadState = { focus: focus.id, scroll: scrollY, key: detail.dataset.expansion, search: document.querySelector("#operations-search").value, sort: sorted?.getAttribute("aria-sort"), heading: text(sorted) };
  for (const section of ["assets", "orders", "operations"]) require(performance.getEntriesByType("resource").some(entry => entry.name.startsWith(origin + "/api/account/" + section + "?")), `No real same-origin ${section} HTTP read observed`);
  pass("real-owner-http-observed");
  return { checks, reloadState };
}

async function reloadChecks({ expected, expectedTotal, origin }) {
  const end = Date.now() + 15000;
  while (true) {
    if (location.origin !== origin) throw Error("Stand origin changed");
    const total = document.querySelector("#operations-total")?.textContent ?? "";
    if (!document.querySelector("#workspace").hidden && total.includes(`Всего: ${expectedTotal}. Страница 2 `) && document.activeElement.id === expected.focus && Math.abs(scrollY - expected.scroll) <= 2) break;
    if (Date.now() > end) throw Error("Reload lost page, focus or scroll; do not treat a previous run as PASS");
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  const sorted = document.querySelector('#operations-view th[aria-sort="ascending"], #operations-view th[aria-sort="descending"]');
  if (document.querySelector("#operations-search").value !== expected.search || sorted?.getAttribute("aria-sort") !== expected.sort || sorted?.textContent?.replace(/\s+/gu, " ").trim() !== expected.heading || ![...document.querySelectorAll("#operations-view details[open]")].some(node => node.dataset.expansion === expected.key)) throw Error("Reload lost search/sort/disclosure context");
  return "reload-search-sort-page-disclosure-scroll-focus";
}

async function narrowChecks({ origin }) {
  if (location.origin !== origin || document.querySelector("#workspace").hidden) throw Error("Owner stand unavailable");
  const wait = async condition => { const end = Date.now() + 10000; while (!condition()) { if (Date.now() > end) throw Error("Narrow account section did not load"); await new Promise(resolve => setTimeout(resolve, 25)); } };
  const oldSize = document.documentElement.style.fontSize;
  try {
    for (const page of ["assets", "orders", "operations"]) {
      document.querySelector(`#nav-${page}`).click();
      await wait(() => !document.querySelector(`#${page}`).hidden && document.querySelector(`#${page}-view .collection-content td`));
      const td = document.querySelector(`#${page}-view .collection-content td`);
      if (getComputedStyle(td).display !== "block" || !getComputedStyle(td, "::before").content.includes(td.dataset.label)) throw Error(`${page}: narrow cards lack labels`);
      document.documentElement.style.fontSize = "200%";
      if (document.documentElement.scrollWidth > innerWidth) throw Error(`${page}: horizontal overflow at200%text`);
      document.documentElement.style.fontSize = oldSize;
    }
    const button = document.querySelector("#operations-sort-time"); button.focus();
    return "narrow390-labelled-cards-text200-no-overflow";
  } finally { document.documentElement.style.fontSize = oldSize; }
}

let viewport;
try {
  const current = new URL((await command("get", "url")).url);
  if (current.origin !== origin || !["/", "/index.html"].includes(current.pathname)) throw Error(`Authorize the named session at ${origin}/ first; the runner does not navigate or log in`);
  viewport = await evaluate(() => ({ width: innerWidth, height: innerHeight }), null);
  await command("set", "viewport", "1440", "1000");
  console.error(`Checking ${options.scenario} persisted preview through ordinary UI`);
  const expectedTotal = options.scenario === "Large" ? 158 : 33;
  const result = await evaluate(mainChecks, { origin, expectedTotal });
  console.error("Checking reload and narrow keyboard layout");
  await command("reload");
  result.checks.push(await evaluate(reloadChecks, { origin, expectedTotal, expected: result.reloadState }));
  await command("set", "viewport", "390", "844");
  result.checks.push(await evaluate(narrowChecks, { origin }));
  await command("press", "Tab");
  const focus = await evaluate(() => ({ visible: document.activeElement instanceof HTMLElement && document.activeElement.matches(":focus-visible"), outline: getComputedStyle(document.activeElement).outlineWidth }), null);
  if (!focus.visible || Number.parseFloat(focus.outline) < 2) throw Error("Keyboard focus is not visibly outlined");
  result.checks.push("keyboard-visible-focus");
  console.log(JSON.stringify({ success: true, scenario: options.scenario, origin, checkedAt: new Date().toISOString(), expected: { assets: 15, activeOrders: 4, operations: expectedTotal, sber: ["2", "4", "6", "107"], cash: { RUB: ["954450", "954350", "100"], USD: ["5", "4", "1"] } }, checks: result.checks }, null, 2));
} catch (error) {
  console.error(`FAIL: ${error instanceof Error ? error.message : "acceptance failed"}`);
  process.exitCode = 1;
} finally {
  if (viewport) {
    try { await command("set", "viewport", String(viewport.width), String(viewport.height)); }
    catch { console.error("Could not restore the initial viewport"); process.exitCode = 1; }
  }
}
