package ru.orbita.account;

import static ru.orbita.account.AccountReadService.*;

import java.math.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.orbita.broker.*;

/** Materializes explicit read evidence; never creates a trading fact or inferred allocation. */
final class AccountProjection {
  private final AccountReadStore store;
  private final JdbcTemplate db;
  private final String account;
  private final Clock clock;
  private final Map<String, Map<String, Object>> securities = new HashMap<>(),
      states = new HashMap<>(),
      positions = new HashMap<>(),
      sources = new LinkedHashMap<>();
  private final Map<String, Map<String, Object>> orders = new LinkedHashMap<>(),
      fills = new LinkedHashMap<>();
  private final Map<String, Map<String, Object>> intents = new LinkedHashMap<>();
  private final List<Map<String, Object>> receipts;

  private record SkipEvidence(String reason, String journalId) {}

  private final Map<String, SkipEvidence> skipEvidence = new HashMap<>();
  private final Set<String> ambiguousSkips = new HashSet<>();
  private final Map<String, AccountReadStore.Document> rawSecurities = new HashMap<>();
  private final Map<String, List<Map<String, Object>>> durableFills = new HashMap<>();

  AccountProjection(AccountReadStore store, JdbcTemplate db, String account, Clock clock) {
    this.store = store;
    this.db = db;
    this.account = account;
    this.clock = clock;
    receipts = db.queryForList("select * from mirror_trade_receipt");
    for (var fill : db.queryForList("select * from broker_fill order by executed_at,id"))
      durableFills.computeIfAbsent(text(fill, "intent_id"), _ -> new ArrayList<>()).add(fill);
  }

  void rebuild() {
    for (var d : store.documents(account, "SECURITY")) {
      securities.put(d.id(), d.data());
      rawSecurities.put(d.id(), d);
    }
    for (var row : db.queryForList("select * from instrument_state"))
      states.put(text(row, "instrument_id"), row);
    for (var row : db.queryForList("select * from mirror_position"))
      positions.put(text(row, "instrument_id"), row);
    for (var row : db.queryForList("select * from source_execution order by executed_at,id"))
      sources.put(text(row, "id"), row);
    for (var row : db.queryForList("select * from copy_intent order by created_at,id"))
      intents.put(text(row, "id"), row);
    loadSkipEvidence();
    var bankOrders = store.documents(account, "BANK_ORDER");
    var consumed = new HashSet<String>();
    for (var intent : intents.values()) {
      var matching =
          bankOrders.stream()
              .filter(
                  d ->
                      text(d.data(), "requestId").equals(text(intent, "id"))
                          || !text(intent, "broker_order_id").isBlank()
                              && d.id().equals(text(intent, "broker_order_id")))
              .toList();
      var bank = matching.size() == 1 ? matching.getFirst() : null;
      if (bank != null) consumed.add(bank.id());
      makeOrder(bank, intent, matching.size() > 1);
    }
    for (var bank : bankOrders) if (!consumed.contains(bank.id())) makeOrder(bank, null, false);
    for (String kind : List.of("ORDER", "ASSET", "OPERATION", "FILL")) store.hide(account, kind);
    for (var order : orders.values())
      store.save(
          account,
          "ORDER",
          text(order, "id"),
          order,
          instant(order.get("updatedAt")),
          active(order));
    for (var fill : fills.values())
      store.save(account, "FILL", text(fill, "id"), fill, clock.instant(), true);
    operations();
    assets();
  }

  private void makeOrder(
      AccountReadStore.Document bank, Map<String, Object> intent, boolean ambiguous) {
    var raw = bank == null ? null : store.convert(bank.data(), AccountRead.Order.class);
    String id = intent == null ? encoded("order", bank.id()) : "intent:" + text(intent, "id");
    String instrument = raw == null ? text(intent, "instrument_id") : raw.instrumentId();
    String side = raw == null ? text(intent, "side") : raw.side().name();
    String brokerId = raw == null ? nullable(intent, "broker_order_id") : raw.brokerOrderId();
    long requested =
        raw == null ? number(intent, "requested_lots").longValue() : raw.lotsRequested();
    if (raw == null && requested == 0) requested = number(intent, "lots").longValue();
    long executed = raw == null ? number(intent, "executed_lots").longValue() : raw.lotsExecuted();
    if (intent != null) executed = Math.max(executed, number(intent, "executed_lots").longValue());
    String status = raw == null ? text(intent, "status") : raw.status().name();
    boolean cancellation =
        intent != null && Boolean.TRUE.equals(intent.get("cancellation_pending"));
    if (intent != null
        && Set.of("SEND_STARTED", "UNKNOWN", "CANCEL_STARTED").contains(text(intent, "status")))
      status = text(intent, "status");
    if (cancellation) status = "CANCEL_STARTED";
    if (intent != null
        && Set.of("FINAL", "RETRIED", "SHADOW", "HALTED").contains(text(intent, "status")))
      status = text(intent, "status");
    var rowFills = new ArrayList<Map<String, Object>>();
    if (raw != null) for (var fill : raw.fills()) rowFills.add(fillView(id, fill));
    if (intent != null)
      for (var fill : durableFills.getOrDefault(text(intent, "id"), List.of())) {
        var price =
            new AccountRead.Price(
                number(fill, "price"), text(fill, "currency"), "currency per instrument unit");
        var execution =
            new AccountRead.Execution(
                text(fill, "trade_id"),
                brokerId,
                instrument,
                BrokerGateway.Side.valueOf(side),
                number(fill, "lots"),
                "lots",
                price,
                instant(fill.get("executed_at")),
                null,
                "Durable broker_fill attached to registered intent");
        var old =
            rowFills.stream()
                .filter(f -> text(f, "tradeId").equals(execution.tradeId()))
                .findFirst()
                .orElse(null);
        if (old == null) rowFills.add(fillView(id, execution));
        else if (!samePersistedExecution(old, execution)) ambiguous = true;
      }
    for (var fill : rowFills) fills.put(text(fill, "id"), fill);
    boolean complete =
        !ambiguous
            && (raw == null
                ? rowFills.stream()
                            .map(f -> number(f, "lots"))
                            .reduce(BigDecimal.ZERO, BigDecimal::add)
                            .compareTo(BigDecimal.valueOf(executed))
                        == 0
                    && !Set.of("UNKNOWN", "SEND_STARTED", "CANCEL_STARTED").contains(status)
                : raw.fillsComplete() || intent != null && text(intent, "status").equals("FINAL"))
            && rowFills.stream()
                    .map(f -> number(f, "lots"))
                    .reduce(BigDecimal.ZERO, BigDecimal::add)
                    .compareTo(BigDecimal.valueOf(executed))
                == 0;
    String origin = intent != null ? "ORBITA" : manual(rowFills) ? "MANUAL" : "UNKNOWN";
    String source = intent == null ? null : text(intent, "execution_id");
    var security = securities.getOrDefault(instrument, Map.of());
    var limit =
        raw == null
            ? (intent.get("limit_price") == null
                ? null
                : price(
                    number(intent, "limit_price"),
                    text(intent, "currency"),
                    "currency per instrument unit",
                    "CONFIRMED",
                    null))
            : raw.limitPrice() == null
                ? null
                : price(
                    raw.limitPrice().value(),
                    raw.limitPrice().currency(),
                    raw.limitPrice().unit(),
                    "CONFIRMED",
                    null);
    var created =
        raw == null
            ? instant(
                intent.get("sent_at") != null ? intent.get("sent_at") : intent.get("created_at"))
            : raw.createdAt();
    var row =
        fields(
            "id",
            id,
            "brokerOrderId",
            brokerId,
            "requestId",
            raw == null ? text(intent, "id") : raw.requestId(),
            "instrumentId",
            instrument,
            "ticker",
            raw != null && !raw.ticker().isBlank() ? raw.ticker() : text(security, "ticker"),
            "name",
            text(security, "name"),
            "side",
            side,
            "lots",
            Long.toString(requested),
            "executedLots",
            Long.toString(executed),
            "remainingLots",
            Long.toString(Math.max(0, requested - executed)),
            "type",
            raw == null ? "LIMIT" : raw.orderType(),
            "createdAt",
            created,
            "timeSemantics",
            raw == null
                ? intent.get("sent_at") != null
                    ? "Local submission attempt time"
                    : "Local intent creation time; broker creation unconfirmed"
                : "Broker order creation time",
            "limitPrice",
            limit,
            "status",
            status,
            "origin",
            origin,
            "cancellationPending",
            cancellation,
            "sourceExecutionIds",
            source == null ? List.of() : List.of(source),
            "fills",
            rowFills,
            "executionPrice",
            average(rowFills, BigDecimal.valueOf(executed), complete),
            "updatedAt",
            bank == null ? clock.instant() : bank.observedAt(),
            "complete",
            complete,
            "reason",
            ambiguous
                ? "Several broker orders match one intent"
                : intent != null
                    ? nullable(intent, "reason")
                    : raw == null ? null : String.join("; ", raw.limitations()));
    orders.put(id, row);
  }

  private boolean manual(List<Map<String, Object>> rowFills) {
    if (rowFills.isEmpty()) return false;
    for (var fill : rowFills) {
      String key = account.length() + ":" + account + text(fill, "tradeId");
      boolean known =
          receipts.stream()
              .anyMatch(
                  r ->
                      text(r, "id").equals(key)
                          && text(r, "instrument_id").equals(text(fill, "instrumentId"))
                          && text(r, "side").equals(text(fill, "side"))
                          && number(r, "lots").compareTo(number(fill, "lots")) == 0
                          && sameStoredTime(r.get("executed_at"), fill.get("executedAt")));
      if (!known) return false;
    }
    return true;
  }

  private Map<String, Object> fillView(String orderId, AccountRead.Execution f) {
    return fields(
        "id",
        encoded("fill", orderId, f.tradeId()),
        "orderId",
        orderId,
        "tradeId",
        f.tradeId(),
        "instrumentId",
        f.instrumentId(),
        "side",
        f.side() == null ? null : f.side().name(),
        "lots",
        str(f.quantity()),
        "price",
        f.price() == null
            ? price(null, null, null, "UNCONFIRMED", "Execution price unavailable")
            : price(f.price().value(), f.price().currency(), f.price().unit(), "CONFIRMED", null),
        "executedAt",
        f.executedAt(),
        "commission",
        f.commission() == null
            ? null
            : price(
                f.commission().value(),
                f.commission().currency(),
                f.commission().unit(),
                "CONFIRMED",
                null),
        "provenance",
        f.source(),
        "complete",
        f.quantity() != null && f.price() != null && f.executedAt() != null);
  }

  private void loadSkipEvidence() {
    for (var event :
        db.queryForList(
            "select id,instrument,detail from journal_entry where category='SOURCE_SKIPPED' order by id")) {
      String detail = text(event, "detail");
      var candidates =
          sources.values().stream()
              .filter(
                  s ->
                      text(s, "instrument_id").equals(text(event, "instrument"))
                          && detail.startsWith(text(s, "id") + " "))
              .map(s -> text(s, "id"))
              .toList();
      if (candidates.size() == 1) {
        String id = candidates.getFirst();
        skipEvidence.put(
            id, new SkipEvidence(detail.substring(id.length() + 1), text(event, "id")));
      } else if (candidates.size() > 1) ambiguousSkips.addAll(candidates);
    }
  }

  private String sourceSkipReason(String sourceId) {
    var exact = skipEvidence.get(sourceId);
    return exact != null
        ? exact.reason()
        : ambiguousSkips.contains(sourceId)
            ? "Причина пропуска не подтверждена: неоднозначная связь с журналом"
            : null;
  }

  private void operations() {
    for (var source : sources.values()) {
      String sourceId = text(source, "id");
      var related =
          orders.values().stream()
              .filter(o -> ((List<?>) o.get("sourceExecutionIds")).contains(sourceId))
              .toList();
      var unique = new LinkedHashMap<String, Map<String, Object>>();
      for (var order : related)
        for (var fill : rowFills(order)) unique.putIfAbsent(text(fill, "id"), fill);
      var groupFills = new ArrayList<>(unique.values());
      BigDecimal executed =
          related.stream()
              .map(o -> number(o, "executedLots"))
              .reduce(BigDecimal.ZERO, BigDecimal::add);
      var intentRows =
          intents.values().stream().filter(i -> text(i, "execution_id").equals(sourceId)).toList();
      BigDecimal target =
          intentRows.stream()
              .filter(i -> i.get("retry_of") == null)
              .map(i -> number(i, "lots"))
              .reduce(BigDecimal.ZERO, BigDecimal::add);
      BigDecimal skipped =
          intentRows.stream()
              .map(i -> number(i, "skipped_lots"))
              .reduce(BigDecimal.ZERO, BigDecimal::add);
      String instrument = text(source, "instrument_id");
      var security = securities.getOrDefault(instrument, Map.of());
      boolean complete = related.stream().allMatch(o -> Boolean.TRUE.equals(o.get("complete")));
      var row =
          fields(
              "id",
              encoded("source", sourceId),
              "sourceExecutionId",
              sourceId,
              "instrumentId",
              instrument,
              "ticker",
              text(security, "ticker"),
              "name",
              text(security, "name"),
              "side",
              text(source, "side"),
              "time",
              instant(source.get("executed_at")),
              "timeSemantics",
              "Source execution time",
              "sourceLots",
              str(source.get("lots")),
              "coefficient",
              str(source.get("coefficient")),
              "targetLots",
              str(target),
              "executedLots",
              str(executed),
              "skippedLots",
              str(skipped),
              "result",
              related.isEmpty()
                  ? text(source, "disposition")
                  : related.stream().map(o -> text(o, "status")).distinct().count() == 1
                      ? text(related.getFirst(), "status")
                      : "MIXED",
              "reason",
              related.stream()
                  .map(o -> nullable(o, "reason"))
                  .filter(Objects::nonNull)
                  .findFirst()
                  .orElse(sourceSkipReason(sourceId)),
              "journalId",
              skipEvidence.containsKey(sourceId) ? skipEvidence.get(sourceId).journalId() : null,
              "origin",
              "ORBITA",
              "orderIds",
              related.stream().map(o -> text(o, "id")).toList(),
              "fills",
              groupFills,
              "lastExecutionAt",
              latest(groupFills),
              "executionPrice",
              average(groupFills, executed, complete),
              "complete",
              complete
                  && (!ambiguousSkips.contains(sourceId) || skipEvidence.containsKey(sourceId)));
      store.save(account, "OPERATION", text(row, "id"), row, clock.instant(), true);
    }
    for (var order : orders.values())
      if (((List<?>) order.get("sourceExecutionIds")).isEmpty()) {
        var row =
            fields(
                "id",
                text(order, "id"),
                "sourceExecutionId",
                null,
                "instrumentId",
                order.get("instrumentId"),
                "ticker",
                order.get("ticker"),
                "name",
                order.get("name"),
                "side",
                order.get("side"),
                "time",
                order.get("createdAt"),
                "timeSemantics",
                order.get("timeSemantics"),
                "sourceLots",
                null,
                "coefficient",
                null,
                "targetLots",
                null,
                "executedLots",
                order.get("executedLots"),
                "skippedLots",
                null,
                "result",
                order.get("status"),
                "journalId",
                null,
                "reason",
                "Нет исходного исполнения автоследования",
                "origin",
                order.get("origin"),
                "orderIds",
                List.of(order.get("id")),
                "fills",
                order.get("fills"),
                "lastExecutionAt",
                latest(rowFills(order)),
                "executionPrice",
                order.get("executionPrice"),
                "complete",
                order.get("complete"));
        store.save(
            account, "OPERATION", text(row, "id"), row, instant(order.get("updatedAt")), true);
      }
    for (var d : store.documents(account, "BANK_FILL")) {
      var execution = store.convert(d.data(), AccountRead.Execution.class);
      if (fills.values().stream().anyMatch(f -> sameExecution(f, execution))) continue;
      var view = fillView(nullSafeOrder(execution), execution);
      view.put("orderId", null);
      fills.put(text(view, "id"), view);
      store.save(account, "FILL", text(view, "id"), view, d.observedAt(), true);
      var security = securities.getOrDefault(execution.instrumentId(), Map.of());
      var row =
          fields(
              "id",
              d.id(),
              "sourceExecutionId",
              null,
              "instrumentId",
              execution.instrumentId(),
              "ticker",
              text(security, "ticker"),
              "name",
              text(security, "name"),
              "side",
              execution.side() == null ? null : execution.side().name(),
              "time",
              execution.executedAt(),
              "timeSemantics",
              "Unlinked broker execution time",
              "sourceLots",
              null,
              "coefficient",
              null,
              "targetLots",
              null,
              "executedLots",
              null,
              "skippedLots",
              null,
              "result",
              "UNLINKED_EVIDENCE",
              "journalId",
              null,
              "reason",
              "Execution observed; order linkage and cross-service deduplication unconfirmed; excluded from grouped totals",
              "origin",
              manual(List.of(view)) ? "MANUAL" : "UNKNOWN",
              "orderIds",
              List.of(),
              "fills",
              List.of(view),
              "lastExecutionAt",
              execution.executedAt(),
              "executionPrice",
              price(null, null, null, "UNCONFIRMED", "Order completeness/link unavailable"),
              "complete",
              false);
      store.save(account, "OPERATION", d.id(), row, d.observedAt(), true);
    }
  }

  private static String nullSafeOrder(AccountRead.Execution e) {
    return e.brokerOrderId() == null ? encoded("unlinked", e.tradeId()) : e.brokerOrderId();
  }

  /** Normalize only a known PostgreSQL timestamptz boundary, never two raw broker observations. */
  private static boolean sameStoredTime(Object storedValue, Object rawValue) {
    Instant stored = instant(storedValue), raw = instant(rawValue);
    if (stored == null || raw == null) return false;
    long micros = (raw.getNano() + 500L) / 1000L;
    return stored.equals(Instant.ofEpochSecond(raw.getEpochSecond(), micros * 1000L));
  }

  private static boolean samePersistedExecution(
      Map<String, Object> raw, AccountRead.Execution stored) {
    if (!sameStoredTime(stored.executedAt(), raw.get("executedAt"))) return false;
    var atStoredPrecision = new LinkedHashMap<String, Object>(raw);
    atStoredPrecision.put("executedAt", stored.executedAt());
    return sameExecution(atStoredPrecision, stored);
  }

  private static boolean sameExecution(Map<String, Object> f, AccountRead.Execution e) {
    var p = (Map<?, ?>) f.get("price");
    return text(f, "tradeId").equals(e.tradeId())
        && text(f, "instrumentId").equals(e.instrumentId())
        && Objects.equals(f.get("side"), e.side() == null ? null : e.side().name())
        && number(f, "lots").compareTo(e.quantity()) == 0
        && e.price() != null
        && Objects.equals(p.get("currency"), e.price().currency())
        && new BigDecimal(p.get("value").toString()).compareTo(e.price().value()) == 0
        && Objects.equals(instant(f.get("executedAt")), e.executedAt());
  }

  private void assets() {
    var freshness = store.freshness(account, clock.instant());
    var executionScope = (Map<?, ?>) freshness.getOrDefault("executions", Map.of());
    boolean scopeComplete = Boolean.TRUE.equals(executionScope.get("complete"));
    for (var d : rawSecurities.values()) {
      if (!d.current()) continue;
      var security = store.convert(d.data(), AccountRead.Security.class);
      String instrument = security.instrumentId();
      var state = states.getOrDefault(instrument, Map.of());
      String reason = nullable(state, "reason");
      var position = positions.get(instrument);
      boolean composition =
          position != null
              && Boolean.TRUE.equals(position.get("broker_inventory_qualified"))
              && security.totalLots() != null
              && number(position, "copied_lots")
                      .add(number(position, "manual_lots"))
                      .compareTo(security.totalLots())
                  == 0;
      var candidates =
          fills.values().stream()
              .filter(
                  f -> text(f, "instrumentId").equals(instrument) && f.get("executedAt") != null)
              .toList();
      var latest = latest(candidates);
      var atLatest =
          latest == null
              ? List.<Map<String, Object>>of()
              : candidates.stream()
                  .filter(f -> latest.equals(instant(f.get("executedAt"))))
                  .toList();
      var lastOrders = atLatest.stream().map(f -> nullable(f, "orderId")).distinct().toList();
      String lastOrder = lastOrders.size() == 1 ? lastOrders.getFirst() : null;
      var order = lastOrder == null ? null : orders.get(lastOrder);
      boolean historyComplete =
          scopeComplete
              && latest != null
              && executionScope.get("from") != null
              && executionScope.get("through") != null
              && !latest.isBefore(instant(executionScope.get("from")))
              && !latest.isAfter(instant(executionScope.get("through")));
      Object executionPrice =
          historyComplete && order != null
              ? order.get("executionPrice")
              : price(
                  null,
                  null,
                  null,
                  historyComplete && latest == null ? "NO_EXECUTIONS" : "UNCONFIRMED",
                  latest == null
                      ? "History does not establish absence of executions"
                      : "Latest execution/order or history completeness unconfirmed");
      String journalId =
          reason == null
              ? null
              : db
                  .query(
                      "select id from journal_entry where instrument=? and detail=? order by id desc limit 1",
                      (rs, n) -> Long.toString(rs.getLong(1)),
                      instrument,
                      reason)
                  .stream()
                  .findFirst()
                  .orElse(null);
      var row =
          fields(
              "id",
              instrument,
              "instrumentId",
              instrument,
              "ticker",
              security.ticker(),
              "name",
              security.name(),
              "state",
              Boolean.TRUE.equals(state.get("paused"))
                  ? "PAUSED"
                  : Boolean.TRUE.equals(state.get("waiting")) ? "WAITING" : "READY",
              "reason",
              reason,
              "journalId",
              journalId,
              "copiedLots",
              composition ? str(position.get("copied_lots")) : null,
              "manualLots",
              composition ? str(position.get("manual_lots")) : null,
              "brokerLots",
              str(security.totalLots()),
              "lastExecutionAt",
              latest,
              "lastExecutionId",
              atLatest.size() == 1 ? atLatest.getFirst().get("id") : null,
              "lastOrderId",
              lastOrder,
              "executionPrice",
              executionPrice,
              "updatedAt",
              d.observedAt(),
              "complete",
              security.totalLots() != null && historyComplete);
      store.save(account, "ASSET", instrument, row, d.observedAt(), true);
    }
  }

  private static boolean active(Map<String, Object> row) {
    return !Set.of(
            "FILLED",
            "CANCELLED",
            "REJECTED",
            "DONE",
            "SKIPPED",
            "DISCARDED",
            "FINAL",
            "WAITING",
            "RETRIED",
            "SHADOW",
            "HALTED")
        .contains(text(row, "status"));
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> rowFills(Map<String, Object> order) {
    return (List<Map<String, Object>>) order.get("fills");
  }

  private static Instant latest(Collection<Map<String, Object>> rows) {
    return rows.stream()
        .map(r -> instant(r.get("executedAt")))
        .filter(Objects::nonNull)
        .max(Comparator.naturalOrder())
        .orElse(null);
  }

  private static Map<String, Object> average(
      List<Map<String, Object>> rows, BigDecimal expected, boolean complete) {
    if (!complete)
      return price(null, null, null, "UNCONFIRMED", "Executed portion incomplete or ambiguous");
    if (expected.signum() == 0 && rows.isEmpty())
      return price(null, null, null, "NO_EXECUTIONS", null);
    BigDecimal qty = BigDecimal.ZERO, numerator = BigDecimal.ZERO;
    String currency = null, unit = null;
    var seen = new HashSet<String>();
    for (var fill : rows) {
      if (!seen.add(text(fill, "id"))) continue;
      var p = (Map<?, ?>) fill.get("price");
      if (!Boolean.TRUE.equals(fill.get("complete")) || p == null || p.get("value") == null)
        return price(null, null, null, "UNCONFIRMED", "Missing actual execution evidence");
      String c = Objects.toString(p.get("currency"), "");
      String u = Objects.toString(p.get("unit"), "");
      if (currency != null && (!currency.equals(c) || !unit.equals(u)))
        return price(null, null, null, "UNCONFIRMED", "Mixed execution currency/unit");
      currency = c;
      unit = u;
      var lots = number(fill, "lots");
      qty = qty.add(lots);
      numerator = numerator.add(lots.multiply(new BigDecimal(p.get("value").toString())));
    }
    if (qty.signum() <= 0 || qty.compareTo(expected) != 0)
      return price(null, currency, unit, "UNCONFIRMED", "Quantity completeness unconfirmed");
    return price(numerator.divide(qty, MathContext.DECIMAL128), currency, unit, "CONFIRMED", null);
  }

  private static Map<String, Object> price(
      BigDecimal value, String currency, String unit, String status, String reason) {
    return fields(
        "value",
        str(value),
        "currency",
        currency,
        "unit",
        unit,
        "status",
        status,
        "reason",
        reason);
  }

  private static String text(Map<String, Object> row, String key) {
    return Objects.toString(row.get(key), "");
  }

  private static String nullable(Map<String, Object> row, String key) {
    Object value = row.get(key);
    return value == null ? null : value.toString();
  }

  private static BigDecimal number(Map<String, Object> row, String key) {
    Object value = row.get(key);
    return value == null ? BigDecimal.ZERO : new BigDecimal(value.toString());
  }

  private static Instant instant(Object value) {
    if (value == null) return null;
    if (value instanceof Instant i) return i;
    if (value instanceof Timestamp t) return t.toInstant();
    if (value instanceof OffsetDateTime t) return t.toInstant();
    return Instant.parse(value.toString());
  }
}
