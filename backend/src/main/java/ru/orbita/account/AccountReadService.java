package ru.orbita.account;

import java.math.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.function.Supplier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.*;
import ru.orbita.control.OrbitaProperties;
import ru.orbita.control.PersistenceBoundary;

/** Persistent display-only account projections. No trading entities or commands are mutated. */
@Service
public class AccountReadService {
  private final AccountReadStore store;
  private final JdbcTemplate db;
  private final TransactionTemplate tx;
  private final ObjectProvider<BrokerGateway> gateways;
  private volatile String account;
  private final boolean configuredAccount;
  private final Clock clock;

  public AccountReadService(
      AccountReadStore store,
      JdbcTemplate db,
      PlatformTransactionManager manager,
      ObjectProvider<BrokerGateway> gateways,
      OrbitaProperties properties,
      Clock clock) {
    this.store = store;
    this.db = db;
    this.tx = new TransactionTemplate(manager);
    this.gateways = gateways;
    this.account =
        properties.mirrorAccount() == null || properties.mirrorAccount().isBlank()
            ? null
            : properties.mirrorAccount();
    this.configuredAccount = this.account != null;
    this.clock = clock;
    this.tx.setIsolationLevel(
        org.springframework.transaction.TransactionDefinition.ISOLATION_REPEATABLE_READ);
  }

  public record Page(
      List<Map<String, Object>> items,
      long total,
      int page,
      int size,
      String sort,
      String direction,
      Map<String, Object> freshness,
      List<Map<String, Object>> cash) {}

  public void refresh() {
    resolveAccount();
    var gateway = gateways.getIfAvailable();
    if (gateway == null) {
      transactionVoid(
          _ -> {
            store.lock(account);
            project(false);
          });
      return;
    }
    var retained =
        db.query(
            "select id from account_read_document where account_id=? and kind='BANK_ORDER' and coalesce(payload->>'status','UNKNOWN') not in ('FILLED','CANCELLED','REJECTED')",
            (rs, n) -> rs.getString(1),
            account);
    retained.addAll(
        db.query(
            "select broker_order_id from copy_intent where broker_order_id is not null and status in ('SEND_STARTED','ACTIVE','UNKNOWN','CANCEL_STARTED')",
            (rs, n) -> rs.getString(1)));
    var through = clock.instant();
    var from = through.minusSeconds(86400);
    AccountRead.Holdings holdings;
    try {
      holdings = gateway.readHoldings();
    } catch (RuntimeException unavailable) {
      PersistenceBoundary.rethrowBrokerPersistence(unavailable);
      holdings =
          new AccountRead.Holdings(failed("Holdings unavailable"), failed("Cash unavailable"));
    }
    var orders = safe(() -> gateway.readOrders(retained), "Orders unavailable");
    var history =
        safe(() -> gateway.readOrderHistory(from, through, retained), "Order history unavailable");
    var executions = safe(() -> gateway.readExecutions(from, through), "Executions unavailable");
    var captured = holdings;
    transactionVoid(
        _ -> {
          store.lock(account);
          ingest("assets", "SECURITY", captured.securities(), null, null, s -> s.instrumentId());
          ingest("cash", "CASH", captured.cash(), null, null, AccountRead.Cash::currency);
          ingestOrders(orders);
          ingestOrders(history);
          observeFreshness("orders", orders, null, null);
          ingest(
              "executions",
              "BANK_FILL",
              executions,
              from,
              through,
              e -> encoded("operation-fill", e.tradeId()));
          boolean complete = history.complete() && executions.complete();
          var combined = new ArrayList<>(history.limitations());
          combined.addAll(executions.limitations());
          store.freshness(
              account,
              "history",
              complete ? "FRESH" : "PARTIAL",
              through,
              complete ? through : null,
              from,
              through,
              complete,
              String.join("; ", combined));
          project(true);
        });
  }

  private <T> AccountRead.Snapshot<T> failed(String detail) {
    return new AccountRead.Snapshot<>(List.of(), null, clock.instant(), false, List.of(detail));
  }

  private <T> AccountRead.Snapshot<T> safe(Supplier<AccountRead.Snapshot<T>> read, String detail) {
    try {
      return read.get();
    } catch (RuntimeException unavailable) {
      PersistenceBoundary.rethrowBrokerPersistence(unavailable);
      return failed(detail);
    }
  }

  private <T> void ingest(
      String section,
      String kind,
      AccountRead.Snapshot<T> snapshot,
      Instant from,
      Instant through,
      java.util.function.Function<T, String> id) {
    if (snapshot.complete() && kind.equals("SECURITY")) store.hide(account, kind);
    boolean cashUpdated = false;
    for (var item : snapshot.items()) {
      Instant at = snapshot.receivedAt() == null ? clock.instant() : snapshot.receivedAt();
      if (item instanceof AccountRead.Cash cash) cashUpdated |= saveCash(cash, at);
      else store.save(account, kind, id.apply(item), item, at, true);
    }
    if (section.equals("cash")) cashFreshness(snapshot, cashUpdated);
    else observeFreshness(section, snapshot, from, through);
  }

  private void observeFreshness(
      String section, AccountRead.Snapshot<?> snapshot, Instant from, Instant through) {
    var at = snapshot.receivedAt() == null ? clock.instant() : snapshot.receivedAt();
    String status =
        snapshot.complete() ? "FRESH" : snapshot.items().isEmpty() ? "ERROR" : "PARTIAL";
    store.freshness(
        account,
        section,
        status,
        at,
        snapshot.complete() || section.equals("cash") && !snapshot.items().isEmpty() ? at : null,
        from,
        through,
        snapshot.complete(),
        String.join("; ", snapshot.limitations()));
  }

  private void ingestOrders(AccountRead.Snapshot<AccountRead.Order> snapshot) {
    var old = new HashMap<String, AccountRead.Order>();
    for (var d : store.documents(account, "BANK_ORDER"))
      old.put(d.id(), store.convert(d.data(), AccountRead.Order.class));
    for (var order : snapshot.items()) {
      var previous = old.get(order.brokerOrderId());
      if (previous != null
          && (!order.fillsComplete() || order.lotsExecuted() < previous.lotsExecuted())) {
        var fills = new LinkedHashMap<String, AccountRead.Execution>();
        for (var f : previous.fills()) fills.put(f.tradeId(), f);
        for (var f : order.fills()) fills.putIfAbsent(f.tradeId(), f);
        order =
            new AccountRead.Order(
                order.brokerOrderId(),
                order.requestId(),
                order.instrumentId(),
                order.ticker(),
                order.side(),
                order.lotsRequested(),
                Math.max(order.lotsExecuted(), previous.lotsExecuted()),
                order.status(),
                order.orderType(),
                order.createdAt(),
                order.limitPrice(),
                new ArrayList<>(fills.values()),
                false,
                null,
                order.origin(),
                java.util.stream.Stream.concat(
                        order.limitations().stream(),
                        java.util.stream.Stream.of(
                            "Retained previously confirmed fills/count; latest incomplete quantity is unconfirmed"))
                    .toList());
      }
      store.save(
          account,
          "BANK_ORDER",
          order.brokerOrderId(),
          order,
          snapshot.receivedAt() == null ? clock.instant() : snapshot.receivedAt(),
          true);
    }
  }

  public void acceptCash(AccountRead.Snapshot<AccountRead.Cash> snapshot) {
    resolveAccount();
    transactionVoid(
        _ -> {
          store.lock(account);
          boolean updated = false;
          for (var cash : snapshot.items())
            updated |=
                saveCash(
                    cash, snapshot.receivedAt() == null ? clock.instant() : snapshot.receivedAt());
          cashFreshness(snapshot, updated);
        });
  }

  private static boolean coherentCash(AccountRead.Cash cash) {
    return cash.bankAt() != null
        && cash.total() != null
        && cash.available() != null
        && cash.blocked() != null
        && cash.total().compareTo(cash.available().add(cash.blocked())) == 0;
  }

  private boolean saveCash(AccountRead.Cash incoming, Instant at) {
    var previous =
        store.documents(account, "CASH").stream()
            .filter(d -> d.id().equals(incoming.currency()))
            .findFirst()
            .orElse(null);
    var old = previous == null ? null : store.convert(previous.data(), AccountRead.Cash.class);
    boolean coherent = coherentCash(incoming);
    if (old != null
        && old.bankAt() != null
        && incoming.bankAt() != null
        && (incoming.bankAt().isBefore(old.bankAt())
            || incoming.bankAt().equals(old.bankAt()) && coherentCash(old))) return false;
    var pending =
        store.documents(account, "CASH_PARTIAL").stream()
            .filter(d -> d.id().equals(incoming.currency()) && d.current())
            .findFirst()
            .orElse(null);
    if (incoming.bankAt() != null && !coherent) {
      if (pending != null) {
        var seen = store.convert(pending.data(), AccountRead.Cash.class);
        if (seen.bankAt() != null && !incoming.bankAt().isAfter(seen.bankAt())) return false;
      }
      store.save(account, "CASH_PARTIAL", incoming.currency(), incoming, at, true);
      if (old == null || !coherentCash(old))
        store.save(account, "CASH", incoming.currency(), incoming, at, true);
      return false;
    }
    var value = incoming;
    if (old != null && incoming.bankAt() == null && old.bankAt() != null) {
      value =
          new AccountRead.Cash(
              old.currency(),
              old.total(),
              old.available(),
              old.blocked(),
              incoming.reportedMoney(),
              old.bankAt(),
              old.source(),
              old.limitation());
      at = previous.observedAt();
    }
    store.save(account, "CASH", incoming.currency(), value, at, true);
    if (coherent && pending != null) {
      var seen = store.convert(pending.data(), AccountRead.Cash.class);
      if (seen.bankAt() == null || !seen.bankAt().isAfter(incoming.bankAt()))
        store.hide(account, "CASH_PARTIAL", incoming.currency());
    }
    return coherent;
  }

  private void cashFreshness(AccountRead.Snapshot<?> snapshot, boolean confirmedUpdate) {
    var at = snapshot.receivedAt() == null ? clock.instant() : snapshot.receivedAt();
    store.freshness(
        account,
        "cash",
        snapshot.items().isEmpty() && !snapshot.complete() ? "ERROR" : "PARTIAL",
        at,
        confirmedUpdate ? at : null,
        null,
        null,
        false,
        String.join("; ", snapshot.limitations())
            + (confirmedUpdate
                ? "; confirmed currency update, inventory partial"
                : "; no new coherent currency tuple confirmed; retained values require qualification"));
  }

  public Page page(String section, AccountQuery query) {
    resolveAccount();
    String kind =
        switch (section) {
          case "assets" -> "ASSET";
          case "orders" -> "ORDER";
          case "operations" -> "OPERATION";
          default -> throw new IllegalArgumentException("Unknown section");
        };
    return transaction(
        _ -> {
          store.lock(account);
          project(false);
          var result = store.page(account, kind, query);
          return new Page(
              result.items(),
              result.total(),
              query.page(),
              query.size(),
              query.sort(),
              query.direction(),
              store.freshness(account, clock.instant()),
              kind.equals("ASSET") ? cashViews() : List.of());
        });
  }

  public Map<String, Object> exact(String kind, String id) {
    resolveAccount();
    return transaction(
        _ -> {
          store.lock(account);
          project(false);
          return store.exact(account, kind, id);
        });
  }

  public Map<String, Object> journal(long id) {
    return db
        .query(
            "select * from journal_entry where id=?",
            (rs, n) ->
                fields(
                    "id",
                    rs.getLong("id"),
                    "occurredAt",
                    rs.getTimestamp("occurred_at").toInstant(),
                    "category",
                    rs.getString("category"),
                    "instrument",
                    rs.getString("instrument"),
                    "detail",
                    rs.getString("detail")),
            id)
        .stream()
        .findFirst()
        .orElseThrow(NoSuchElementException::new);
  }

  private List<Map<String, Object>> cashViews() {
    var partials = new HashMap<String, AccountReadStore.Document>();
    for (var partial : store.documents(account, "CASH_PARTIAL"))
      if (partial.current()) partials.put(partial.id(), partial);
    return store.documents(account, "CASH").stream()
        .filter(AccountReadStore.Document::current)
        .map(
            d -> {
              var row = new LinkedHashMap<>(d.data());
              for (String key : List.of("total", "available", "blocked", "reportedMoney"))
                row.put(key, str(row.get(key)));
              row.put("updatedAt", d.observedAt());
              var bankAt = row.get("bankAt");
              var partial = partials.get(d.id());
              if (partial != null) {
                var p = partial.data();
                row.put(
                    "limitation",
                    Objects.toString(row.get("limitation"), "")
                        + "; Partial currency observation at "
                        + p.get("bankAt")
                        + ": available="
                        + str(p.get("available"))
                        + ", blocked="
                        + str(p.get("blocked"))
                        + "; total unconfirmed; last coherent tuple and its times retained");
              }
              row.put(
                  "freshness",
                  partial != null
                          || !coherentCash(store.convert(d.data(), AccountRead.Cash.class))
                          || bankAt == null
                      ? "UNCONFIRMED"
                      : Instant.parse(bankAt.toString()).plusSeconds(30).isBefore(clock.instant())
                              || d.observedAt().plusSeconds(30).isBefore(clock.instant())
                          ? "STALE"
                          : "FRESH");
              return (Map<String, Object>) row;
            })
        .toList();
  }

  private void project(boolean forced) {
    String revision =
        db.queryForObject(
            """
   select md5(concat(
    (select coalesce(jsonb_agg(to_jsonb(t) order by id)::text,'') from copy_intent t),
    (select coalesce(jsonb_agg(to_jsonb(t) order by id)::text,'') from source_execution t),
    (select coalesce(jsonb_agg(to_jsonb(t) order by instrument_id)::text,'') from instrument_state t),
    (select coalesce(jsonb_agg(to_jsonb(t) order by instrument_id)::text,'') from mirror_position t),
    (select coalesce(jsonb_agg(to_jsonb(t) order by id)::text,'') from broker_fill t),
    (select coalesce(jsonb_agg(to_jsonb(t) order by id)::text,'') from mirror_trade_receipt t),
    (select coalesce(max(id)::text,'') from journal_entry)))
   """,
            String.class);
    var previous =
        db.query(
            "select local_revision from account_read_revision where account_id=?",
            (rs, n) -> rs.getString(1),
            account);
    if (!forced && !previous.isEmpty() && previous.getFirst().equals(revision)) return;
    var builder = new AccountProjection(store, db, account, clock);
    builder.rebuild();
    db.update(
        "insert into account_read_revision values(?,?) on conflict(account_id) do update set local_revision=excluded.local_revision",
        account,
        revision);
  }

  private <T> T transaction(
      java.util.function.Function<org.springframework.transaction.TransactionStatus, T> action) {
    for (int attempt = 0; ; attempt++)
      try {
        return tx.execute(action::apply);
      } catch (org.springframework.dao.ConcurrencyFailureException concurrent) {
        if (attempt >= 2) throw concurrent;
      }
  }

  private void transactionVoid(
      java.util.function.Consumer<org.springframework.transaction.TransactionStatus> action) {
    transaction(
        status -> {
          action.accept(status);
          return null;
        });
  }

  private void resolveAccount() {
    if (configuredAccount) return;
    var saved =
        db.query(
            "select distinct account_id from (select account_id from account_read_document union select account_id from account_read_freshness) a where account_id<>'unconfigured' order by account_id",
            (rs, n) -> rs.getString(1));
    if (saved.size() > 1)
      throw new IllegalStateException(
          "Several persisted read accounts; configure the expected mirror account");
    account = saved.isEmpty() ? "unconfigured" : saved.getFirst();
  }

  public static LinkedHashMap<String, Object> fields(Object... pairs) {
    var result = new LinkedHashMap<String, Object>();
    for (int i = 0; i < pairs.length; i += 2) result.put((String) pairs[i], pairs[i + 1]);
    return result;
  }

  static String str(Object value) {
    if (value == null) return null;
    return new BigDecimal(value.toString()).stripTrailingZeros().toPlainString();
  }

  static String encoded(String prefix, String... parts) {
    var text = new StringBuilder();
    for (String part : parts) text.append(part.length()).append(':').append(part);
    return prefix
        + ":"
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(text.toString().getBytes(StandardCharsets.UTF_8));
  }
}
