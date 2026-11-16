package ru.orbita.account;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

@Repository
class AccountReadStore {
  private final JdbcTemplate db;
  private final NamedParameterJdbcTemplate named;
  private final JsonMapper json;

  AccountReadStore(JdbcTemplate db, JsonMapper json) {
    this.db = db;
    this.named = new NamedParameterJdbcTemplate(db);
    this.json =
        json.rebuild()
            .enable(tools.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();
  }

  record Document(String id, Map<String, Object> data, Instant observedAt, boolean current) {}

  void lock(String account) {
    db.queryForObject(
        "select pg_advisory_xact_lock(hashtextextended(?,0))",
        Object.class,
        "orbita-account-read:" + account);
  }

  List<Document> documents(String account, String kind) {
    return db.query(
        "select id,payload::text,observed_at,current from account_read_document where account_id=? and kind=? order by id",
        (rs, n) ->
            new Document(
                rs.getString(1),
                decode(rs.getString(2)),
                rs.getTimestamp(3).toInstant(),
                rs.getBoolean(4)),
        account,
        kind);
  }

  Map<String, Object> decode(String value) {
    return json.readValue(value, new TypeReference<LinkedHashMap<String, Object>>() {});
  }

  <T> T convert(Map<String, Object> data, Class<T> type) {
    return json.convertValue(data, type);
  }

  Map<String, Object> map(Object value) {
    return json.convertValue(value, new TypeReference<LinkedHashMap<String, Object>>() {});
  }

  void hide(String account, String kind) {
    db.update(
        "update account_read_document set current=false where account_id=? and kind=? and current",
        account,
        kind);
  }

  void hide(String account, String kind, String id) {
    db.update(
        "update account_read_document set current=false where account_id=? and kind=? and id=? and current",
        account,
        kind,
        id);
  }

  void save(
      String account, String kind, String id, Object value, Instant observed, boolean current) {
    var data = map(value);
    Map<String, Object> price =
        data.get("executionPrice") instanceof Map<?, ?> p ? (Map<String, Object>) p : Map.of();
    Object time =
        data.getOrDefault("time", data.getOrDefault("createdAt", data.get("lastExecutionAt")));
    var args =
        new org.springframework.jdbc.core.namedparam.MapSqlParameterSource()
            .addValue("account", account)
            .addValue("kind", kind)
            .addValue("id", id)
            .addValue("payload", json.writeValueAsString(value))
            .addValue("instrument", data.get("instrumentId"))
            .addValue("ticker", data.get("ticker"))
            .addValue("name", data.get("name"))
            .addValue("side", data.get("side"))
            .addValue("status", data.getOrDefault("status", data.get("result")))
            .addValue("origin", data.get("origin"))
            .addValue("state", data.get("state"))
            .addValue(
                "attention",
                Boolean.TRUE.equals(data.get("attention"))
                    || "PAUSED".equals(data.get("state"))
                    || "WAITING".equals(data.get("state")))
            .addValue("time", time == null ? null : Timestamp.from(Instant.parse(time.toString())))
            .addValue(
                "lots",
                decimal(
                    data.getOrDefault(
                        "lots", data.getOrDefault("brokerLots", data.get("targetLots")))))
            .addValue("executed", decimal(data.get("executedLots")))
            .addValue("price", decimal(price.get("value")))
            .addValue("current", current)
            .addValue("at", Timestamp.from(observed));
    named.update(
        """
   insert into account_read_document(account_id,kind,id,payload,instrument_id,ticker,name,side,status,origin,state,attention,event_at,lots,executed_lots,execution_price,current,observed_at)
   values(:account,:kind,:id,cast(:payload as jsonb),:instrument,:ticker,:name,:side,:status,:origin,:state,:attention,:time,:lots,:executed,:price,:current,:at)
   on conflict(account_id,kind,id) do update set payload=excluded.payload,instrument_id=excluded.instrument_id,ticker=excluded.ticker,name=excluded.name,side=excluded.side,status=excluded.status,origin=excluded.origin,state=excluded.state,attention=excluded.attention,event_at=excluded.event_at,lots=excluded.lots,executed_lots=excluded.executed_lots,execution_price=excluded.execution_price,current=excluded.current,observed_at=excluded.observed_at
   where account_read_document.payload is distinct from excluded.payload or account_read_document.current is distinct from excluded.current or account_read_document.observed_at is distinct from excluded.observed_at
   """,
        args);
  }

  private static BigDecimal decimal(Object value) {
    return value == null ? null : new BigDecimal(value.toString());
  }

  void freshness(
      String account,
      String section,
      String status,
      Instant attempt,
      Instant success,
      Instant from,
      Instant through,
      boolean complete,
      String detail) {
    db.update(
        """
  insert into account_read_freshness values(?,?,?,?,?,?,?,?,?) on conflict(account_id,section) do update set status=excluded.status,last_attempt_at=excluded.last_attempt_at,last_success_at=coalesce(excluded.last_success_at,account_read_freshness.last_success_at),from_inclusive=excluded.from_inclusive,through_inclusive=excluded.through_inclusive,complete=excluded.complete,detail=excluded.detail
  """,
        account,
        section,
        status,
        ts(attempt),
        ts(success),
        ts(from),
        ts(through),
        complete,
        detail);
  }

  Map<String, Object> freshness(String account, Instant now) {
    var out = new LinkedHashMap<String, Object>();
    for (String section : List.of("assets", "orders", "history", "cash"))
      out.put(
          section,
          AccountReadService.fields(
              "status",
              "NEVER",
              "lastAttemptAt",
              null,
              "lastSuccessAt",
              null,
              "from",
              null,
              "through",
              null,
              "complete",
              false,
              "detail",
              "No broker observation"));
    db.query(
        "select * from account_read_freshness where account_id=?",
        rs -> {
          Instant attempted = instant(rs.getTimestamp("last_attempt_at"));
          String status = rs.getString("status");
          Instant freshAt =
              rs.getString("section").equals("cash") && rs.getTimestamp("last_success_at") != null
                  ? instant(rs.getTimestamp("last_success_at"))
                  : attempted;
          if (freshAt != null && freshAt.plusSeconds(30).isBefore(now)) status = "STALE";
          out.put(
              rs.getString("section"),
              AccountReadService.fields(
                  "status",
                  status,
                  "lastAttemptAt",
                  attempted,
                  "lastSuccessAt",
                  instant(rs.getTimestamp("last_success_at")),
                  "from",
                  instant(rs.getTimestamp("from_inclusive")),
                  "through",
                  instant(rs.getTimestamp("through_inclusive")),
                  "complete",
                  rs.getBoolean("complete"),
                  "detail",
                  rs.getString("detail")));
        },
        account);
    var oldest =
        db.queryForObject(
            "select min(least(observed_at,(payload->>'bankAt')::timestamptz)) from account_read_document where account_id=? and kind='CASH' and payload->>'bankAt' is not null",
            Timestamp.class,
            account);
    if (oldest != null && oldest.toInstant().plusSeconds(30).isBefore(now))
      ((Map<String, Object>) out.get("cash")).put("status", "STALE");
    return out;
  }

  record Result(List<Map<String, Object>> items, long total) {}

  Result page(String account, String kind, AccountQuery q) {
    var params = new HashMap<String, Object>();
    params.put("account", account);
    params.put("kind", kind);
    StringBuilder where = new StringBuilder("account_id=:account and kind=:kind and current");
    for (var filter :
        List.of(
            new String[] {"instrument_id", q.instrumentId()},
            new String[] {"side", q.side()},
            new String[] {"status", q.status()},
            new String[] {"origin", q.origin()},
            new String[] {"state", q.state()}))
      if (filter[1] != null) {
        where.append(" and ").append(filter[0]).append("=:").append(filter[0]);
        params.put(filter[0], filter[1]);
      }
    if (q.q() != null && !q.q().isBlank()) {
      where.append(
          " and (strpos(lower(coalesce(ticker,'')),lower(:search))>0 or strpos(lower(coalesce(name,'')),lower(:search))>0)");
      params.put("search", q.q());
    }
    if (q.attention() != null) {
      where.append(" and attention=:attention");
      params.put("attention", q.attention());
    }
    if (q.from() != null) {
      where.append(" and event_at>=:from");
      params.put("from", ts(q.from()));
    }
    if (q.to() != null) {
      where.append(" and event_at<=:to");
      params.put("to", ts(q.to()));
    }
    String sort =
        Map.of(
                "time",
                "event_at",
                "instrument",
                "coalesce(ticker,instrument_id)",
                "side",
                "side",
                "status",
                "status",
                "origin",
                "origin",
                "lots",
                "lots",
                "executedLots",
                "executed_lots",
                "executionPrice",
                "execution_price")
            .get(q.sort());
    long count =
        named.queryForObject(
            "select count(*) from account_read_document where " + where, params, Long.class);
    params.put("limit", q.size());
    params.put("offset", (long) q.page() * q.size());
    var rows =
        named.query(
            "select payload::text from account_read_document where "
                + where
                + " order by "
                + sort
                + " "
                + q.direction()
                + " nulls last,id asc limit :limit offset :offset",
            params,
            (rs, n) -> decode(rs.getString(1)));
    return new Result(rows, count);
  }

  Map<String, Object> exact(String account, String kind, String id) {
    if (id == null || id.isBlank() || id.length() > 8000)
      throw new IllegalArgumentException("Invalid exact ID");
    return db
        .query(
            "select payload::text from account_read_document where account_id=? and kind=? and id=?",
            (rs, n) -> decode(rs.getString(1)),
            account,
            kind,
            id)
        .stream()
        .findFirst()
        .orElseThrow(NoSuchElementException::new);
  }

  List<Map<String, Object>> relatedOperations(String account, String kind, String id) {
    Map<String, Object> proof =
        switch (kind) {
          case "ORDER" -> Map.of("orderIds", List.of(id));
          case "FILL" -> Map.of("fills", List.of(Map.of("id", id)));
          case "JOURNAL" -> Map.of("journalId", id);
          default -> throw new IllegalArgumentException("Unsupported exact relationship");
        };
    return db.query(
        "select payload::text from account_read_document where account_id=? and kind='OPERATION' and current and payload @> cast(? as jsonb) order by id",
        (rs, n) -> decode(rs.getString(1)),
        account,
        json.writeValueAsString(proof));
  }

  private static Timestamp ts(Instant value) {
    return value == null ? null : Timestamp.from(value);
  }

  private static Instant instant(Timestamp value) {
    return value == null ? null : value.toInstant();
  }
}
