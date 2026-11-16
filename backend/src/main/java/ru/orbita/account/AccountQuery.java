package ru.orbita.account;

import java.time.Instant;
import java.util.*;

/** Validated public query over the entire persisted projection, before pagination. */
public record AccountQuery(
    String q,
    String instrumentId,
    String side,
    String status,
    String origin,
    String state,
    Boolean attention,
    Instant from,
    Instant to,
    String sort,
    String direction,
    int page,
    int size) {
  public AccountQuery {
    if (q != null && q.length() > 200
        || instrumentId != null && instrumentId.length() > 2000
        || page < 0
        || page > 1_000_000
        || !Set.of(25, 50, 100).contains(size))
      throw new IllegalArgumentException("Invalid page/search");
    if (side != null && !Set.of("BUY", "SELL").contains(side)
        || origin != null && !Set.of("ORBITA", "MANUAL", "UNKNOWN").contains(origin)
        || state != null && !Set.of("PAUSED", "WAITING", "READY").contains(state))
      throw new IllegalArgumentException("Invalid filter");
    if (status != null
        && !Set.of(
                "NEW",
                "PARTIALLY_FILLED",
                "FILLED",
                "CANCELLED",
                "REJECTED",
                "UNKNOWN",
                "WAITING",
                "SEND_STARTED",
                "CANCEL_STARTED",
                "FINAL",
                "RETRIED",
                "SHADOW",
                "HALTED",
                "RECEIVED",
                "ACCEPTED",
                "SKIPPED",
                "MIXED",
                "UNLINKED_EVIDENCE")
            .contains(status)) throw new IllegalArgumentException("Invalid status");
    if (from != null && to != null && from.isAfter(to)
        || !Set.of(
                "time",
                "instrument",
                "side",
                "status",
                "origin",
                "lots",
                "executedLots",
                "executionPrice")
            .contains(sort)
        || !Set.of("asc", "desc").contains(direction))
      throw new IllegalArgumentException("Invalid sort/period");
  }

  public static AccountQuery defaults() {
    return new AccountQuery(
        null, null, null, null, null, null, null, null, null, "time", "desc", 0, 25);
  }

  public static AccountQuery parse(Map<String, String> p) {
    if (!Set.of(
            "q",
            "instrumentId",
            "side",
            "status",
            "origin",
            "state",
            "attention",
            "from",
            "to",
            "sort",
            "direction",
            "page",
            "size")
        .containsAll(p.keySet())) throw new IllegalArgumentException("Unknown parameter");
    String a = p.get("attention");
    if (a != null && !Set.of("true", "false").contains(a))
      throw new IllegalArgumentException("Invalid attention");
    try {
      return new AccountQuery(
          p.get("q"),
          p.get("instrumentId"),
          p.get("side"),
          p.get("status"),
          p.get("origin"),
          p.get("state"),
          a == null ? null : Boolean.valueOf(a),
          p.containsKey("from") ? Instant.parse(p.get("from")) : null,
          p.containsKey("to") ? Instant.parse(p.get("to")) : null,
          p.getOrDefault("sort", "time"),
          p.getOrDefault("direction", "desc"),
          Integer.parseInt(p.getOrDefault("page", "0")),
          Integer.parseInt(p.getOrDefault("size", "25")));
    } catch (java.time.format.DateTimeParseException | NumberFormatException invalid) {
      throw new IllegalArgumentException("Invalid query", invalid);
    }
  }
}
