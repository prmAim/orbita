package ru.orbita.notifications;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class NotificationService implements NotificationPublisher {
  private static final DateTimeFormatter MOSCOW =
      DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss 'МСК'").withZone(ZoneId.of("Europe/Moscow"));
  private static final Duration[] INTERVALS = {
    Duration.ofMinutes(1),
    Duration.ofMinutes(2),
    Duration.ofMinutes(15),
    Duration.ofHours(1),
    Duration.ofHours(24)
  };
  private static final String EVENT =
      "select n.*,j.detail as owner_text from notification n join journal_entry j on j.id=n.journal_id ";
  private final JdbcTemplate db;
  private final TransactionTemplate transactions;
  private final TelegramTransport transport;
  private final Clock clock;
  private final ru.orbita.runtime.ApplicationReadiness readiness;

  public NotificationService(
      JdbcTemplate db,
      TransactionTemplate transactions,
      TelegramTransport transport,
      Clock clock,
      ru.orbita.runtime.ApplicationReadiness readiness) {
    this.db = db;
    this.transactions = transactions;
    this.transport = transport;
    this.clock = clock;
    this.readiness = readiness;
  }

  @Override
  @Transactional(propagation = Propagation.MANDATORY)
  public UUID publish(String category, String instrument, Instant occurredAt, String ownerText) {
    if (ownerText == null || ownerText.isBlank() || ownerText.length() > 3000)
      throw new IllegalArgumentException(
          "Текст уведомления должен содержать от 1 до 3000 символов");
    UUID id = UUID.randomUUID();
    String text =
        ownerText + "\n" + MOSCOW.format(occurredAt) + "\nИдентификатор уведомления: " + id;
    Long journal =
        db.queryForObject(
            "insert into journal_entry(occurred_at,category,instrument,detail) values(?,?,?,?) returning id",
            Long.class,
            Timestamp.from(occurredAt),
            "NOTIFICATION_" + category,
            instrument,
            text);
    db.update(
        "insert into notification(id,category,instrument,occurred_at,journal_id,status,next_attempt_at) values(?,?,?,?,?,'PENDING',?)",
        id,
        category,
        Objects.requireNonNullElse(instrument, ""),
        Timestamp.from(occurredAt),
        journal,
        Timestamp.from(clock.instant()));
    return id;
  }

  public void deliverDue() {
    if (!readiness.initialized()) return;
    Claim recovered = recoverInterrupted(null);
    if (recovered != null) {
      deliver(recovered);
      return;
    }
    Claim claim = transactions.execute(status -> claim(null, "AUTOMATIC"));
    if (claim != null) deliver(claim);
  }

  public View retryNow(UUID id) {
    if (!readiness.initialized()) return get(id);
    Claim recovered = recoverInterrupted(id);
    if (recovered != null) {
      deliver(recovered);
      return get(id);
    }
    Claim claim = transactions.execute(status -> claim(id, "MANUAL"));
    if (claim != null) {
      Claim overdue = deliver(claim);
      if (overdue != null) deliver(overdue);
    }
    return get(id);
  }

  public static String formatTime(Instant at) {
    return MOSCOW.format(at);
  }

  /** Adds a pause end to its undelivered error message while keeping the outbox schedule. */
  @Transactional
  public boolean annotateUndelivered(UUID id, Instant at, String ownerText) {
    if (id == null) throw new IllegalArgumentException("Notification ID required");
    if (at == null) throw new IllegalArgumentException("Annotation time required");
    if (ownerText == null || ownerText.isBlank() || ownerText.length() > 1000)
      throw new IllegalArgumentException("Annotation must contain 1 to 1000 characters");

    var rows = db.query(EVENT + "where n.id=? for update of n", (rs, n) -> view(rs), id);
    if (rows.isEmpty()) return false;
    var event = rows.getFirst();
    if (event.resolvedAt() != null
        || !(event.status().equals("PENDING") || event.status().equals("SENDING"))) return false;
    String marker = "\nИдентификатор уведомления:";
    String updated = event.text();
    if (!updated.contains(ownerText)) {
      if (!updated.contains(marker)) return false;
      updated = updated.replace(marker, "\n" + ownerText + marker);
      if (db.update(
              "update journal_entry set detail=? where id=(select journal_id from notification where id=?)",
              updated,
              event.id())
          != 1) return false;
    }
    return db.update(
            "update notification set resolved_at=?,resolution_text=?,retry_annotated_notification_on_completion=true where id=?",
            Timestamp.from(at),
            ownerText,
            id)
        == 1;
  }

  private Claim claim(UUID requested, String kind) {
    Instant now = clock.instant();
    String scope;
    Object[] parameters;
    if (requested == null) {
      scope = "n.status='PENDING' and n.next_attempt_at<=?";
      parameters = new Object[] {Timestamp.from(now)};
    } else if (kind.equals("AUTOMATIC")) {
      scope = "n.id=? and n.status='PENDING' and n.next_attempt_at<=?";
      parameters = new Object[] {requested, Timestamp.from(now)};
    } else {
      scope = "n.id=?";
      parameters = new Object[] {requested};
    }
    var rows =
        db.query(
            EVENT
                + "where "
                + scope
                + " order by n.occurred_at,n.id limit 1 for update of n skip locked",
            (rs, n) -> view(rs),
            parameters);
    if (rows.isEmpty()) return null;
    var event = rows.getFirst();
    if (!event.status().equals("PENDING")
        || (event.telegramBlockUntil() != null && event.telegramBlockUntil().isAfter(now)))
      return null;
    UUID attempt = UUID.randomUUID();
    db.update(
        "update notification set status='SENDING',claim_id=?,claim_started_at=?,claim_kind=?,version=version+1 where id=?",
        attempt,
        Timestamp.from(now),
        kind,
        event.id());
    db.update(
        "insert into notification_attempt(id,notification_id,kind,started_at,payload) values(?,?,?,?,?)",
        attempt,
        event.id(),
        kind,
        Timestamp.from(now),
        event.text());
    return new Claim(event.id(), attempt, kind, event.text());
  }

  private Claim deliver(Claim claim) {
    TelegramTransport.Result result;
    try {
      result = transport.send(claim.text());
    } catch (RuntimeException exception) {
      result = TelegramTransport.Result.noResponse();
    }
    return complete(claim, result);
  }

  private Claim complete(Claim claim, TelegramTransport.Result result) {
    return transactions.execute(
        status -> {
          var rows =
              db.query(
                  EVENT + "where n.id=? and n.claim_id=? and n.status='SENDING' for update of n",
                  (rs, n) -> view(rs),
                  claim.id(),
                  claim.attempt());
          if (rows.isEmpty()) return null;
          View event = rows.getFirst();
          Instant now = clock.instant();
          String currentText =
              db.queryForObject(
                  "select detail from journal_entry where id=(select journal_id from notification where id=?)",
                  String.class,
                  claim.id());
          int failures = event.automaticFailures();
          boolean automatic = claim.kind().equals("AUTOMATIC");
          if (automatic && result.outcome() == TelegramTransport.Outcome.FAILURE) failures++;
          Instant next = event.nextAttemptAt();
          if (automatic) next = now.plus(interval(failures));
          Instant blocked = event.telegramBlockUntil();
          if (!result.retryAfter().isZero()) {
            blocked = now.plus(result.retryAfter());
            if (next == null || blocked.isAfter(next)) next = blocked;
          }
          boolean success = result.outcome() == TelegramTransport.Outcome.SUCCESS;
          boolean retryAnnotated =
              success
                  && event.resolvedAt() != null
                  && !claim.text().equals(currentText)
                  && event.retryAnnotatedNotificationOnCompletion();
          if (success
              && event.resolvedAt() != null
              && !claim.text().equals(currentText)
              && !retryAnnotated) {
            // The original payload actually arrived; preserve it in the journal and emit the
            // resolution once when it was not already published separately.
            db.update(
                "update journal_entry set detail=? where id=(select journal_id from notification where id=?)",
                claim.text(),
                claim.id());
            publish(
                event.resolutionCategory(),
                event.instrument(),
                event.resolvedAt(),
                Objects.requireNonNullElse(
                    event.resolutionText(),
                    event.instrument() + ": сверка завершена, временная блокировка снята."));
          }
          db.update(
              "update notification_attempt set completed_at=?,result=?,detail=? where id=?",
              Timestamp.from(now),
              result.outcome().name(),
              result.detail(),
              claim.attempt());
          db.update(
              "update notification set status=?,automatic_failures=?,next_attempt_at=?,telegram_block_until=?,claim_id=null,claim_started_at=null,claim_kind=null,retry_annotated_notification_on_completion=false,last_result=?,version=version+1 where id=?",
              success && !retryAnnotated ? "DELIVERED" : "PENDING",
              failures,
              retryAnnotated ? Timestamp.from(now) : success ? null : timestamp(next),
              timestamp(blocked),
              result.detail(),
              claim.id());
          return automatic ? null : claim(claim.id(), "AUTOMATIC");
        });
  }

  /** A process can die after Telegram received a message. Retain the ID and record uncertainty. */
  private Claim recoverInterrupted(UUID requested) {
    return transactions.execute(
        status -> {
          Instant now = clock.instant();
          Timestamp cutoff = Timestamp.from(now.minusSeconds(10));
          String requestedScope = requested == null ? "" : " and id=?";
          Object[] selectedParameters =
              requested == null
                  ? new Object[] {Timestamp.from(now), cutoff}
                  : new Object[] {Timestamp.from(now), cutoff, requested};
          var overdueManual =
              db.query(
                  "select id,claim_id,claim_kind,automatic_failures,next_attempt_at from notification where status='SENDING' and claim_kind='MANUAL' and next_attempt_at<=? and claim_started_at<=?"
                      + requestedScope
                      + " order by occurred_at,id limit 1 for update",
                  (rs, n) ->
                      new Stale(
                          rs.getObject(1, UUID.class),
                          rs.getObject(2, UUID.class),
                          rs.getString(3),
                          rs.getInt(4),
                          instant(rs, 5)),
                  selectedParameters);
          if (!overdueManual.isEmpty()) {
            Stale row = overdueManual.getFirst();
            recover(row, now);
            return claim(row.id(), "AUTOMATIC");
          }

          Object[] staleParameters =
              requested == null
                  ? new Object[] {cutoff, Timestamp.from(now)}
                  : new Object[] {cutoff, Timestamp.from(now), requested};
          var stale =
              db.query(
                  "select id,claim_id,claim_kind,automatic_failures,next_attempt_at from notification where status='SENDING' and claim_started_at<=?"
                      + " and (claim_kind is distinct from 'MANUAL' or next_attempt_at is null or next_attempt_at>?)"
                      + requestedScope
                      + " order by occurred_at,id for update skip locked",
                  (rs, n) ->
                      new Stale(
                          rs.getObject(1, UUID.class),
                          rs.getObject(2, UUID.class),
                          rs.getString(3),
                          rs.getInt(4),
                          instant(rs, 5)),
                  staleParameters);
          for (var row : stale) {
            recover(row, now);
          }
          return null;
        });
  }

  private void recover(Stale row, Instant now) {
    Instant next = row.kind().equals("MANUAL") ? row.next() : now.plus(interval(row.failures()));
    db.update(
        "update notification_attempt set completed_at=?,result='NO_RESPONSE',detail='результат не дождался ответа' where id=? and completed_at is null",
        Timestamp.from(now),
        row.attempt());
    db.update(
        "update notification set status='PENDING',next_attempt_at=?,claim_id=null,claim_started_at=null,claim_kind=null,last_result='результат не дождался ответа',version=version+1 where id=?",
        timestamp(next),
        row.id());
  }

  /** Resolve exactly one block. Undelivered notification gets both facts under its original ID. */
  @Transactional
  public void resolve(UUID id, Instant at, String ownerText) {
    resolve(id, at, ownerText, "RESOLVED");
  }

  @Transactional
  public void resolve(UUID id, Instant at, String ownerText, String resolutionCategory) {
    if (resolutionCategory == null || resolutionCategory.isBlank())
      throw new IllegalArgumentException("Resolution category required");
    var event =
        db.query(EVENT + "where n.id=? for update of n", (rs, n) -> view(rs), id).stream()
            .findFirst()
            .orElseThrow();
    if (event.resolvedAt() != null || event.status().equals("IGNORED")) return;
    String text = ownerText + "\n" + MOSCOW.format(at);
    if (event.status().equals("DELIVERED"))
      publish(resolutionCategory, event.instrument(), at, ownerText);
    else
      db.update(
          "update journal_entry set detail=? where id=(select journal_id from notification where id=?)",
          event
              .text()
              .replace(
                  "\nИдентификатор уведомления:", "\n" + text + "\nИдентификатор уведомления:"),
          id);
    db.update(
        "update notification set resolved_at=?,resolution_text=?,resolution_category=? where id=?",
        Timestamp.from(at),
        ownerText,
        resolutionCategory,
        id);
  }

  @Transactional
  public int ignoreRestoredQueue() {
    db.update(
        "update notification_attempt set completed_at=?,result='RESTORED_COPY',detail='Очередь восстановленной копии не отправляется' where completed_at is null",
        Timestamp.from(clock.instant()));
    return db.update(
        "update notification set status='IGNORED',claim_id=null,claim_started_at=null,claim_kind=null,next_attempt_at=null where status in ('PENDING','SENDING')");
  }

  public View get(UUID id) {
    return db.query(EVENT + "where n.id=?", (rs, n) -> view(rs), id).stream()
        .findFirst()
        .orElseThrow();
  }

  public List<View> list(int limit) {
    return db.query(
        EVENT + "order by n.occurred_at desc,n.id desc limit ?",
        (rs, n) -> view(rs),
        Math.clamp(limit, 1, 200));
  }

  public List<Attempt> attempts(UUID id) {
    return db.query(
        "select id,kind,started_at,completed_at,result,detail,payload from notification_attempt where notification_id=? order by started_at,id",
        (rs, n) ->
            new Attempt(
                rs.getObject(1, UUID.class),
                rs.getString(2),
                instant(rs, 3),
                instant(rs, 4),
                rs.getString(5),
                rs.getString(6),
                rs.getString(7)),
        id);
  }

  private static Duration interval(int failures) {
    return INTERVALS[Math.clamp(failures - 1, 0, INTERVALS.length - 1)];
  }

  private static Timestamp timestamp(Instant instant) {
    return instant == null ? null : Timestamp.from(instant);
  }

  private static Instant instant(ResultSet rs, int column) throws SQLException {
    var value = rs.getTimestamp(column);
    return value == null ? null : value.toInstant();
  }

  private static View view(ResultSet rs) throws SQLException {
    return new View(
        rs.getObject("id", UUID.class),
        rs.getString("category"),
        rs.getString("instrument"),
        rs.getString("owner_text"),
        rs.getString("status"),
        rs.getInt("automatic_failures"),
        instant(rs, rs.findColumn("next_attempt_at")),
        instant(rs, rs.findColumn("telegram_block_until")),
        rs.getString("last_result"),
        instant(rs, rs.findColumn("resolved_at")),
        rs.getString("resolution_text"),
        rs.getString("resolution_category"),
        rs.getBoolean("retry_annotated_notification_on_completion"));
  }

  private record Claim(UUID id, UUID attempt, String kind, String text) {}

  private record Stale(UUID id, UUID attempt, String kind, int failures, Instant next) {}

  public record View(
      UUID id,
      String category,
      String instrument,
      String text,
      String status,
      int automaticFailures,
      Instant nextAttemptAt,
      Instant telegramBlockUntil,
      String lastResult,
      Instant resolvedAt,
      String resolutionText,
      String resolutionCategory,
      boolean retryAnnotatedNotificationOnCompletion) {}

  public record Attempt(
      UUID id,
      String kind,
      Instant startedAt,
      Instant completedAt,
      String result,
      String detail,
      String payload) {}
}
