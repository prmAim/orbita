package ru.orbita.broker;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.control.OrbitaProperties;
import ru.orbita.notifications.NotificationPublisher;
import ru.orbita.notifications.NotificationService;

/** Durable connectivity transitions; observation/qualification/trading gates remain independent. */
@Service
public final class BrokerConnectivityService implements BrokerConnectivityEvents {
  private final JdbcTemplate db;
  private final TransactionTemplate transactions;
  private final NotificationPublisher publisher;
  private final NotificationService notifications;
  private final Clock clock;
  private final boolean restoredProcess;
  private final UUID session = UUID.randomUUID();

  public BrokerConnectivityService(
      JdbcTemplate db,
      TransactionTemplate transactions,
      NotificationPublisher publisher,
      NotificationService notifications,
      Clock clock,
      OrbitaProperties properties) {
    this.db = db;
    this.transactions = transactions;
    this.publisher = publisher;
    this.notifications = notifications;
    this.clock = clock;
    this.restoredProcess = properties.restored();
  }

  public record Status(
      boolean disconnected, Instant lostAt, UUID lossNotificationId, Instant lastRecoveredAt) {}

  private record State(UUID notice, Instant lostAt, Instant recoveredAt, UUID restoreSession) {}

  @Override
  public void connectionLost(Cause cause) {
    if (cause == null) throw new IllegalArgumentException("Typed connection-loss cause required");
    durable(
        "connection loss",
        () -> {
          var state = current();
          if (state.notice() != null) return null;
          Instant at = clock.instant();
          UUID id =
              publisher.publish(
                  "BROKER_CONNECTION_LOST",
                  null,
                  at,
                  "Потеря связи с брокером. Причина: " + cause.ownerText() + ".");
          db.update(
              "update broker_connectivity set loss_notification_id=?,lost_at=? where singleton",
              id,
              Timestamp.from(at));
          return null;
        });
  }

  @Override
  public void readSucceeded() {
    durable(
        "successful broker read",
        () -> {
          var state = current();
          if (state.notice() == null) return null;
          Instant at = clock.instant();
          // Resolve locks the outbox row and decides pending/in-flight/delivered atomically.
          String mode =
              db.queryForObject("select mode from control_state where id=1", String.class);
          notifications.resolve(
              state.notice(),
              at,
              "Связь с брокером восстановлена. Текущий режим: "
                  + modeText(mode)
                  + " ("
                  + mode
                  + ").",
              "BROKER_CONNECTION_RESTORED");
          db.update(
              "update broker_connectivity set loss_notification_id=null,lost_at=null,last_recovered_at=? where singleton",
              Timestamp.from(at));
          return null;
        });
  }

  @Override
  public void restoredCopyOpened() {
    durable(
        "restored connectivity state",
        () -> {
          // Test/maintenance truncation and a newly migrated restored copy may not retain the
          // singleton.
          db.update(
              "insert into broker_connectivity(singleton) values(true) on conflict do nothing");
          resetRestored(lock());
          return null;
        });
  }

  public Status status() {
    return durable(
        "connectivity status",
        () -> {
          var state = current();
          return new Status(
              state.notice() != null, state.lostAt(), state.notice(), state.recoveredAt());
        });
  }

  private State current() {
    var state = lock();
    // Scheduled read callbacks can precede ApplicationReady/control restoration on startup.
    if (restoredProcess && !session.equals(state.restoreSession())) return resetRestored(state);
    return state;
  }

  private State resetRestored(State state) {
    notifications.ignoreRestoredQueue();
    if (state.notice() != null)
      db.update(
          "insert into journal_entry(occurred_at,category,detail) values(?,'BROKER_CONNECTIVITY_RESTORE_RESET',?)",
          Timestamp.from(clock.instant()),
          "Сведения о потере связи из восстановленной копии не переотправляются. Идентификатор: "
              + state.notice());
    db.update(
        "update broker_connectivity set loss_notification_id=null,lost_at=null,last_recovered_at=null,restore_session=? where singleton",
        session);
    return new State(null, null, null, session);
  }

  private State lock() {
    return db.queryForObject(
        "select loss_notification_id,lost_at,last_recovered_at,restore_session from broker_connectivity where singleton for update",
        (r, n) ->
            new State(
                r.getObject(1, UUID.class),
                instant(r.getTimestamp(2)),
                instant(r.getTimestamp(3)),
                r.getObject(4, UUID.class)));
  }

  private <T> T durable(String operation, Supplier<T> work) {
    try {
      return transactions.execute(_ -> work.get());
    } catch (BrokerPersistenceFailure failure) {
      throw failure;
    } catch (RuntimeException failure) {
      throw new BrokerPersistenceFailure(operation, failure);
    }
  }

  private static Instant instant(Timestamp value) {
    return value == null ? null : value.toInstant();
  }

  private static String modeText(String mode) {
    return switch (mode) {
      case "NOT_STARTED" -> "Не запущено";
      case "SHADOW" -> "Теневой режим";
      case "AUTOMATIC" -> "Автоматическая отправка";
      case "STOPPING" -> "Останавливается";
      case "STOPPED" -> "Остановлено";
      case "RESTORED" -> "Восстановленная копия";
      default -> "Неизвестный режим";
    };
  }
}
