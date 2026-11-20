package ru.orbita.operations;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.notifications.NotificationPublisher;
import ru.orbita.notifications.NotificationService;
import ru.orbita.notifications.TelegramTransport;

@SpringBootTest(properties = {"orbita.telegram.enabled=false", "orbita.worker.enabled=false"})
@Testcontainers
@Import({
  NotificationDeliveryIT.TransportConfiguration.class,
  NotificationDeliveryIT.TransactionConfiguration.class
})
class NotificationDeliveryIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine");

  @DynamicPropertySource
  static void database(DynamicPropertyRegistry properties) {
    properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    properties.add("spring.datasource.username", POSTGRES::getUsername);
    properties.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @Autowired NotificationPublisher publisher;
  @Autowired NotificationService notifications;
  @Autowired CommitAwareTransactionTemplate transactions;
  @Autowired Simulator telegram;
  @Autowired MutableClock clock;
  @Autowired JdbcTemplate db;

  @BeforeEach
  void resetFixture() {
    clock.now = Instant.parse("2026-11-13T09:00:00Z");
    notifications.ignoreRestoredQueue();
    telegram.results.clear();
    telegram.messages.clear();
    telegram.lastText = null;
    telegram.beforeSend = null;
    transactions.clearObserver();
  }

  @Test
  void committedEventIsDeliveredWithStableIdAndRecordedAttempt() {
    UUID id =
        transactions.execute(
            status ->
                publisher.publish(
                    "SKIP",
                    "SBER",
                    Instant.parse("2026-11-13T09:00:00Z"),
                    "Покупка SBER. Недостаточно рублей."));
    notifications.deliverDue();
    var event = notifications.get(id);
    assertThat(event.status()).isEqualTo("DELIVERED");
    assertThat(telegram.lastText)
        .isEqualTo(event.text())
        .contains("13.11.2026 12:00:00 МСК", id.toString());
    assertThat(notifications.attempts(id))
        .singleElement()
        .satisfies(attempt -> assertThat(attempt.result()).isEqualTo("SUCCESS"));
  }

  @Test
  void interruptedSendRecoveryPlansUsePartialIndexInsteadOfNotificationHistoryScan() {
    String instrument = "REVIEW_PLAN_" + UUID.randomUUID();
    Instant now = Instant.parse("2026-11-20T23:00:00Z");
    try {
      db.update(
          """
          with synthetic_journal as (
            insert into journal_entry(occurred_at,category,instrument,detail)
            select timestamptz '2000-01-01 00:00:00+00' + n * interval '1 second',
                   'NOTIFICATION_ERROR', ?, 'synthetic delivered history'
              from generate_series(1,50000) n
            returning id,occurred_at
          )
          insert into notification(id,category,instrument,occurred_at,journal_id,status,next_attempt_at)
          select gen_random_uuid(),'ERROR',?,occurred_at,id,'DELIVERED',null
            from synthetic_journal
          """,
          instrument,
          instrument);
      db.update(
          """
          update notification
             set status='SENDING',claim_id=gen_random_uuid(),claim_started_at=?,
                 claim_kind='MANUAL',next_attempt_at=?
           where id in (
             select id from notification where instrument=? order by occurred_at desc limit 4
           )
          """,
          Timestamp.from(now.minusSeconds(20)),
          Timestamp.from(now.plusSeconds(60)),
          instrument);
      int madeOverdue =
          db.update(
              "update notification set next_attempt_at=? where instrument=? and status='SENDING' and journal_id=(select min(journal_id) from notification where instrument=? and status='SENDING')",
              Timestamp.from(now.minusSeconds(1)),
              instrument,
              instrument);
      assertThat(madeOverdue).isEqualTo(1);
      assertThat(
              db.queryForObject(
                  "select count(*) from notification where instrument=? and status='SENDING' and claim_kind='MANUAL' and next_attempt_at<=? and claim_started_at<=?",
                  Integer.class,
                  instrument,
                  Timestamp.from(now),
                  Timestamp.from(now.minusSeconds(10))))
          .isEqualTo(1);
      assertThat(
              db.queryForObject(
                  "select count(*) from notification where instrument=? and status='SENDING' and claim_started_at<=? and (claim_kind is distinct from 'MANUAL' or next_attempt_at is null or next_attempt_at>?)",
                  Integer.class,
                  instrument,
                  Timestamp.from(now.minusSeconds(10)),
                  Timestamp.from(now)))
          .isEqualTo(3);
      db.execute("analyze notification");

      String manualDuePlan =
          String.join(
              "\n",
              db.queryForList(
                  """
                  explain (format text)
                  select id,claim_id,claim_kind,automatic_failures,next_attempt_at
                    from notification
                   where status='SENDING' and claim_kind='MANUAL'
                     and next_attempt_at<=timestamptz '2026-11-20 23:00:00+00'
                     and claim_started_at<=timestamptz '2026-11-20 22:59:50+00'
                   order by occurred_at,id limit 1 for update
                  """,
                  String.class));
      String otherStalePlan =
          String.join(
              "\n",
              db.queryForList(
                  """
                  explain (format text)
                  select id,claim_id,claim_kind,automatic_failures,next_attempt_at
                    from notification
                   where status='SENDING'
                     and claim_started_at<=timestamptz '2026-11-20 22:59:50+00'
                     and (claim_kind is distinct from 'MANUAL'
                          or next_attempt_at is null
                          or next_attempt_at>timestamptz '2026-11-20 23:00:00+00')
                   order by occurred_at,id for update skip locked
                  """,
                  String.class));

      assertThat(manualDuePlan).contains("notification_sending_recovery_idx");
      assertThat(otherStalePlan).contains("notification_sending_recovery_idx");
    } finally {
      db.update("delete from notification where instrument=?", instrument);
      db.update("delete from journal_entry where instrument=?", instrument);
    }
  }

  @Test
  void telegramRateLimitBlocksManualClicksAndPreservesStableId() {
    clock.now = Instant.parse("2026-11-13T09:00:00Z");
    telegram.results.add(
        TelegramTransport.Result.failure(
            "Telegram ограничил частоту отправки", Duration.ofSeconds(90)));
    UUID id =
        transactions.execute(
            status ->
                publisher.publish(
                    "SKIP", "GAZP", clock.instant(), "Продажа GAZP. Нет доступных лотов."));
    notifications.deliverDue();
    assertThat(notifications.get(id).nextAttemptAt())
        .isEqualTo(Instant.parse("2026-11-13T09:01:30Z"));
    assertThat(notifications.get(id).automaticFailures()).isEqualTo(1);
    int sends = telegram.messages.size();
    clock.now = clock.now.plusSeconds(30);
    notifications.retryNow(id);
    assertThat(telegram.messages).hasSize(sends);
    clock.now = Instant.parse("2026-11-13T09:01:30Z");
    notifications.deliverDue();
    assertThat(notifications.get(id).status()).isEqualTo("DELIVERED");
    assertThat(telegram.lastText).contains(id.toString());
    assertThat(notifications.attempts(id)).hasSize(2);
  }

  @Test
  void unansweredFirstAutomaticAttemptDoesNotAdvanceConfirmedFailureCounter() {
    clock.now = Instant.parse("2026-11-13T10:00:00Z");
    telegram.results.add(TelegramTransport.Result.noResponse());
    UUID id =
        transactions.execute(
            status ->
                publisher.publish(
                    "ERROR", "SBER", clock.instant(), "SBER: неизвестный результат заявки."));
    notifications.deliverDue();
    assertThat(notifications.get(id).automaticFailures()).isZero();
    assertThat(notifications.get(id).nextAttemptAt())
        .isEqualTo(Instant.parse("2026-11-13T10:01:00Z"));
    assertThat(notifications.attempts(id))
        .singleElement()
        .satisfies(
            attempt -> assertThat(attempt.detail()).isEqualTo("результат не дождался ответа"));
  }

  @Test
  void rolledBackEventNeverReachesOutboxOrTelegram() {
    int sends = telegram.messages.size();
    UUID[] id = new UUID[1];
    transactions.executeWithoutResult(
        status -> {
          id[0] = publisher.publish("ERROR", "SBER", clock.instant(), "Неуспешная транзакция");
          status.setRollbackOnly();
        });
    notifications.deliverDue();
    assertThat(notifications.list(200)).noneMatch(event -> event.id().equals(id[0]));
    assertThat(telegram.messages).hasSize(sends);
  }

  @Test
  void manualFailurePreservesAutomaticDeadlineAndFailureStep() {
    clock.now = Instant.parse("2026-11-13T11:00:00Z");
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    UUID id =
        transactions.execute(
            status -> publisher.publish("ERROR", "SBER", clock.instant(), "SBER: ошибка чтения."));
    notifications.deliverDue();
    clock.now = clock.now.plusSeconds(10);
    telegram.results.add(TelegramTransport.Result.failure("Ручная неудача", Duration.ZERO));
    notifications.retryNow(id);
    assertThat(notifications.get(id).automaticFailures()).isEqualTo(1);
    assertThat(notifications.get(id).nextAttemptAt())
        .isEqualTo(Instant.parse("2026-11-13T11:01:00Z"));
    clock.now = Instant.parse("2026-11-13T11:01:00Z");
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    notifications.deliverDue();
    assertThat(notifications.get(id).automaticFailures()).isEqualTo(2);
    assertThat(notifications.get(id).nextAttemptAt())
        .isEqualTo(Instant.parse("2026-11-13T11:03:00Z"));
    assertThat(notifications.attempts(id)).hasSize(3);
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(
      value = TelegramTransport.Outcome.class,
      names = {"FAILURE", "NO_RESPONSE"})
  void overdueAutomaticAttemptRunsOnceForSelectedNotificationAfterFailedManualAttempt(
      TelegramTransport.Outcome manualOutcome) {
    Instant start = Instant.parse("2026-11-13T12:00:00Z");
    clock.now = start;
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    UUID a =
        transactions.execute(
            status -> publisher.publish("ERROR", "SBER", clock.instant(), "A: ошибка чтения."));
    clock.now = start.plusSeconds(1);
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    UUID b =
        transactions.execute(
            status -> publisher.publish("ERROR", "GAZP", clock.instant(), "B: ошибка чтения."));
    notifications.deliverDue();
    notifications.deliverDue();
    assertThat(notifications.attempts(a))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("AUTOMATIC");
    assertThat(notifications.attempts(b))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("AUTOMATIC");

    clock.now = Instant.parse("2026-11-13T12:00:55Z");
    telegram.beforeSend = () -> clock.now = Instant.parse("2026-11-13T12:01:05Z");
    telegram.results.add(
        manualOutcome == TelegramTransport.Outcome.FAILURE
            ? TelegramTransport.Result.failure("Ручная ошибка", Duration.ZERO)
            : TelegramTransport.Result.noResponse());
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    notifications.retryNow(b);
    assertThat(notifications.attempts(a))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("AUTOMATIC");
    assertThat(notifications.attempts(b))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("AUTOMATIC", "MANUAL", "AUTOMATIC");
    assertThat(notifications.get(b).automaticFailures()).isEqualTo(2);
    assertThat(notifications.get(b).nextAttemptAt())
        .isEqualTo(Instant.parse("2026-11-13T12:03:05Z"));
  }

  @Test
  void successfulManualRetryDoesNotRepeatTheSelectedNotificationOrDrainOlderDueEvents() {
    Instant start = Instant.parse("2026-11-13T13:00:00Z");
    clock.now = start;
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    UUID a =
        transactions.execute(
            status -> publisher.publish("ERROR", "SBER", clock.instant(), "A: ошибка чтения."));
    clock.now = start.plusSeconds(1);
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    UUID b =
        transactions.execute(
            status -> publisher.publish("ERROR", "GAZP", clock.instant(), "B: ошибка чтения."));
    notifications.deliverDue();
    notifications.deliverDue();

    clock.now = Instant.parse("2026-11-13T13:01:05Z");
    telegram.results.add(TelegramTransport.Result.success());
    notifications.retryNow(b);

    assertThat(notifications.attempts(b))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("AUTOMATIC", "MANUAL");
    assertThat(notifications.get(b).status()).isEqualTo("DELIVERED");
    assertThat(notifications.attempts(a))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("AUTOMATIC");
  }

  @Test
  void failedManualAttemptCommitsItsOverdueAutomaticRetryBeforeReturning() {
    Instant start = Instant.parse("2026-11-13T14:00:00Z");
    clock.now = start;
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    UUID a =
        transactions.execute(
            status -> publisher.publish("ERROR", "SBER", clock.instant(), "A: ошибка чтения."));
    clock.now = start.plusSeconds(1);
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    UUID b =
        transactions.execute(
            status -> publisher.publish("ERROR", "GAZP", clock.instant(), "B: ошибка чтения."));
    notifications.deliverDue();
    notifications.deliverDue();

    clock.now = start.plusSeconds(70);
    telegram.results.add(TelegramTransport.Result.failure("Ручная ошибка", Duration.ZERO));
    telegram.beforeSend = () -> clock.now = clock.now.plusMillis(1);
    transactions.afterEachCommit(
        () -> {
          boolean manualFailureCommitted =
              notifications.attempts(b).stream()
                  .anyMatch(
                      attempt ->
                          attempt.kind().equals("MANUAL") && "FAILURE".equals(attempt.result()));
          if (manualFailureCommitted) throw new SimulatedProcessCrash();
        });
    try {
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> notifications.retryNow(b))
          .isInstanceOf(SimulatedProcessCrash.class);
    } finally {
      transactions.clearObserver();
    }

    assertThat(notifications.attempts(b))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("AUTOMATIC", "MANUAL", "AUTOMATIC");
    assertThat(notifications.attempts(b).getLast().completedAt()).isNull();
    assertThat(notifications.get(b).status()).isEqualTo("SENDING");
    assertThat(notifications.attempts(a))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("AUTOMATIC");
  }

  @Test
  void interruptedManualAttemptReservesItsOverdueAutomaticRetryBeforeOlderEvents() {
    Instant start = Instant.parse("2026-11-13T14:00:00Z");
    clock.now = start;
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    UUID older =
        transactions.execute(
            status -> publisher.publish("ERROR", "SBER", clock.instant(), "A: ошибка чтения."));
    notifications.deliverDue();

    clock.now = start.plusSeconds(1);
    UUID selected =
        transactions.execute(
            status -> publisher.publish("ERROR", "GAZP", clock.instant(), "B: ошибка чтения."));
    interruptManualAttempt(selected, start.plusSeconds(2));

    clock.now = start.plusSeconds(70);
    telegram.messages.clear();
    telegram.results.add(TelegramTransport.Result.success());
    notifications.deliverDue();

    assertThat(notifications.attempts(selected))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("MANUAL", "AUTOMATIC");
    assertThat(notifications.attempts(selected).getFirst().result()).isEqualTo("NO_RESPONSE");
    assertThat(notifications.attempts(selected).getLast().result()).isEqualTo("SUCCESS");
    assertThat(notifications.get(selected).status()).isEqualTo("DELIVERED");
    assertThat(notifications.attempts(older))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("AUTOMATIC");
    assertThat(notifications.get(older).status()).isEqualTo("PENDING");
    assertThat(telegram.messages).containsExactly(notifications.get(selected).text());
  }

  @Test
  void manualRetryDoesNotRecoverAnUnrelatedInterruptedAttempt() {
    Instant start = Instant.parse("2026-11-13T14:00:00Z");
    clock.now = start;
    UUID unrelated =
        transactions.execute(
            status -> publisher.publish("ERROR", "SBER", clock.instant(), "A: ошибка чтения."));
    clock.now = start.plusSeconds(1);
    UUID selected =
        transactions.execute(
            status -> publisher.publish("ERROR", "GAZP", clock.instant(), "B: ошибка чтения."));
    interruptManualAttempt(unrelated, start.plusSeconds(2));

    clock.now = start.plusSeconds(70);
    telegram.results.add(TelegramTransport.Result.success());
    notifications.retryNow(selected);

    assertThat(telegram.messages).containsExactly(notifications.get(selected).text());
    assertThat(notifications.attempts(unrelated))
        .singleElement()
        .satisfies(
            attempt -> {
              assertThat(attempt.kind()).isEqualTo("MANUAL");
              assertThat(attempt.completedAt()).isNull();
            });
    assertThat(notifications.get(unrelated).status()).isEqualTo("SENDING");
    assertThat(notifications.attempts(selected))
        .extracting(NotificationService.Attempt::kind)
        .containsExactly("MANUAL");
    assertThat(notifications.attempts(selected).getFirst().result()).isEqualTo("SUCCESS");
  }

  @Test
  void deliveryWorkerReservesOnlyOneInterruptedManualRetryBeforeSending() {
    Instant start = Instant.parse("2026-11-13T14:00:00Z");
    clock.now = start;
    UUID first =
        transactions.execute(
            status -> publisher.publish("ERROR", "SBER", clock.instant(), "A: ошибка чтения."));
    clock.now = start.plusSeconds(1);
    UUID second =
        transactions.execute(
            status -> publisher.publish("ERROR", "GAZP", clock.instant(), "B: ошибка чтения."));
    clock.now = start.plusSeconds(2);
    UUID third =
        transactions.execute(
            status -> publisher.publish("ERROR", "LKOH", clock.instant(), "C: ошибка чтения."));
    interruptManualAttempt(first, start.plusSeconds(3));
    interruptManualAttempt(second, start.plusSeconds(4));
    interruptManualAttempt(third, start.plusSeconds(5));

    clock.now = start.plusSeconds(70);
    notifications.deliverDue();

    assertThat(telegram.messages).hasSize(1);
    assertThat(
            List.of(
                notifications.get(first).status(),
                notifications.get(second).status(),
                notifications.get(third).status()))
        .containsExactlyInAnyOrder("DELIVERED", "SENDING", "SENDING");
    assertThat(
            notifications.attempts(first).stream()
                    .filter(attempt -> attempt.kind().equals("AUTOMATIC"))
                    .count()
                + notifications.attempts(second).stream()
                    .filter(attempt -> attempt.kind().equals("AUTOMATIC"))
                    .count()
                + notifications.attempts(third).stream()
                    .filter(attempt -> attempt.kind().equals("AUTOMATIC"))
                    .count())
        .isEqualTo(1);
  }

  @Test
  void workerWaitsForLockedManualRetryBeforeSelectingAnOlderEvent() throws Exception {
    Instant start = Instant.parse("2026-11-13T14:00:00Z");
    clock.now = start;
    UUID older =
        transactions.execute(
            status -> publisher.publish("ERROR", "SBER", clock.instant(), "A: ошибка чтения."));
    clock.now = start.plusSeconds(1);
    UUID selected =
        transactions.execute(
            status -> publisher.publish("ERROR", "GAZP", clock.instant(), "B: ошибка чтения."));
    interruptManualAttempt(selected, start.plusSeconds(2));

    clock.now = start.plusSeconds(70);
    telegram.results.add(TelegramTransport.Result.success());
    CompletableFuture<Void> delivery;
    boolean workerBlocked;
    try (var connection = db.getDataSource().getConnection()) {
      connection.setAutoCommit(false);
      int blockerPid;
      try (var statement = connection.createStatement();
          var row = statement.executeQuery("select pg_backend_pid()")) {
        assertThat(row.next()).isTrue();
        blockerPid = row.getInt(1);
      }
      try (var statement =
          connection.prepareStatement("select id from notification where id=? for update")) {
        statement.setObject(1, selected);
        try (var row = statement.executeQuery()) {
          assertThat(row.next()).isTrue();
        }
      }
      delivery = CompletableFuture.runAsync(notifications::deliverDue);
      workerBlocked = awaitWorkerBlockedBy(blockerPid, delivery);
      connection.commit();
    }
    delivery.get(5, TimeUnit.SECONDS);

    assertThat(workerBlocked).isTrue();
    assertThat(telegram.messages).containsExactly(notifications.get(selected).text());
    assertThat(notifications.get(selected).status()).isEqualTo("DELIVERED");
    assertThat(notifications.attempts(older)).isEmpty();
  }

  @Test
  void resolvedUndeliveredBlockKeepsOneIdAndBothOriginalMoscowTimes() {
    long resolvedBefore =
        notifications.list(200).stream()
            .filter(event -> event.category().equals("RESOLVED"))
            .count();
    clock.now = Instant.parse("2026-11-13T09:00:00Z");
    telegram.results.add(TelegramTransport.Result.failure("Недоступно", Duration.ZERO));
    UUID id =
        transactions.execute(
            status ->
                publisher.publish(
                    "UNKNOWN_RESULT",
                    "SBER",
                    clock.instant(),
                    "SBER: временная блокировка новых заявок."));
    notifications.deliverDue();
    notifications.resolve(
        id,
        Instant.parse("2026-11-13T09:00:30Z"),
        "SBER: сверка завершена, временная блокировка снята.");
    clock.now = Instant.parse("2026-11-13T09:01:00Z");
    notifications.deliverDue();
    assertThat(telegram.lastText)
        .isEqualTo(notifications.get(id).text())
        .contains("13.11.2026 12:00:00 МСК", "13.11.2026 12:00:30 МСК", id.toString());
    assertThat(
            notifications.list(200).stream()
                .filter(event -> event.category().equals("RESOLVED"))
                .count())
        .isEqualTo(resolvedBefore);
  }

  @Test
  void successfulReplyAfterBlockResolutionPreservesOriginalTextAndEmitsOneResolutionNotice() {
    clock.now = Instant.parse("2026-11-13T14:00:00Z");
    long resolvedBefore =
        notifications.list(200).stream()
            .filter(event -> event.category().equals("RESOLVED"))
            .count();
    UUID id =
        transactions.execute(
            status ->
                publisher.publish(
                    "UNKNOWN_RESULT",
                    "SBER",
                    clock.instant(),
                    "SBER: временная блокировка новых заявок."));
    String original = notifications.get(id).text();
    telegram.beforeSend =
        () -> {
          clock.now = clock.now.plusSeconds(1);
          notifications.resolve(
              id, clock.instant(), "SBER: сверка завершена, временная блокировка снята.");
        };
    notifications.deliverDue();
    assertThat(notifications.get(id).text()).isEqualTo(original);
    assertThat(notifications.get(id).status()).isEqualTo("DELIVERED");
    assertThat(notifications.attempts(id))
        .singleElement()
        .satisfies(attempt -> assertThat(attempt.payload()).isEqualTo(original));
    assertThat(
            notifications.list(200).stream()
                .filter(event -> event.category().equals("RESOLVED"))
                .count())
        .isEqualTo(resolvedBefore + 1);
    notifications.deliverDue();
    assertThat(telegram.lastText)
        .contains("SBER: сверка завершена, временная блокировка снята.", "13.11.2026 17:00:01 МСК");
    notifications.resolve(id, clock.instant(), "SBER: повторная сверка.");
    assertThat(
            notifications.list(200).stream()
                .filter(event -> event.category().equals("RESOLVED"))
                .count())
        .isEqualTo(resolvedBefore + 1);
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(
      value = TelegramTransport.Outcome.class,
      names = {"FAILURE", "NO_RESPONSE"})
  void unresolvedReplyAfterConcurrentResolutionRetainsCombinedSameId(
      TelegramTransport.Outcome outcome) {
    clock.now = Instant.parse("2026-11-13T15:00:00Z");
    long resolvedBefore =
        notifications.list(200).stream()
            .filter(event -> event.category().equals("RESOLVED"))
            .count();
    UUID id =
        transactions.execute(
            status ->
                publisher.publish(
                    "UNKNOWN_RESULT",
                    "GAZP",
                    clock.instant(),
                    "GAZP: временная блокировка новых заявок."));
    String original = notifications.get(id).text();
    telegram.results.add(
        outcome == TelegramTransport.Outcome.FAILURE
            ? TelegramTransport.Result.failure("Недоступно", Duration.ZERO)
            : TelegramTransport.Result.noResponse());
    telegram.beforeSend =
        () -> {
          clock.now = clock.now.plusSeconds(1);
          notifications.resolve(id, clock.instant(), "GAZP: временная блокировка снята.");
        };
    notifications.deliverDue();
    assertThat(notifications.get(id).status()).isEqualTo("PENDING");
    assertThat(notifications.get(id).text())
        .contains(
            "GAZP: временная блокировка новых заявок.",
            "GAZP: временная блокировка снята.",
            id.toString());
    assertThat(notifications.attempts(id))
        .singleElement()
        .satisfies(attempt -> assertThat(attempt.payload()).isEqualTo(original));
    clock.now = clock.now.plusSeconds(60);
    notifications.deliverDue();
    assertThat(telegram.lastText).isEqualTo(notifications.get(id).text());
    assertThat(
            notifications.list(200).stream()
                .filter(event -> event.category().equals("RESOLVED"))
                .count())
        .isEqualTo(resolvedBefore);
  }

  private void interruptManualAttempt(UUID notificationId, Instant startedAt) {
    UUID attemptId = UUID.randomUUID();
    db.update(
        "update notification set status='SENDING',claim_id=?,claim_started_at=?,claim_kind='MANUAL' where id=?",
        attemptId,
        Timestamp.from(startedAt),
        notificationId);
    db.update(
        "insert into notification_attempt(id,notification_id,kind,started_at,payload) values(?,?,'MANUAL',?,?)",
        attemptId,
        notificationId,
        Timestamp.from(startedAt),
        notifications.get(notificationId).text());
  }

  private boolean awaitWorkerBlockedBy(int blockerPid, CompletableFuture<?> worker)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      Boolean blocked =
          db.queryForObject(
              "select exists (select 1 from pg_stat_activity where pid<>pg_backend_pid() and ?=any(pg_blocking_pids(pid)))",
              Boolean.class,
              blockerPid);
      if (Boolean.TRUE.equals(blocked)) return true;
      if (worker.isDone()) return false;
      Thread.sleep(10);
    }
    return false;
  }

  static class Simulator implements TelegramTransport {
    String lastText;
    final ArrayDeque<Result> results = new ArrayDeque<>();
    final ArrayList<String> messages = new ArrayList<>();
    Runnable beforeSend;

    public Result send(String text) {
      lastText = text;
      messages.add(text);
      if (beforeSend != null) {
        var run = beforeSend;
        beforeSend = null;
        run.run();
      }
      return results.isEmpty() ? Result.success() : results.remove();
    }
  }

  static class MutableClock extends Clock {
    Instant now = Instant.parse("2026-11-13T09:00:00Z");

    public ZoneId getZone() {
      return ZoneId.of("UTC");
    }

    public Clock withZone(ZoneId zone) {
      return this;
    }

    public Instant instant() {
      return now;
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class TransportConfiguration {
    @Bean
    @Primary
    Simulator simulator() {
      return new Simulator();
    }

    @Bean
    @Primary
    MutableClock testClock() {
      return new MutableClock();
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class TransactionConfiguration {
    @Bean
    @Primary
    CommitAwareTransactionTemplate commitAwareTransactionTemplate(
        PlatformTransactionManager manager) {
      return new CommitAwareTransactionTemplate(manager);
    }
  }

  static final class CommitAwareTransactionTemplate extends TransactionTemplate {
    private final AtomicReference<Runnable> observer = new AtomicReference<>();

    CommitAwareTransactionTemplate(PlatformTransactionManager manager) {
      super(manager);
    }

    void afterEachCommit(Runnable callback) {
      observer.set(callback);
    }

    void clearObserver() {
      observer.set(null);
    }

    @Override
    public <T> T execute(TransactionCallback<T> action) {
      T result = super.execute(action);
      Runnable callback = observer.get();
      if (callback != null) {
        try {
          callback.run();
        } catch (RuntimeException | Error failure) {
          observer.compareAndSet(callback, null);
          throw failure;
        }
      }
      return result;
    }
  }

  static final class SimulatedProcessCrash extends Error {}
}
