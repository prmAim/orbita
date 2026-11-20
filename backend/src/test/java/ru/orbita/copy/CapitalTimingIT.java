package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.control.ControlService;
import ru.orbita.notifications.NotificationService;
import ru.orbita.notifications.TelegramTransport;
import ru.orbita.runtime.ApplicationReadiness;

@SpringBootTest(
    properties = {
      "orbita.source-account=source",
      "orbita.mirror-account=mirror",
      "orbita.telegram.enabled=false",
      "orbita.worker.enabled=false"
    })
@Import({CopyLifecycleIT.Fixture.class, CapitalTimingIT.JdbcTiming.class})
class CapitalTimingIT {
  @Autowired ControlService control;
  @Autowired NotificationService notifications;
  @Autowired CopyService copies;
  @Autowired CopyLifecycleIT.Simulator broker;
  @Autowired CopyLifecycleIT.MutableClock clock;
  @Autowired JdbcTemplate database;
  @Autowired CopyLifecycleIT.TelegramSimulator telegram;
  @Autowired TransactionTemplate transactions;
  @Autowired CommitDelay delay;
  @Autowired ApplicationReadiness readiness;

  @BeforeEach
  void reset() {
    delay.reset();
    CopyLifecycleIT.reset(database, broker, clock);
    telegram.reset();
  }

  @Test
  void delayedRecalculationErrorKeepsItsTimeIdAndDeadlineAndRecoveryIsIndependent() {
    control.markReconciled();
    control.start();
    clock.now = clock.now.plusSeconds(1);
    Instant pauseStartedAt = clock.instant();
    broker.failCapital = true;
    control.mandatoryRecalculation();

    var firstError =
        notifications.list(200).stream()
            .filter(event -> event.category().equals("RECALCULATION_ERROR"))
            .findFirst()
            .orElseThrow();
    assertThat(
            database.queryForObject(
                "select recalculation_error_notification_id from control_state where id=1",
                java.util.UUID.class))
        .isEqualTo(firstError.id());
    String originalText = firstError.text();
    telegram.results.add(
        TelegramTransport.Result.failure("Имитатор: задержка доставки", Duration.ofMinutes(2)));
    notifications.deliverDue();
    var pending = notifications.get(firstError.id());
    Instant retryAt = pending.nextAttemptAt();
    assertThat(pending.status()).isEqualTo("PENDING");
    assertThat(retryAt).isEqualTo(pauseStartedAt.plus(Duration.ofMinutes(2)));

    clock.now = pauseStartedAt.plusSeconds(61);
    broker.failCapital = false;
    control.mandatoryRecalculation();

    assertThat(control.status().recalculationPaused()).isFalse();
    var annotated = notifications.get(firstError.id());
    assertThat(annotated.status()).isEqualTo("PENDING");
    assertThat(annotated.nextAttemptAt()).isEqualTo(retryAt);
    String originalPrefix =
        originalText.substring(0, originalText.indexOf("\nИдентификатор уведомления:"));
    assertThat(annotated.text())
        .startsWith(originalPrefix)
        .contains(
            "Пересчёт восстановлен; начало " + NotificationService.formatTime(pauseStartedAt),
            "окончание " + NotificationService.formatTime(clock.instant()),
            firstError.id().toString());

    var recovery =
        notifications.list(200).stream()
            .filter(event -> event.category().equals("RECALCULATION_RECOVERED"))
            .findFirst()
            .orElseThrow();
    assertThat(recovery.id()).isNotEqualTo(firstError.id());
    notifications.deliverDue();
    assertThat(telegram.lastText).isEqualTo(recovery.text());
    assertThat(notifications.get(firstError.id()).nextAttemptAt()).isEqualTo(retryAt);

    clock.now = retryAt;
    notifications.deliverDue();
    assertThat(notifications.get(firstError.id()).status()).isEqualTo("DELIVERED");
    assertThat(telegram.lastText).isEqualTo(notifications.get(firstError.id()).text());
    assertThat(telegram.lastText)
        .contains(
            NotificationService.formatTime(pauseStartedAt),
            "Пересчёт восстановлен; начало " + NotificationService.formatTime(pauseStartedAt),
            "окончание " + NotificationService.formatTime(pauseStartedAt.plusSeconds(61)),
            firstError.id().toString());
    assertThat(notifications.attempts(firstError.id()))
        .extracting(NotificationService.Attempt::payload)
        .containsExactly(originalText, annotated.text());
    assertThat(telegram.messages).containsExactly(originalText, recovery.text(), annotated.text());
  }

  @Test
  void clockCorrectionDoesNotRollBackSuccessfulRecalculation() {
    control.markReconciled();
    control.start();
    clock.now = clock.now.plusSeconds(1);
    Instant pauseStartedAt = clock.instant();
    broker.failCapital = true;
    control.mandatoryRecalculation();
    var firstError =
        notifications.list(200).stream()
            .filter(event -> event.category().equals("RECALCULATION_ERROR"))
            .findFirst()
            .orElseThrow();

    clock.now = pauseStartedAt.minus(Duration.ofMinutes(5));
    broker.failCapital = false;
    control.recalculate();

    assertThat(control.status().recalculationPaused()).isFalse();
    assertThat(notifications.get(firstError.id()).text())
        .contains(
            "Пересчёт восстановлен; начало " + NotificationService.formatTime(pauseStartedAt),
            "длительность не подтверждена");
    assertThat(notifications.list(200))
        .filteredOn(event -> event.category().equals("RECALCULATION_RECOVERED"))
        .hasSize(1);
  }

  @Test
  void firstErrorNotificationKeepsItsIdentityAndSuppressesReturnedReasons() {
    control.markReconciled();
    control.start();
    clock.now = clock.now.plusSeconds(1);
    Instant pauseStartedAt = clock.instant();
    broker.failCapital = true;
    control.mandatoryRecalculation();
    var firstError =
        notifications.list(200).stream()
            .filter(event -> event.category().equals("RECALCULATION_ERROR"))
            .findFirst()
            .orElseThrow();
    telegram.results.add(
        TelegramTransport.Result.failure("Имитатор: задержка доставки", Duration.ofMinutes(2)));
    notifications.deliverDue();
    Instant retryAt = notifications.get(firstError.id()).nextAttemptAt();

    clock.now = pauseStartedAt.plusSeconds(61);
    broker.failCapital = false;
    broker.capitalResponse =
        _ -> {
          throw new IllegalArgumentException("second simulator failure");
        };
    control.mandatoryRecalculation();
    var secondError =
        notifications.list(200).stream()
            .filter(
                event ->
                    event.category().equals("RECALCULATION_ERROR")
                        && !event.id().equals(firstError.id()))
            .findFirst()
            .orElseThrow();
    assertThat(
            database.queryForObject(
                "select recalculation_error_notification_id from control_state where id=1",
                UUID.class))
        .isEqualTo(firstError.id());

    clock.now = pauseStartedAt.plusSeconds(123);
    broker.capitalResponse = null;
    broker.failCapital = true;
    control.mandatoryRecalculation();

    assertThat(notifications.list(200))
        .filteredOn(event -> event.category().equals("RECALCULATION_ERROR"))
        .hasSize(2);
    assertThat(
            database.queryForObject(
                "select count(*) from journal_entry where category='RECALCULATION_FAILED'",
                Integer.class))
        .isEqualTo(3);
    assertThat(
            database.queryForObject(
                "select recalculation_error_notification_id from control_state where id=1",
                UUID.class))
        .isEqualTo(firstError.id());

    clock.now = pauseStartedAt.plusSeconds(185);
    broker.failCapital = false;
    broker.capitalResponse = null;
    control.recalculate();

    assertThat(notifications.get(firstError.id()).nextAttemptAt()).isEqualTo(retryAt);
    assertThat(notifications.get(firstError.id()).text())
        .contains("окончание " + NotificationService.formatTime(clock.instant()));
    assertThat(notifications.get(secondError.id()).text()).doesNotContain("Пересчёт восстановлен;");
  }

  @Test
  void sameFailureAfterRecoveryWithBackwardClockStartsANewPause() {
    control.markReconciled();
    control.start();
    clock.now = clock.now.plusSeconds(1);
    Instant firstPauseStartedAt = clock.instant();
    broker.failCapital = true;
    control.mandatoryRecalculation();
    var firstError =
        notifications.list(200).stream()
            .filter(event -> event.category().equals("RECALCULATION_ERROR"))
            .findFirst()
            .orElseThrow();

    clock.now = firstPauseStartedAt.plusSeconds(61);
    broker.failCapital = false;
    control.recalculate();
    clock.now = firstPauseStartedAt.minusSeconds(60);
    broker.failCapital = true;
    control.mandatoryRecalculation();

    var errors =
        notifications.list(200).stream()
            .filter(event -> event.category().equals("RECALCULATION_ERROR"))
            .toList();
    assertThat(errors).hasSize(2);
    assertThat(
            database.queryForObject(
                "select recalculation_error_notification_id from control_state where id=1",
                UUID.class))
        .isNotNull()
        .isNotEqualTo(firstError.id());
  }

  @Test
  void inFlightSuccessRedeliversAnnotatedMessageWithTheSameId() throws Exception {
    control.markReconciled();
    control.start();
    clock.now = clock.now.plusSeconds(1);
    Instant pauseStartedAt = clock.instant();
    broker.failCapital = true;
    control.mandatoryRecalculation();
    var firstError =
        notifications.list(200).stream()
            .filter(event -> event.category().equals("RECALCULATION_ERROR"))
            .findFirst()
            .orElseThrow();
    String sentPayload = firstError.text();

    telegram.sendStarted = new CountDownLatch(1);
    telegram.allowSend = new CountDownLatch(1);
    var delivery = CompletableFuture.runAsync(notifications::deliverDue);
    try {
      assertThat(telegram.sendStarted.await(5, TimeUnit.SECONDS)).isTrue();
      clock.now = clock.now.plusSeconds(61);
      broker.failCapital = false;
      control.mandatoryRecalculation();
    } finally {
      telegram.allowSend.countDown();
    }
    delivery.join();

    var pendingCorrection = notifications.get(firstError.id());
    assertThat(pendingCorrection.status()).isEqualTo("PENDING");
    assertThat(pendingCorrection.nextAttemptAt()).isEqualTo(clock.instant());
    assertThat(pendingCorrection.text())
        .contains(
            "Пересчёт восстановлен; начало " + NotificationService.formatTime(pauseStartedAt),
            firstError.id().toString());
    notifications.deliverDue();
    assertThat(notifications.get(firstError.id()).status()).isEqualTo("DELIVERED");
    assertThat(telegram.messages).containsExactly(sentPayload, pendingCorrection.text());
    assertThat(telegram.lastText).isEqualTo(pendingCorrection.text());
    assertThat(notifications.attempts(firstError.id()))
        .extracting(NotificationService.Attempt::payload)
        .containsExactly(sentPayload, pendingCorrection.text());
    assertThat(notifications.list(200))
        .filteredOn(event -> event.category().equals("RECALCULATION_RECOVERED"))
        .hasSize(1);
  }

  @Test
  void completionWaitingOnAnnotationLockUsesTheCommittedPayload() throws Exception {
    Instant failedAt = clock.instant();
    UUID id =
        transactions.execute(
            _ ->
                notifications.publish(
                    "RECALCULATION_ERROR", null, failedAt, "simulator unavailable"));
    Instant recoveredAt = failedAt.plusSeconds(61);
    String recoveryText =
        "Пересчёт восстановлен; начало "
            + NotificationService.formatTime(failedAt)
            + ", окончание "
            + NotificationService.formatTime(recoveredAt);
    var annotationStarted = new CountDownLatch(1);
    var commitAnnotation = new CountDownLatch(1);
    telegram.sendStarted = new CountDownLatch(1);
    telegram.allowSend = new CountDownLatch(1);
    var delivery = CompletableFuture.runAsync(notifications::deliverDue);
    CompletableFuture<Void> annotation = null;
    try {
      assertThat(telegram.sendStarted.await(5, TimeUnit.SECONDS)).isTrue();
      annotation =
          CompletableFuture.runAsync(
              () ->
                  transactions.executeWithoutResult(
                      _ -> {
                        assertThat(notifications.annotateUndelivered(id, recoveredAt, recoveryText))
                            .isTrue();
                        notifications.publish(
                            "RECALCULATION_RECOVERED", null, recoveredAt, recoveryText);
                        annotationStarted.countDown();
                        await(commitAnnotation);
                      }));
      assertThat(annotationStarted.await(5, TimeUnit.SECONDS)).isTrue();
      telegram.allowSend.countDown();
      awaitBlockedCompletionQuery();
      commitAnnotation.countDown();
      annotation.join();
      delivery.join();

      var pending = notifications.get(id);
      assertThat(pending.status()).isEqualTo("PENDING");
      assertThat(pending.text()).contains(recoveryText, id.toString());
      assertThat(pending.nextAttemptAt()).isEqualTo(clock.instant());
      notifications.deliverDue();
      assertThat(notifications.get(id).status()).isEqualTo("DELIVERED");
      assertThat(telegram.messages).hasSize(2);
      assertThat(telegram.messages.get(0)).contains("simulator unavailable");
      assertThat(telegram.messages.get(1)).contains(recoveryText, id.toString());
    } finally {
      telegram.allowSend.countDown();
      commitAnnotation.countDown();
      if (annotation != null) annotation.join();
      delivery.join();
    }
  }

  @Test
  void v17ActivePauseMigrationBackfillsTheCurrentPauseAfterManualStopAndRestart() {
    try (var postgres = new PostgreSQLContainer("postgres:17.11-alpine")) {
      postgres.start();
      Flyway.configure()
          .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
          .target("17")
          .load()
          .migrate();
      var database =
          new JdbcTemplate(
              new DriverManagerDataSource(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()));
      Instant previousPauseAt = Instant.parse("2026-11-18T10:00:00Z");
      UUID previousRecoveryId = UUID.randomUUID();
      insertLegacyNotification(
          database,
          previousRecoveryId,
          "RECALCULATION_RECOVERED",
          previousPauseAt.minusSeconds(60),
          "Предыдущая пауза восстановлена");
      UUID previousErrorId = UUID.randomUUID();
      Long previousErrorJournalId =
          insertLegacyRecalculationError(
              database,
              previousErrorId,
              previousPauseAt,
              "previous reason; обязательный пересчёт приостановлен");
      Instant stoppedAt = previousPauseAt.plusSeconds(60);
      insertLegacyJournalEntry(database, stoppedAt, "MANUAL_STOP", "Остановить повторение");
      Long restartJournalId =
          insertLegacyJournalEntry(
              database,
              stoppedAt.plusSeconds(1),
              "COEFFICIENT_SAVED",
              "коэффициент сохранён после успешного запуска");

      Instant pauseStartedAt = Instant.parse("2026-11-19T10:00:00Z");
      database.update(
          "update control_state set recalculation_paused=true,pause_started_at=?,failed_attempts=2,last_error=? where id=1",
          Timestamp.from(pauseStartedAt),
          "second reason");
      UUID firstId = UUID.randomUUID();
      UUID secondId = UUID.randomUUID();
      insertLegacyRecalculationError(
          database, firstId, pauseStartedAt, "first reason; обязательный пересчёт приостановлен");
      insertLegacyRecalculationError(
          database,
          secondId,
          pauseStartedAt.plusSeconds(1),
          "second reason; обязательный пересчёт приостановлен");

      Flyway.configure()
          .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
          .load()
          .migrate();

      assertThat(
              database.queryForObject(
                  "select recalculation_error_notification_id from control_state where id=1",
                  UUID.class))
          .isEqualTo(firstId)
          .isNotEqualTo(previousErrorId);
      assertThat(
              database.queryForObject(
                  "select recalculation_pause_journal_boundary from control_state where id=1",
                  Long.class))
          .isEqualTo(restartJournalId)
          .isGreaterThan(previousErrorJournalId);
    }
  }

  @Test
  void delayedErrorsFromCompletedV17PausesKeepTheirOwnRecoveryAndRetryDeadline() {
    try (var postgres = new PostgreSQLContainer("postgres:17.11-alpine")) {
      postgres.start();
      Flyway.configure()
          .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
          .target("17")
          .load()
          .migrate();
      var dataSource =
          new DriverManagerDataSource(
              postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
      var legacyDatabase = new JdbcTemplate(dataSource);

      UUID firstErrorId = UUID.randomUUID();
      UUID firstRecoveryId = UUID.randomUUID();
      Instant firstStartedAt = Instant.parse("2026-11-19T10:00:00Z");
      Instant firstEndedAt = Instant.parse("2026-11-19T10:01:01Z");
      String firstOwnerText =
          "первая ошибка; обязательный пересчёт приостановлен\n"
              + "19.11.2026 13:00:00 МСК\nИдентификатор уведомления: "
              + firstErrorId;
      String firstResolution =
          "Пересчёт восстановлен; начало 19.11.2026 13:00:00 МСК, окончание "
              + "19.11.2026 13:01:01 МСК, длительность PT1M1S, ошибок 1";
      String firstRecoveryText =
          firstResolution
              + "\n19.11.2026 13:01:01 МСК\nИдентификатор уведомления: "
              + firstRecoveryId;
      insertLegacyNotification(
          legacyDatabase, firstErrorId, "RECALCULATION_ERROR", firstStartedAt, firstOwnerText);
      insertLegacyNotification(
          legacyDatabase,
          firstRecoveryId,
          "RECALCULATION_RECOVERED",
          firstEndedAt,
          firstRecoveryText);

      UUID secondErrorId = UUID.randomUUID();
      UUID secondRecoveryId = UUID.randomUUID();
      Instant secondStartedAt = Instant.parse("2026-11-19T09:00:00Z");
      Instant secondEndedAt = Instant.parse("2026-11-19T09:00:30Z");
      String secondOwnerText =
          "вторая ошибка; обязательный пересчёт приостановлен\n"
              + "19.11.2026 12:00:00 МСК\nИдентификатор уведомления: "
              + secondErrorId;
      String secondResolution =
          "Пересчёт восстановлен; начало 19.11.2026 12:00:00 МСК, окончание "
              + "19.11.2026 12:00:30 МСК, длительность PT30S, ошибок 1";
      String secondRecoveryText =
          secondResolution
              + "\n19.11.2026 12:00:30 МСК\nИдентификатор уведомления: "
              + secondRecoveryId;
      insertLegacyNotification(
          legacyDatabase, secondErrorId, "RECALCULATION_ERROR", secondStartedAt, secondOwnerText);
      insertLegacyNotification(
          legacyDatabase,
          secondRecoveryId,
          "RECALCULATION_RECOVERED",
          secondEndedAt,
          secondRecoveryText);

      Instant firstRetryAt = Instant.parse("2026-11-20T11:00:00Z");
      Instant secondRetryAt = Instant.parse("2026-11-20T12:00:00Z");
      legacyDatabase.update(
          "update notification set next_attempt_at=? where id in (?,?)",
          Timestamp.from(firstRetryAt),
          firstErrorId,
          firstRecoveryId);
      legacyDatabase.update(
          "update notification set next_attempt_at=? where id in (?,?)",
          Timestamp.from(secondRetryAt),
          secondErrorId,
          secondRecoveryId);
      Flyway.configure()
          .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
          .load()
          .migrate();

      clock.now = Instant.parse("2026-11-20T10:00:00Z");
      var localNotifications =
          new NotificationService(
              legacyDatabase,
              new TransactionTemplate(new DataSourceTransactionManager(dataSource)),
              telegram,
              clock,
              readiness);
      localNotifications.deliverDue();

      var first = localNotifications.get(firstErrorId);
      var second = localNotifications.get(secondErrorId);
      assertThat(first.text())
          .startsWith(firstOwnerText.substring(0, firstOwnerText.indexOf("\nИдентификатор")))
          .contains(firstResolution, firstErrorId.toString());
      assertThat(first.resolvedAt()).isEqualTo(firstEndedAt);
      assertThat(first.resolutionText()).isEqualTo(firstResolution);
      assertThat(first.retryAnnotatedNotificationOnCompletion()).isTrue();
      assertThat(first.nextAttemptAt()).isEqualTo(firstRetryAt);
      assertThat(second.text())
          .startsWith(secondOwnerText.substring(0, secondOwnerText.indexOf("\nИдентификатор")))
          .contains(secondResolution, secondErrorId.toString());
      assertThat(second.resolvedAt()).isEqualTo(secondEndedAt);
      assertThat(second.resolutionText()).isEqualTo(secondResolution);
      assertThat(second.retryAnnotatedNotificationOnCompletion()).isTrue();
      assertThat(second.nextAttemptAt()).isEqualTo(secondRetryAt);
      assertThat(localNotifications.get(firstRecoveryId).text()).isEqualTo(firstRecoveryText);
      assertThat(localNotifications.get(secondRecoveryId).text()).isEqualTo(secondRecoveryText);
      assertThat(telegram.messages).isEmpty();
    }
  }

  private static Long insertLegacyRecalculationError(
      JdbcTemplate database, UUID id, Instant at, String ownerText) {
    return insertLegacyNotification(database, id, "RECALCULATION_ERROR", at, ownerText);
  }

  private static Long insertLegacyNotification(
      JdbcTemplate database, UUID id, String category, Instant at, String ownerText) {
    Long journalId =
        database.queryForObject(
            "insert into journal_entry(occurred_at,category,instrument,detail) values(?,?,'',?) returning id",
            Long.class,
            Timestamp.from(at),
            "NOTIFICATION_" + category,
            ownerText);
    database.update(
        "insert into notification(id,category,instrument,occurred_at,journal_id,status,next_attempt_at) values(?,?,'',?,?,'PENDING',?)",
        id,
        category,
        Timestamp.from(at),
        journalId,
        Timestamp.from(at));
    return journalId;
  }

  private static Long insertLegacyJournalEntry(
      JdbcTemplate database, Instant at, String category, String detail) {
    return database.queryForObject(
        "insert into journal_entry(occurred_at,category,instrument,detail) values(?,?,'',?) returning id",
        Long.class,
        Timestamp.from(at),
        category,
        detail);
  }

  private void awaitBlockedCompletionQuery() {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      Integer blocked =
          database.queryForObject(
              "select count(*) from pg_stat_activity where state='active' and wait_event_type='Lock' and query ilike '%n.claim_id=%' and query ilike '%for update of n%' ",
              Integer.class);
      if (blocked != null && blocked > 0) return;
      try {
        Thread.sleep(10);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted while awaiting the locked notification read", exception);
      }
    }
    throw new AssertionError("Notification completion never waited for the annotation lock");
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS))
        throw new AssertionError("Timed out waiting for a notification transaction latch");
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while awaiting a notification transaction latch", exception);
    }
  }

  @ParameterizedTest
  @EnumSource(PersistenceFault.class)
  void numericPersistenceFailureEndsTheCallerAndRequiresFreshReconciliation(
      PersistenceFault fault) {
    control.markReconciled();
    control.start();
    broker.mirrorCapital = new BigDecimal("200000");
    if (fault == PersistenceFault.INSERT)
      database.execute(
          "alter table capital_candidate add constraint numeric_write_fault check(source_capital<0) not valid");
    else delay.persistenceFailure.set(fault);
    var continued = new java.util.concurrent.atomic.AtomicBoolean();
    Throwable failure;
    try {
      failure =
          org.assertj.core.api.Assertions.catchThrowable(
              () -> {
                control.recalculate();
                continued.set(true);
              });
    } finally {
      database.execute(
          "alter table capital_candidate drop constraint if exists numeric_write_fault");
    }
    assertThat(failure).isInstanceOf(RuntimeException.class);
    assertThat(continued).isFalse();
    assertThat(control.status().coefficient()).isEqualByComparingTo("1");
    control.recoverAbandonedCapitalAttempts();
    assertThat(control.status().recalculationRunning()).isFalse();
    assertThat(control.status().reconciled()).isFalse();
    assertThat(control.recalculate().coefficient()).isEqualByComparingTo("2");
    assertThat(broker.posts).isZero();
  }

  enum PersistenceFault {
    INSERT,
    BEFORE_COMMIT,
    AFTER_COMMIT
  }

  @ParameterizedTest
  @EnumSource(CommitPhase.class)
  void lateNumericCommitOrItsAcknowledgementNeverPublishesANewAcceptedRatio(CommitPhase phase)
      throws Exception {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    clock.now = clock.now.plusSeconds(86400);
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) delay.arm(phase);
        };
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var result = executor.submit(control::recalculate);
      assertThat(delay.entered.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(control.status().coefficient()).isEqualByComparingTo("1");
      assertThat(result.get(15, TimeUnit.SECONDS).coefficient()).isEqualByComparingTo("1");
    }
    assertThat(control.status().lastError()).contains("5 секунд");
    assertThat(control.mandatoryDue()).isTrue();
    var execution =
        new BrokerGateway.SourceExecution(
            "source",
            "after-late-commit",
            "late-order",
            "instrument",
            BrokerGateway.Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    assertThat(control.gate().exactCoefficient().decimal(50)).isEqualByComparingTo("1");
    assertThat(control.gate().recalculationPaused()).isTrue();
    assertThat(copies.accept(execution).acceptedCoefficient()).isNull();
    assertThat(broker.posts).isZero();
  }

  @ParameterizedTest
  @EnumSource(CommitPhase.class)
  void firstStartRemainsUnstartedWhenNumericCommitOrItsAcknowledgementIsLate(CommitPhase phase)
      throws Exception {
    control.markReconciled();
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 2) delay.arm(phase);
        };
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var result = executor.submit(control::start);
      assertThat(delay.entered.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(control.status().coefficient()).isNull();
      assertThat(result.get(15, TimeUnit.SECONDS).coefficient()).isNull();
    }
    assertThat(control.status().mode()).isEqualTo("NOT_STARTED");
    assertThat(control.status().lastError()).contains("5 секунд");
    assertThat(broker.posts).isZero();
  }

  @Test
  void firstObserverDelayCannotHideTheRawReplyGapWithFrozenWallTime() {
    control.markReconciled();
    control.start();
    broker.capitalResponse =
        account -> {
          if (account.equals("mirror")) pause(1700);
          var receipt =
              new BrokerGateway.CapitalEvidence(
                  account,
                  new BigDecimal(account.equals("source") ? "100000" : "200000"),
                  clock.instant(),
                  clock.instant(),
                  true,
                  "raw RPC simulator",
                  ru.orbita.broker.BrokerReceiptClock.nanoTime(),
                  ru.orbita.broker.BrokerReceiptClock.domain());
          // The first RPC reply is already complete; the following durable observer delays return.
          if (account.equals("source")) pause(1700);
          return receipt;
        };
    var result = control.recalculate();
    assertThat(result.coefficient()).isEqualByComparingTo("1");
    assertThat(result.lastError()).contains("3 секунды");
    assertThat(broker.posts).isZero();
  }

  private static void pause(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(failure);
    }
  }

  @Test
  void frozenWallClockDoesNotHideThreePointTwoSecondsBetweenFullCapitalReplies() {
    control.markReconciled();
    control.start();
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4)
            try {
              Thread.sleep(3200);
            } catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(interrupted);
            }
        };
    var result = control.recalculate();
    assertThat(result.coefficient()).isEqualByComparingTo("1");
    assertThat(result.lastError()).contains("3 секунды");
  }

  @Test
  void timelyDurableNumericSaveDoesNotRequireAnotherTimedReferenceConfirmation() throws Exception {
    control.markReconciled();
    control.start();
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) {
            delay.afterPromotionCommit = () -> clock.now = clock.now.plusSeconds(86400);
            delay.armPromotion();
          }
        };
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var result = executor.submit(control::recalculate);
      assertThat(delay.entered.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(result.get(15, TimeUnit.SECONDS).coefficient()).isEqualByComparingTo("2");
    }
    assertThat(control.status().lastError()).isNull();
    assertThat(control.mandatoryDue()).isTrue();
  }

  @Test
  void restartDoesNotPromoteAnUnacknowledgedDurableCandidate() {
    control.markReconciled();
    control.start();
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) delay.armCrash();
        };
    org.assertj.core.api.Assertions.assertThatThrownBy(control::recalculate)
        .isInstanceOf(SimulatedCrash.class);
    assertThat(control.status().coefficient()).isEqualByComparingTo("1");
    assertThat(control.status().recalculationRunning()).isTrue();
    control.recoverStartup();
    assertThat(control.status().coefficient()).isEqualByComparingTo("1");
    assertThat(control.status().recalculationRunning()).isFalse();
    control.recalculate();
    assertThat(control.status().coefficient()).isEqualByComparingTo("2");
    assertThat(broker.capitalCalls).isEqualTo(6);
  }

  @ParameterizedTest
  @EnumSource(CommitPhase.class)
  void aPersistenceIntervalCrossingMoscowTwoOClockIsRejectedInsteadOfCompletingTheDay(
      CommitPhase phase) {
    clock.now = java.time.Instant.parse("2026-11-15T22:59:58Z");
    control.markReconciled();
    control.start();
    clock.now = java.time.Instant.parse("2026-11-15T22:59:59.900Z");
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) {
            Runnable cross = () -> clock.now = java.time.Instant.parse("2026-11-15T23:00:00.100Z");
            if (phase == CommitPhase.BEFORE_DELEGATE) delay.beforeNumericCommit = cross;
            else delay.afterNumericCommit = cross;
            delay.armClockAdvance(phase);
          }
        };
    var result = control.recalculate();
    assertThat(result.coefficient()).isEqualByComparingTo("1");
    assertThat(result.lastError()).contains("02:00");
    assertThat(control.mandatoryDue()).isTrue();
  }

  @Test
  void aProvenSaveBeforeMoscowTwoLeavesTheUpcomingDeadlinePending() {
    clock.now = java.time.Instant.parse("2026-11-15T22:59:58Z");
    control.markReconciled();
    control.start();
    clock.now = java.time.Instant.parse("2026-11-15T22:59:59.500Z");
    broker.mirrorCapital = new BigDecimal("200000");
    assertThat(control.recalculate().coefficient()).isEqualByComparingTo("2");
    clock.now = java.time.Instant.parse("2026-11-15T23:00:00.100Z");
    assertThat(control.mandatoryDue()).isTrue();
  }

  @Test
  void aProvenSaveAfterMoscowTwoCompletesTheCurrentDeadline() {
    clock.now = java.time.Instant.parse("2026-11-15T22:59:58Z");
    control.markReconciled();
    control.start();
    clock.now = java.time.Instant.parse("2026-11-15T22:59:59.900Z");
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4)
            clock.now = java.time.Instant.parse("2026-11-15T23:00:00.100Z");
        };
    assertThat(control.recalculate().coefficient()).isEqualByComparingTo("2");
    assertThat(control.mandatoryDue()).isFalse();
  }

  @Test
  void stopAndSourceAdmissionCannotInterleaveBetweenDurableNumericSaveAndAcceptance()
      throws Exception {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    clock.now = clock.now.plusSeconds(1);
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) delay.armBarrier();
        };
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var recalculation = executor.submit(control::recalculate);
      assertThat(delay.entered.await(10, TimeUnit.SECONDS)).isTrue();
      var stop = executor.submit(control::stop);
      var admission =
          executor.submit(
              () ->
                  copies.accept(
                      new BrokerGateway.SourceExecution(
                          "source",
                          "stage-race",
                          "stage-order",
                          "instrument",
                          BrokerGateway.Side.BUY,
                          BigDecimal.ONE,
                          new BigDecimal("100"),
                          "RUB",
                          clock.instant(),
                          clock.instant())));
      try {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> stop.get(300, TimeUnit.MILLISECONDS))
            .isInstanceOf(TimeoutException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> admission.get(300, TimeUnit.MILLISECONDS))
            .isInstanceOf(TimeoutException.class);
      } finally {
        delay.release.countDown();
      }
      assertThat(stop.get(10, TimeUnit.SECONDS).coefficient()).isEqualByComparingTo("2");
      var event = admission.get(10, TimeUnit.SECONDS);
      if (event.acceptedCoefficient() != null)
        assertThat(event.acceptedCoefficient()).isEqualByComparingTo("2");
      else assertThat(event.disposition()).isEqualTo("SKIPPED");
      recalculation.get(10, TimeUnit.SECONDS);
    }
    assertThat(control.status().mode()).isEqualTo("STOPPING");
    assertThat(control.status().coefficient()).isEqualByComparingTo("2");
  }

  enum CommitPhase {
    BEFORE_DELEGATE,
    AFTER_DELEGATE
  }

  enum WriteTarget {
    NUMERIC,
    PROMOTION
  }

  record DelayPlan(
      CommitPhase phase, WriteTarget target, boolean crash, long millis, boolean barrier) {}

  static final class SimulatedCrash extends Error {}

  static final class CommitDelay {
    final AtomicReference<DelayPlan> phase = new AtomicReference<>();
    final AtomicReference<PersistenceFault> persistenceFailure = new AtomicReference<>();
    volatile CountDownLatch entered = new CountDownLatch(1);
    volatile CountDownLatch release = new CountDownLatch(1);
    volatile Runnable afterPromotionCommit;
    volatile Runnable beforeNumericCommit, afterNumericCommit;

    void reset() {
      phase.set(null);
      persistenceFailure.set(null);
      entered = new CountDownLatch(1);
      release = new CountDownLatch(1);
      afterPromotionCommit = null;
      beforeNumericCommit = null;
      afterNumericCommit = null;
    }

    void arm(CommitPhase value) {
      phase.set(new DelayPlan(value, WriteTarget.NUMERIC, false, 5100, false));
    }

    void armPromotion() {
      phase.set(
          new DelayPlan(CommitPhase.AFTER_DELEGATE, WriteTarget.PROMOTION, false, 5100, false));
    }

    void armCrash() {
      phase.set(new DelayPlan(CommitPhase.AFTER_DELEGATE, WriteTarget.NUMERIC, true, 0, false));
    }

    void armClockAdvance(CommitPhase value) {
      phase.set(new DelayPlan(value, WriteTarget.NUMERIC, false, 0, false));
    }

    void armBarrier() {
      phase.set(new DelayPlan(CommitPhase.AFTER_DELEGATE, WriteTarget.NUMERIC, false, 0, true));
    }

    void pause(DelayPlan plan) throws Exception {
      if (plan.barrier()) {
        if (!release.await(4, TimeUnit.SECONDS))
          throw new IllegalStateException("Fixture barrier expired");
      } else if (plan.millis() > 0) Thread.sleep(plan.millis());
    }

    Connection wrap(Connection connection) {
      boolean[] capitalWrite = {false};
      boolean[] numericWrite = {false};
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if (method.getName().equals("prepareStatement")) {
                  String sql = ((String) args[0]).toLowerCase(Locale.ROOT);
                  if (sql.startsWith("insert into capital_candidate")) numericWrite[0] = true;
                  DelayPlan selected = phase.get();
                  if (selected != null
                      && (selected.target() == WriteTarget.NUMERIC
                              && sql.startsWith("insert into capital_candidate")
                          || selected.target() == WriteTarget.PROMOTION
                              && sql.startsWith("update control_state "))) capitalWrite[0] = true;
                }
                if (method.getName().equals("commit") && numericWrite[0]) {
                  numericWrite[0] = false;
                  PersistenceFault failure = persistenceFailure.getAndSet(null);
                  if (failure != null) {
                    if (failure == PersistenceFault.AFTER_COMMIT) connection.commit();
                    throw new SQLTransientConnectionException(
                        "Fixture lost numeric COMMIT response", "08006");
                  }
                }
                if (method.getName().equals("commit") && capitalWrite[0]) {
                  DelayPlan selected = phase.getAndSet(null);
                  capitalWrite[0] = false;
                  if (selected != null) {
                    if (selected.phase() == CommitPhase.BEFORE_DELEGATE) {
                      entered.countDown();
                      pause(selected);
                    }
                    if (selected.target() == WriteTarget.NUMERIC && beforeNumericCommit != null)
                      beforeNumericCommit.run();
                    Object result = invoke(connection, method, args);
                    if (selected.target() == WriteTarget.NUMERIC && afterNumericCommit != null)
                      afterNumericCommit.run();
                    if (selected.phase() == CommitPhase.AFTER_DELEGATE) entered.countDown();
                    if (selected.crash()) throw new SimulatedCrash();
                    if (selected.target() == WriteTarget.PROMOTION && afterPromotionCommit != null)
                      afterPromotionCommit.run();
                    if (selected.phase() == CommitPhase.AFTER_DELEGATE) pause(selected);
                    return result;
                  }
                }
                return invoke(connection, method, args);
              });
    }

    static Object invoke(Object target, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException failure) {
        throw failure.getCause();
      }
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class JdbcTiming {
    @Bean
    @Role(org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE)
    static CommitDelay capitalCommitDelay() {
      return new CommitDelay();
    }

    @Bean
    static BeanPostProcessor delayedCapitalDataSource(CommitDelay delay) {
      return new BeanPostProcessor() {
        @Override
        public Object postProcessAfterInitialization(Object bean, String name) {
          if (!(bean instanceof DataSource source)) return bean;
          return new DelegatingDataSource(source) {
            @Override
            public Connection getConnection() throws SQLException {
              return delay.wrap(super.getConnection());
            }

            @Override
            public Connection getConnection(String user, String password) throws SQLException {
              return delay.wrap(super.getConnection(user, password));
            }
          };
        }
      };
    }
  }
}
