package ru.orbita.operations;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.notifications.NotificationPublisher;
import ru.orbita.notifications.NotificationService;
import ru.orbita.notifications.TelegramTransport;

@SpringBootTest(properties = {"orbita.telegram.enabled=false", "orbita.worker.enabled=false"})
@Testcontainers
@Import(NotificationDeliveryIT.TransportConfiguration.class)
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
  @Autowired TransactionTemplate transactions;
  @Autowired Simulator telegram;
  @Autowired MutableClock clock;

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
  void telegramRateLimitBlocksManualClicksAndPreservesStableId() {
    notifications.ignoreRestoredQueue();
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
    notifications.ignoreRestoredQueue();
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
    notifications.ignoreRestoredQueue();
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
    notifications.ignoreRestoredQueue();
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
    notifications.ignoreRestoredQueue();
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
    notifications.ignoreRestoredQueue();
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
  void resolvedUndeliveredBlockKeepsOneIdAndBothOriginalMoscowTimes() {
    notifications.ignoreRestoredQueue();
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
    notifications.ignoreRestoredQueue();
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
    notifications.ignoreRestoredQueue();
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
}
