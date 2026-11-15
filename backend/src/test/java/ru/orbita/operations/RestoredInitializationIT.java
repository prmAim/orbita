package ru.orbita.operations;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.OrbitaApplication;
import ru.orbita.notifications.NotificationPublisher;
import ru.orbita.notifications.NotificationService;
import ru.orbita.notifications.TelegramTransport;

@Testcontainers
class RestoredInitializationIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine");

  @Test
  void restoredQueueCannotSendBeforeInitializationAndFreshEventsDeliverWithTradingWorkerDisabled()
      throws Exception {
    var probe = new Probe();
    var launch =
        CompletableFuture.supplyAsync(
            () ->
                new SpringApplicationBuilder(OrbitaApplication.class, Configuration.class)
                    .initializers(
                        context ->
                            context
                                .getBeanFactory()
                                .registerSingleton("initializationProbe", probe))
                    .run(
                        "--server.port=0",
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--orbita.restored=true",
                        "--orbita.worker.enabled=false",
                        "--orbita.telegram.enabled=true",
                        "--orbita.telegram.poll-interval=PT0.02S",
                        "--orbita.backup.enabled=false"));
    try {
      try {
        assertThat(probe.seeded.await(45, TimeUnit.SECONDS)).isTrue();
        assertThat(probe.sent.await(300, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(probe.messages).isEmpty();
      } finally {
        probe.allowReady.countDown();
      }
      var context = launch.get(30, TimeUnit.SECONDS);
      var notifications = context.getBean(NotificationService.class);
      assertThat(notifications.get(probe.old).status()).isEqualTo("IGNORED");
      UUID fresh =
          context
              .getBean(TransactionTemplate.class)
              .execute(
                  status ->
                      context
                          .getBean(NotificationPublisher.class)
                          .publish(
                              "FRESH", "SBER", Instant.now(), "Новое событие после инициализации"));
      assertThat(probe.sent.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(probe.messages)
          .singleElement()
          .asString()
          .contains(fresh.toString())
          .doesNotContain(probe.old.toString());
    } finally {
      probe.allowReady.countDown();
      launch.get(30, TimeUnit.SECONDS).close();
    }
  }

  static class Probe {
    final CountDownLatch seeded = new CountDownLatch(1),
        allowReady = new CountDownLatch(1),
        sent = new CountDownLatch(1);
    final List<String> messages = new CopyOnWriteArrayList<>();
    UUID old;
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class Configuration {
    @Bean
    @Primary
    TelegramTransport simulatedTelegram(Probe probe) {
      return text -> {
        probe.messages.add(text);
        probe.sent.countDown();
        return TelegramTransport.Result.success();
      };
    }

    @Bean
    ApplicationRunner holdBeforeReady(
        Probe probe, TransactionTemplate tx, NotificationPublisher publisher) {
      return args -> {
        probe.old =
            tx.execute(
                status ->
                    publisher.publish(
                        "OLD", "SBER", Instant.now(), "Старая очередь восстановленной копии"));
        probe.seeded.countDown();
        if (!probe.allowReady.await(20, TimeUnit.SECONDS))
          throw new IllegalStateException("Initialization test barrier timed out");
      };
    }
  }
}
