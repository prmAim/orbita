package ru.orbita.copy;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.*;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.OrbitaApplication;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.control.*;

@Testcontainers
class OrderEvidenceRestartIT {
  @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11");

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void lostResponseAndCommittedUnappliedEvidenceSurviveNewApplicationContextWithoutSecondPost(
      boolean resourceReason) {
    var clock = new CopyLifecycleIT.MutableClock();
    var broker = new CopyLifecycleIT.Simulator(clock);
    OrderRequest request;
    try (var first = start(clock, broker)) {
      var control = first.getBean(ControlService.class);
      var copies = first.getBean(CopyService.class);
      var trading = first.getBean(TradingService.class);
      CopyLifecycleIT.reset(first.getBean(JdbcTemplate.class), broker, clock);
      control.markReconciled();
      control.start();
      control.markFeedReady(true);
      control.shadowChecked(first.getBean(AlgorithmVersion.class).current(), "BUY", true);
      control.shadowChecked(first.getBean(AlgorithmVersion.class).current(), "SELL", true);
      control
          .confirmLiveAcceptance(); // Synthetic multi-lot fixture, not a real-account acceptance.
      clock.now = clock.now.plusSeconds(1);
      control.issuePermit(new BigDecimal("1000"));
      clock.now = clock.now.plusSeconds(1);
      broker.partialNext = 0;
      broker.afterPostHook =
          () -> {
            throw new IllegalStateException("synthetic accepted request, lost reply");
          };
      copies.accept(
          new SourceExecution(
              "source",
              "restart-trade",
              "restart-order",
              "instrument",
              Side.BUY,
              BigDecimal.valueOf(3),
              new BigDecimal("100"),
              "RUB",
              clock.instant(),
              clock.instant()));
      trading.dispatchReady();
      request = broker.sent;
      broker.afterPostHook = null;
      broker.orders.clear();
      trading.tick();
      trading.tick();
      assertThat(copies.intents())
          .singleElement()
          .satisfies(
              i -> {
                assertThat(i.status()).isEqualTo("UNKNOWN");
                assertThat(i.reservedMoney()).isPositive();
              });
      var db = first.getBean(JdbcTemplate.class);
      if (resourceReason) trading.apply(resourceEvidence(request, clock, false), false);
      db.execute(
          "create function synthetic_restart_failure() returns trigger language plpgsql as $$ begin if new.category='ORDER_EVIDENCE_APPLIED' then raise exception 'synthetic crash before decision commit'; end if; return new; end $$");
      db.execute(
          "create trigger synthetic_restart_failure before insert on journal_entry for each row execute function synthetic_restart_failure()");
      assertThatThrownBy(
              () ->
                  trading.apply(
                      resourceReason
                          ? resourceEvidence(request, clock, true)
                          : broker.evidence(request, OrderStatus.CANCELLED, 0),
                      false))
          .isInstanceOf(RuntimeException.class);
      assertThat(copies.intents())
          .singleElement()
          .satisfies(i -> assertThat(i.status()).isEqualTo(resourceReason ? "FINAL" : "UNKNOWN"));
      db.execute("drop trigger synthetic_restart_failure on journal_entry");
      db.execute("drop function synthetic_restart_failure()");
    }
    // Every application service, persistence context, readiness latch and clock authority is new.
    // Only the synthetic external broker and PostgreSQL remain alive.
    try (var second = start(clock, broker)) {
      var trading = second.getBean(TradingService.class);
      var copies = second.getBean(CopyService.class);
      trading.replayOrderEvidence();
      trading.tick();
      trading.replayOrderEvidence();
      assertThat(copies.intents())
          .singleElement()
          .satisfies(
              i -> {
                assertThat(i.id()).isEqualTo(request.requestId());
                assertThat(i.status()).isEqualTo("FINAL");
                assertThat(i.executedLots()).isZero();
                assertThat(i.skippedLots()).isEqualTo(3);
                assertThat(i.reservedMoney()).isPositive();
              });
      assertThat(
              copies.journal(0, 200).stream()
                  .filter(e -> e.category().equals("NOTIFICATION_ORDER_FINAL_SKIP"))
                  .count())
          .isEqualTo(1);
      assertThat(second.getBean(ControlService.class).status().permitLimit()).isNull();
      if (resourceReason) {
        assertThat(copies.intents().getFirst().reason())
            .isEqualTo("нехватка средств; подтверждена потоком состояний");
        assertThat(
                copies.journal(0, 200).stream()
                    .filter(e -> e.category().equals("ORDER_REASON_QUALIFIED"))
                    .count())
            .isEqualTo(1);
      }
      assertThat(broker.posts).isEqualTo(1);
    }
  }

  private OrderEvidence resourceEvidence(OrderRequest request, Clock clock, boolean stream) {
    var raw =
        new ru.orbita.broker.OrderSdkEvidence(
            stream
                ? ru.orbita.broker.OrderSdkEvidence.Source.ORDER_STATE_STREAM
                : ru.orbita.broker.OrderSdkEvidence.Source.GET_ORDER_STATE,
            "1.51",
            "restart-synthetic",
            request.requestId().toString(),
            stream ? "mirror" : null,
            stream ? "internal-id" : "exchange-id",
            stream ? "exchange-id" : null,
            request.instrumentId(),
            1,
            3,
            stream,
            stream ? 2 : null,
            3L,
            0L,
            stream ? 0L : null,
            stream ? 3L : null,
            List.of(),
            clock.instant(),
            null,
            null,
            stream ? 1 : null,
            List.of());
    return new OrderEvidence(
        request.requestId(),
        "mirror",
        stream ? null : "exchange-id",
        request.instrumentId(),
        Side.BUY,
        3,
        0,
        OrderStatus.CANCELLED,
        List.of(),
        clock.instant(),
        "synthetic durable resource proof",
        RejectionReason.OTHER,
        null,
        null,
        raw);
  }

  private ConfigurableApplicationContext start(
      CopyLifecycleIT.MutableClock clock, CopyLifecycleIT.Simulator broker) {
    return new SpringApplicationBuilder(OrbitaApplication.class)
        .web(WebApplicationType.NONE)
        .registerShutdownHook(false)
        .initializers(
            context -> {
              var registry = (GenericApplicationContext) context;
              registry.registerBean(
                  "restartBroker", BrokerGateway.class, () -> broker, d -> d.setPrimary(true));
              registry.registerBean(
                  "restartClock", Clock.class, () -> clock, d -> d.setPrimary(true));
              registry.registerBean(
                  "restartUtc",
                  ClockEvidence.class,
                  () ->
                      () ->
                          Optional.of(
                              new ClockEvidence.Bound(
                                  clock.instant(), clock.instant(), "synthetic restart clock")),
                  d -> d.setPrimary(true));
            })
        .run(
            "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
            "--spring.datasource.username=" + POSTGRES.getUsername(),
            "--spring.datasource.password=" + POSTGRES.getPassword(),
            "--orbita.source-account=source",
            "--orbita.mirror-account=mirror",
            "--orbita.margin-disabled-confirmed=true",
            "--orbita.worker.enabled=false",
            "--orbita.broker.enabled=false",
            "--orbita.telegram.enabled=false",
            "--orbita.backup.enabled=false");
  }
}
