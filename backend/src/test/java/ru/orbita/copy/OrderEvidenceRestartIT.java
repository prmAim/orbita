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
  @org.junit.jupiter.params.provider.CsvSource({
    "unknown,false", "resource,false", "lot-size,false", "lot-size,true",
    "volume,false", "volume,true", "uuid-less,false", "uuid-less,true",
    "metadata,false", "metadata,true", "trade,false", "trade,true"
  })
  void lostResponseAndCommittedUnappliedEvidenceSurviveNewApplicationContextWithoutSecondPost(
      String scenario, boolean late) {
    boolean resourceReason = !scenario.equals("unknown");
    boolean mismatch = scenario.equals("lot-size");
    boolean volume = scenario.equals("volume");
    boolean fill = scenario.equals("uuid-less");
    boolean metadata = scenario.equals("metadata");
    boolean trade = scenario.equals("trade");
    boolean durableConflict = mismatch || volume || fill || trade;
    long expectedExecutedLots = fill || trade ? 1 : 0;
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
      if (trade && !late)
        trading.apply(
            OrderEvidenceFixtures.trade(
                request, "restart-trade-fill", 1, "exchange-id", clock.instant()),
            false);
      if (resourceReason && (!metadata || late))
        trading.apply(
            OrderEvidenceFixtures.unary(
                request, OrderStatus.CANCELLED, "exchange-id", clock.instant()),
            false);
      if (late || metadata)
        trading.apply(
            OrderEvidenceFixtures.terminalStream(
                request, OrderStatus.CANCELLED, "exchange-id", 1, clock.instant()),
            false);
      if (trade && late)
        trading.apply(
            OrderEvidenceFixtures.trade(
                request, "restart-trade-fill", 1, "exchange-id", clock.instant()),
            false);
      if (fill) {
        var unlinked =
            new OrderRequest(
                request.accountId(),
                null,
                request.instrumentId(),
                Side.BUY,
                3,
                request.limitPrice());
        trading.apply(
            OrderEvidenceFixtures.stream(
                unlinked,
                OrderStatus.PARTIALLY_FILLED,
                1,
                2,
                0,
                null,
                "exchange-id",
                1,
                clock.instant()),
            false);
      }
      if (volume)
        trading.apply(
            OrderEvidenceFixtures.stream(
                request, OrderStatus.NEW, 0, 2, 1, null, "exchange-id", 1, clock.instant()),
            false);
      var beforeCrash = copies.intents().getFirst().status();
      String expectedBeforeCrash = scenario.equals("unknown") ? "UNKNOWN" : "FINAL";
      assertThat(beforeCrash).isEqualTo(expectedBeforeCrash);
      OrderEvidence receipt;
      if (metadata) {
        receipt =
            OrderEvidenceFixtures.unary(
                request, OrderStatus.CANCELLED, "exchange-id", clock.instant());
      } else if (resourceReason) {
        receipt =
            OrderEvidenceFixtures.terminalStream(
                request, OrderStatus.CANCELLED, "exchange-id", mismatch ? 20 : 1, clock.instant());
      } else {
        receipt = broker.evidence(request, OrderStatus.CANCELLED, 0);
      }
      try {
        db.execute(
            "create function synthetic_restart_failure() returns trigger language plpgsql as $$ begin if new.category='ORDER_EVIDENCE_APPLIED' then raise exception 'synthetic crash before decision commit'; end if; return new; end $$");
        db.execute(
            "create trigger synthetic_restart_failure before insert on journal_entry for each row execute function synthetic_restart_failure()");
        assertThatThrownBy(() -> trading.apply(receipt, false))
            .isInstanceOf(RuntimeException.class);
        assertThat(copies.intents())
            .singleElement()
            .satisfies(i -> assertThat(i.status()).isEqualTo(beforeCrash));
      } finally {
        db.execute("drop trigger if exists synthetic_restart_failure on journal_entry");
        db.execute("drop function if exists synthetic_restart_failure()");
      }
    }
    // Every application service, persistence context, readiness latch and clock authority is new.
    // Only the synthetic external broker and PostgreSQL remain alive.
    if (metadata)
      broker.instrumentHook =
          () -> {
            throw new IllegalStateException("synthetic metadata outage");
          };
    try (var second = start(clock, broker)) {
      var trading = second.getBean(TradingService.class);
      var copies = second.getBean(CopyService.class);
      trading.replayOrderEvidence();
      var journal = second.getBean(OrderEvidenceJournal.class);
      if (durableConflict || metadata) {
        if (metadata) assertThat(copies.intents().getFirst().status()).isEqualTo("FINAL");
        assertThat(
                journal
                    .latestReason(request.requestId())
                    .map(OrderEvidenceJournal.ReasonEvidence::reason))
            .isEqualTo(late ? Optional.of(RejectionReason.OTHER) : Optional.empty());
      }
      broker.instrumentHook = null;
      // A pending UUID-less receipt can precede the receipt that establishes its link.
      trading.tick();
      if (durableConflict || metadata) {
        assertThat(copies.intents().getFirst().executedLots()).isEqualTo(expectedExecutedLots);
        trading.apply(
            OrderEvidenceFixtures.unary(
                request, OrderStatus.CANCELLED, "exchange-id", clock.instant()),
            false);
        var expectedReason =
            metadata
                ? Optional.of(RejectionReason.INSUFFICIENT_RESOURCES)
                : late ? Optional.of(RejectionReason.OTHER) : Optional.empty();
        assertThat(
                journal
                    .latestReason(request.requestId())
                    .map(OrderEvidenceJournal.ReasonEvidence::reason))
            .isEqualTo(expectedReason);
        assertThat(copies.intents())
            .singleElement()
            .satisfies(
                i -> {
                  assertThat(i.status()).isEqualTo("FINAL");
                  assertThat(i.executedLots()).isEqualTo(expectedExecutedLots);
                  assertThat(i.skippedLots()).isEqualTo(3 - expectedExecutedLots);
                  assertThat(i.reservedMoney()).isPositive();
                });
        trading.apply(
            OrderEvidenceFixtures.terminalStream(
                request, OrderStatus.CANCELLED, "exchange-id", 1, clock.instant()),
            false);
      }
      trading.replayOrderEvidence();
      assertThat(copies.intents())
          .singleElement()
          .satisfies(
              i -> {
                assertThat(i.id()).isEqualTo(request.requestId());
                assertThat(i.status()).isEqualTo("FINAL");
                assertThat(i.executedLots()).isEqualTo(expectedExecutedLots);
                assertThat(i.skippedLots()).isEqualTo(3 - expectedExecutedLots);
                assertThat(i.reservedMoney()).isPositive();
              });
      assertThat(
              copies.journal(0, 200).stream()
                  .filter(e -> e.category().equals("NOTIFICATION_ORDER_FINAL_SKIP"))
                  .count())
          .isEqualTo(1);
      assertThat(second.getBean(ControlService.class).status().permitLimit()).isNull();
      if (resourceReason && !durableConflict) {
        assertThat(journal.latestReason(request.requestId()).orElseThrow().reason())
            .isEqualTo(RejectionReason.INSUFFICIENT_RESOURCES);
        assertThat(
                copies.journal(0, 200).stream()
                    .filter(e -> e.category().equals("ORDER_REASON_QUALIFIED"))
                    .count())
            .isEqualTo(metadata && late ? 2 : 1);
      }
      if (durableConflict) {
        assertThat(
                journal
                    .latestReason(request.requestId())
                    .map(OrderEvidenceJournal.ReasonEvidence::reason))
            .isEqualTo(late ? Optional.of(RejectionReason.OTHER) : Optional.empty());
        assertThat(
                copies.journal(0, 200).stream()
                    .filter(e -> e.category().equals("ORDER_REASON_QUALIFIED"))
                    .count())
            .isEqualTo(late ? 1 : 0);
        assertThat(
                copies.journal(0, 200).stream()
                    .filter(e -> e.category().equals("ORDER_REASON_INVALIDATED"))
                    .count())
            .isEqualTo(late ? 1 : 0);
        assertThat(second.getBean(ControlService.class).instruments())
            .singleElement()
            .satisfies(i -> assertThat(i.paused()).isTrue());
      }
      assertThat(broker.posts).isEqualTo(1);
    }
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
