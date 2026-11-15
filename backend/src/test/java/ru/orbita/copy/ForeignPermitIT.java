package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.control.*;
import ru.orbita.valuation.*;

@SpringBootTest(
    properties = {
      "orbita.source-account=source",
      "orbita.mirror-account=mirror",
      "orbita.margin-disabled-confirmed=true",
      "orbita.worker.enabled=false"
    })
@Import({
  CopyLifecycleIT.Fixture.class,
  ForeignPermitIT.ConversionFixture.class,
  CommitRecoveryIT.JdbcFaults.class
})
class ForeignPermitIT {
  @Autowired CopyService copies;
  @Autowired TradingService trading;
  @Autowired AcceptanceService acceptance;
  @Autowired ControlService control;
  @Autowired AlgorithmVersion algorithms;
  @Autowired CopyLifecycleIT.MutableClock clock;
  @Autowired CopyLifecycleIT.Simulator broker;
  @Autowired JdbcTemplate db;
  @Autowired Rates rates;
  @Autowired CommitRecoveryIT.CommitFault fault;

  @BeforeEach
  void reset() {
    CopyLifecycleIT.reset(db, broker, clock);
    rates.available = true;
    rates.shortQuote = false;
    fault.plan.set(null);
    fault.slowSendCommitMillis.set(0);
    fault.afterSendCommit = null;
    fault.beforeSendCommit = null;
    broker.fillEveryPost = true;
    broker.settlementCurrency = "USD";
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    control.confirmLiveAcceptance();
    clock.now = clock.now.plusSeconds(1);
  }

  @Test
  void laterForeignPermitUsesExactQualifiedRubCostAndFinalIndependentActualCommission() {
    control.issuePermit(new BigDecimal("2210"));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("qualified-foreign"));
    trading.tick();
    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.sent.lots()).isEqualTo(2);
    assertThat(control.status().permitLimit()).isNull();
    assertThat(copies.journal(0, 200).stream().filter(e -> e.category().equals("SEND_STARTED")))
        .singleElement()
        .satisfies(
            e ->
                assertThat(e.detail())
                    .contains("numerator=2210", "denominator=1", "fixture-PRE_SEND"));
    trading.reconcileOnly();
    acceptance.verifyLive();
    assertThat(
            copies.journal(0, 200).stream().filter(e -> e.category().equals("LIVE_COST_VERIFIED")))
        .singleElement()
        .satisfies(
            e ->
                assertThat(e.detail())
                    .contains(
                        "numerator=2210",
                        "denominator=1",
                        "actualFee=1 USD",
                        "fixture-FINAL_COST"));
  }

  @Test
  void unqualifiedConversionAndOverCapForeignCostStayShadowWithoutShrinkingOrConsumingPermit() {
    control.issuePermit(new BigDecimal("2210"));
    rates.available = false;
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("missing-rate"));
    trading.tick();
    assertThat(broker.posts).isZero();
    assertThat(control.status().permitLimit()).isEqualByComparingTo("2210");
    rates.available = true;
    control.issuePermit(new BigDecimal("2209"));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("over-cap"));
    trading.tick();
    assertThat(broker.posts).isZero();
    assertThat(control.status().permitLimit()).isEqualByComparingTo("2209");
  }

  @Test
  void conversionThatExpiresDuringCommitDoesNotAuthorizeThePostAndRequiresAFreshQuote() {
    control.issuePermit(new BigDecimal("2210"));
    rates.shortQuote = true;
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("expired-at-post"));
    fault.slowSendCommitMillis.set(1);
    fault.afterSendCommit = () -> clock.now = clock.now.plusSeconds(2);
    trading.dispatchReady();
    assertThat(broker.posts).isZero();
    assertThat(control.status().permitLimit()).isEqualByComparingTo("2210");
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.status()).isEqualTo("WAITING");
              assertThat(i.reservedMoney()).isZero();
            });
    trading.dispatchReady();
    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.sent.lots()).isEqualTo(2);
  }

  private SourceExecution source(String id) {
    return new SourceExecution(
        "source",
        id,
        "order-" + id,
        "instrument",
        Side.BUY,
        new BigDecimal("2"),
        new BigDecimal("10"),
        "USD",
        clock.instant(),
        clock.instant());
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class ConversionFixture {
    @Bean
    Rates rates(CopyLifecycleIT.MutableClock clock) {
      return new Rates(clock);
    }
  }

  static final class Rates implements QualifiedRateProvider {
    final Clock clock;
    boolean available = true;
    boolean shortQuote;

    Rates(Clock clock) {
      this.clock = clock;
    }

    public Optional<Rate> current(
        Scope scope, Instant from, Instant through, ClockEvidence.Bound current) {
      if (!available) return Optional.empty();
      boolean finalCost = scope.purpose() == Purpose.FINAL_COST;
      return Optional.of(
          new Rate(
              scope,
              new BigInteger(finalCost ? "22100" : "100"),
              new BigInteger(finalCost ? "211" : "1"),
              from.minusSeconds(1),
              through.plusSeconds(shortQuote && !finalCost ? 1 : 5),
              clock.instant().minusSeconds(1),
              clock.instant().plusSeconds(5),
              "fixture-" + scope.purpose()));
    }
  }
}
