package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.orbita.account.*;
import ru.orbita.broker.*;
import ru.orbita.control.*;

@SpringBootTest(
    properties = {
      "orbita.source-account=source",
      "orbita.mirror-account=mirror",
      "orbita.clock-uncertainty-millis=0",
      "orbita.margin-disabled-confirmed=true",
      "orbita.worker.enabled=false",
      "orbita.account-read.enabled=false"
    })
@Import({CopyLifecycleIT.Fixture.class, AccountLocalReadIT.ReadFixture.class})
class AccountLocalReadIT {
  @Autowired AccountReadService reads;
  @Autowired ControlService control;
  @Autowired CopyService copies;
  @Autowired TradingService trading;
  @Autowired ReadSimulator broker;
  @Autowired CopyLifecycleIT.MutableClock clock;
  @Autowired JdbcTemplate db;
  @Autowired AlgorithmVersion algorithms;
  @Autowired ManualReconciliationService manual;

  @BeforeEach
  void reset() {
    CopyLifecycleIT.reset(db, broker, clock);
    db.execute("truncate account_read_document,account_read_freshness,account_read_revision");
    broker.displayOrders = List.of();
  }

  @Test
  void unknownOwnIntentHasStableIdentityWithoutBankIdAndFinalMovesToHistory() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    var decision =
        copies.accept(
            new BrokerGateway.SourceExecution(
                "source",
                "local-source",
                "local-source-order",
                "instrument",
                BrokerGateway.Side.BUY,
                BigDecimal.ONE,
                new BigDecimal("100"),
                "RUB",
                clock.instant(),
                clock.instant()));
    assertThat(reads.page("orders", AccountQuery.defaults()).items()).isEmpty();
    var operation = reads.page("operations", AccountQuery.defaults()).items().getFirst();
    var waiting = reads.exact("ORDER", ((List<?>) operation.get("orderIds")).getFirst().toString());
    assertThat(waiting.get("origin")).isEqualTo("ORBITA");
    assertThat(waiting.get("brokerOrderId")).isNull();
    trading.tick();
    var unknown = reads.page("orders", AccountQuery.defaults()).items().getFirst();
    assertThat(unknown.get("status")).isEqualTo("UNKNOWN");
    assertThat(unknown.get("id")).isEqualTo(waiting.get("id"));
    trading.tick();
    trading.tick();
    assertThat(reads.page("orders", AccountQuery.defaults()).total()).isZero();
    var group =
        reads.page("operations", AccountQuery.defaults()).items().stream()
            .filter(o -> decision.executionId().equals(o.get("sourceExecutionId")))
            .findFirst()
            .orElseThrow();
    assertThat(group.get("executedLots")).isEqualTo("1");
    assertThat(((Map<?, ?>) group.get("executionPrice")).get("value")).isEqualTo("100.5");
    assertThat(((List<?>) group.get("fills"))).hasSize(1);
  }

  @Test
  void manualOriginRequiresExactDurableReceiptAndNeverInventsSourceOrCoefficient() {
    control.markReconciled();
    control.start();
    control.pauseInstrument("instrument", "Synthetic manual reconciliation");
    clock.now = clock.now.plusSeconds(1);
    var trade =
        new BrokerGateway.SourceExecution(
            "mirror",
            "manual-fill",
            "",
            "instrument",
            BrokerGateway.Side.BUY,
            new BigDecimal("2"),
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    broker.mirrorExecutions.add(trade);
    var fill =
        new AccountRead.Execution(
            "manual-fill",
            "manual-order",
            "instrument",
            BrokerGateway.Side.BUY,
            new BigDecimal("2"),
            "lots",
            new AccountRead.Price(new BigDecimal("100"), "RUB", "currency per instrument unit"),
            clock.instant(),
            null,
            "Synthetic bank port");
    broker.displayOrders =
        List.of(
            new AccountRead.Order(
                "manual-order",
                "",
                "instrument",
                "SYN",
                BrokerGateway.Side.BUY,
                2,
                2,
                BrokerGateway.OrderStatus.FILLED,
                "LIMIT",
                clock.instant(),
                null,
                List.of(fill),
                true,
                fill.price(),
                AccountRead.Origin.UNKNOWN,
                List.of()));
    reads.refresh();
    assertThat(reads.page("operations", AccountQuery.defaults()).items().getFirst().get("origin"))
        .isEqualTo("UNKNOWN");
    manual.reconcilePaused();
    var operation = reads.page("operations", AccountQuery.defaults()).items().getFirst();
    assertThat(operation.get("origin")).isEqualTo("MANUAL");
    assertThat(operation.get("sourceExecutionId")).isNull();
    assertThat(operation.get("coefficient")).isNull();
    assertThat(broker.posts).isZero();
  }

  @Test
  void repeatedOwnOrdersKeepOneSourceTargetAndUniqueExecutionTotal() {
    broker.fillEveryPost = true;
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    control.confirmLiveAcceptance();
    control.enableAutomatic();
    clock.now = clock.now.plusSeconds(1);
    broker.rejectNext = BrokerGateway.RejectionReason.HALTED;
    var event =
        new BrokerGateway.SourceExecution(
            "source",
            "retry-source",
            "retry-source-order",
            "instrument",
            BrokerGateway.Side.BUY,
            BigDecimal.TEN,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    var source = copies.accept(event);
    trading.tick();
    trading.tick();
    var group =
        reads.page("operations", AccountQuery.defaults()).items().stream()
            .filter(o -> source.executionId().equals(o.get("sourceExecutionId")))
            .findFirst()
            .orElseThrow();
    assertThat((List<?>) group.get("orderIds")).hasSize(2);
    assertThat(group.get("targetLots")).isEqualTo("10");
    assertThat(group.get("executedLots")).isEqualTo("10");
    assertThat((List<?>) group.get("fills")).hasSize(1);
    assertThat(((Map<?, ?>) group.get("executionPrice")).get("value")).isEqualTo("100.5");
    assertThat(
            reads
                .page("operations", AccountQuery.defaults())
                .items()
                .getFirst()
                .get("executedLots"))
        .isEqualTo("10");
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class ReadFixture {
    @Bean
    @Primary
    ReadSimulator readSimulator(CopyLifecycleIT.MutableClock clock) {
      return new ReadSimulator(clock);
    }
  }

  static class ReadSimulator extends CopyLifecycleIT.Simulator {
    List<AccountRead.Order> displayOrders = List.of();

    ReadSimulator(Clock clock) {
      super(clock);
    }

    public AccountRead.Snapshot<AccountRead.Order> readOrders(Collection<String> ids) {
      return new AccountRead.Snapshot<>(
          displayOrders, clock.instant(), clock.instant(), true, List.of());
    }

    public AccountRead.Snapshot<AccountRead.Order> readOrderHistory(
        Instant from, Instant through, Collection<String> ids) {
      return readOrders(ids);
    }

    public AccountRead.Snapshot<AccountRead.Execution> readExecutions(
        Instant from, Instant through) {
      return new AccountRead.Snapshot<>(List.of(), from, through, true, List.of());
    }
  }
}
