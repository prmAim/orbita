package ru.orbita.copy;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.OrbitaApplication;
import ru.orbita.broker.BrokerConnectivityEvents;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.broker.OrderSdkEvidence;
import ru.orbita.broker.WholeChainBrokerSimulator;
import ru.orbita.control.*;
import ru.tinkoff.piapi.contract.v1.OrderExecutionReportStatus;

/** Full local TLS and PostgreSQL evidence path. No live broker or account is used. */
@Testcontainers
class BuyResourceRejectionWholeChainIT {
  @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11");

  private static final BrokerConnectivityEvents NO_EVENTS =
      new BrokerConnectivityEvents() {
        public void connectionLost(Cause cause) {}

        public void readSucceeded() {}

        public void restoredCopyOpened() {}
      };

  @TempDir Path directory;
  private WholeChainBrokerSimulator bank;
  private CopyLifecycleIT.MutableClock clock;
  private ConfigurableApplicationContext app;

  @BeforeEach
  void open() throws Exception {
    bank = WholeChainBrokerSimulator.open(directory.resolve(UUID.randomUUID().toString()));
    clock = new CopyLifecycleIT.MutableClock();
    app = start();
    CopyLifecycleIT.reset(app.getBean(JdbcTemplate.class), clock);
  }

  @AfterEach
  void close() throws Exception {
    closeApplication();
    bank.close();
  }

  @ParameterizedTest
  @EnumSource(
      value = OrderExecutionReportStatus.class,
      names = {"EXECUTION_REPORT_STATUS_CANCELLED", "EXECUTION_REPORT_STATUS_REJECTED"})
  void b01RealSdkStreamAndUnaryFinalizeQualifiedZeroFillOnce(
      OrderExecutionReportStatus terminalStatus) throws Exception {
    prepare(true);
    bank.setFinalCommissionAvailable(false);
    UUID requestId = postSource("b01-trade", "b01-order", 10, new BigDecimal("100"), 0);
    OrderStatus expectedStatus = orderStatus(terminalStatus);

    bank.finishWithoutStream(requestId, terminalStatus);
    bank.emitOrderState(requestId, 2);
    awaitFinal(requestId);
    app.getBean(TradingService.class).reconcileOnly();

    assertFinalZeroFill(requestId);
    var attempt = orderRequest(requestId, 10, new BigDecimal("100"));
    assertPersistedSdkSources(attempt, expectedStatus, true);
    assertQualifiedReason(requestId, expectedStatus);
    assertThat(count("ORDER_REASON_QUALIFIED")).isEqualTo(1);
    assertThat(count("NOTIFICATION_ORDER_FINAL_SKIP")).isEqualTo(1);
    assertThat(bank.calls("PostOrder"))
        .singleElement()
        .satisfies(
            call -> {
              var post = (ru.tinkoff.piapi.contract.v1.PostOrderRequest) call.request();
              assertThat(post.getQuantity()).isEqualTo(10);
              assertThat(post.getOrderId()).isEqualTo(requestId.toString());
            });
  }

  @Test
  void b03RestartAndNewSourceExecutionSendOnlyTheNewTwoLots() throws Exception {
    prepare(false);
    UUID first = postSource("b03-first-trade", "same-source-order", 10, new BigDecimal("100"), 0);
    bank.cancelWithoutStream(first);
    bank.emitOrderState(first, 2);
    awaitFinal(first);
    app.getBean(TradingService.class).reconcileOnly();
    assertFinalZeroFill(first);
    var firstAttempt = orderRequest(first, 10, new BigDecimal("100"));
    assertPersistedSdkSources(firstAttempt, OrderStatus.CANCELLED, true);
    assertQualifiedReason(first, OrderStatus.CANCELLED);
    assertThat(app.getBean(ControlService.class).status().coefficient())
        .isEqualByComparingTo(BigDecimal.ONE);

    // Top up the broker resources and change the quote returned by getOrderBook.
    bank.setCapitals(new BigDecimal("10000"), new BigDecimal("20000"));
    bank.setResources(WholeChainBrokerSimulator.INSTRUMENT, new BigDecimal("20000"), 100, 100);
    bank.setQuote(WholeChainBrokerSimulator.INSTRUMENT, new BigDecimal("105"));
    closeApplication();
    app = start(); // The static PostgreSQL container and TLS broker survive this context restart.
    attachFeed();
    var control = app.getBean(ControlService.class);
    assertThat(control.status().mode().toString()).isNotEqualTo("AUTOMATIC");
    assertThat(control.status().coefficient()).isEqualByComparingTo(BigDecimal.ONE);
    control.issuePermit(new BigDecimal("10000"));

    // Replayed source and state events cannot reopen or repost the qualified first attempt.
    bank.emitOrderState(first, 2);
    bank.replaySource("b03-first-trade");
    await(() -> countStreamEvidence(firstAttempt) >= 2);
    var trading = app.getBean(TradingService.class);
    trading.dispatchReady();
    trading.reconcileOnly();
    trading.dispatchReady();
    assertThat(bank.calls("PostOrder")).hasSize(1);
    assertFinalZeroFill(first);
    assertPersistedSdkSources(firstAttempt, OrderStatus.CANCELLED, true);
    assertQualifiedReason(first, OrderStatus.CANCELLED);

    UUID second = postSource("b03-second-trade", "same-source-order", 2, new BigDecimal("110"), 2);
    awaitFinal(second);

    var intents = app.getBean(CopyService.class).intents();
    assertThat(intents).hasSize(2);
    assertThat(intents)
        .extracting(CopyIntent.View::sourceOrderId)
        .containsOnly("same-source-order");
    assertThat(intents).extracting(CopyIntent.View::lots).containsExactly(2L, 10L);
    assertThat(intents.stream().mapToLong(CopyIntent.View::lots).sum()).isEqualTo(12);
    assertThat(intents)
        .anySatisfy(
            intent -> {
              assertThat(intent.id()).isEqualTo(first);
              assertThat(intent.status()).isEqualTo("FINAL");
              assertThat(intent.executedLots()).isZero();
              assertThat(intent.skippedLots()).isEqualTo(10);
            });
    assertThat(bank.calls("PostOrder"))
        .extracting(call -> ((ru.tinkoff.piapi.contract.v1.PostOrderRequest) call.request()))
        .extracting(ru.tinkoff.piapi.contract.v1.PostOrderRequest::getQuantity)
        .containsExactly(10L, 2L);
    var posted =
        bank.calls("PostOrder").stream()
            .map(call -> (ru.tinkoff.piapi.contract.v1.PostOrderRequest) call.request())
            .toList();
    assertThat(price(posted.get(0))).isNotEqualByComparingTo(price(posted.get(1)));
    assertThat(count("NOTIFICATION_ORDER_FINAL_SKIP")).isEqualTo(1);
  }

  @Test
  void b04LateQualifiedStreamRefinesOtherWithoutRepeatingTheFinalDecision() throws Exception {
    prepare(true);
    bank.setFinalCommissionAvailable(false);
    UUID requestId = postSource("b04-trade", "b04-order", 10, new BigDecimal("100"), 0);
    bank.cancelWithoutStream(requestId);

    // The independent unary result is terminal OTHER before the cause-bearing stream arrives.
    app.getBean(TradingService.class).reconcileOnly();
    awaitFinal(requestId);
    assertThat(app.getBean(OrderEvidenceJournal.class).latestReason(requestId)).isEmpty();
    assertThat(count("NOTIFICATION_ORDER_FINAL_SKIP")).isEqualTo(1);
    var attempt = orderRequest(requestId, 10, new BigDecimal("100"));
    assertThat(evidenceHistory(attempt))
        .anySatisfy(
            evidence -> {
              assertThat(evidence.sdkEvidence().source())
                  .isEqualTo(OrderSdkEvidence.Source.GET_ORDER_STATE);
              assertThat(evidence.status()).isEqualTo(OrderStatus.CANCELLED);
              assertThat(evidence.rejectionReason()).isEqualTo(RejectionReason.OTHER);
            });

    bank.emitOrderState(requestId, 2);
    bank.emitOrderState(requestId, 2);
    await(() -> countStreamEvidence(attempt) == 2);
    app.getBean(TradingService.class).reconcileOnly();
    await(
        () ->
            app.getBean(OrderEvidenceJournal.class)
                    .latestReason(requestId)
                    .map(r -> r.reason() == RejectionReason.INSUFFICIENT_RESOURCES)
                    .orElse(false)
                && count("ORDER_REASON_QUALIFIED") == 1);

    assertFinalZeroFill(requestId);
    assertQualifiedReason(requestId, OrderStatus.CANCELLED);
    assertThat(count("ORDER_REASON_QUALIFIED")).isEqualTo(1);
    assertThat(count("ORDER_REASON_INVALIDATED")).isZero();
    assertThat(count("NOTIFICATION_ORDER_FINAL_SKIP")).isEqualTo(1);
    assertThat(bank.calls("PostOrder")).hasSize(1);
  }

  @Test
  void b05HoldsFeeReserveAndConsumedPermitUntilIndependentFeeEvidence() throws Exception {
    prepare(false);
    var control = app.getBean(ControlService.class);
    UUID permitId = control.gate().permitId();
    assertThat(permitId).isNotNull();
    bank.setFinalCommissionAvailable(false);
    UUID requestId = postSource("b05-trade", "b05-order", 10, new BigDecimal("100"), 0);
    control.pauseInstrument(WholeChainBrokerSimulator.INSTRUMENT, "owner manual protection");

    bank.cancelWithoutStream(requestId);
    bank.emitOrderState(requestId, 2);
    awaitFinal(requestId);

    var beforeUnary = persisted(requestId);
    assertThat(beforeUnary.get("broker_order_id")).isEqualTo(bank.brokerOrderId(requestId));
    assertThat(beforeUnary.get("consumed_permit_id")).isEqualTo(permitId);
    assertThat(beforeUnary.get("actual_commission")).isNull();
    assertThat((BigDecimal) beforeUnary.get("reserved_money"))
        .isEqualByComparingTo(WholeChainBrokerSimulator.COMMISSION.multiply(BigDecimal.valueOf(2)));
    assertThat(app.getBean(CopyService.class).intents().getFirst().reservedMoney())
        .isEqualByComparingTo("2"); // Principal released; fee and additional reserve remain.

    app.getBean(TradingService.class).reconcileOnly();
    var withoutFee = persisted(requestId);
    assertThat(withoutFee.get("broker_order_id")).isEqualTo(bank.brokerOrderId(requestId));
    assertThat(withoutFee.get("consumed_permit_id")).isEqualTo(permitId);
    assertThat(withoutFee.get("actual_commission")).isNull();
    assertThat((BigDecimal) withoutFee.get("reserved_money"))
        .isEqualByComparingTo(WholeChainBrokerSimulator.COMMISSION.multiply(BigDecimal.valueOf(2)));
    assertThat(control.status().permitLimit()).isNull();
    assertThatThrownBy(() -> control.issuePermit(new BigDecimal("10000")))
        .isInstanceOf(IllegalStateException.class);
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isTrue());
    assertThat(bank.calls("PostOrder")).hasSize(1);

    bank.setFinalCommissionAvailable(true);
    app.getBean(TradingService.class).reconcileOnly();
    await(
        () -> {
          var finalEvidence = persisted(requestId);
          return finalEvidence.get("actual_commission") != null
              && ((BigDecimal) finalEvidence.get("reserved_money")).signum() == 0;
        });
    var withFee = persisted(requestId);
    // The final explicit SDK report independently sets commission to zero after missing data.
    assertThat((BigDecimal) withFee.get("actual_commission")).isEqualByComparingTo(BigDecimal.ZERO);
    assertThat((BigDecimal) withFee.get("reserved_money")).isEqualByComparingTo(BigDecimal.ZERO);
    assertThat(withFee.get("consumed_permit_id")).isEqualTo(permitId);
    assertThat(control.status().permitLimit()).isNull();
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isTrue());
    assertThat(bank.calls("PostOrder")).hasSize(1);
  }

  private void prepare(boolean automatic) throws Exception {
    var control = app.getBean(ControlService.class);
    control.markReconciled();
    var started = control.start();
    assertThat(started.mode()).as("lastError: %s", started.lastError()).isEqualTo("SHADOW");
    attachFeed();
    control.shadowChecked(app.getBean(AlgorithmVersion.class).current(), "BUY", true);
    control.shadowChecked(app.getBean(AlgorithmVersion.class).current(), "SELL", true);
    control.confirmLiveAcceptance();
    if (automatic) control.enableAutomatic();
    else control.issuePermit(new BigDecimal("10000"));
  }

  private UUID postSource(
      String tradeId, String sourceOrderId, long lots, BigDecimal price, long fill)
      throws Exception {
    int postsBefore = bank.calls("PostOrder").size();
    int intentsBefore = app.getBean(CopyService.class).intents().size();
    bank.setNextPostFill(fill);
    clock.now = clock.now.plusSeconds(1);
    bank.emitSource(
        WholeChainBrokerSimulator.INSTRUMENT,
        tradeId,
        sourceOrderId,
        Side.BUY,
        lots,
        price,
        clock.instant());
    await(() -> app.getBean(CopyService.class).intents().size() > intentsBefore);
    app.getBean(TradingService.class).dispatchReady();
    assertThat(bank.calls("PostOrder")).hasSize(postsBefore + 1);
    var post =
        (ru.tinkoff.piapi.contract.v1.PostOrderRequest) bank.calls("PostOrder").getLast().request();
    return UUID.fromString(post.getOrderId());
  }

  private void awaitFinal(UUID requestId) throws Exception {
    await(
        () ->
            app.getBean(CopyService.class).intents().stream()
                .anyMatch(i -> i.id().equals(requestId) && "FINAL".equals(i.status())));
  }

  private void assertFinalZeroFill(UUID requestId) {
    assertThat(app.getBean(CopyService.class).intents())
        .anySatisfy(
            intent -> {
              assertThat(intent.id()).isEqualTo(requestId);
              assertThat(intent.status()).isEqualTo("FINAL");
              assertThat(intent.executedLots()).isZero();
              assertThat(intent.skippedLots()).isEqualTo(10);
            });
    assertThat(persisted(requestId).get("status")).isEqualTo("FINAL");
    assertThat(((Number) persisted(requestId).get("executed_lots")).longValue()).isZero();
    assertThat(((Number) persisted(requestId).get("skipped_lots")).longValue()).isEqualTo(10);
  }

  private void assertPersistedSdkSources(
      OrderRequest attempt, OrderStatus expectedStatus, boolean requireUnary) {
    var history = evidenceHistory(attempt);
    assertThat(history)
        .anySatisfy(
            evidence ->
                assertThat(evidence.sdkEvidence().source())
                    .isEqualTo(OrderSdkEvidence.Source.POST_ORDER));
    assertThat(history)
        .anySatisfy(
            evidence -> {
              var sdk = evidence.sdkEvidence();
              assertThat(sdk.source()).isEqualTo(OrderSdkEvidence.Source.ORDER_STATE_STREAM);
              assertThat(evidence.status()).isEqualTo(expectedStatus);
              assertThat(sdk.responseRequestId()).isEqualTo(attempt.requestId().toString());
              assertThat(sdk.responseAccountId()).isEqualTo(attempt.accountId());
              assertThat(sdk.executionStatusValue()).isEqualTo(expectedStatusValue(expectedStatus));
              assertThat(sdk.causeValue()).isEqualTo(2);
              assertThat(sdk.requestedLots()).isEqualTo(attempt.lots());
              assertThat(sdk.executedLots()).isZero();
              assertThat(sdk.remainingLots()).isZero();
              assertThat(sdk.cancelledLots()).isEqualTo(attempt.lots());
            });
    if (requireUnary)
      assertThat(history)
          .anySatisfy(
              evidence -> {
                var sdk = evidence.sdkEvidence();
                assertThat(sdk.source()).isEqualTo(OrderSdkEvidence.Source.GET_ORDER_STATE);
                assertThat(evidence.status()).isEqualTo(expectedStatus);
                assertThat(evidence.rejectionReason()).isEqualTo(RejectionReason.OTHER);
                assertThat(sdk.responseRequestId()).isEqualTo(attempt.requestId().toString());
                assertThat(sdk.responseOrderId())
                    .isEqualTo(bank.brokerOrderId(attempt.requestId()));
                assertThat(sdk.executionStatusValue())
                    .isEqualTo(expectedStatusValue(expectedStatus));
                assertThat(sdk.requestedLots()).isEqualTo(attempt.lots());
                assertThat(sdk.executedLots()).isZero();
              });
    assertThat(bank.calls("PostOrder"))
        .anySatisfy(
            call -> {
              var post = (ru.tinkoff.piapi.contract.v1.PostOrderRequest) call.request();
              assertThat(post.getOrderId()).isEqualTo(attempt.requestId().toString());
            });
    if (requireUnary)
      assertThat(bank.calls("GetOrderState"))
          .anySatisfy(
              call -> {
                var lookup = (ru.tinkoff.piapi.contract.v1.GetOrderStateRequest) call.request();
                assertThat(lookup.getOrderId()).isEqualTo(attempt.requestId().toString());
                assertThat(lookup.getOrderIdType())
                    .isEqualTo(ru.tinkoff.piapi.contract.v1.OrderIdType.ORDER_ID_TYPE_REQUEST);
              });
  }

  private List<OrderEvidence> evidenceHistory(OrderRequest attempt) {
    return app.getBean(OrderEvidenceJournal.class).history(attempt).stream()
        .map(pending -> pending.evidence())
        .toList();
  }

  private long countStreamEvidence(OrderRequest attempt) {
    return evidenceHistory(attempt).stream()
        .filter(
            evidence ->
                evidence.sdkEvidence().source() == OrderSdkEvidence.Source.ORDER_STATE_STREAM)
        .count();
  }

  private void assertQualifiedReason(UUID requestId, OrderStatus expectedStatus) {
    var reason = app.getBean(OrderEvidenceJournal.class).latestReason(requestId).orElseThrow();
    assertThat(reason.status()).isEqualTo(expectedStatus);
    assertThat(reason.reason()).isEqualTo(RejectionReason.INSUFFICIENT_RESOURCES);
  }

  private static OrderRequest orderRequest(UUID requestId, long lots, BigDecimal price) {
    return new OrderRequest(
        WholeChainBrokerSimulator.MIRROR,
        requestId,
        WholeChainBrokerSimulator.INSTRUMENT,
        Side.BUY,
        lots,
        price);
  }

  private static OrderStatus orderStatus(OrderExecutionReportStatus status) {
    return status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED
        ? OrderStatus.CANCELLED
        : OrderStatus.REJECTED;
  }

  private static int expectedStatusValue(OrderStatus status) {
    return status == OrderStatus.CANCELLED
        ? OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED.getNumber()
        : OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED.getNumber();
  }

  private long count(String category) {
    return app.getBean(CopyService.class).journal(0, 200).stream()
        .filter(e -> e.category().equals(category))
        .count();
  }

  private java.util.Map<String, Object> persisted(UUID requestId) {
    return app.getBean(JdbcTemplate.class)
        .queryForMap(
            "select status, executed_lots, skipped_lots, reserved_money, broker_order_id, actual_commission, consumed_permit_id from copy_intent where id=?",
            requestId);
  }

  private void attachFeed() throws Exception {
    app.getBean(ru.orbita.broker.SourceFeedLifecycle.class).ensureObservation();
    assertThat(bank.awaitSubscription(Duration.ofSeconds(5))).isTrue();
    assertThat(bank.awaitMirrorOrderSubscription(Duration.ofSeconds(5))).isTrue();
    await(
        () ->
            Boolean.TRUE.equals(
                app.getBean(JdbcTemplate.class)
                    .queryForObject(
                        "select feed_ready from control_state where id=1", Boolean.class)));
    assertThat(app.getBean(ReconciliationService.class).reconcile()).isTrue();
  }

  private ConfigurableApplicationContext start() {
    return new SpringApplicationBuilder(OrbitaApplication.class)
        .web(WebApplicationType.NONE)
        .registerShutdownHook(false)
        .initializers(
            context -> {
              var registry = (GenericApplicationContext) context;
              registry.registerBean(
                  "wholeChainGateway",
                  BrokerGateway.class,
                  () -> {
                    try {
                      return bank.gateway(clock, NO_EVENTS);
                    } catch (Exception failure) {
                      throw new IllegalStateException("Local TLS gateway setup failed", failure);
                    }
                  },
                  definition -> {
                    definition.setPrimary(true);
                    definition.setDestroyMethodName("close");
                  });
              registry.registerBean(
                  "wholeChainClock",
                  Clock.class,
                  () -> clock,
                  definition -> definition.setPrimary(true));
              registry.registerBean(
                  "wholeChainClockEvidence",
                  ClockEvidence.class,
                  () ->
                      new ClockEvidence() {
                        public Optional<Bound> current() {
                          return Optional.of(
                              new Bound(
                                  clock.instant(), clock.instant(), "synthetic whole-chain clock"));
                        }

                        public Optional<Bound> atReceipt(long received) {
                          return current();
                        }
                      },
                  definition -> definition.setPrimary(true));
            })
        .run(
            "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
            "--spring.datasource.username=" + POSTGRES.getUsername(),
            "--spring.datasource.password=" + POSTGRES.getPassword(),
            "--orbita.source-account=" + WholeChainBrokerSimulator.SOURCE,
            "--orbita.mirror-account=" + WholeChainBrokerSimulator.MIRROR,
            "--orbita.margin-disabled-confirmed=true",
            "--orbita.worker.enabled=false",
            "--orbita.broker.enabled=false",
            "--orbita.telegram.enabled=false",
            "--orbita.backup.enabled=false");
  }

  private void closeApplication() throws Exception {
    if (app != null) {
      app.close();
      app = null;
    }
  }

  private void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
    assertThat(condition.getAsBoolean()).isTrue();
  }

  private static BigDecimal price(ru.tinkoff.piapi.contract.v1.PostOrderRequest request) {
    var quotation = request.getPrice();
    return BigDecimal.valueOf(quotation.getUnits()).add(BigDecimal.valueOf(quotation.getNano(), 9));
  }
}
