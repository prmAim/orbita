package ru.orbita.copy;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
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
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.broker.OrderSdkEvidence;
import ru.orbita.broker.WholeChainBrokerSimulator;
import ru.orbita.control.*;
import ru.tinkoff.piapi.contract.v1.OrderExecutionReportStatus;
import ru.tinkoff.piapi.contract.v1.OrderStateStreamResponse;

/** Full local TLS and PostgreSQL evidence path. No live broker or account is used. */
@Testcontainers
class BuyResourceRejectionWholeChainIT {
  @Container final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11");

  private enum PauseScenario {
    MANUAL,
    PROTECTED_POSITION_MISMATCH
  }

  @TempDir Path directory;
  private WholeChainBrokerSimulator bank;
  private CopyLifecycleIT.MutableClock clock;
  private ConfigurableApplicationContext app;

  @BeforeEach
  void open() throws Exception {
    bank = WholeChainBrokerSimulator.open(directory.resolve(UUID.randomUUID().toString()));
    clock = new CopyLifecycleIT.MutableClock();
    app = start();
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
    assertPersistedSdkSources(attempt, expectedStatus);
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
    assertPersistedSdkSources(firstAttempt, OrderStatus.CANCELLED);
    assertQualifiedReason(first, OrderStatus.CANCELLED);
    assertThat(app.getBean(ControlService.class).status().coefficient())
        .isEqualByComparingTo(BigDecimal.ONE);
    assertThat(app.getBean(BrokerGateway.class).resources(firstAttempt).ownMoney())
        .isEqualByComparingTo("10000");
    assertThat((BigDecimal) persisted(first).get("first_best_opposite"))
        .isEqualByComparingTo("100");

    // Top up the synthetic cash available to the empty mirror account.
    bank.setCapitals(new BigDecimal("10000"), new BigDecimal("20000"));
    bank.setResources(WholeChainBrokerSimulator.INSTRUMENT, new BigDecimal("20000"), 100, 100);
    bank.setQuote(WholeChainBrokerSimulator.INSTRUMENT, new BigDecimal("105"));
    assertThat(app.getBean(BrokerGateway.class).resources(firstAttempt).ownMoney())
        .isEqualByComparingTo("20000");
    closeApplication();
    app = start();
    assertFinalZeroFill(first);
    assertQualifiedReason(first, OrderStatus.CANCELLED);
    var control = app.getBean(ControlService.class);
    assertThat(control.status().mode().toString()).isEqualTo("SHADOW");
    assertThat(control.status().coefficient()).isEqualByComparingTo(BigDecimal.ONE);
    attachFeed();
    control.issuePermit(new BigDecimal("10000"));

    // Replayed source and state events cannot reopen or repost the qualified first attempt.
    bank.emitOrderState(first, 2);
    var copies = app.getBean(CopyService.class);
    var original =
        copies.remembered(WholeChainBrokerSimulator.SOURCE, "b03-first-trade").orElseThrow();
    var replayed = copies.accept(original);
    assertThat(replayed.executionId())
        .isEqualTo(CopyService.key(original.accountId(), original.tradeId()));
    assertThat(copies.intents()).hasSize(1);
    await(() -> countStreamEvidence(firstAttempt) >= 2);
    var trading = app.getBean(TradingService.class);
    trading.dispatchReady();
    trading.reconcileOnly();
    trading.dispatchReady();
    assertThat(bank.calls("PostOrder")).hasSize(1);
    assertFinalZeroFill(first);
    assertPersistedSdkSources(firstAttempt, OrderStatus.CANCELLED);
    assertQualifiedReason(first, OrderStatus.CANCELLED);

    UUID second = postSource("b03-second-trade", "same-source-order", 2, new BigDecimal("110"), 2);
    awaitFinal(second);
    assertThat((BigDecimal) persisted(second).get("first_best_opposite"))
        .isEqualByComparingTo("105");

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
    assertThat(intents)
        .anySatisfy(
            intent -> {
              assertThat(intent.id()).isEqualTo(second);
              assertThat(intent.status()).isEqualTo("FINAL");
              assertThat(intent.executedLots()).isEqualTo(2);
              assertThat(intent.skippedLots()).isZero();
            });
    assertThat(bank.calls("PostOrder"))
        .extracting(call -> ((ru.tinkoff.piapi.contract.v1.PostOrderRequest) call.request()))
        .extracting(ru.tinkoff.piapi.contract.v1.PostOrderRequest::getQuantity)
        .containsExactly(10L, 2L);
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
    var journal = app.getBean(OrderEvidenceJournal.class);
    long originalUnarySequence =
        journal.history(attempt).stream()
            .filter(
                pending -> {
                  var evidence = pending.evidence();
                  return evidence.sdkEvidence().source() == OrderSdkEvidence.Source.GET_ORDER_STATE
                      && evidence.status() == OrderStatus.CANCELLED
                      && evidence.rejectionReason() == RejectionReason.OTHER;
                })
            .findFirst()
            .orElseThrow()
            .sequence();

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
    var retainedUnary =
        journal.history(attempt).stream()
            .filter(pending -> pending.sequence() == originalUnarySequence)
            .findFirst()
            .orElseThrow()
            .evidence();
    assertThat(retainedUnary.sdkEvidence().source())
        .isEqualTo(OrderSdkEvidence.Source.GET_ORDER_STATE);
    assertThat(retainedUnary.status()).isEqualTo(OrderStatus.CANCELLED);
    assertThat(retainedUnary.rejectionReason()).isEqualTo(RejectionReason.OTHER);
    assertThat(journal.latestReason(requestId).orElseThrow().receipts())
        .contains(originalUnarySequence);
    assertThat(count("ORDER_REASON_QUALIFIED")).isEqualTo(1);
    assertThat(count("ORDER_REASON_INVALIDATED")).isZero();
    assertThat(count("NOTIFICATION_ORDER_FINAL_SKIP")).isEqualTo(1);
    assertThat(bank.calls("PostOrder")).hasSize(1);
  }

  @ParameterizedTest
  @EnumSource(PauseScenario.class)
  void b05HoldsFeeReserveAndConsumedPermitUntilIndependentFeeEvidence(PauseScenario scenario)
      throws Exception {
    prepare(false);
    var control = app.getBean(ControlService.class);
    UUID permitId = control.gate().permitId();
    assertThat(permitId).isNotNull();
    bank.setFinalCommissionAvailable(false);
    UUID requestId = postSource("b05-trade", "b05-order", 10, new BigDecimal("100"), 0);
    if (scenario == PauseScenario.MANUAL) {
      control.pauseInstrument(WholeChainBrokerSimulator.INSTRUMENT, "owner manual protection");
    } else {
      control.instrumentReconciled(
          WholeChainBrokerSimulator.INSTRUMENT, false, false, "protected position mismatch");
    }

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
    assertQualifiedReason(requestId, OrderStatus.CANCELLED);
    assertFinalZeroFill(requestId);
    assertThat(app.getBean(ReconciliationService.class).reconcile()).isTrue();
    var withoutFee = persisted(requestId);
    assertThat(withoutFee.get("broker_order_id")).isEqualTo(bank.brokerOrderId(requestId));
    assertThat(withoutFee.get("consumed_permit_id")).isEqualTo(permitId);
    assertThat(withoutFee.get("actual_commission")).isNull();
    assertThat((Boolean) withoutFee.get("acceptance_order_reconciled")).isTrue();
    assertThat((Boolean) withoutFee.get("acceptance_reconciled")).isFalse();
    assertThat((BigDecimal) withoutFee.get("reserved_money"))
        .isEqualByComparingTo(WholeChainBrokerSimulator.COMMISSION.multiply(BigDecimal.valueOf(2)));
    assertThat(control.status().reconciled()).isTrue();
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
              && ((BigDecimal) finalEvidence.get("actual_commission")).signum() == 0
              && ((BigDecimal) finalEvidence.get("reserved_money")).signum() == 0;
        });
    assertThat(app.getBean(ReconciliationService.class).reconcile()).isTrue();
    var withFee = persisted(requestId);
    // The final explicit SDK report independently sets commission to zero after missing data.
    assertThat((BigDecimal) withFee.get("actual_commission")).isEqualByComparingTo(BigDecimal.ZERO);
    assertThat((BigDecimal) withFee.get("reserved_money")).isEqualByComparingTo(BigDecimal.ZERO);
    assertThat((Boolean) withFee.get("acceptance_order_reconciled")).isTrue();
    assertThat((Boolean) withFee.get("acceptance_reconciled")).isTrue();
    assertThat(withFee.get("consumed_permit_id")).isEqualTo(permitId);
    assertThat(control.status().permitLimit()).isNull();
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isTrue());
    assertThat(bank.calls("PostOrder")).hasSize(1);
  }

  @Test
  void sellWithChangingStreamOrderIdBooksOneLotAcrossRestart() throws Exception {
    prepare(true);
    var price = new BigDecimal("100");
    // Orbita sells owned lots. Two opening lots expose a second debit after replay.
    UUID openingBuy = postSource("opening-trade", "opening-buy", 2, price, 2);
    awaitFinal(openingBuy);
    UUID sell = postSource("sale-trade", "sale-order", Side.SELL, 1, price, 0);
    var attempt =
        new OrderRequest(
            WholeChainBrokerSimulator.MIRROR,
            sell,
            WholeChainBrokerSimulator.INSTRUMENT,
            Side.SELL,
            1,
            price);
    var newStatus = OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW;
    var fillStatus = OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL;
    String exchangeId = bank.brokerOrderId(sell);
    String internalId = "internal-before-placement";
    var firstNew =
        OrderStateStreamResponse.OrderState.newBuilder()
            .setAccountId(attempt.accountId())
            .setOrderRequestId(sell.toString())
            .setInstrumentUid(attempt.instrumentId())
            .setDirection(ru.tinkoff.piapi.contract.v1.OrderDirection.ORDER_DIRECTION_SELL)
            .setLotSize(1)
            .setLotsRequested(1)
            .setOrderId(internalId)
            .setExecutionReportStatus(newStatus)
            .build();
    var secondNew = firstNew.toBuilder().setOrderId(exchangeId).setLotsLeft(1).build();
    var fill =
        secondNew.toBuilder()
            .setExecutionReportStatus(fillStatus)
            .setLotsLeft(0)
            .setLotsExecuted(1)
            .build();
    assertThat(pausedFor(attempt)).as("before the first NEW").isEmpty();
    bank.emitOrderState(firstNew);
    await(() -> pausedFor(attempt).isPresent());
    var pauseReason = pausedFor(attempt).orElseThrow();
    assertThat(count("ORDER_EVIDENCE_CONTRADICTION")).isOne();
    assertThat(persisted(sell).get("status")).isEqualTo("UNKNOWN");
    bank.emitOrderState(secondNew);
    await(() -> "ACTIVE".equals(persisted(sell).get("status")));
    assertThat(count("ORDER_EVIDENCE_CONTRADICTION")).isOne();
    bank.fillOrder(sell, 1, clock.instant());
    bank.emitOrderState(fill);
    awaitFinal(sell);
    assertSingleSale(openingBuy, sell);
    assertThat(pausedFor(attempt)).as("after stream FILL").contains(pauseReason);
    reconcileFilledSaleByUuid(attempt);
    assertThat(pausedFor(attempt)).as("after confirmed FILL by UUID").contains(pauseReason);
    closeApplication();
    app = start();

    assertThat(pausedFor(attempt)).as("immediately after restart").contains(pauseReason);
    assertSingleSale(openingBuy, sell);
    assertThat(
            evidenceHistory(attempt).stream()
                .map(OrderEvidence::sdkEvidence)
                .filter(e -> e.source() == OrderSdkEvidence.Source.ORDER_STATE_STREAM))
        .extracting(
            OrderSdkEvidence::responseRequestId,
            OrderSdkEvidence::responseOrderId,
            OrderSdkEvidence::executionStatusValue,
            OrderSdkEvidence::executedLots,
            OrderSdkEvidence::remainingLots)
        .containsExactly(
            tuple(sell.toString(), internalId, newStatus.getNumber(), 0L, 0L),
            tuple(sell.toString(), exchangeId, newStatus.getNumber(), 0L, 1L),
            tuple(sell.toString(), exchangeId, fillStatus.getNumber(), 1L, 0L));

    attachFeed();
    bank.emitOrderState(fill);
    await(() -> countStreamEvidence(attempt) == 4);
    var trading = app.getBean(TradingService.class);
    trading.replayOrderEvidence();
    reconcileFilledSaleByUuid(attempt);
    assertThat(pausedFor(attempt)).as("after replay and reconciliation").contains(pauseReason);
    assertSingleSale(openingBuy, sell);
    assertThat(
            app.getBean(JdbcTemplate.class)
                .queryForList(
                    "select lots from broker_fill where intent_id=?", BigDecimal.class, sell))
        .singleElement()
        .satisfies(lots -> assertThat(lots).isEqualByComparingTo(BigDecimal.ONE));
    assertThat(bank.calls("PostOrder")).hasSize(2); // One opening BUY and one SELL.
  }

  private Optional<String> pausedFor(OrderRequest attempt) {
    return app.getBean(ControlService.class).instruments().stream()
        .filter(i -> i.instrumentId().equals(attempt.instrumentId()) && i.paused())
        .map(ControlService.InstrumentStatus::reason)
        .findFirst();
  }

  private void reconcileFilledSaleByUuid(OrderRequest attempt) {
    var priorSequence = app.getBean(OrderEvidenceJournal.class).highWater();
    int priorLookups = bank.calls("GetOrderState").size();
    app.getBean(TradingService.class).reconcileOnly();
    assertThat(bank.calls("GetOrderState").stream().skip(priorLookups))
        .extracting(call -> (ru.tinkoff.piapi.contract.v1.GetOrderStateRequest) call.request())
        .anyMatch(
            r ->
                r.getAccountId().equals(attempt.accountId())
                    && r.getOrderId().equals(attempt.requestId().toString())
                    && r.getOrderIdType()
                        == ru.tinkoff.piapi.contract.v1.OrderIdType.ORDER_ID_TYPE_REQUEST);
    assertThat(app.getBean(OrderEvidenceJournal.class).history(attempt))
        .filteredOn(p -> p.sequence() > priorSequence)
        .extracting(OrderEvidenceJournal.Pending::evidence)
        .anySatisfy(
            e -> {
              assertThat(e.sdkEvidence().source())
                  .isEqualTo(OrderSdkEvidence.Source.GET_ORDER_STATE);
              assertThat(e.sdkEvidence().responseRequestId())
                  .isEqualTo(attempt.requestId().toString());
              assertThat(e.status()).isEqualTo(OrderStatus.FILLED);
              assertThat(e.lotsExecuted()).isOne();
            });
  }

  private void assertSingleSale(UUID openingBuy, UUID sell) {
    var copies = app.getBean(CopyService.class);
    assertThat(copies.intents())
        .extracting(CopyIntent.View::id)
        .containsExactlyInAnyOrder(openingBuy, sell);
    assertThat(copies.intents())
        .filteredOn(i -> i.side().equals("SELL"))
        .extracting(
            CopyIntent.View::id,
            CopyIntent.View::status,
            CopyIntent.View::lots,
            CopyIntent.View::executedLots,
            CopyIntent.View::skippedLots)
        .containsExactly(tuple(sell, "FINAL", 1L, 1L, 0L));
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isOne());
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
    return postSource(tradeId, sourceOrderId, Side.BUY, lots, price, fill);
  }

  private UUID postSource(
      String tradeId, String sourceOrderId, Side side, long lots, BigDecimal price, long fill)
      throws Exception {
    int postsBefore = bank.calls("PostOrder").size();
    int intentsBefore = app.getBean(CopyService.class).intents().size();
    bank.setNextPostFill(fill);
    clock.now = clock.now.plusSeconds(1);
    bank.emitSource(
        WholeChainBrokerSimulator.INSTRUMENT,
        tradeId,
        sourceOrderId,
        side,
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

  private void assertPersistedSdkSources(OrderRequest attempt, OrderStatus expectedStatus) {
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
    assertThat(history)
        .anySatisfy(
            evidence -> {
              var sdk = evidence.sdkEvidence();
              assertThat(sdk.source()).isEqualTo(OrderSdkEvidence.Source.GET_ORDER_STATE);
              assertThat(evidence.status()).isEqualTo(expectedStatus);
              assertThat(evidence.rejectionReason()).isEqualTo(RejectionReason.OTHER);
              assertThat(sdk.responseRequestId()).isEqualTo(attempt.requestId().toString());
              assertThat(sdk.responseOrderId()).isEqualTo(bank.brokerOrderId(attempt.requestId()));
              assertThat(sdk.executionStatusValue()).isEqualTo(expectedStatusValue(expectedStatus));
              assertThat(sdk.requestedLots()).isEqualTo(attempt.lots());
              assertThat(sdk.executedLots()).isZero();
            });
    assertThat(bank.calls("PostOrder"))
        .anySatisfy(
            call -> {
              var post = (ru.tinkoff.piapi.contract.v1.PostOrderRequest) call.request();
              assertThat(post.getOrderId()).isEqualTo(attempt.requestId().toString());
            });
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
            "select status, executed_lots, skipped_lots, reserved_money, broker_order_id, actual_commission, acceptance_order_reconciled, acceptance_reconciled, consumed_permit_id, first_best_opposite from copy_intent where id=?",
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
    return WholeChainTestApplication.start(POSTGRES, bank, clock);
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
}
