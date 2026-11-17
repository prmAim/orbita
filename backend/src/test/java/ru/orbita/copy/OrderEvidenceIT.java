package ru.orbita.copy;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.broker.OrderSdkEvidence;
import ru.orbita.control.*;

@SpringBootTest(
    properties = {
      "orbita.source-account=source", "orbita.mirror-account=mirror",
      "orbita.margin-disabled-confirmed=true", "orbita.worker.enabled=false"
    })
@Import(CopyLifecycleIT.Fixture.class)
class OrderEvidenceIT {
  @Autowired CopyService copies;
  @Autowired TradingService trading;
  @Autowired ControlService control;
  @Autowired AlgorithmVersion algorithms;
  @Autowired CopyLifecycleIT.MutableClock clock;
  @Autowired CopyLifecycleIT.Simulator broker;
  @Autowired JdbcTemplate db;
  @Autowired DurableSourceFeedJournal feed;
  @Autowired ru.orbita.runtime.ApplicationReadiness readiness;

  @BeforeEach
  void reset() {
    CopyLifecycleIT.reset(db, broker, clock);
  }

  @Test
  void ownPendingObservationWithUnqualifiedInstrumentCannotReopenPersistenceReadiness() {
    var request = send(3);
    feed.mirrorPersistenceUnavailable();
    broker.instrumentLotSize = 0;
    try {
      assertThatThrownBy(() -> feed.mirrorOrder(state(request, OrderStatus.NEW, 0, 3, 0, null)))
          .isInstanceOf(RuntimeException.class);
      assertThatThrownBy(feed::mirrorRecovered).isInstanceOf(RuntimeException.class);
      assertThat(readiness.ready()).isFalse();
      assertThat(copies.intents())
          .singleElement()
          .satisfies(i -> assertThat(i.status()).isEqualTo("ACTIVE"));
    } finally {
      broker.instrumentLotSize = 10;
      feed.mirrorRecovered();
    }
  }

  @Test
  void streamBeforePostReplyCannotConsumeIndependentTimerAnchor() {
    broker.afterPostHook =
        () -> trading.apply(state(broker.sent, OrderStatus.NEW, 0, 3, 0, null), false);
    send(3);
    broker.afterPostHook = null;
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.deadlineReliable()).isTrue());
    clock.now = clock.now.plusSeconds(61);
    trading.reconcileOnly();
    assertThat(broker.cancels).isEqualTo(1);
  }

  @Test
  void streamTerminalAfterUnknownStillRequiresIndependentUnaryReconciliation() {
    broker.afterPostHook =
        () -> {
          throw new IllegalStateException("synthetic lost response");
        };
    var request = send(3);
    trading.apply(state(request, OrderStatus.CANCELLED, 0, 0, 3, null), false);
    int reads = broker.orderReads;
    broker.orders.put(request.requestId(), broker.evidence(request, OrderStatus.CANCELLED, 0));
    trading.reconcileOnly();
    assertThat(broker.orderReads).isGreaterThan(reads);
    assertThat(copies.journal(0, 200))
        .anyMatch(
            e ->
                e.category().equals("NOTIFICATION_ORDER_UNKNOWN")
                    && e.detail().contains("Неопределённость результата заявки снята"));
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("FINAL"));
    assertThat(broker.posts).isEqualTo(1);
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(
      value = OrderStatus.class,
      names = {"FILLED", "CANCELLED", "REJECTED"})
  void terminalStatusCannotReleaseReserveWithActiveRemainingQuantity(OrderStatus status) {
    var request = send(3);
    var reserve = copies.intents().getFirst().reservedMoney();
    trading.apply(state(request, status, 0, 3, 0, null), false);
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.status()).isEqualTo("UNKNOWN");
              assertThat(i.reservedMoney()).isEqualByComparingTo(reserve);
              assertThat(i.executedLots()).isZero();
              assertThat(i.skippedLots()).isZero();
            });
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isTrue());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"instrument", "side", "volume", "missing-uuid"})
  void correlationRequiresCompleteConsistentAttemptIdentity(String changed) {
    var request = send(3);
    var altered =
        new OrderRequest(
            "mirror",
            changed.equals("missing-uuid") ? null : request.requestId(),
            changed.equals("instrument") ? "other-instrument" : request.instrumentId(),
            changed.equals("side") ? Side.SELL : request.side(),
            changed.equals("volume") ? 4 : 3,
            request.limitPrice());
    trading.apply(state(altered, OrderStatus.REJECTED, 0, 0, altered.lots(), 2), false);
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.status())
                  .isEqualTo(changed.equals("missing-uuid") ? "ACTIVE" : "UNKNOWN");
              assertThat(i.executedLots()).isZero();
              assertThat(i.skippedLots()).isZero();
              assertThat(i.reservedMoney()).isPositive();
            });
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void duplicateTerminalAndLateUnknownDoNotReopenOrNotifyTwice() {
    var request = send(3);
    var terminal = broker.evidence(request, OrderStatus.CANCELLED, 0);
    trading.apply(terminal, false);
    trading.apply(terminal, false);
    trading.apply(broker.evidence(request, OrderStatus.UNKNOWN, 0), false);
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.status()).isEqualTo("FINAL");
              assertThat(i.skippedLots()).isEqualTo(3);
              assertThat(i.reservedMoney()).isPositive();
            });
    assertThat(
            copies.journal(Long.MAX_VALUE, 200).stream()
                .filter(e -> e.category().equals("ORDER_EVIDENCE_RECEIVED"))
                .count())
        .isEqualTo(4);
    assertThat(count("NOTIFICATION_ORDER_FINAL_SKIP")).isEqualTo(1);
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void lostPostResponseAndNotFoundKeepReserveAndOnePostUntilIndependentTerminal() {
    broker.afterPostHook =
        () -> {
          throw new IllegalStateException("synthetic lost response");
        };
    var request = send(3);
    broker.orders.clear();
    for (int pass = 0; pass < 3; pass++) trading.tick();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.status()).isEqualTo("UNKNOWN");
              assertThat(i.reservedMoney()).isPositive();
              assertThat(i.skippedLots()).isZero();
            });
    control.pauseInstrument("instrument", "owner protective pause");
    var terminal = broker.evidence(request, OrderStatus.CANCELLED, 0);
    broker.orders.put(request.requestId(), terminal);
    trading.reconcileOnly();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("FINAL"));
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isTrue());
    assertThat(control.status().permitLimit()).isNull();
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void foreignAndUnlinkedEventsDoNotApplyAndOwnContradictionKeepsFills() {
    var request = send(3);
    trading.apply(broker.evidence(request, OrderStatus.PARTIALLY_FILLED, 1), false);
    var foreign =
        new OrderRequest(
            "foreign", request.requestId(), "instrument", Side.BUY, 3, request.limitPrice());
    trading.apply(state(foreign, OrderStatus.REJECTED, 0, 0, 3, 2), false);
    var unlinked =
        new OrderRequest(
            "mirror", UUID.randomUUID(), "instrument", Side.BUY, 3, request.limitPrice());
    trading.apply(state(unlinked, OrderStatus.REJECTED, 0, 0, 3, 2), false);
    assertThat(control.instruments()).noneMatch(ControlService.InstrumentStatus::paused);
    var wrongSide =
        new OrderRequest(
            "mirror", request.requestId(), "instrument", Side.SELL, 3, request.limitPrice());
    trading.apply(state(wrongSide, OrderStatus.REJECTED, 0, 0, 3, 2), false);
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.status()).isEqualTo("UNKNOWN");
              assertThat(i.executedLots()).isEqualTo(1);
              assertThat(i.skippedLots()).isZero();
              assertThat(i.reservedMoney()).isPositive();
            });
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isTrue());
  }

  @Test
  void partialAfterCancelAndLateUniqueTradePreserveTerminalAndCorrectUnits() {
    var request = send(3);
    trading.apply(broker.evidence(request, OrderStatus.PARTIALLY_FILLED, 1), false);
    trading.apply(state(request, OrderStatus.PARTIALLY_FILLED, 1, 0, 2, 2), false);
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("ACTIVE"));
    trading.apply(broker.evidence(request, OrderStatus.CANCELLED, 1), false);
    var raw =
        new OrderSdkEvidence(
            OrderSdkEvidence.Source.TRADES_STREAM,
            "1.51",
            "f04-a2-v1",
            null,
            "mirror",
            "exchange-id",
            null,
            "instrument",
            1,
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            List.of(),
            clock.instant(),
            null,
            null,
            null,
            List.of(
                new OrderSdkEvidence.TradeFact(
                    "late-unique", 10, new BigDecimal("100"), clock.instant())));
    var trade =
        new OrderEvidence(
            null,
            "mirror",
            null,
            "instrument",
            Side.BUY,
            0,
            0,
            OrderStatus.UNKNOWN,
            List.of(),
            clock.instant(),
            "mirror trade pieces",
            RejectionReason.NONE,
            null,
            null,
            raw);
    trading.apply(trade, false);
    trading.apply(trade, false);
    trading.apply(state(request, OrderStatus.PARTIALLY_FILLED, 1, 0, 2, 2), false);
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.status()).isEqualTo("FINAL");
              assertThat(i.executedLots()).isEqualTo(2);
              assertThat(i.skippedLots()).isEqualTo(1);
            });
    assertThat(count("NOTIFICATION_ORDER_FINAL_SKIP")).isEqualTo(1);
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void lateCauseAndStateBeforeUnaryDoNotDuplicateFinalDecision() {
    broker.afterPostHook =
        () -> trading.apply(state(broker.sent, OrderStatus.REJECTED, 0, 0, 3, null), false);
    var request = send(3);
    broker.afterPostHook = null;
    trading.apply(state(request, OrderStatus.REJECTED, 0, 0, 3, 2), false);
    trading.replayOrderEvidence();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.status()).isEqualTo("FINAL");
              assertThat(i.skippedLots()).isEqualTo(3);
              assertThat(i.reservedMoney()).isPositive();
            });
    assertThat(count("NOTIFICATION_ORDER_FINAL_SKIP")).isEqualTo(1);
    assertThat(copies.journal(0, 200))
        .anySatisfy(
            e -> {
              assertThat(e.category()).isEqualTo("ORDER_EVIDENCE_RECEIVED");
              assertThat(e.detail()).contains("\"causeValue\":2", "internal-id", "exchange-id");
            });
  }

  @Test
  void receiptFailurePreventsApplicationAndCommittedReceiptReplaysAfterDecisionFailure() {
    var request = send(3);
    var terminal = broker.evidence(request, OrderStatus.CANCELLED, 0);
    failJournal("ORDER_EVIDENCE_RECEIVED");
    try {
      assertThatThrownBy(() -> trading.apply(terminal, false)).isInstanceOf(RuntimeException.class);
    } finally {
      clearFailure();
    }
    trading.replayOrderEvidence();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("ACTIVE"));
    failJournal("ORDER_EVIDENCE_APPLIED");
    try {
      assertThatThrownBy(() -> trading.apply(terminal, false)).isInstanceOf(RuntimeException.class);
    } finally {
      clearFailure();
    }
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("ACTIVE"));
    trading.replayOrderEvidence();
    trading.replayOrderEvidence();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("FINAL"));
    assertThat(count("NOTIFICATION_ORDER_FINAL_SKIP")).isEqualTo(1);
    assertThat(broker.posts).isEqualTo(1);
  }

  private long count(String category) {
    return copies.journal(0, 200).stream().filter(e -> e.category().equals(category)).count();
  }

  private void failJournal(String category) {
    db.execute(
        "create or replace function synthetic_order_write_failure() returns trigger language plpgsql as $$ begin if new.category='"
            + category
            + "' then raise exception 'synthetic evidence failure'; end if; return new; end $$");
    db.execute(
        "create trigger synthetic_order_write_failure before insert on journal_entry for each row execute function synthetic_order_write_failure()");
  }

  private void clearFailure() {
    db.execute("drop trigger if exists synthetic_order_write_failure on journal_entry");
    db.execute("drop function if exists synthetic_order_write_failure()");
  }

  private OrderEvidence state(
      OrderRequest request,
      OrderStatus status,
      long executed,
      long left,
      long cancelled,
      Integer cause) {
    int statusValue =
        switch (status) {
          case NEW -> 4;
          case PARTIALLY_FILLED -> 5;
          case FILLED -> 1;
          case CANCELLED -> 3;
          case REJECTED -> 2;
          default -> 0;
        };
    var raw =
        new OrderSdkEvidence(
            OrderSdkEvidence.Source.ORDER_STATE_STREAM,
            "1.51",
            "f04-a2-v1",
            request.requestId() == null ? null : request.requestId().toString(),
            request.accountId(),
            "internal-id",
            "exchange-id",
            request.instrumentId(),
            request.side() == Side.BUY ? 1 : 2,
            statusValue,
            cause != null,
            cause,
            request.lots(),
            executed,
            left,
            cancelled,
            List.of(),
            clock.instant(),
            null,
            null,
            10,
            List.of());
    return new OrderEvidence(
        request.requestId(),
        request.accountId(),
        null,
        request.instrumentId(),
        request.side(),
        request.lots(),
        executed,
        status,
        List.of(),
        clock.instant(),
        "raw stream state",
        RejectionReason.OTHER,
        null,
        null,
        raw);
  }

  private OrderRequest send(long lots) {
    broker.instrumentLotSize = 10;
    broker.individualLotFills = true;
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    control.confirmLiveAcceptance(); // Synthetic fixture permits a multi-lot attempt; no live
    // acceptance is performed.
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("10000"));
    clock.now = clock.now.plusSeconds(1);
    broker.partialNext = 0;
    copies.accept(
        new SourceExecution(
            "source",
            "evidence-trade",
            "evidence-order",
            "instrument",
            Side.BUY,
            BigDecimal.valueOf(lots),
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    trading.dispatchReady();
    return broker.sent;
  }
}
