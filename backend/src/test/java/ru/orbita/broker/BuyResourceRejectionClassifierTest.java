package ru.orbita.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.orbita.broker.BrokerGateway.*;
import ru.tinkoff.piapi.contract.v1.*;

class BuyResourceRejectionClassifierTest {
  static final UUID ID = UUID.fromString("0d222a67-6b64-40a7-b324-432e9ca0057c");
  static final Instant AT = Instant.parse("2026-11-18T00:00:00Z");
  static final OrderRequest REQUEST =
      new OrderRequest("mirror", ID, "instrument", Side.BUY, 3, BigDecimal.TEN);

  @ParameterizedTest
  @org.junit.jupiter.params.provider.NullSource
  @ValueSource(ints = {0, -1, 20})
  void missingOrContradictoryIndependentLotSizeCannotQualify(Integer lotSize) {
    assertThat(
            BuyResourceRejectionClassifier.classify(
                REQUEST, List.of(stream(2), unary(2)), true, lotSize))
        .isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"same-ids", "foreign-account", "other-internal", "other-exchange"})
  void receivedStateWithoutUuidUsesBothRoleSpecificIdsForContradictionOnly(String relation) {
    var b =
        streamBuilder(5)
            .clearOrderRequestId()
            .clearStatusInfo()
            .setLotsExecuted(1)
            .setLotsLeft(2)
            .setLotsCancelled(0);
    if (relation.equals("foreign-account")) b.setAccountId("foreign");
    if (relation.equals("other-internal")) b.setOrderId("other-internal");
    if (relation.equals("other-exchange")) b.setTradeOrderId("other-exchange");
    var received = MirrorOrderParser.state(b.build(), AT);
    assertThat(
            BuyResourceRejectionClassifier.classify(
                    REQUEST, List.of(received, stream(2), unary(2)), true, 10)
                .isPresent())
        .isEqualTo(!relation.equals("same-ids"));
  }

  @ParameterizedTest
  @ValueSource(ints = {4, 2})
  void earlierStatusVolumeContradictionPreventsLaterZeroQualification(int earlierStatus) {
    var received =
        MirrorOrderParser.state(
            streamBuilder(earlierStatus)
                .clearStatusInfo()
                .setLotsLeft(earlierStatus == 4 ? 2 : 3)
                .setLotsCancelled(earlierStatus == 4 ? 1 : 0)
                .build(),
            AT);
    assertThat(
            BuyResourceRejectionClassifier.classify(
                REQUEST, List.of(received, stream(2), unary(2)), true, 10))
        .isEmpty();
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  void agreesOnExactTerminalZeroWithoutChangingCancelledToRejected(int status) {
    var stream = stream(status);
    var unary = unary(status);
    var result = BuyResourceRejectionClassifier.classify(REQUEST, List.of(stream, unary), true, 10);
    assertThat(result).isPresent();
    assertThat(result.orElseThrow().status())
        .isEqualTo(status == 2 ? OrderStatus.REJECTED : OrderStatus.CANCELLED);
    assertThat(result.orElseThrow().reason()).isEqualTo(RejectionReason.INSUFFICIENT_RESOURCES);
    assertThat(result.orElseThrow().detail())
        .isEqualTo("нехватка средств; подтверждена потоком состояний");
  }

  static OrderEvidence stream(int status) {
    return MirrorOrderParser.state(streamBuilder(status).build(), AT);
  }

  static OrderStateStreamResponse.OrderState.Builder streamBuilder(int status) {
    return OrderStateStreamResponse.OrderState.newBuilder()
        .setAccountId("mirror")
        .setOrderRequestId(ID.toString())
        .setOrderId("internal")
        .setTradeOrderId("exchange")
        .setInstrumentUid("instrument")
        .setDirectionValue(1)
        .setExecutionReportStatusValue(status)
        .setStatusInfoValue(2)
        .setLotsRequested(3)
        .setLotsExecuted(0)
        .setLotsLeft(0)
        .setLotsCancelled(3)
        .setLotSize(10);
  }

  static OrderEvidence unary(int status) {
    return unary(unaryBuilder(status).build());
  }

  static OrderState.Builder unaryBuilder(int status) {
    return OrderState.newBuilder()
        .setOrderRequestId(ID.toString())
        .setOrderId("exchange")
        .setInstrumentUid("instrument")
        .setDirectionValue(1)
        .setExecutionReportStatusValue(status)
        .setLotsRequested(3)
        .setLotsExecuted(0);
  }

  static OrderEvidence unary(OrderState response) {
    var raw = OrderSdkEvidence.state(response, AT);
    return new OrderEvidence(
        ID,
        "mirror",
        "exchange",
        "instrument",
        Side.BUY,
        3,
        0,
        response.getExecutionReportStatusValue() == 2
            ? OrderStatus.REJECTED
            : OrderStatus.CANCELLED,
        List.of(),
        AT,
        "unary",
        RejectionReason.OTHER,
        null,
        null,
        raw);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "absent-cause",
        "other-cause",
        "future-cause",
        "active",
        "partial",
        "filled",
        "requested-zero",
        "requested-different",
        "executed",
        "left",
        "cancelled",
        "trade",
        "account",
        "uuid",
        "absent-uuid",
        "instrument",
        "direction",
        "internal-id",
        "exchange-id"
      })
  void anyMissingStreamConditionCannotQualify(String changed) {
    var builder = streamBuilder(2);
    switch (changed) {
      case "absent-cause" -> builder.clearStatusInfo();
      case "other-cause" -> builder.setStatusInfoValue(4);
      case "future-cause" -> builder.setStatusInfoValue(999);
      case "active" -> builder.setExecutionReportStatusValue(4);
      case "partial" -> builder.setExecutionReportStatusValue(5);
      case "filled" -> builder.setExecutionReportStatusValue(1);
      case "requested-zero" -> builder.setLotsRequested(0);
      case "requested-different" -> builder.setLotsRequested(4);
      case "executed" -> builder.setLotsExecuted(1);
      case "left" -> builder.setLotsLeft(1);
      case "cancelled" -> builder.setLotsCancelled(2);
      case "trade" -> builder.addTrades(OrderTrade.newBuilder().setTradeId("fill").setQuantity(1));
      case "account" -> builder.setAccountId("foreign");
      case "uuid" -> builder.setOrderRequestId(UUID.randomUUID().toString());
      case "absent-uuid" -> builder.clearOrderRequestId();
      case "instrument" -> builder.setInstrumentUid("other");
      case "direction" -> builder.setDirectionValue(2);
      case "internal-id" -> builder.clearOrderId();
      case "exchange-id" -> builder.clearTradeOrderId();
      default -> throw new AssertionError(changed);
    }
    var stream = MirrorOrderParser.state(builder.build(), AT);
    assertThat(
            BuyResourceRejectionClassifier.classify(REQUEST, List.of(stream, unary(2)), true, 10))
        .isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "uuid",
        "absent-uuid",
        "instrument",
        "direction",
        "requested",
        "executed",
        "stage",
        "other-terminal",
        "active",
        "internal-only-id",
        "absent-id"
      })
  void independentUnaryMustConfirmSameExactAttemptAndTerminalZero(String changed) {
    var builder = unaryBuilder(2);
    switch (changed) {
      case "uuid" -> builder.setOrderRequestId(UUID.randomUUID().toString());
      case "absent-uuid" -> builder.clearOrderRequestId();
      case "instrument" -> builder.setInstrumentUid("other");
      case "direction" -> builder.setDirectionValue(2);
      case "requested" -> builder.setLotsRequested(4);
      case "executed" -> builder.setLotsExecuted(1);
      case "stage" -> builder.addStages(OrderStage.newBuilder().setTradeId("fill").setQuantity(1));
      case "other-terminal" -> builder.setExecutionReportStatusValue(3);
      case "active" -> builder.setExecutionReportStatusValue(4);
      case "internal-only-id" -> builder.setOrderId("internal");
      case "absent-id" -> builder.clearOrderId();
      default -> throw new AssertionError(changed);
    }
    assertThat(
            BuyResourceRejectionClassifier.classify(
                REQUEST, List.of(stream(2), unary(builder.build())), true, 10))
        .isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELL",
        "zero-volume",
        "missing-stream",
        "missing-unary",
        "received-fill",
        "received-contradiction"
      })
  void savedAttemptAndAllReceivedEvidenceConstrainQualification(String changed) {
    var attempt =
        new OrderRequest(
            "mirror",
            ID,
            "instrument",
            changed.equals("SELL") ? Side.SELL : Side.BUY,
            changed.equals("zero-volume") ? 0 : 3,
            BigDecimal.TEN);
    var history = new java.util.ArrayList<>(List.of(stream(2), unary(2)));
    if (changed.equals("missing-stream")) history.removeFirst();
    if (changed.equals("missing-unary")) history.removeLast();
    if (changed.equals("received-fill"))
      history.add(unary(unaryBuilder(2).setLotsExecuted(1).build()));
    if (changed.equals("received-contradiction"))
      history.add(unary(unaryBuilder(2).setInstrumentUid("other").build()));
    assertThat(BuyResourceRejectionClassifier.classify(attempt, history, true, 10)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "requested",
        "executed",
        "left",
        "cancelled",
        "direction",
        "status",
        "absent-cause-presence"
      })
  void missingRawScalarEvidenceIsNeverReconstructed(String missing) {
    var e = stream(2);
    var r = e.sdkEvidence();
    var raw =
        new OrderSdkEvidence(
            r.source(),
            r.sdkVersion(),
            r.classifierVersion(),
            r.responseRequestId(),
            r.responseAccountId(),
            r.responseOrderId(),
            r.tradeOrderId(),
            r.responseInstrumentId(),
            missing.equals("direction") ? null : r.directionValue(),
            missing.equals("status") ? null : r.executionStatusValue(),
            !missing.equals("absent-cause-presence"),
            r.causeValue(),
            missing.equals("requested") ? null : r.requestedLots(),
            missing.equals("executed") ? null : r.executedLots(),
            missing.equals("left") ? null : r.remainingLots(),
            missing.equals("cancelled") ? null : r.cancelledLots(),
            r.stages(),
            r.receivedAt(),
            r.eventAt(),
            r.transport(),
            r.lotSize(),
            r.trades());
    assertThat(
            BuyResourceRejectionClassifier.classify(
                REQUEST, List.of(withRaw(e, raw), unary(2)), true, 10))
        .isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "unary-status",
        "unary-account",
        "unary-uuid",
        "unary-instrument",
        "unary-side",
        "unary-volume",
        "unary-executed",
        "unary-order-id",
        "unary-fill"
      })
  void qualifiedWrapperCannotContradictItsRawResponse(String changed) {
    var e = unary(2);
    var bad =
        new OrderEvidence(
            changed.equals("unary-uuid") ? UUID.randomUUID() : e.requestId(),
            changed.equals("unary-account") ? "other" : e.accountId(),
            changed.equals("unary-order-id") ? "internal" : e.brokerOrderId(),
            changed.equals("unary-instrument") ? "other" : e.instrumentId(),
            changed.equals("unary-side") ? Side.SELL : e.side(),
            changed.equals("unary-volume") ? 4 : e.lotsRequested(),
            changed.equals("unary-executed") ? 1 : e.lotsExecuted(),
            changed.equals("unary-status") ? OrderStatus.CANCELLED : e.status(),
            changed.equals("unary-fill")
                ? List.of(new Fill("fill", BigDecimal.ONE, BigDecimal.TEN, "RUB", AT))
                : e.fills(),
            e.observedAt(),
            e.detail(),
            e.rejectionReason(),
            null,
            null,
            e.sdkEvidence());
    assertThat(BuyResourceRejectionClassifier.classify(REQUEST, List.of(stream(2), bad), true, 10))
        .isEmpty();
  }

  static OrderEvidence withRaw(OrderEvidence e, OrderSdkEvidence raw) {
    return new OrderEvidence(
        e.requestId(),
        e.accountId(),
        e.brokerOrderId(),
        e.instrumentId(),
        e.side(),
        e.lotsRequested(),
        e.lotsExecuted(),
        e.status(),
        e.fills(),
        e.observedAt(),
        e.detail(),
        e.rejectionReason(),
        null,
        null,
        raw);
  }

  @org.junit.jupiter.api.Test
  void earlierActiveObservationWithoutExchangeIdDoesNotContradictLaterExplicitLink() {
    var early =
        MirrorOrderParser.state(
            streamBuilder(4)
                .clearTradeOrderId()
                .clearStatusInfo()
                .setLotsLeft(3)
                .setLotsCancelled(0)
                .build(),
            AT);
    assertThat(
            BuyResourceRejectionClassifier.classify(
                REQUEST, List.of(early, stream(2), unary(2)), true, 10))
        .isPresent();
  }

  @org.junit.jupiter.api.Test
  void anotherMutatingInstructionExcludesThisNarrowOriginalAttemptPath() {
    assertThat(
            BuyResourceRejectionClassifier.classify(
                REQUEST, List.of(stream(2), unary(2)), false, 10))
        .isEmpty();
  }

  @org.junit.jupiter.api.Test
  void alreadySavedKnownBrokerIdCannotBeIgnoredEvenWithoutSdkPayload() {
    var known =
        new OrderEvidence(
            ID,
            "mirror",
            "another-exchange-order",
            "instrument",
            Side.BUY,
            3,
            0,
            OrderStatus.NEW,
            List.of(),
            AT,
            "saved legacy observation");
    assertThat(
            BuyResourceRejectionClassifier.classify(
                REQUEST, List.of(known, stream(2), unary(2)), true, 10))
        .isEmpty();
  }
}
