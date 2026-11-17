package ru.orbita.broker;

import java.time.Instant;
import java.util.*;
import ru.tinkoff.piapi.contract.v1.*;

/**
 * Raw mirror observations; correlation and decisions belong to the durable application boundary.
 */
public final class MirrorOrderParser {
  private MirrorOrderParser() {}

  static BrokerGateway.OrderEvidence state(OrderStateStreamResponse.OrderState s, Instant at) {
    var raw =
        new OrderSdkEvidence(
            OrderSdkEvidence.Source.ORDER_STATE_STREAM,
            "1.51",
            "f04-a2-v1",
            s.hasOrderRequestId() ? s.getOrderRequestId() : null,
            s.getAccountId(),
            s.getOrderId(),
            s.getTradeOrderId(),
            s.getInstrumentUid(),
            s.getDirectionValue(),
            s.getExecutionReportStatusValue(),
            s.hasStatusInfo(),
            s.hasStatusInfo() ? s.getStatusInfoValue() : null,
            s.getLotsRequested(),
            s.getLotsExecuted(),
            s.getLotsLeft(),
            s.getLotsCancelled(),
            List.of(),
            at,
            s.hasCompletionTime()
                ? date(s.getCompletionTime())
                : s.hasCreatedAt() ? date(s.getCreatedAt()) : null,
            null,
            s.getLotSize(),
            trades(s.getTradesList()));
    return observation(
        raw,
        uuid(raw.responseRequestId()),
        s.getLotsRequested(),
        s.getLotsExecuted(),
        TInvestBrokerGateway.status(s.getExecutionReportStatus()));
  }

  static BrokerGateway.OrderEvidence trades(OrderTrades s, Instant at) {
    var raw =
        new OrderSdkEvidence(
            OrderSdkEvidence.Source.TRADES_STREAM,
            "1.51",
            "f04-a2-v1",
            null,
            s.getAccountId(),
            s.getOrderId(),
            null,
            s.getInstrumentUid(),
            s.getDirectionValue(),
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            List.of(),
            at,
            s.hasCreatedAt() ? date(s.getCreatedAt()) : null,
            null,
            null,
            trades(s.getTradesList()));
    return observation(raw, null, 0, 0, BrokerGateway.OrderStatus.UNKNOWN);
  }

  private static BrokerGateway.OrderEvidence observation(
      OrderSdkEvidence r,
      UUID id,
      long requested,
      long executed,
      BrokerGateway.OrderStatus status) {
    return new BrokerGateway.OrderEvidence(
        id,
        r.responseAccountId(),
        null,
        r.responseInstrumentId(),
        r.directionValue() == OrderDirection.ORDER_DIRECTION_BUY_VALUE
            ? BrokerGateway.Side.BUY
            : r.directionValue() == OrderDirection.ORDER_DIRECTION_SELL_VALUE
                ? BrokerGateway.Side.SELL
                : null,
        requested,
        executed,
        status,
        List.of(),
        r.receivedAt(),
        "Mirror " + r.source(),
        status == BrokerGateway.OrderStatus.REJECTED
                || status == BrokerGateway.OrderStatus.CANCELLED
            ? BrokerGateway.RejectionReason.OTHER
            : BrokerGateway.RejectionReason.NONE,
        null,
        null,
        r);
  }

  private static List<OrderSdkEvidence.TradeFact> trades(List<OrderTrade> trades) {
    return trades.stream()
        .map(
            t ->
                new OrderSdkEvidence.TradeFact(
                    t.getTradeId(),
                    t.getQuantity(),
                    price(t),
                    t.hasDateTime() ? date(t.getDateTime()) : null))
        .toList();
  }

  private static java.math.BigDecimal price(OrderTrade trade) {
    if (!trade.hasPrice()) return null;
    try {
      return TInvestBrokerGateway.decimal(trade.getPrice().getUnits(), trade.getPrice().getNano());
    } catch (IllegalArgumentException invalid) {
      return null;
    }
  }

  private static UUID uuid(String value) {
    try {
      return value == null ? null : UUID.fromString(value);
    } catch (IllegalArgumentException invalid) {
      return null;
    }
  }

  private static Instant date(com.google.protobuf.Timestamp value) {
    try {
      return TInvestBrokerGateway.instant(value);
    } catch (IllegalArgumentException invalid) {
      return null;
    }
  }
}
