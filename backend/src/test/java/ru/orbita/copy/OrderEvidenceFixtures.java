package ru.orbita.copy;

import java.time.Instant;
import java.util.List;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.broker.OrderSdkEvidence;

final class OrderEvidenceFixtures {
  private OrderEvidenceFixtures() {}

  static OrderEvidence terminalStream(
      OrderRequest request, OrderStatus status, String exchangeId, int lotSize, Instant at) {
    return stream(request, status, 0, 0, request.lots(), 2, exchangeId, lotSize, at);
  }

  static OrderEvidence unary(
      OrderRequest request, OrderStatus status, String exchangeId, Instant at) {
    var raw =
        new OrderSdkEvidence(
            OrderSdkEvidence.Source.GET_ORDER_STATE,
            "1.51",
            "f04-a1-v1",
            request.requestId().toString(),
            null,
            exchangeId,
            null,
            request.instrumentId(),
            request.side() == Side.BUY ? 1 : 2,
            statusValue(status),
            false,
            null,
            request.lots(),
            0L,
            null,
            null,
            List.of(),
            at,
            null,
            null,
            null,
            List.of());
    return new OrderEvidence(
        request.requestId(),
        request.accountId(),
        exchangeId,
        request.instrumentId(),
        request.side(),
        request.lots(),
        0,
        status,
        List.of(),
        at,
        "independent REQUEST lookup",
        RejectionReason.OTHER,
        null,
        null,
        raw);
  }

  static OrderEvidence trade(
      OrderRequest request, String id, long pieces, String exchangeId, Instant at) {
    var raw =
        new OrderSdkEvidence(
            OrderSdkEvidence.Source.TRADES_STREAM,
            "1.51",
            "f04-a2-v1",
            null,
            request.accountId(),
            exchangeId,
            null,
            request.instrumentId(),
            request.side() == Side.BUY ? 1 : 2,
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            List.of(),
            at,
            null,
            null,
            null,
            List.of(new OrderSdkEvidence.TradeFact(id, pieces, request.limitPrice(), at)));
    return new OrderEvidence(
        null,
        request.accountId(),
        null,
        request.instrumentId(),
        request.side(),
        0,
        0,
        OrderStatus.UNKNOWN,
        List.of(),
        at,
        "synthetic trade stream",
        RejectionReason.NONE,
        null,
        null,
        raw);
  }

  static OrderEvidence stream(
      OrderRequest request,
      OrderStatus status,
      long executed,
      long left,
      long cancelled,
      Integer cause,
      String exchangeId,
      int lotSize,
      Instant at) {
    var raw =
        new OrderSdkEvidence(
            OrderSdkEvidence.Source.ORDER_STATE_STREAM,
            "1.51",
            "f04-a2-v1",
            request.requestId() == null ? null : request.requestId().toString(),
            request.accountId(),
            "internal-id",
            exchangeId,
            request.instrumentId(),
            request.side() == Side.BUY ? 1 : 2,
            statusValue(status),
            cause != null,
            cause,
            request.lots(),
            executed,
            left,
            cancelled,
            List.of(),
            at,
            null,
            null,
            lotSize,
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
        at,
        "raw stream state",
        RejectionReason.OTHER,
        null,
        null,
        raw);
  }

  private static int statusValue(OrderStatus status) {
    return switch (status) {
      case NEW -> 4;
      case PARTIALLY_FILLED -> 5;
      case FILLED -> 1;
      case CANCELLED -> 3;
      case REJECTED -> 2;
      default -> 0;
    };
  }
}
