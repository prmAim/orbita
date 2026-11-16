package ru.orbita.broker;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Read-only bank facts. Null means unconfirmed, never zero. No record authorizes a mutation. */
public final class AccountRead {
  private AccountRead() {}

  public enum Origin {
    ORBITA,
    MANUAL,
    UNKNOWN
  }

  public record Price(BigDecimal value, String currency, String unit) {}

  public record Execution(
      String tradeId,
      String brokerOrderId,
      String instrumentId,
      BrokerGateway.Side side,
      BigDecimal quantity,
      String quantityUnit,
      Price price,
      Instant executedAt,
      Price commission,
      String source) {}

  public record Order(
      String brokerOrderId,
      String requestId,
      String instrumentId,
      String ticker,
      BrokerGateway.Side side,
      long lotsRequested,
      long lotsExecuted,
      BrokerGateway.OrderStatus status,
      String orderType,
      Instant createdAt,
      Price limitPrice,
      List<Execution> fills,
      boolean fillsComplete,
      Price executionPrice,
      Origin origin,
      List<String> limitations) {
    public Order {
      fills = List.copyOf(fills);
      limitations = List.copyOf(limitations);
    }
  }

  public record Cash(
      String currency,
      BigDecimal total,
      BigDecimal available,
      BigDecimal blocked,
      BigDecimal reportedMoney,
      Instant bankAt,
      String source,
      String limitation) {}

  public record Security(
      String instrumentId,
      String ticker,
      String name,
      Integer lotSize,
      BigDecimal totalPieces,
      BigDecimal totalLots,
      String source,
      String limitation) {}

  public record Snapshot<T>(
      List<T> items,
      Instant requestStarted,
      Instant receivedAt,
      boolean complete,
      List<String> limitations) {
    public Snapshot {
      items = List.copyOf(items);
      limitations = List.copyOf(limitations);
    }
  }

  public record Holdings(Snapshot<Security> securities, Snapshot<Cash> cash) {}
}
