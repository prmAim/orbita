package ru.orbita.broker;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Broker boundary. Unknown evidence is never a rejection or a zero resource. */
public interface BrokerGateway extends AutoCloseable {
  enum Side {
    BUY,
    SELL
  }

  enum OrderStatus {
    NEW,
    PARTIALLY_FILLED,
    FILLED,
    CANCELLED,
    REJECTED,
    UNKNOWN
  }

  enum RejectionReason {
    NONE,
    HALTED,
    INSUFFICIENT_RESOURCES,
    OTHER
  }

  record SourceExecution(
      String accountId,
      String tradeId,
      String sourceOrderId,
      String instrumentId,
      Side side,
      BigDecimal lots,
      BigDecimal price,
      String currency,
      Instant executedAt,
      Instant receivedAt) {}

  record Instrument(
      String id,
      int lotSize,
      BigDecimal priceIncrement,
      String currency,
      boolean buyAvailable,
      boolean sellAvailable,
      boolean apiAvailable) {}

  record OrderRequest(
      String accountId,
      UUID requestId,
      String instrumentId,
      Side side,
      long lots,
      BigDecimal limitPrice) {}

  record Fill(
      String tradeId, BigDecimal lots, BigDecimal price, String currency, Instant executedAt) {}

  record OrderEvidence(
      UUID requestId,
      String accountId,
      String brokerOrderId,
      String instrumentId,
      Side side,
      long lotsRequested,
      long lotsExecuted,
      OrderStatus status,
      List<Fill> fills,
      Instant observedAt,
      String detail,
      RejectionReason rejectionReason,
      Long receivedMonotonicNanos,
      String receiptClockDomain,
      OrderSdkEvidence sdkEvidence) {
    public OrderEvidence {
      fills = List.copyOf(fills);
    }

    public OrderEvidence(
        UUID requestId,
        String accountId,
        String brokerOrderId,
        String instrumentId,
        Side side,
        long lotsRequested,
        long lotsExecuted,
        OrderStatus status,
        List<Fill> fills,
        Instant observedAt,
        String detail,
        RejectionReason rejectionReason,
        Long receivedMonotonicNanos,
        String receiptClockDomain) {
      this(
          requestId,
          accountId,
          brokerOrderId,
          instrumentId,
          side,
          lotsRequested,
          lotsExecuted,
          status,
          fills,
          observedAt,
          detail,
          rejectionReason,
          receivedMonotonicNanos,
          receiptClockDomain,
          null);
    }

    public OrderEvidence(
        UUID requestId,
        String accountId,
        String brokerOrderId,
        String instrumentId,
        Side side,
        long lotsRequested,
        long lotsExecuted,
        OrderStatus status,
        List<Fill> fills,
        Instant observedAt,
        String detail,
        RejectionReason rejectionReason) {
      this(
          requestId,
          accountId,
          brokerOrderId,
          instrumentId,
          side,
          lotsRequested,
          lotsExecuted,
          status,
          fills,
          observedAt,
          detail,
          rejectionReason,
          null,
          null);
    }

    public OrderEvidence(
        UUID requestId,
        String accountId,
        String brokerOrderId,
        String instrumentId,
        Side side,
        long lotsRequested,
        long lotsExecuted,
        OrderStatus status,
        List<Fill> fills,
        Instant observedAt,
        String detail) {
      this(
          requestId,
          accountId,
          brokerOrderId,
          instrumentId,
          side,
          lotsRequested,
          lotsExecuted,
          status,
          fills,
          observedAt,
          detail,
          status == OrderStatus.REJECTED ? RejectionReason.OTHER : RejectionReason.NONE);
    }

    public boolean known() {
      return status != OrderStatus.UNKNOWN;
    }
  }

  record CapitalEvidence(
      String accountId,
      BigDecimal totalRub,
      Instant requestStarted,
      Instant receivedAt,
      boolean compositionConfirmed,
      String detail,
      Long receivedMonotonicNanos,
      String receiptClockDomain) {
    public CapitalEvidence(
        String accountId,
        BigDecimal totalRub,
        Instant requestStarted,
        Instant receivedAt,
        boolean compositionConfirmed,
        String detail) {
      this(
          accountId,
          totalRub,
          requestStarted,
          receivedAt,
          compositionConfirmed,
          detail,
          null,
          null);
    }
  }

  /**
   * Raw own-resource limits remain separately qualified. Preliminary commission covers the quantity
   * in the resource request and is never inferred as zero.
   */
  record ResourceEvidence(
      String accountId,
      Instrument instrument,
      BigDecimal ownMoney,
      long buyMaxLots,
      long sellMaxLots,
      BigDecimal preliminaryCommission,
      BigDecimal additionalCommissionReserve,
      Instant requestStarted,
      Instant receivedAt,
      boolean qualified,
      String detail,
      BigDecimal rubTotal,
      java.util.Map<UUID, BigDecimal> bankReflectedReservations) {
    public ResourceEvidence {
      bankReflectedReservations = java.util.Map.copyOf(bankReflectedReservations);
    }

    public ResourceEvidence(
        String accountId,
        Instrument instrument,
        BigDecimal ownMoney,
        long buyMaxLots,
        long sellMaxLots,
        BigDecimal preliminaryCommission,
        BigDecimal additionalCommissionReserve,
        Instant requestStarted,
        Instant receivedAt,
        boolean qualified,
        String detail) {
      this(
          accountId,
          instrument,
          ownMoney,
          buyMaxLots,
          sellMaxLots,
          preliminaryCommission,
          additionalCommissionReserve,
          requestStarted,
          receivedAt,
          qualified,
          detail,
          null,
          java.util.Map.of());
    }

    public ResourceEvidence(
        String accountId,
        Instrument instrument,
        BigDecimal ownMoney,
        long buyMaxLots,
        long sellMaxLots,
        BigDecimal preliminaryCommission,
        BigDecimal additionalCommissionReserve,
        Instant requestStarted,
        Instant receivedAt,
        boolean qualified,
        String detail,
        BigDecimal rubTotal) {
      this(
          accountId,
          instrument,
          ownMoney,
          buyMaxLots,
          sellMaxLots,
          preliminaryCommission,
          additionalCommissionReserve,
          requestStarted,
          receivedAt,
          qualified,
          detail,
          rubTotal,
          java.util.Map.of());
    }
  }

  record History(
      List<SourceExecution> executions,
      List<String> gaps,
      Instant fromInclusive,
      Instant throughInclusive) {
    public History {
      executions = List.copyOf(executions);
      gaps = List.copyOf(gaps);
    }

    public boolean complete() {
      return gaps.isEmpty();
    }
  }

  record Position(
      String instrumentId,
      BigDecimal availablePieces,
      BigDecimal blockedPieces,
      BigDecimal totalPieces,
      boolean availabilityQualified) {
    public Position(String instrumentId, BigDecimal availablePieces, BigDecimal blockedPieces) {
      this(instrumentId, availablePieces, blockedPieces, availablePieces.add(blockedPieces), true);
    }
  }

  record CommissionEvidence(
      String accountId,
      UUID requestId,
      BigDecimal amount,
      String currency,
      boolean qualified,
      Instant observedAt,
      String detail) {}

  record PositionEvidence(
      String accountId,
      List<Position> positions,
      Instant requestStarted,
      Instant receivedAt,
      boolean complete,
      String detail) {
    public PositionEvidence {
      positions = List.copyOf(positions);
    }
  }

  /** Display-only all-account reads; unknown snapshots must never replace saved facts with zero. */
  default AccountRead.Snapshot<AccountRead.Order> readOrders(
      java.util.Collection<String> retainedBrokerOrderIds) {
    return new AccountRead.Snapshot<>(
        List.of(), null, null, false, List.of("Account order reader unavailable"));
  }

  default AccountRead.Holdings readHoldings() {
    return new AccountRead.Holdings(
        new AccountRead.Snapshot<>(
            List.of(), null, null, false, List.of("Holdings reader unavailable")),
        new AccountRead.Snapshot<>(
            List.of(), null, null, false, List.of("Cash reader unavailable")));
  }

  default AccountRead.Snapshot<AccountRead.Order> readOrderHistory(
      Instant from, Instant through, java.util.Collection<String> retainedBrokerOrderIds) {
    return new AccountRead.Snapshot<>(
        List.of(), null, null, false, List.of("Order history unavailable"));
  }

  default AccountRead.Snapshot<AccountRead.Execution> readExecutions(
      Instant from, Instant through) {
    return new AccountRead.Snapshot<>(
        List.of(), null, null, false, List.of("Execution reader unavailable"));
  }

  default AutoCloseable observeCash(
      java.util.function.Consumer<AccountRead.Snapshot<AccountRead.Cash>> sink) {
    throw new UnsupportedOperationException("Cash stream unavailable");
  }

  Instrument instrument(String instrumentId);

  CapitalEvidence capital(String accountId);

  ResourceEvidence resources(OrderRequest request);

  History history(Instant fromInclusive, Instant throughInclusive);

  default History mirrorHistory(Instant fromInclusive, Instant throughInclusive) {
    return new History(
        List.of(), List.of("Mirror history unavailable"), fromInclusive, throughInclusive);
  }

  default java.util.Optional<BigDecimal> bestOppositePrice(String instrumentId, Side side) {
    return java.util.Optional.empty();
  }

  PositionEvidence positions(String accountId);

  default boolean positionAbsentMeansZero(String instrumentId, PositionEvidence evidence) {
    return false;
  }

  AutoCloseable observe(SourceFeedJournal journal);

  /** Exactly one synchronous PostOrder call; caller persists intent/barrier before invoking. */
  OrderEvidence post(OrderRequest request);

  OrderEvidence reconcile(OrderRequest request);

  OrderEvidence cancel(OrderRequest request, String brokerOrderId);

  default CommissionEvidence commission(OrderRequest request) {
    return new CommissionEvidence(
        request.accountId(),
        request.requestId(),
        null,
        null,
        false,
        null,
        "Independent final commission evidence unavailable");
  }

  @Override
  void close();
}
