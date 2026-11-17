package ru.orbita.broker;

import static ru.orbita.broker.TInvestBrokerGateway.status;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import ru.orbita.broker.BrokerGateway.*;

/** Narrow F04/B1 rule. A cause refines a separately proven outcome; it never authorizes trading. */
public final class BuyResourceRejectionClassifier {
  public static final String VERSION = "f04-b1-v1";
  public static final String DETAIL = "нехватка средств; подтверждена потоком состояний";

  private BuyResourceRejectionClassifier() {}

  public record Qualification(OrderStatus status, RejectionReason reason, String detail) {}

  /** The caller supplies the saved original attempt and all its received durable evidence. */
  public static Optional<Qualification> classify(
      OrderRequest attempt, List<OrderEvidence> history, boolean onlyOriginalMutation) {
    if (!onlyOriginalMutation
        || attempt.side() != Side.BUY
        || attempt.lots() <= 0
        || attempt.requestId() == null
        || blank(attempt.accountId())
        || blank(attempt.instrumentId())) return Optional.empty();
    for (var stream : history) {
      var s = stream.sdkEvidence();
      if (s == null
          || !identity(attempt, stream)
          || s.source() != OrderSdkEvidence.Source.ORDER_STATE_STREAM
          || !attempt.accountId().equals(s.responseAccountId())
          || !attempt.requestId().toString().equals(s.responseRequestId())
          || blank(s.responseOrderId())
          || blank(s.tradeOrderId())
          || !s.statusInfoPresent()
          || !Integer.valueOf(2).equals(s.causeValue())
          || !terminal(s.executionStatusValue())
          || !Long.valueOf(attempt.lots()).equals(s.requestedLots())
          || !Long.valueOf(0).equals(s.executedLots())
          || !Long.valueOf(0).equals(s.remainingLots())
          || !Long.valueOf(attempt.lots()).equals(s.cancelledLots())
          || !s.trades().isEmpty()) continue;
      for (var unary : history) {
        var u = unary.sdkEvidence();
        if (u != null
            && identity(attempt, unary)
            && u.source() == OrderSdkEvidence.Source.GET_ORDER_STATE
            && attempt.requestId().toString().equals(u.responseRequestId())
            && s.tradeOrderId().equals(u.responseOrderId())
            && s.tradeOrderId().equals(unary.brokerOrderId())
            && s.executionStatusValue().equals(u.executionStatusValue())
            && Long.valueOf(attempt.lots()).equals(u.requestedLots())
            && Long.valueOf(0).equals(u.executedLots())
            && u.stages().isEmpty()
            && history.stream().noneMatch(e -> contradiction(attempt, s, e)))
          return Optional.of(
              new Qualification(stream.status(), RejectionReason.INSUFFICIENT_RESOURCES, DETAIL));
      }
    }
    return Optional.empty();
  }

  private static boolean identity(OrderRequest a, OrderEvidence e) {
    var r = e.sdkEvidence();
    return a.requestId().equals(e.requestId())
        && a.accountId().equals(e.accountId())
        && a.instrumentId().equals(e.instrumentId())
        && e.side() == a.side()
        && e.lotsRequested() == a.lots()
        && e.lotsExecuted() == 0
        && e.fills().isEmpty()
        && r != null
        && r.transport() == null
        && a.instrumentId().equals(r.responseInstrumentId())
        && Integer.valueOf(1).equals(r.directionValue())
        && e.status() == status(r.executionStatusValue());
  }

  private static boolean contradiction(
      OrderRequest a, OrderSdkEvidence candidate, OrderEvidence e) {
    var r = e.sdkEvidence();
    // Foreign accounts are not observations of this attempt even if a UUID happens to match.
    if (!a.accountId().equals(e.accountId())) return false;
    boolean linkedTrade =
        r != null
            && r.source() == OrderSdkEvidence.Source.TRADES_STREAM
            && candidate.tradeOrderId().equals(r.responseOrderId());
    boolean linkedState =
        r != null
            && r.source() == OrderSdkEvidence.Source.ORDER_STATE_STREAM
            && candidate.responseOrderId().equals(r.responseOrderId())
            && candidate.tradeOrderId().equals(r.tradeOrderId());
    if (!a.requestId().equals(e.requestId())
        && !linkedTrade
        && !linkedState
        && (r == null || !a.requestId().toString().equals(r.responseRequestId()))) return false;
    if (!a.instrumentId().equals(e.instrumentId())
        || e.side() != a.side()
        || e.lotsExecuted() != 0
        || !e.fills().isEmpty()) return true;
    if (r == null)
      return e.known()
          && (e.lotsRequested() != a.lots()
              || !blank(e.brokerOrderId()) && !candidate.tradeOrderId().equals(e.brokerOrderId())
              || e.status() == OrderStatus.FILLED
              || e.status() == OrderStatus.PARTIALLY_FILLED
              || terminalStatus(e.status())
                  && e.status() != status(candidate.executionStatusValue()));
    if (!r.trades().isEmpty()
        || !r.stages().isEmpty()
        || r.executedLots() != null && r.executedLots() != 0) return true;
    if (r.source() == OrderSdkEvidence.Source.CANCEL_ORDER) return true;
    if (r.transport() != null) return false; // A failed read supplies no contrary order fact.
    if (e.known() && e.status() != status(r.executionStatusValue())) return true;
    if (!a.instrumentId().equals(r.responseInstrumentId())
        || !Integer.valueOf(1).equals(r.directionValue())) return true;
    if (r.responseAccountId() != null && !a.accountId().equals(r.responseAccountId())) return true;
    if (r.responseRequestId() != null && !a.requestId().toString().equals(r.responseRequestId()))
      return true;
    if (r.requestedLots() != null && r.requestedLots() != a.lots()) return true;
    if (Integer.valueOf(1).equals(r.executionStatusValue())
        || Integer.valueOf(5).equals(r.executionStatusValue())
        || terminal(r.executionStatusValue())
            && !r.executionStatusValue().equals(candidate.executionStatusValue())) return true;
    if (r.source() == OrderSdkEvidence.Source.ORDER_STATE_STREAM) {
      if (!MirrorOrderParser.consistentStateQuantities(r, a.lots())) return true;
      if (!blank(r.responseOrderId())
              && !Objects.equals(candidate.responseOrderId(), r.responseOrderId())
          || !blank(r.tradeOrderId())
              && !Objects.equals(candidate.tradeOrderId(), r.tradeOrderId())) return true;
    } else if (!blank(r.responseOrderId()) && !candidate.tradeOrderId().equals(r.responseOrderId()))
      return true;
    return false;
  }

  private static boolean terminalStatus(OrderStatus s) {
    return s == OrderStatus.REJECTED || s == OrderStatus.CANCELLED;
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static boolean terminal(Integer status) {
    return Integer.valueOf(2).equals(status) || Integer.valueOf(3).equals(status);
  }
}
