package ru.orbita.broker;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import ru.tinkoff.piapi.contract.v1.OrderState;
import ru.tinkoff.piapi.contract.v1.PostOrderResponse;
import ru.ttech.piapi.core.connector.exception.ServiceRuntimeException;

/** Immutable SDK facts attached to the one qualified OrderEvidence; not a second order state. */
public record OrderSdkEvidence(
    Source source,
    String sdkVersion,
    String classifierVersion,
    String responseRequestId,
    String responseAccountId,
    String responseOrderId,
    String tradeOrderId,
    String responseInstrumentId,
    Integer directionValue,
    Integer executionStatusValue,
    boolean statusInfoPresent,
    Integer causeValue,
    Long requestedLots,
    Long executedLots,
    Long remainingLots,
    Long cancelledLots,
    List<ru.tinkoff.piapi.contract.v1.OrderStage> stages,
    Instant receivedAt,
    Instant eventAt,
    Transport transport,
    Integer lotSize,
    List<TradeFact> trades) {
  public enum Source {
    POST_ORDER,
    GET_ORDER_STATE,
    CANCEL_ORDER,
    ORDER_STATE_STREAM,
    TRADES_STREAM
  }

  public record TradeFact(String tradeId, long pieces, java.math.BigDecimal price, Instant at) {}

  public record Transport(
      String grpcStatus, String errorCode, String description, String trackingId) {
    public Transport {
      grpcStatus = diagnostic(grpcStatus, 64);
      errorCode = diagnostic(errorCode, 256);
      description = diagnostic(description, 512);
      trackingId = diagnostic(trackingId, 128);
    }
  }

  public OrderSdkEvidence {
    stages = List.copyOf(stages);
    trades = List.copyOf(trades);
  }

  static OrderSdkEvidence post(PostOrderResponse reply, Instant at) {
    return new OrderSdkEvidence(
        Source.POST_ORDER,
        "1.51",
        "f04-a1-v1",
        reply.getOrderRequestId().isEmpty() ? null : reply.getOrderRequestId(),
        null,
        reply.getOrderId(),
        null,
        reply.getInstrumentUid(),
        reply.getDirectionValue(),
        reply.getExecutionReportStatusValue(),
        false,
        null,
        reply.getLotsRequested(),
        reply.getLotsExecuted(),
        null,
        null,
        List.of(),
        at,
        null,
        null,
        null,
        List.of());
  }

  static OrderSdkEvidence state(OrderState reply, Instant at) {
    return new OrderSdkEvidence(
        Source.GET_ORDER_STATE,
        "1.51",
        "f04-a1-v1",
        reply.getOrderRequestId(),
        null,
        reply.getOrderId(),
        null,
        reply.getInstrumentUid(),
        reply.getDirectionValue(),
        reply.getExecutionReportStatusValue(),
        false,
        null,
        reply.getLotsRequested(),
        reply.getLotsExecuted(),
        null,
        null,
        reply.getStagesList(),
        at,
        orderDate(reply),
        null,
        null,
        List.of());
  }

  private static Instant orderDate(OrderState reply) {
    if (!reply.hasOrderDate()) return null;
    try {
      return TInvestBrokerGateway.instant(reply.getOrderDate());
    } catch (IllegalArgumentException invalidOptionalDate) {
      // Creation time is descriptive; a malformed optional date must not erase fills or identity.
      return null;
    }
  }

  static OrderSdkEvidence failure(Source source, Throwable failure, Instant at) {
    return new OrderSdkEvidence(
        source,
        "1.51",
        "f04-a1-v1",
        null,
        null,
        null,
        null,
        null,
        null,
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
        transport(failure),
        null,
        List.of());
  }

  /** Only the four whitelisted SDK fields are retained. Never call SDK toString/metadata dump. */
  public static Transport transport(Throwable failure) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    StatusRuntimeException grpc = null;
    for (Throwable current = failure;
        current != null && visited.add(current);
        current = current.getCause()) {
      if (current instanceof ServiceRuntimeException sdk) {
        return new Transport(
            sdk.getErrorType().name(),
            sdk.getErrorCode(),
            sdk.getDescription(),
            sdk.getTrackingId());
      }
      if (current instanceof StatusRuntimeException status) grpc = status;
    }
    var status = grpc == null ? Status.fromThrowable(failure) : grpc.getStatus();
    return new Transport(status.getCode().name(), status.getDescription(), null, null);
  }

  private static String diagnostic(String value, int limit) {
    if (value == null) return null;
    // Never persist credentials echoed by a remote error in one of the diagnostic fields.
    String safe =
        value
            .replaceAll("[\\p{Cc}\\p{Cf}]", "")
            .replaceAll("(?i)((?:proxy-)?authorization)\\s*[:=]\\s*[^,;]+", "$1 [redacted]")
            .replaceAll(
                "(?i)(bearer|basic|token|api[_-]?key)\\s*[:=]?\\s*[^\\s,;]+", "$1 [redacted]");
    return safe.length() <= limit ? safe : safe.substring(0, limit);
  }
}
