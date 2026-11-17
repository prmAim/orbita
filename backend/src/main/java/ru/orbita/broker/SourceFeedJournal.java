package ru.orbita.broker;

import java.time.Instant;
import java.util.Optional;

/** Durable application seam; implementations must commit observations before acknowledging them. */
public interface SourceFeedJournal {
  record TimeWindow(Instant earliest, Instant latest) {
    public TimeWindow {
      if (earliest == null || latest == null || latest.isBefore(earliest))
        throw new IllegalArgumentException("Ordered receipt time bounds required");
    }
  }

  Instant observationBoundary();

  Optional<String> sourceOrderId(String accountId, String tradeId);

  Optional<BrokerGateway.SourceExecution> remembered(String accountId, String tradeId);

  java.util.List<BrokerGateway.SourceExecution> pendingTimestampRecovery();

  Instant firstReceived(String accountId, String tradeId, Instant receivedAt);

  default Instant firstReceived(
      String accountId, String tradeId, Instant receivedAt, TimeWindow capturedWindow) {
    return firstReceived(accountId, tradeId, receivedAt);
  }

  default Optional<TimeWindow> receiptTimeWindow(String accountId, String tradeId) {
    return Optional.empty();
  }

  default Optional<TimeWindow> currentTimeWindow() {
    return Optional.empty();
  }

  /** Qualified UTC at the raw arrival point, never a provider-completion timestamp. */
  default Optional<TimeWindow> arrivalTimeWindow(long arrivedNanos, String clockDomain) {
    return Optional.empty();
  }

  void remember(BrokerGateway.SourceExecution execution);

  void accept(BrokerGateway.SourceExecution execution);

  default void acceptRecovered(BrokerGateway.SourceExecution execution) {
    accept(execution);
  }

  void skip(BrokerGateway.SourceExecution execution, String reason);

  void gap(String reason);

  void recovered();

  default void connectionLost(BrokerConnectivityEvents.Cause cause) {}

  default void readSucceeded() {}

  default void persistenceUnavailable() {}

  default void mirrorOrder(BrokerGateway.OrderEvidence evidence) {}

  default boolean supportsMirrorOrders() {
    return false;
  }

  default void mirrorGap() {}

  default void mirrorPersistenceUnavailable() {
    persistenceUnavailable();
  }

  default void mirrorRecovered() {}
}
