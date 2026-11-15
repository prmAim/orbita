package ru.orbita.copy;

import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.*;
import ru.orbita.control.ControlService;
import ru.orbita.control.TimeAuthority;
import ru.orbita.runtime.ApplicationReadiness;

@Component
public class DurableSourceFeedJournal implements SourceFeedJournal {
  private final CopyService copies;
  private final ControlService control;
  private final EntityManager em;
  private final TransactionTemplate tx;
  private final TimeAuthority time;
  private final BrokerConnectivityEvents connectivity;
  private final ApplicationReadiness readiness;

  public DurableSourceFeedJournal(
      CopyService copies,
      ControlService control,
      EntityManager em,
      TransactionTemplate tx,
      TimeAuthority time,
      BrokerConnectivityEvents connectivity,
      ApplicationReadiness readiness) {
    this.copies = copies;
    this.control = control;
    this.em = em;
    this.tx = tx;
    this.time = time;
    this.connectivity = connectivity;
    this.readiness = readiness;
  }

  public Instant observationBoundary() {
    return control.status().observationBoundary();
  }

  public Optional<String> sourceOrderId(String account, String trade) {
    return copies.sourceOrderId(account, trade);
  }

  public Optional<BrokerGateway.SourceExecution> remembered(String account, String trade) {
    return copies.remembered(account, trade);
  }

  public List<BrokerGateway.SourceExecution> pendingTimestampRecovery() {
    return tx.execute(
        _ ->
            em
                .createQuery(
                    "from SourceExecutionEntity where executedAt is null and disposition='RECEIVED' order by receivedAt,id",
                    SourceExecutionEntity.class)
                .getResultList()
                .stream()
                .map(
                    e ->
                        new BrokerGateway.SourceExecution(
                            e.accountId,
                            e.tradeId,
                            e.sourceOrderId,
                            e.instrumentId,
                            BrokerGateway.Side.valueOf(e.side),
                            e.lots,
                            e.price,
                            e.currency,
                            null,
                            e.receivedAt))
                .toList());
  }

  public Instant firstReceived(String account, String trade, Instant at) {
    // An Instant alone does not identify a qualified historical monotonic observation.
    return firstReceived(account, trade, at, null);
  }

  public Instant firstReceived(String account, String trade, Instant at, TimeWindow captured) {
    return tx.execute(
        _ -> {
          control.gate();
          String key = CopyService.key(account, trade);
          var receipt = em.find(IncomingReceipt.class, key);
          if (receipt == null) {
            receipt = new IncomingReceipt(key, at);
            if (captured != null) {
              receipt.firstEarliest = captured.earliest();
              receipt.firstLatest = captured.latest();
            }
            em.persist(receipt);
          }
          return receipt.firstReceived;
        });
  }

  public Optional<TimeWindow> receiptTimeWindow(String account, String trade) {
    return tx.execute(
        _ ->
            Optional.ofNullable(em.find(IncomingReceipt.class, CopyService.key(account, trade)))
                .filter(r -> r.firstEarliest != null && r.firstLatest != null)
                .map(r -> new TimeWindow(r.firstEarliest, r.firstLatest)));
  }

  public Optional<TimeWindow> currentTimeWindow() {
    return time.bound().map(bound -> new TimeWindow(bound.earliest(), bound.latest()));
  }

  public Optional<TimeWindow> arrivalTimeWindow(
      long receivedMonotonicNanos, String receiptClockDomain) {
    return time.boundAtReceipt(receivedMonotonicNanos, receiptClockDomain)
        .map(bound -> new TimeWindow(bound.earliest(), bound.latest()));
  }

  public void accept(BrokerGateway.SourceExecution execution) {
    copies.accept(execution);
  }

  public void remember(BrokerGateway.SourceExecution execution) {
    copies.accept(execution);
  }

  public void acceptRecovered(BrokerGateway.SourceExecution execution) {
    copies.acceptRecovered(execution);
  }

  public void skip(BrokerGateway.SourceExecution execution, String reason) {
    copies.skipSource(execution, reason);
  }

  public void gap(String reason) {
    control.markFeedReady(false);
    control.requireReconciliation(reason);
  }

  public void recovered() {
    control.markFeedReady(true);
    readiness.brokerPersistenceRecovered();
  }

  public void connectionLost(BrokerConnectivityEvents.Cause cause) {
    connectivity.connectionLost(cause);
  }

  public void readSucceeded() {
    connectivity.readSucceeded();
  }

  public void persistenceUnavailable() {
    readiness.brokerPersistenceUnavailable();
  }
}
