package ru.orbita.copy;

import jakarta.persistence.*;
import java.time.Clock;
import java.util.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.control.*;
import ru.orbita.journal.JournalEntry;

/**
 * Independent manual trades are attributed only while the instrument remains paused. No trading
 * command.
 */
@Service
public class ManualReconciliationService {
  private final EntityManager em;
  private final TransactionTemplate tx;
  private final ControlService control;
  private final ObjectProvider<BrokerGateway> gateways;
  private final OrbitaProperties properties;
  private final Clock clock;

  public ManualReconciliationService(
      EntityManager em,
      TransactionTemplate tx,
      ControlService control,
      ObjectProvider<BrokerGateway> gateways,
      OrbitaProperties properties,
      Clock clock) {
    this.em = em;
    this.tx = tx;
    this.control = control;
    this.gateways = gateways;
    this.properties = properties;
    this.clock = clock;
  }

  public void reconcilePaused() {
    var paused =
        control.instruments().stream()
            .filter(ControlService.InstrumentStatus::paused)
            .map(ControlService.InstrumentStatus::instrumentId)
            .collect(java.util.stream.Collectors.toSet());
    if (paused.isEmpty()) return;
    var boundary = control.status().observationBoundary();
    var gateway = gateways.getIfAvailable();
    if (boundary == null || gateway == null) return;
    BrokerGateway.History history;
    try {
      history = gateway.mirrorHistory(boundary, clock.instant());
    } catch (RuntimeException e) {
      PersistenceBoundary.rethrowBrokerPersistence(e);
      return;
    }
    if (!history.complete()) return;
    for (var trade : history.executions()) {
      if (!properties.mirrorAccount().equals(trade.accountId())
          || !paused.contains(trade.instrumentId())
          || trade.executedAt() == null) continue;
      tx.executeWithoutResult(
          _ -> {
            control.gate();
            String id = CopyService.key(trade.accountId(), trade.tradeId());
            if (em.find(MirrorTradeReceipt.class, id) != null) return;
            boolean program =
                em.createQuery(
                            "select count(f) from BrokerFillEntity f where f.tradeId=:trade",
                            Long.class)
                        .setParameter("trade", trade.tradeId())
                        .getSingleResult()
                    > 0;
            if (program) return;
            boolean unresolved =
                em.createQuery(
                            "select count(i) from CopyIntent i where i.instrumentId=:instrument and i.status in ('SEND_STARTED','ACTIVE','UNKNOWN','CANCEL_STARTED')",
                            Long.class)
                        .setParameter("instrument", trade.instrumentId())
                        .getSingleResult()
                    > 0;
            if (unresolved) return;
            long lots;
            try {
              lots = trade.lots().longValueExact();
            } catch (ArithmeticException e) {
              return;
            }
            if (lots <= 0) return;
            MirrorPosition position =
                em.find(MirrorPosition.class, trade.instrumentId(), LockModeType.PESSIMISTIC_WRITE);
            if (position == null) {
              position = new MirrorPosition(trade.instrumentId());
              em.persist(position);
            }
            var receipt = new MirrorTradeReceipt();
            receipt.id = id;
            receipt.instrumentId = trade.instrumentId();
            receipt.side = trade.side().name();
            receipt.lots = lots;
            receipt.executedAt = trade.executedAt();
            if (trade.side() == BrokerGateway.Side.BUY) {
              position.manualLots = Math.addExact(position.manualLots, lots);
              receipt.manualLotsApplied = lots;
            } else {
              long manual = Math.min(position.manualLots, lots), copied = lots - manual;
              if (copied > position.copiedLots) return;
              if (copied > 0) receipt.sourceCycleId = singleOutstandingCycle(trade.instrumentId());
              position.manualLots -= manual;
              position.copiedLots -= copied;
              receipt.manualLotsApplied = -manual;
              receipt.copiedLotsApplied = -copied;
            }
            em.persist(receipt);
            em.persist(
                new JournalEntry(
                    clock.instant(),
                    "MANUAL_TRADE_RECONCILED",
                    trade.instrumentId(),
                    "brokerTrade="
                        + trade.tradeId()
                        + " "
                        + trade.side()
                        + " lots="
                        + lots
                        + " manualDelta="
                        + receipt.manualLotsApplied
                        + " copiedDelta="
                        + receipt.copiedLotsApplied
                        + "; проданные лоты не восстанавливаются"));
          });
    }
  }

  private String singleOutstandingCycle(String instrument) {
    Map<String, String> cycles;
    try {
      cycles =
          SourceCycles.map(
              em.createQuery(
                      "from SourceExecutionEntity where instrumentId=:instrument and executedAt is not null order by executedAt,id",
                      SourceExecutionEntity.class)
                  .setParameter("instrument", instrument)
                  .getResultList());
    } catch (SourceCycles.UnprovenHistory unproven) {
      return null;
    }
    var outstanding = new HashMap<String, Long>();
    for (var intent :
        em.createQuery(
                "from CopyIntent where instrumentId=:instrument and executedLots>0",
                CopyIntent.class)
            .setParameter("instrument", instrument)
            .getResultList()) {
      String cycle = cycles.get(intent.executionId);
      if (cycle == null) return null;
      outstanding.merge(
          cycle, intent.executedLots * ("BUY".equals(intent.side) ? 1 : -1), Long::sum);
    }
    for (var receipt :
        em.createQuery(
                "from MirrorTradeReceipt where instrumentId=:instrument and copiedLotsApplied<>0",
                MirrorTradeReceipt.class)
            .setParameter("instrument", instrument)
            .getResultList()) {
      if (receipt.sourceCycleId == null) return null;
      outstanding.merge(receipt.sourceCycleId, receipt.copiedLotsApplied, Long::sum);
    }
    var positive =
        outstanding.entrySet().stream()
            .filter(e -> e.getValue() > 0)
            .map(Map.Entry::getKey)
            .toList();
    return positive.size() == 1 ? positive.getFirst() : null;
  }
}
