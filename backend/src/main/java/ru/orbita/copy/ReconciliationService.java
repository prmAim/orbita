package ru.orbita.copy;

import jakarta.persistence.EntityManager;
import java.math.BigDecimal;
import java.util.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.control.*;

/**
 * Independent positions are compared after order evidence, without treating equal snapshots as
 * trade order proof.
 */
@Service
public class ReconciliationService {
  private final EntityManager em;
  private final TransactionTemplate tx;
  private final ControlService control;
  private final ObjectProvider<BrokerGateway> gateways;
  private final OrbitaProperties properties;

  public ReconciliationService(
      EntityManager em,
      TransactionTemplate tx,
      ControlService control,
      ObjectProvider<BrokerGateway> gateways,
      OrbitaProperties properties) {
    this.em = em;
    this.tx = tx;
    this.control = control;
    this.gateways = gateways;
    this.properties = properties;
  }

  public boolean reconcile() {
    var gateway = gateways.getIfAvailable();
    if (gateway == null || !properties.accountsConfigured()) return false;
    // Only attempts already independently terminal before these position reads can be cleared.
    var acceptanceCandidates =
        tx.execute(
            _ ->
                em.createQuery(
                        "select i.id from CopyIntent i where i.acceptanceAttempt=true and i.acceptanceReconciled=false and i.status='FINAL' and i.acceptanceOrderReconciled=true and i.actualCommission is not null",
                        UUID.class)
                    .getResultList());
    PositionEvidence source, mirror;
    try {
      source = gateway.positions(properties.sourceAccount());
      mirror = gateway.positions(properties.mirrorAccount());
    } catch (RuntimeException e) {
      PersistenceBoundary.rethrowBrokerPersistence(e);
      control.requireReconciliation("Нет подтверждённых снимков позиций брокера");
      return false;
    }
    if (!source.complete()
        || !mirror.complete()
        || !properties.sourceAccount().equals(source.accountId())
        || !properties.mirrorAccount().equals(mirror.accountId())) {
      control.requireReconciliation("Неполные снимки позиций");
      return false;
    }
    Map<String, BigDecimal> sourceLots =
        tx.execute(
            _ -> {
              Map<String, BigDecimal> result = new HashMap<>();
              for (var e :
                  em.createQuery("from SourceExecutionEntity", SourceExecutionEntity.class)
                      .getResultList())
                result.merge(
                    e.instrumentId,
                    e.lots.multiply(BigDecimal.valueOf("BUY".equals(e.side) ? 1 : -1)),
                    BigDecimal::add);
              return result;
            });
    Map<String, Long> mirrorLots =
        tx.execute(
            _ -> {
              Map<String, Long> result = new HashMap<>();
              for (var p :
                  em.createQuery("from MirrorPosition", MirrorPosition.class).getResultList())
                result.put(p.instrumentId, p.copiedLots + p.manualLots);
              return result;
            });
    Set<String> instruments = new HashSet<>(sourceLots.keySet());
    instruments.addAll(mirrorLots.keySet());
    source.positions().stream()
        .filter(Position::availabilityQualified)
        .forEach(p -> instruments.add(p.instrumentId()));
    mirror.positions().stream()
        .filter(Position::availabilityQualified)
        .forEach(p -> instruments.add(p.instrumentId()));
    boolean all = true;
    Set<String> matchedInstruments = new HashSet<>();
    for (String id : instruments) {
      Instrument instrument;
      try {
        instrument = gateway.instrument(id);
      } catch (RuntimeException e) {
        PersistenceBoundary.rethrowBrokerPersistence(e);
        all = false;
        continue;
      }
      boolean sourcePresent =
          source.positions().stream().anyMatch(p -> p.instrumentId().equals(id));
      boolean mirrorPresent =
          mirror.positions().stream().anyMatch(p -> p.instrumentId().equals(id));
      if ((!sourcePresent && !gateway.positionAbsentMeansZero(id, source))
          || (!mirrorPresent && !gateway.positionAbsentMeansZero(id, mirror))) {
        control.instrumentReconciled(
            id,
            true,
            false,
            "Нет подтверждённого отображения позиции этого класса инструмента в снимке брокера");
        all = false;
        continue;
      }
      BigDecimal bankSource = pieces(source, id), bankMirror = pieces(mirror, id);
      boolean inventoryQualified =
          source.positions().stream()
                  .filter(p -> p.instrumentId().equals(id))
                  .allMatch(Position::availabilityQualified)
              && mirror.positions().stream()
                  .filter(p -> p.instrumentId().equals(id))
                  .allMatch(Position::availabilityQualified);
      tx.executeWithoutResult(
          _ -> {
            control.gate();
            var p = em.find(MirrorPosition.class, id);
            if (p == null) {
              p = new MirrorPosition(id);
              em.persist(p);
            }
            p.brokerLots =
                bankMirror.divide(
                    BigDecimal.valueOf(instrument.lotSize()), 9, java.math.RoundingMode.HALF_EVEN);
            p.brokerReadAt = mirror.receivedAt();
            p.brokerInventoryQualified = inventoryQualified;
          });
      if (!inventoryQualified) {
        control.instrumentReconciled(
            id,
            true,
            false,
            "Общий валютный остаток известен, но начальные деньги, переводы и остаток повторения ещё не разделены; новые заявки по инструменту ожидают квалификации");
        all = false;
        continue;
      }
      BigDecimal expectedSource =
          sourceLots
              .getOrDefault(id, BigDecimal.ZERO)
              .multiply(BigDecimal.valueOf(instrument.lotSize()));
      BigDecimal expectedMirror =
          BigDecimal.valueOf(mirrorLots.getOrDefault(id, 0L))
              .multiply(BigDecimal.valueOf(instrument.lotSize()));
      boolean active =
          tx.execute(
                  _ ->
                      em.createQuery(
                              "select count(i) from CopyIntent i where i.instrumentId=:id and i.status in ('SEND_STARTED','ACTIVE','UNKNOWN','CANCEL_STARTED')",
                              Long.class)
                          .setParameter("id", id)
                          .getSingleResult())
              > 0;
      boolean matches =
          expectedMirror.compareTo(bankMirror) == 0 && expectedSource.compareTo(bankSource) == 0;
      if (active && !matches) {
        control.instrumentReconciled(
            id, true, false, "Ожидание порядка исполнения активных заявок");
        all = false;
        continue;
      }
      boolean previouslyWaiting =
          control.instruments().stream().anyMatch(s -> s.instrumentId().equals(id) && s.waiting());
      control.instrumentReconciled(
          id,
          matches,
          !previouslyWaiting,
          "Расхождение позиции: программа "
              + expectedMirror
              + ", брокер "
              + bankMirror
              + " бумаг; источник программа "
              + expectedSource
              + ", брокер "
              + bankSource);
      if (!matches) all = false;
      else matchedInstruments.add(id);
    }
    // A known discrepancy pauses its instrument; the other instruments retain their enabled mode.
    control.markReconciled();
    if (!acceptanceCandidates.isEmpty()) {
      tx.executeWithoutResult(
          _ -> {
            control.gate();
            for (UUID id : acceptanceCandidates) {
              var attempt = em.find(CopyIntent.class, id);
              if (attempt == null
                  || !"FINAL".equals(attempt.status)
                  || !attempt.acceptanceOrderReconciled
                  || attempt.actualCommission == null
                  || !matchedInstruments.contains(attempt.instrumentId)) continue;
              // Fresh matching positions follow independent terminal order and fee evidence.
              // The owner's next explicit permit supplies the review and a new RUB cap.
              attempt.acceptanceReconciled = true;
            }
          });
    }
    return all;
  }

  private static BigDecimal pieces(PositionEvidence evidence, String id) {
    return evidence.positions().stream()
        .filter(p -> p.instrumentId().equals(id))
        .map(Position::totalPieces)
        .reduce(BigDecimal.ZERO, BigDecimal::add);
  }
}
