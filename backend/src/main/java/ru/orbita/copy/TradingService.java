package ru.orbita.copy;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.control.*;
import ru.orbita.journal.JournalEntry;
import ru.orbita.notifications.NotificationPublisher;
import ru.orbita.notifications.NotificationService;

/**
 * Persisted intent barrier is committed before every external trading action. Never retries
 * PostOrder.
 */
@Service
public class TradingService implements UnsentIntentStop {
  private static final Set<String> LIVE =
      Set.of("SEND_STARTED", "ACTIVE", "UNKNOWN", "CANCEL_STARTED");
  private final EntityManager em;
  private final TransactionTemplate tx;
  private final ControlService control;
  private final ObjectProvider<BrokerGateway> gateways;
  private final OrbitaProperties properties;
  private final Clock clock;
  private final NotificationPublisher notifications;
  private final TimeAuthority time;
  private final NotificationService notificationService;
  private final ru.orbita.valuation.RubValuation rubValuation;

  public TradingService(
      EntityManager em,
      TransactionTemplate tx,
      ControlService control,
      ObjectProvider<BrokerGateway> gateways,
      OrbitaProperties properties,
      Clock clock,
      NotificationPublisher notifications,
      TimeAuthority time,
      NotificationService notificationService,
      ru.orbita.valuation.RubValuation rubValuation) {
    this.em = em;
    this.tx = tx;
    this.control = control;
    this.gateways = gateways;
    this.properties = properties;
    this.clock = clock;
    this.notifications = notifications;
    this.time = time;
    this.notificationService = notificationService;
    this.rubValuation = rubValuation;
  }

  public void tick() {
    reconcileOnly();
    dispatchReady();
  }

  @Override
  @org.springframework.transaction.annotation.Transactional(
      propagation = org.springframework.transaction.annotation.Propagation.MANDATORY)
  public void discardUnsent(String reason) {
    control.gate();
    var waiting =
        em.createQuery(
                "from CopyIntent where status in ('WAITING','HALTED') order by prioritySequence,id",
                CopyIntent.class)
            .getResultList();
    for (var intent : waiting) skipInside(intent, reason);
  }

  public void reconcileOnly() {
    BrokerGateway gateway = gateways.getIfAvailable();
    if (gateway == null) return;
    // Failure to read or commit any transaction escapes the worker: no further sends or cancels in
    // this tick.
    List<UUID> live =
        tx.execute(
            _ ->
                em.createQuery(
                        "select i.id from CopyIntent i where i.status in :states order by i.createdAt,i.id",
                        UUID.class)
                    .setParameter("states", LIVE)
                    .getResultList());
    for (UUID id : live) reconcile(gateway, id);
    var fees =
        tx.execute(
            _ ->
                em.createQuery(
                        "from CopyIntent where status in ('FINAL','HALTED','RETRIED') and actualCommission is null and commissionReserve>0 order by prioritySequence,id",
                        CopyIntent.class)
                    .getResultList());
    for (var i : fees) {
      CommissionEvidence evidence;
      try {
        evidence = gateway.commission(request(i));
      } catch (RuntimeException e) {
        PersistenceBoundary.rethrowBrokerPersistence(e);
        continue;
      }
      if (evidence.qualified()
          && i.id.equals(evidence.requestId())
          && properties.mirrorAccount().equals(evidence.accountId())
          && i.currency.equalsIgnoreCase(evidence.currency()))
        reconcileCommission(i.id, evidence.amount(), true);
    }
    if ("STOPPING".equals(control.status().mode())
        && tx.execute(
                _ ->
                    em.createQuery(
                            "select count(i) from CopyIntent i where i.status in :states",
                            Long.class)
                        .setParameter("states", LIVE)
                        .getSingleResult())
            == 0) control.stoppedAfterReconciliation();
  }

  public void dispatchReady() {
    BrokerGateway gateway = gateways.getIfAvailable();
    if (gateway == null) return;
    List<UUID> waiting =
        tx.execute(
            _ ->
                em.createQuery(
                        "select i.id from CopyIntent i where i.status in ('WAITING','HALTED') order by i.prioritySequence,i.id",
                        UUID.class)
                    .setMaxResults(100)
                    .getResultList());
    for (UUID id : waiting) prepare(gateway, id);
  }

  private OrderRequest request(CopyIntent i) {
    return new OrderRequest(
        properties.mirrorAccount(),
        i.id,
        i.instrumentId,
        Side.valueOf(i.side),
        i.requestedLots > 0 ? i.requestedLots : i.lots - i.skippedLots,
        i.limitPrice);
  }

  private void prepare(BrokerGateway gateway, UUID id) {
    CopyIntent detached = tx.execute(_ -> em.find(CopyIntent.class, id));
    if (detached != null && "HALTED".equals(detached.status)) {
      retryHalted(gateway, detached);
      return;
    }
    if (detached == null || !"WAITING".equals(detached.status)) return;
    var gate = control.gate();
    if (!gate.running()) {
      skip(id, "Общая ручная остановка");
      return;
    }
    if (gate.recalculationPaused() || !gate.reconciled()) return;
    AgeCheck age = age(detached.executedAt);
    if (age.expired) {
      skip(
          id,
          age.reason + (detached.reason == null ? "" : "; последнее ожидание: " + detached.reason));
      return;
    }
    if (!age.allowed) return;
    Instrument requestedInstrument;
    ResourceEvidence resources;
    BigDecimal best;
    BigDecimal queriedLimit;
    long inspectionStartedNanos = System.nanoTime();
    try {
      requestedInstrument = gateway.instrument(detached.instrumentId);
      if (!requestedInstrument.apiAvailable()
          || !("BUY".equals(detached.side)
              ? requestedInstrument.buyAvailable()
              : requestedInstrument.sellAvailable())) return;
      queriedLimit =
          TradePolicy.limit(
              Side.valueOf(detached.side),
              detached.referencePrice,
              requestedInstrument.priceIncrement());
      resources =
          gateway.resources(
              new OrderRequest(
                  properties.mirrorAccount(),
                  id,
                  detached.instrumentId,
                  Side.valueOf(detached.side),
                  detached.lots - detached.skippedLots,
                  queriedLimit));
      best =
          gateway
              .bestOppositePrice(detached.instrumentId, Side.valueOf(detached.side))
              .orElse(null);
    } catch (RuntimeException e) {
      PersistenceBoundary.rethrowBrokerPersistence(e);
      return;
    }
    Instrument instrument = resources.instrument();
    if (instrument == null
        || !instrument.apiAvailable()
        || instrument.lotSize() <= 0
        || TradePolicy.limit(
                    Side.valueOf(detached.side),
                    detached.referencePrice,
                    instrument.priceIncrement())
                .compareTo(queriedLimit)
            != 0) return;
    if (!resources.qualified()) {
      tx.executeWithoutResult(
          _ -> {
            control.gate();
            var i = em.find(CopyIntent.class, id);
            if ("WAITING".equals(i.status)) i.reason = resources.detail();
          });
      return;
    }
    var unitRub =
        rubValuation.value(
            properties.mirrorAccount(),
            detached.instrumentId,
            detached.currency,
            BigDecimal.ONE,
            ru.orbita.valuation.QualifiedRateProvider.Purpose.PRE_SEND,
            time.bound().map(ClockEvidence.Bound::latest).orElse(clock.instant()));
    Dispatch send =
        tx.execute(
            _ -> {
              var currentGate = control.gate();
              CopyIntent i = em.find(CopyIntent.class, id, LockModeType.PESSIMISTIC_WRITE);
              if (!"WAITING".equals(i.status)
                  || !currentGate.running()
                  || currentGate.recalculationPaused()
                  || !currentGate.reconciled()) return null;
              if (!age(i.executedAt).allowed) return null;
              if (resources.requestStarted() == null
                  || clock.instant().isBefore(resources.requestStarted())
                  || clock.instant().isAfter(resources.requestStarted().plusSeconds(5)))
                return null;
              long inspectionElapsed = System.nanoTime() - inspectionStartedNanos;
              if (inspectionElapsed < 0 || inspectionElapsed > Duration.ofSeconds(5).toNanos())
                return null;
              if (em.createQuery(
                          "select count(e) from SourceExecutionEntity e where e.instrumentId=:instrument and e.requiresRecovery=true",
                          Long.class)
                      .setParameter("instrument", i.instrumentId)
                      .getSingleResult()
                  > 0) return null;
              if (!properties.mirrorAccount().equals(resources.accountId())
                  || !i.instrumentId.equals(resources.instrument().id())) return null;
              var state = em.find(InstrumentState.class, i.instrumentId);
              if (state != null
                  && (control.instruments().stream()
                      .anyMatch(
                          s ->
                              s.instrumentId().equals(i.instrumentId)
                                  && (s.paused() || s.waiting())))) return null;
              List<CopyIntent> others =
                  em.createQuery(
                          "from CopyIntent where instrumentId=:instrument and status in :states",
                          CopyIntent.class)
                      .setParameter("instrument", i.instrumentId)
                      .setParameter("states", LIVE)
                      .getResultList();
              if (others.stream()
                  .anyMatch(o -> !o.side.equals(i.side) || !"ACTIVE".equals(o.status))) return null;
              i.limitPrice =
                  TradePolicy.limit(
                      Side.valueOf(i.side), i.referencePrice, instrument.priceIncrement());
              BigDecimal reserved = BigDecimal.ZERO;
              for (var reservation :
                  em.createQuery(
                          "from CopyIntent i where i.currency=:currency and i.reservedMoney>0",
                          CopyIntent.class)
                      .setParameter("currency", i.currency)
                      .getResultList()) {
                BigDecimal reflected =
                    resources
                        .bankReflectedReservations()
                        .getOrDefault(reservation.id, BigDecimal.ZERO);
                if (reflected.signum() < 0 || reflected.compareTo(reservation.reservedMoney) > 0)
                  throw new IllegalArgumentException("Unqualified bank reservation coverage");
                reserved = reserved.add(reservation.reservedMoney.subtract(reflected));
              }
              long allowed;
              long requested = i.lots - i.skippedLots;
              if ("BUY".equals(i.side))
                allowed =
                    Math.min(
                        resources.buyMaxLots(),
                        TradePolicy.affordable(
                            requested,
                            instrument.lotSize(),
                            i.limitPrice,
                            resources.ownMoney().subtract(reserved),
                            resources.preliminaryCommission(),
                            resources.additionalCommissionReserve()));
              else {
                MirrorPosition position = position(i.instrumentId);
                long reservedLots =
                    em.createQuery(
                            "select coalesce(sum(i.reservedLots),0) from CopyIntent i where i.instrumentId=:instrument",
                            Long.class)
                        .setParameter("instrument", i.instrumentId)
                        .getSingleResult();
                allowed =
                    Math.min(
                        requested,
                        Math.min(
                            resources.sellMaxLots(),
                            Math.max(0, position.copiedLots - reservedLots)));
                if (resources
                        .ownMoney()
                        .subtract(reserved)
                        .compareTo(
                            resources
                                .preliminaryCommission()
                                .add(resources.additionalCommissionReserve()))
                    < 0) allowed = 0;
              }
              if (allowed <= 0) {
                skipInside(i, "Недостаточно собственных средств или бумаг с комиссией");
                return null;
              }
              BigDecimal principal =
                  i.limitPrice
                      .multiply(BigDecimal.valueOf(instrument.lotSize()))
                      .multiply(BigDecimal.valueOf(allowed));
              BigDecimal fees =
                  resources.preliminaryCommission().add(resources.additionalCommissionReserve());
              var totalCurrency = ru.orbita.valuation.ExactRub.of(principal.add(fees));
              var converted =
                  new ru.orbita.valuation.RubValuation.Result(
                      unitRub.qualified(),
                      unitRub.rub() == null
                          ? null
                          : unitRub
                              .rub()
                              .multiply(totalCurrency.numerator(), totalCurrency.denominator()),
                      unitRub.diagnostic(),
                      unitRub.reference(),
                      unitRub.evidenceAsOf(),
                      unitRub.evidenceExpiresAt());
              boolean automatic =
                  "AUTOMATIC".equals(currentGate.mode())
                      && currentGate.automaticBoundary() != null
                      && i.executedAt.isAfter(currentGate.automaticBoundary());
              BigDecimal rub =
                  automatic
                      ? null
                      : converted.within(currentGate.permitLimit()) ? converted.rubCeiling() : null;
              // A permit cap never shrinks an otherwise computed order. Resource reduction above
              // follows R16.
              if (!control.consumeTradingPermission(i.executedAt, allowed, i.currency, rub)) {
                i.status = "SHADOW";
                i.reason =
                    "Теневой расчёт: отсутствует подходящий допуск или активация автоотправки";
                em.persist(
                    new JournalEntry(
                        clock.instant(), "SHADOW_DECISION", i.instrumentId, i.view().toString()));
                return null;
              }
              i.acceptanceAttempt =
                  !("AUTOMATIC".equals(currentGate.mode())
                      && currentGate.automaticBoundary() != null
                      && i.executedAt.isAfter(currentGate.automaticBoundary()));
              if (i.acceptanceAttempt) {
                i.permittedRubLimit = currentGate.permitLimit();
                i.consumedPermitId = currentGate.permitId();
                i.consumedPermitEpoch = currentGate.permitEpoch();
                i.controlGeneration = currentGate.generation();
                i.permittedIssuedAt = currentGate.permitIssuedAt();
              }
              i.controlGeneration = currentGate.generation();
              i.skippedLots = i.lots - allowed;
              i.requestedLots = allowed;
              if (i.firstBestOpposite == null) i.firstBestOpposite = best;
              if (i.skippedLots > 0)
                notifications.publish(
                    "RESOURCE_SKIP",
                    i.instrumentId,
                    clock.instant(),
                    i.side
                        + ": окончательно пропущено "
                        + i.skippedLots
                        + " из "
                        + i.lots
                        + " лотов из-за ресурсов");
              i.reservedMoney = "BUY".equals(i.side) ? principal.add(fees) : fees;
              i.commissionReserve = fees;
              i.reservedLots = "SELL".equals(i.side) ? allowed : 0;
              i.status = "SEND_STARTED";
              em.persist(
                  new JournalEntry(
                      clock.instant(),
                      "SEND_STARTED",
                      i.instrumentId,
                      i.id
                          + " lots="
                          + allowed
                          + " reserve="
                          + i.reservedMoney
                          + (i.acceptanceAttempt
                              ? " RUBcost="
                                  + converted.rub()
                                  + " cap="
                                  + currentGate.permitLimit()
                                  + " conversion="
                                  + converted.reference()
                                  + " evidence="
                                  + converted.evidenceAsOf()
                                  + "/"
                                  + converted.evidenceExpiresAt()
                              : " automatic")));
              return new Dispatch(request(i), i.acceptanceAttempt);
            });
    if (send != null) {
      boolean processReady = control.tradingReady();
      var finalBound = time.bound();
      AgeCheck freshAge = age(detached.executedAt, finalBound);
      boolean conversionExpired =
          send.permit()
              && unitRub.evidenceExpiresAt() != null
              && finalBound
                  .map(
                      bound ->
                          bound.latest().isAfter(unitRub.evidenceExpiresAt())
                              || bound.earliest().isBefore(unitRub.evidenceAsOf()))
                  .orElse(true);
      // Evidence acquisition can block on a qualified collector file; include all of it in R59.
      long elapsedAfterCommit = System.nanoTime() - inspectionStartedNanos;
      if (!processReady
          || !control.tradingReady()
          || elapsedAfterCommit < 0
          || elapsedAfterCommit > Duration.ofSeconds(5).toNanos()
          || !freshAge.allowed
          || conversionExpired) {
        knownUnsent(id, freshAge);
        return;
      }
      OrderEvidence evidence;
      try {
        evidence = gateway.post(send.request());
      } catch (RuntimeException e) {
        PersistenceBoundary.rethrowBrokerPersistence(e);
        evidence = unknown(send.request());
      }
      apply(evidence, true);
    }
  }

  private record Dispatch(OrderRequest request, boolean permit) {}

  private void knownUnsent(UUID id, AgeCheck currentAge) {
    tx.executeWithoutResult(
        _ -> {
          var gate = control.gate();
          var i = em.find(CopyIntent.class, id, LockModeType.PESSIMISTIC_WRITE);
          if (!"SEND_STARTED".equals(i.status) || i.sentAt != null || i.brokerOrderId != null)
            throw new IllegalStateException("Known-unsent barrier no longer matches");
          i.reservedMoney = BigDecimal.ZERO;
          i.commissionReserve = BigDecimal.ZERO;
          i.reservedLots = 0;
          i.requestedLots = 0;
          if (i.acceptanceAttempt)
            control.restoreKnownUnsentPermit(
                i.consumedPermitId,
                i.consumedPermitEpoch,
                i.controlGeneration,
                i.permittedIssuedAt,
                i.permittedRubLimit);
          i.status = "WAITING";
          i.reason = "Внешней отправки не было; требуется свежая проверка ресурсов и возраста";
          em.persist(
              new JournalEntry(
                  clock.instant(), "KNOWN_NOT_SENT", i.instrumentId, i.id + " " + i.reason));
          if (currentAge.expired) skipInside(i, currentAge.reason);
          else if (!gate.running() || gate.generation() != i.controlGeneration)
            skipInside(i, "Отправка не начата до изменения режима или ручной остановки");
        });
  }

  private void reconcile(BrokerGateway gateway, UUID id) {
    CopyIntent i = tx.execute(_ -> em.find(CopyIntent.class, id));
    if (i == null) return;
    OrderRequest req = request(i);
    OrderEvidence evidence;
    try {
      evidence = gateway.reconcile(req);
    } catch (RuntimeException e) {
      PersistenceBoundary.rethrowBrokerPersistence(e);
      return;
    }
    apply(evidence, false);
    if (!evidence.known()
        || evidence.status() == OrderStatus.FILLED
        || evidence.status() == OrderStatus.CANCELLED
        || evidence.status() == OrderStatus.REJECTED) return;
    boolean cancel =
        tx.execute(
            _ -> {
              var gate = control.gate();
              CopyIntent current = em.find(CopyIntent.class, id, LockModeType.PESSIMISTIC_WRITE);
              if (!control.tradingReady()) return false;
              if (current.cancellationPending) return false;
              if ("RESTORED".equals(gate.mode())) return false;
              var instrument =
                  control.instruments().stream()
                      .filter(s -> s.instrumentId().equals(current.instrumentId))
                      .findFirst();
              boolean opposite =
                  em.createQuery(
                              "select count(i) from CopyIntent i where i.instrumentId=:instrument and i.side<>:side and i.status='WAITING'",
                              Long.class)
                          .setParameter("instrument", current.instrumentId)
                          .setParameter("side", current.side)
                          .getSingleResult()
                      > 0;
              boolean elapsed =
                  current.deadlineReliable
                      && current.deadline != null
                      && time.bound()
                          .map(bound -> !bound.earliest().isBefore(current.deadline))
                          .orElse(false);
              if (!gate.running()
                  || instrument.map(s -> s.paused() || s.waiting()).orElse(false)
                  || opposite
                  || elapsed) {
                current.status = "CANCEL_STARTED";
                current.cancellationPending = true;
                em.persist(
                    new JournalEntry(
                        clock.instant(),
                        "CANCEL_STARTED",
                        current.instrumentId,
                        current.id.toString()));
                return true;
              }
              return false;
            });
    if (cancel) {
      if (!control.tradingReady()) {
        tx.executeWithoutResult(
            _ -> {
              control.gate();
              var current = em.find(CopyIntent.class, id, LockModeType.PESSIMISTIC_WRITE);
              if (!"CANCEL_STARTED".equals(current.status) || !current.cancellationPending) return;
              current.cancellationPending = false;
              current.status = "ACTIVE";
              em.persist(
                  new JournalEntry(
                      clock.instant(),
                      "CANCEL_KNOWN_NOT_SENT",
                      current.instrumentId,
                      id + ": требуется восстановление долговечного наблюдения перед отменой"));
            });
        return;
      }
      OrderEvidence cancellationEvidence;
      try {
        cancellationEvidence = gateway.cancel(req, evidence.brokerOrderId());
      } catch (RuntimeException e) {
        PersistenceBoundary.rethrowBrokerPersistence(e);
        cancellationEvidence = unknown(req);
      }
      apply(cancellationEvidence, false);
    }
  }

  public void apply(OrderEvidence evidence, boolean postResponse) {
    var confirmationBound =
        evidence.receivedMonotonicNanos() == null
            ? Optional.<ClockEvidence.Bound>empty()
            : time.boundAtReceipt(evidence.receivedMonotonicNanos(), evidence.receiptClockDomain());
    tx.executeWithoutResult(
        _ -> {
          control.gate();
          CopyIntent i =
              em.find(CopyIntent.class, evidence.requestId(), LockModeType.PESSIMISTIC_WRITE);
          if (i == null) throw new IllegalArgumentException("Unknown order intent");
          if (!properties.mirrorAccount().equals(evidence.accountId())
              || !i.instrumentId.equals(evidence.instrumentId())
              || !i.side.equals(evidence.side().name()))
            throw new IllegalStateException("Order evidence identity mismatch");
          if (evidence.known()
              && i.requestedLots > 0
              && evidence.lotsRequested() != i.requestedLots)
            throw new IllegalStateException("Order request volume evidence mismatch");
          if (!evidence.known()) {
            i.status = "UNKNOWN";
            i.reason = "Неопределённый результат заявки; резерв сохранён";
            if (i.unknownNotification == null)
              i.unknownNotification =
                  notifications.publish("ORDER_UNKNOWN", i.instrumentId, clock.instant(), i.reason);
            return;
          }
          if (evidence.lotsExecuted() < i.executedLots)
            return; // older snapshot cannot reverse a confirmed fill
          if (evidence.lotsExecuted() > i.lots - i.skippedLots)
            throw new IllegalStateException("Broker execution exceeds intent");
          long delta = evidence.lotsExecuted() - i.executedLots;
          if (delta > 0) {
            MirrorPosition p = position(i.instrumentId);
            p.copiedLots += delta * ("BUY".equals(i.side) ? 1 : -1);
            if (p.copiedLots < 0) throw new IllegalStateException("Negative copied holding");
          }
          i.executedLots = evidence.lotsExecuted();
          i.brokerOrderId = evidence.brokerOrderId();
          for (Fill fill : evidence.fills()) {
            String key = i.id + ":" + fill.tradeId();
            if (em.find(BrokerFillEntity.class, key) == null) {
              var row = new BrokerFillEntity();
              row.id = key;
              row.intentId = i.id;
              row.tradeId = fill.tradeId();
              row.lots = fill.lots();
              row.price = fill.price();
              row.currency = fill.currency();
              row.executedAt = fill.executedAt();
              em.persist(row);
            }
          }
          boolean terminal =
              evidence.status() == OrderStatus.FILLED
                  || evidence.status() == OrderStatus.CANCELLED
                  || evidence.status() == OrderStatus.REJECTED;
          if (terminal) {
            i.cancellationPending = false;
            if (evidence.status() == OrderStatus.REJECTED
                && evidence.rejectionReason() == RejectionReason.HALTED
                && i.executedLots == 0
                && !i.acceptanceAttempt
                && i.firstBestOpposite != null) {
              i.status = "HALTED";
              i.reservedLots = 0;
              i.reservedMoney = i.commissionReserve;
              i.reason =
                  "Подтверждённый отказ из-за остановки торгов; ожидание прежней лучшей встречной цены";
              em.persist(
                  new JournalEntry(
                      clock.instant(),
                      "ORDER_HALTED",
                      i.instrumentId,
                      i.id + " best=" + i.firstBestOpposite));
              return;
            }
            i.skippedLots = i.lots - i.executedLots;
            i.status = "FINAL";
            i.reservedLots = 0;
            // Fee reserve is released only after independent final commission reconciliation.
            i.reservedMoney = i.commissionReserve;
            allocateRecovery(i);
            if (i.skippedLots > 0) {
              String text =
                  i.side
                      + ": исполнено "
                      + i.executedLots
                      + "/"
                      + i.lots
                      + ", пропущено "
                      + i.skippedLots
                      + "; "
                      + evidence.detail();
              if (i.fullClose)
                control.pauseInstrument(
                    i.instrumentId,
                    text + "; после полного закрытия источника остались бумаги повторения");
              else notifications.publish("ORDER_FINAL_SKIP", i.instrumentId, clock.instant(), text);
            }
          } else {
            i.status = i.cancellationPending ? "CANCEL_STARTED" : "ACTIVE";
            if (i.sentAt == null) {
              i.sentAt =
                  Objects.requireNonNull(
                      evidence.observedAt(), "Raw order-response receipt required");
              i.deadline =
                  confirmationBound
                      .map(bound -> bound.latest().plusSeconds(60))
                      .orElse(i.sentAt.plusSeconds(60));
              i.deadlineReliable = confirmationBound.isPresent();
              if (!i.deadlineReliable)
                i.reason =
                    "Исходная точка таймера сохранена; достоверность оставшегося времени не подтверждена, новая минута не назначается";
            }
          }
          if (i.unknownNotification != null && (terminal || !i.cancellationPending)) {
            notificationService.resolve(
                i.unknownNotification,
                clock.instant(),
                "Неопределённость результата заявки снята после сверки; текущий режим "
                    + control.status().mode());
            i.unknownNotification = null;
          }
          em.persist(
              new JournalEntry(
                  clock.instant(), "ORDER_RECONCILED", i.instrumentId, i.view().toString()));
        });
  }

  public void reconcileCommission(
      UUID id, BigDecimal actualCommission, boolean independentConfirmed) {
    tx.executeWithoutResult(
        _ -> {
          control.gate();
          CopyIntent i = em.find(CopyIntent.class, id, LockModeType.PESSIMISTIC_WRITE);
          if (!independentConfirmed
              || !Set.of("FINAL", "HALTED", "RETRIED").contains(i.status)
              || actualCommission == null
              || actualCommission.signum() < 0)
            throw new IllegalArgumentException("Final independent commission evidence required");
          i.reservedMoney = BigDecimal.ZERO;
          i.commissionReserve = BigDecimal.ZERO;
          i.actualCommission = actualCommission;
          em.persist(
              new JournalEntry(
                  clock.instant(),
                  "COMMISSION_RECONCILED",
                  i.instrumentId,
                  id + " actual=" + actualCommission));
        });
  }

  private MirrorPosition position(String instrument) {
    MirrorPosition p = em.find(MirrorPosition.class, instrument, LockModeType.PESSIMISTIC_WRITE);
    if (p == null) {
      p = new MirrorPosition(instrument);
      em.persist(p);
    }
    return p;
  }

  private void skip(UUID id, String reason) {
    tx.executeWithoutResult(
        _ -> {
          control.gate();
          CopyIntent i = em.find(CopyIntent.class, id, LockModeType.PESSIMISTIC_WRITE);
          if (Set.of("WAITING", "HALTED").contains(i.status)) skipInside(i, reason);
        });
  }

  private void skipInside(CopyIntent i, String reason) {
    i.status = "FINAL";
    i.skippedLots = i.lots;
    i.reason = reason;
    allocateRecovery(i);
    String text = i.side + ": " + reason + "; пропущено " + i.lots + " лотов";
    if (i.fullClose) control.pauseInstrument(i.instrumentId, text + "; остались бумаги повторения");
    else notifications.publish("COPY_SKIPPED", i.instrumentId, clock.instant(), text);
  }

  private void allocateRecovery(CopyIntent i) {
    if (i.recoveryFirstBuy == null) return;
    var shares =
        PositionRecovery.allocateFinalCorrection(
            i.recoveryFirstTarget, i.recoverySecondTarget, i.lots, i.executedLots);
    allocation(i.recoveryFirstBuy, i.instrumentId, shares.getFirst());
    allocation(i.recoverySecondBuy, i.instrumentId, shares.getLast());
    em.persist(
        new JournalEntry(
            clock.instant(),
            "RECOVERY_ALLOCATION",
            i.instrumentId,
            "actual=" + (shares.getFirst() + shares.getLast()) + " shares=" + shares));
  }

  private void allocation(String source, String instrument, long lots) {
    var row = em.find(HoldingAllocation.class, source);
    if (row == null) em.persist(new HoldingAllocation(source, instrument, lots));
    else row.lots = lots;
  }

  private void retryHalted(BrokerGateway gateway, CopyIntent original) {
    var age = age(original.executedAt);
    if (age.expired) {
      skip(original.id, "Истёк возраст исходного исполнения после остановки торгов");
      return;
    }
    if (!age.allowed) return;
    var gate = control.gate();
    if (!gate.running()) {
      skip(original.id, "Общая ручная остановка");
      return;
    }
    var quote = gateway.bestOppositePrice(original.instrumentId, Side.valueOf(original.side));
    if (quote.isEmpty()
        || original.firstBestOpposite == null
        || quote.get().compareTo(original.firstBestOpposite) != 0) return;
    UUID next =
        tx.execute(
            _ -> {
              control.gate();
              var old = em.find(CopyIntent.class, original.id, LockModeType.PESSIMISTIC_WRITE);
              if (!"HALTED".equals(old.status)) return null;
              var retry = new CopyIntent();
              retry.id = UUID.randomUUID();
              retry.retryOf = old.id;
              retry.executionId = old.executionId;
              retry.instrumentId = old.instrumentId;
              retry.sourceOrderId = old.sourceOrderId;
              retry.side = old.side;
              retry.currency = old.currency;
              retry.lots = old.requestedLots;
              retry.referencePrice = old.referencePrice;
              retry.executedAt = old.executedAt;
              retry.createdAt = clock.instant();
              retry.prioritySequence = old.prioritySequence;
              retry.firstBestOpposite = old.firstBestOpposite;
              retry.fullClose = old.fullClose;
              retry.recoveryFirstBuy = old.recoveryFirstBuy;
              retry.recoverySecondBuy = old.recoverySecondBuy;
              retry.recoveryFirstTarget = old.recoveryFirstTarget;
              retry.recoverySecondTarget = old.recoverySecondTarget;
              old.status = "RETRIED";
              em.persist(retry);
              em.persist(
                  new JournalEntry(
                      clock.instant(),
                      "HALT_RETRY_PREPARED",
                      old.instrumentId,
                      old.id + " -> " + retry.id + " lots=" + retry.lots));
              return retry.id;
            });
    if (next != null) prepare(gateway, next);
  }

  private record AgeCheck(boolean allowed, boolean expired, String reason) {}

  private AgeCheck age(Instant executionTime) {
    return age(executionTime, time.bound());
  }

  private AgeCheck age(Instant executionTime, Optional<ClockEvidence.Bound> bound) {
    var result =
        bound
            .map(value -> TradePolicy.ageRange(executionTime, value.earliest(), value.latest()))
            .orElse(TradePolicy.Age.WAIT_FOR_TIME);
    return new AgeCheck(
        result == TradePolicy.Age.ALLOWED,
        result == TradePolicy.Age.EXPIRED || result == TradePolicy.Age.FUTURE_TIMESTAMP,
        result == TradePolicy.Age.EXPIRED
            ? "Возраст исходного исполнения превышает 60 секунд"
            : "Метка исходного исполнения находится в будущем");
  }

  private OrderEvidence unknown(OrderRequest r) {
    return new OrderEvidence(
        r.requestId(),
        r.accountId(),
        null,
        r.instrumentId(),
        r.side(),
        r.lots(),
        0,
        OrderStatus.UNKNOWN,
        List.of(),
        clock.instant(),
        "Transport outcome unknown");
  }
}
