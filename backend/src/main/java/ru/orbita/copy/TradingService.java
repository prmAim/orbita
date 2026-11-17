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
  private final OrderEvidenceJournal orderJournal;

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
      ru.orbita.valuation.RubValuation rubValuation,
      OrderEvidenceJournal orderJournal) {
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
    this.orderJournal = orderJournal;
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
    replayOrderEvidence();
    BrokerGateway gateway = gateways.getIfAvailable();
    if (gateway == null) return;
    // Failure to read or commit any transaction escapes the worker: no further sends or cancels in
    // this tick.
    List<UUID> live =
        tx.execute(
            _ ->
                em.createQuery(
                        "select i.id from CopyIntent i where i.status in :states or (i.acceptanceAttempt=true and i.status='FINAL' and i.acceptanceOrderReconciled=false and i.requestedLots>0) or (i.status='FINAL' and exists (select r.id from JournalEntry r where r.category='ORDER_EVIDENCE_RECHECK' and r.detail=str(i.id) and not exists (select c.id from JournalEntry c where c.category='ORDER_EVIDENCE_RECHECKED' and c.detail=r.detail and c.id>r.id))) order by i.createdAt,i.id",
                        UUID.class)
                    .setParameter("states", LIVE)
                    .getResultList());
    for (UUID id : live) reconcile(gateway, id);
    var fees =
        tx.execute(
            _ ->
                em.createQuery(
                        "from CopyIntent where status in ('FINAL','HALTED','RETRIED') and actualCommission is null and (commissionReserve>0 or (acceptanceAttempt=true and acceptanceReconciled=false)) order by prioritySequence,id",
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
    replayOrderEvidence();
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
              if (i.retryOf != null
                  && (best == null
                      || i.firstBestOpposite == null
                      || best.compareTo(i.firstBestOpposite) != 0)) return null;
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
    orderJournal.receive(evidence, postResponse);
    replayOrderEvidence();
  }

  /** Replays committed observations only; each marker commits atomically with its decision. */
  public synchronized void replayOrderEvidence() {
    long through = orderJournal.highWater(), after = 0;
    while (after < through) {
      var batch = orderJournal.pending(after, through);
      if (batch.isEmpty()) break;
      for (var pending : batch) applySaved(pending);
      after = batch.getLast().sequence();
    }
  }

  public void mirrorStreamGap() {
    var uncertain =
        tx.execute(
            _ ->
                em.createQuery("from CopyIntent where status in :states", CopyIntent.class)
                    .setParameter("states", LIVE)
                    .getResultList());
    for (var intent : uncertain) apply(unknown(request(intent)), false);
  }

  private void applySaved(OrderEvidenceJournal.Pending pending) {
    var original = pending.evidence();
    var raw = original.sdkEvidence();
    boolean stream =
        raw != null
            && (raw.source() == ru.orbita.broker.OrderSdkEvidence.Source.ORDER_STATE_STREAM
                || raw.source() == ru.orbita.broker.OrderSdkEvidence.Source.TRADES_STREAM);
    var prepared = stream ? prepareMirror(original) : new PreparedMirror(original, null);
    if (prepared == null) return; // Unlinked trades remain durable until a UUID mapping arrives.
    final var evidence = prepared.evidence();
    var qualifiedLotSize = stream ? prepared.qualifiedLotSize() : qualifiedBuyLotSize(evidence);
    boolean postResponse = pending.postResponse();
    var confirmationBound =
        evidence.receivedMonotonicNanos() == null
            ? Optional.<ClockEvidence.Bound>empty()
            : time.boundAtReceipt(evidence.receivedMonotonicNanos(), evidence.receiptClockDomain());
    tx.executeWithoutResult(
        _ -> {
          control.gate();
          if (orderJournal.applied(pending.sequence())) return;
          // Marking and state changes share this transaction. Any failure rolls both back.
          orderJournal.markApplied(pending.sequence(), evidence.instrumentId());
          if (evidence.requestId() == null
              || !properties.mirrorAccount().equals(evidence.accountId())) return;
          CopyIntent i =
              em.find(CopyIntent.class, evidence.requestId(), LockModeType.PESSIMISTIC_WRITE);
          if (i == null) return;
          refineBuyResourceReason(i, evidence, qualifiedLotSize);
          if (!i.instrumentId.equals(evidence.instrumentId())
              || evidence.side() == null
              || !i.side.equals(evidence.side().name())) {
            contradiction(i, "Order evidence identity mismatch");
            return;
          }
          if (evidence.known()
              && i.requestedLots > 0
              && evidence.lotsRequested() != i.requestedLots) {
            contradiction(i, "Order request volume evidence mismatch");
            return;
          }
          if (stream && !validMirror(raw, i)) {
            contradiction(i, "Mirror stream identity/quantity contradicts attempt");
            return;
          }
          boolean wasFinal = "FINAL".equals(i.status);
          if (!evidence.known()) {
            if (wasFinal) return;
            i.acceptanceOrderReconciled = false;
            i.acceptanceReconciled = false;
            i.status = "UNKNOWN";
            i.reason = "Неопределённый результат заявки; резерв сохранён";
            if (i.unknownNotification == null)
              i.unknownNotification =
                  notifications.publish("ORDER_UNKNOWN", i.instrumentId, clock.instant(), i.reason);
            return;
          }
          if (evidence.lotsExecuted() < i.executedLots) {
            if (evidence.status() == OrderStatus.FILLED
                || evidence.status() == OrderStatus.CANCELLED
                || evidence.status() == OrderStatus.REJECTED)
              contradiction(i, "Terminal evidence contradicts confirmed fills");
            return;
          }
          if (evidence.lotsExecuted() > i.requestedLots || evidence.lotsExecuted() < 0) {
            contradiction(i, "Broker execution exceeds intent or units are unqualified");
            return;
          }
          Set<String> incomingTradeIds = new HashSet<>();
          for (Fill fill : evidence.fills()) {
            var previous =
                fill.tradeId() == null
                    ? null
                    : em.find(BrokerFillEntity.class, i.id + ":" + fill.tradeId());
            if (fill.tradeId() == null
                || fill.tradeId().isBlank()
                || !incomingTradeIds.add(fill.tradeId())
                || fill.lots() == null
                || fill.lots().signum() <= 0
                || fill.price() == null
                || fill.price().signum() <= 0
                || fill.currency() == null
                || fill.currency().isBlank()
                || fill.executedAt() == null
                || previous != null
                    && (previous.lots.compareTo(fill.lots()) != 0
                        || previous.price.compareTo(fill.price()) != 0
                        || !previous.currency.equalsIgnoreCase(fill.currency()))) {
              contradiction(i, "Conflicting or duplicate trade identity/quantity");
              return;
            }
          }
          if (stream) orderJournal.rememberLink(pending.sequence(), evidence);
          long delta = evidence.lotsExecuted() - i.executedLots;
          if (delta > 0) {
            MirrorPosition p = position(i.instrumentId);
            p.copiedLots += delta * ("BUY".equals(i.side) ? 1 : -1);
            if (p.copiedLots < 0) throw new IllegalStateException("Negative copied holding");
          }
          i.executedLots = evidence.lotsExecuted();
          if (wasFinal && delta > 0) {
            i.skippedLots = i.lots - i.executedLots;
            i.acceptanceOrderReconciled = false;
            i.acceptanceReconciled = false;
            // The former final fee cannot qualify an acceptance attempt whose execution changed.
            // The existing independent-fee pass will read it again before R125 can be cleared.
            if (i.acceptanceAttempt) i.actualCommission = null;
            em.persist(
                new JournalEntry(
                    clock.instant(), "ORDER_EVIDENCE_RECHECK", i.instrumentId, i.id.toString()));
          }
          if (evidence.brokerOrderId() != null && !evidence.brokerOrderId().isBlank())
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
            if (i.acceptanceAttempt && !postResponse && !stream) i.acceptanceOrderReconciled = true;
            if (stream && !(wasFinal && delta > 0))
              em.persist(
                  new JournalEntry(
                      clock.instant(), "ORDER_EVIDENCE_RECHECK", i.instrumentId, i.id.toString()));
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
            if (i.skippedLots > 0 && !wasFinal) {
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
            if (wasFinal) {
              if (delta > 0) allocateRecovery(i);
              return;
            }
            i.acceptanceOrderReconciled = false;
            i.acceptanceReconciled = false;
            i.status = i.cancellationPending ? "CANCEL_STARTED" : "ACTIVE";
            if (i.sentAt == null && !stream) {
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
          if (i.unknownNotification != null && !stream && (terminal || !i.cancellationPending)) {
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
          if (!stream && !postResponse)
            em.persist(
                new JournalEntry(
                    clock.instant(), "ORDER_EVIDENCE_RECHECKED", i.instrumentId, i.id.toString()));
        });
  }

  private record PreparedMirror(OrderEvidence evidence, Integer qualifiedLotSize) {}

  private PreparedMirror prepareMirror(OrderEvidence evidence) {
    var raw = evidence.sdkEvidence();
    if (!properties.mirrorAccount().equals(evidence.accountId()))
      return new PreparedMirror(evidence, null);
    UUID id = evidence.requestId();
    if (raw.source() == ru.orbita.broker.OrderSdkEvidence.Source.TRADES_STREAM
        || id == null && (raw.responseRequestId() == null || raw.responseRequestId().isBlank())) {
      id = tx.execute(_ -> orderJournal.linkedRequest(raw).orElse(null));
      if (id == null) return null;
    }
    final UUID linkedId = id;
    var intent = id == null ? null : tx.execute(_ -> em.find(CopyIntent.class, linkedId));
    if (intent == null || !properties.mirrorAccount().equals(evidence.accountId()))
      return new PreparedMirror(evidence, null);
    // Instrument lookup belongs outside the control transaction. Never trust stream lot size alone.
    var gateway = gateways.getIfAvailable();
    if (gateway == null)
      throw new IllegalStateException(
          "Own mirror observation requires broker instrument qualification");
    var instrument = gateway.instrument(intent.instrumentId);
    if (instrument.lotSize() <= 0 || !intent.instrumentId.equals(instrument.id()))
      throw new IllegalStateException("Own mirror observation has unqualified instrument lot size");
    List<Fill> fills = new ArrayList<>();
    Set<String> unique = new HashSet<>();
    boolean valid = raw.lotSize() == null || raw.lotSize() == instrument.lotSize();
    for (var t : raw.trades()) {
      if (t.tradeId() == null
          || t.tradeId().isBlank()
          || !unique.add(t.tradeId())
          || t.pieces() <= 0
          || t.pieces() % instrument.lotSize() != 0
          || t.price() == null
          || t.at() == null) {
        valid = false;
        break;
      }
      fills.add(
          new Fill(
              t.tradeId(),
              BigDecimal.valueOf(t.pieces() / instrument.lotSize()),
              t.price(),
              intent.currency,
              t.at()));
    }
    long total;
    if (raw.source() == ru.orbita.broker.OrderSdkEvidence.Source.TRADES_STREAM) {
      total =
          tx.execute(
              _ -> {
                var known =
                    em.createQuery(
                            "from BrokerFillEntity f where f.intentId=:id", BrokerFillEntity.class)
                        .setParameter("id", linkedId)
                        .getResultList();
                Map<String, BigDecimal> quantities = new HashMap<>();
                for (var f : known) quantities.put(f.tradeId, f.lots);
                for (var f : fills) quantities.putIfAbsent(f.tradeId(), f.lots());
                return quantities.values().stream()
                    .reduce(BigDecimal.ZERO, BigDecimal::add)
                    .longValueExact();
              });
      // A prior unary snapshot may already include a trade whose individual record is still late.
      total = Math.max(total, intent.executedLots);
    } else {
      total = evidence.lotsExecuted();
      if (!fills.isEmpty()
          && fills.stream()
                  .map(Fill::lots)
                  .reduce(BigDecimal.ZERO, BigDecimal::add)
                  .longValueExact()
              != total) valid = false;
    }
    var status =
        raw.source() == ru.orbita.broker.OrderSdkEvidence.Source.TRADES_STREAM
            ? total == intent.requestedLots ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED
            : evidence.status();
    // PARTIALLY_FILL after cancel is deliberately active until independent unary reconciliation.
    return new PreparedMirror(
        new OrderEvidence(
            linkedId,
            evidence.accountId(),
            null,
            evidence.instrumentId(),
            evidence.side(),
            intent.requestedLots,
            valid ? total : -1,
            status,
            fills,
            evidence.observedAt(),
            evidence.detail(),
            evidence.rejectionReason(),
            null,
            null,
            raw),
        instrument.lotSize());
  }

  /**
   * Resolve independent instrument facts outside the control transaction, including unary replay.
   */
  private Integer qualifiedBuyLotSize(OrderEvidence evidence) {
    if (evidence.requestId() == null || !properties.mirrorAccount().equals(evidence.accountId()))
      return null;
    var intent = tx.execute(_ -> em.find(CopyIntent.class, evidence.requestId()));
    if (intent == null || !"BUY".equals(intent.side)) return null;
    boolean terminal =
        evidence.status() == OrderStatus.REJECTED || evidence.status() == OrderStatus.CANCELLED;
    if (!terminal
        && !tx.execute(
            _ ->
                orderJournal
                    .latestReason(intent.id)
                    .map(r -> r.reason() == RejectionReason.INSUFFICIENT_RESOURCES)
                    .orElse(false))) return null;
    var gateway = gateways.getIfAvailable();
    if (gateway == null) return null;
    Instrument instrument;
    try {
      instrument = gateway.instrument(intent.instrumentId);
    } catch (RuntimeException unavailable) {
      // Reason refinement must never prevent application of independently known unary facts.
      return null;
    }
    if (instrument == null) return null;
    return intent.instrumentId.equals(instrument.id()) && instrument.lotSize() > 0
        ? instrument.lotSize()
        : null;
  }

  private void refineBuyResourceReason(
      CopyIntent i, OrderEvidence current, Integer qualifiedLotSize) {
    if (!"BUY".equals(i.side) || i.requestedLots <= 0) return;
    boolean qualified =
        orderJournal
            .latestReason(i.id)
            .map(r -> r.reason() == RejectionReason.INSUFFICIENT_RESOURCES)
            .orElse(false);
    boolean terminal =
        current.status() == OrderStatus.REJECTED || current.status() == OrderStatus.CANCELLED;
    if (!terminal && !qualified) return;
    var attempt = request(i);
    var history = orderJournal.history(attempt);
    var result =
        i.executedLots == 0
            ? ru.orbita.broker.BuyResourceRejectionClassifier.classify(
                attempt,
                history.stream().map(OrderEvidenceJournal.Pending::evidence).toList(),
                orderJournal.onlyOriginalMutation(i.id),
                qualifiedLotSize)
            : Optional.<ru.orbita.broker.BuyResourceRejectionClassifier.Qualification>empty();
    if (result.isPresent()) {
      if (!qualified && terminal && result.get().status() == current.status()) {
        i.reason = result.get().detail();
        orderJournal.qualifyReason(attempt, result.get(), history);
      }
    } else if (qualified) {
      i.reason =
          qualifiedLotSize == null
              ? "Причина OTHER; независимый размер лота недоступен; требуется сверка"
              : "Причина OTHER; прежняя квалификация нехватки средств опровергнута; требуется сверка";
      orderJournal.invalidateReason(attempt, current.status(), i.reason, history);
      contradiction(
          i,
          qualifiedLotSize == null
              ? "Independent instrument metadata unavailable; resource reason requires reconciliation"
              : "Late evidence contradicts qualified zero resource rejection");
    }
  }

  private boolean validMirror(ru.orbita.broker.OrderSdkEvidence raw, CopyIntent i) {
    if (raw.source() == ru.orbita.broker.OrderSdkEvidence.Source.TRADES_STREAM) return true;
    return ru.orbita.broker.MirrorOrderParser.consistentStateQuantities(raw, i.requestedLots);
  }

  private void contradiction(CopyIntent i, String detail) {
    control.pauseInstrument(i.instrumentId, detail);
    i.acceptanceOrderReconciled = false;
    i.acceptanceReconciled = false;
    if (!"FINAL".equals(i.status)) {
      i.status = "UNKNOWN";
      i.acceptanceOrderReconciled = false;
      i.acceptanceReconciled = false;
      if (i.unknownNotification == null)
        i.unknownNotification =
            notifications.publish("ORDER_UNKNOWN", i.instrumentId, clock.instant(), detail);
    }
    em.persist(
        new JournalEntry(
            clock.instant(), "ORDER_EVIDENCE_CONTRADICTION", i.instrumentId, i.id + ": " + detail));
    em.persist(
        new JournalEntry(
            clock.instant(), "ORDER_EVIDENCE_RECHECK", i.instrumentId, i.id.toString()));
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
