package ru.orbita.control;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.BrokerGateway.CapitalEvidence;
import ru.orbita.copy.Rational;
import ru.orbita.journal.JournalEntry;
import ru.orbita.notifications.NotificationPublisher;
import ru.orbita.notifications.NotificationService;

/**
 * Serializes owner commands and coefficient acceptance without holding DB locks over broker calls.
 */
@Service
public class ControlService {
  private static final ZoneId MOSCOW = ZoneId.of("Europe/Moscow");
  private final EntityManager em;
  private final TransactionTemplate tx;
  private final Clock clock;
  private final OrbitaProperties properties;
  private final ObjectProvider<BrokerGateway> broker;
  private final NotificationPublisher notifications;
  private final NotificationService notificationService;
  private final AlgorithmVersion algorithms;
  private final TimeAuthority time;
  private final CapitalCandidateService candidates;
  private final ObjectProvider<UnsentIntentStop> unsentIntents;
  private final ru.orbita.broker.BrokerConnectivityEvents connectivity;
  private final ru.orbita.runtime.ApplicationReadiness readiness;
  private final ConcurrentMap<UUID, AbandonedCapital> abandonedCapital = new ConcurrentHashMap<>();

  private record AbandonedCapital(Attempt attempt, String reason) {}

  public ControlService(
      EntityManager em,
      TransactionTemplate tx,
      Clock clock,
      OrbitaProperties properties,
      ObjectProvider<BrokerGateway> broker,
      NotificationPublisher notifications,
      NotificationService notificationService,
      AlgorithmVersion algorithms,
      TimeAuthority time,
      CapitalCandidateService candidates,
      ObjectProvider<UnsentIntentStop> unsentIntents,
      ru.orbita.broker.BrokerConnectivityEvents connectivity,
      ru.orbita.runtime.ApplicationReadiness readiness) {
    this.em = em;
    this.tx = tx;
    this.clock = clock;
    this.properties = properties;
    this.broker = broker;
    this.notifications = notifications;
    this.notificationService = notificationService;
    this.algorithms = algorithms;
    this.time = time;
    this.candidates = candidates;
    this.unsentIntents = unsentIntents;
    this.connectivity = connectivity;
    this.readiness = readiness;
  }

  public record Status(
      String mode,
      @com.fasterxml.jackson.annotation.JsonFormat(
              shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
          BigDecimal coefficient,
      Instant observationBoundary,
      Instant automaticBoundary,
      @com.fasterxml.jackson.annotation.JsonFormat(
              shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
          BigDecimal permitLimit,
      Instant permitIssuedAt,
      boolean recalculationRunning,
      boolean recalculationPaused,
      boolean reconciled,
      String lastError,
      @com.fasterxml.jackson.annotation.JsonFormat(
              shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
          java.math.BigInteger ratioNumerator,
      @com.fasterxml.jackson.annotation.JsonFormat(
              shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
          java.math.BigInteger ratioDenominator,
      int failedAttempts) {}

  public record Attempt(
      UUID id,
      long generation,
      String kind,
      long firstRequestNanos,
      long sourceReplyNanos,
      long mirrorReplyNanos,
      Instant firstRequestAt) {}

  public record Gate(
      BigDecimal coefficient,
      Rational exactCoefficient,
      Instant boundary,
      boolean running,
      boolean recalculationPaused,
      boolean reconciled,
      String mode,
      Instant permitIssuedAt,
      BigDecimal permitLimit,
      Instant automaticBoundary,
      boolean firstAcceptance,
      boolean liveAccepted,
      UUID permitId,
      long permitEpoch,
      long generation) {}

  public record InstrumentStatus(
      String instrumentId, boolean paused, boolean waiting, boolean reconciled, String reason) {}

  ControlState lock() {
    return em.find(
        ControlState.class,
        1,
        LockModeType.PESSIMISTIC_WRITE,
        Map.of("jakarta.persistence.lock.timeout", 5000));
  }

  private Status view(ControlState s) {
    var values = candidates.accepted(s);
    return new Status(
        s.mode,
        values.coefficient(),
        s.observationBoundary,
        s.automaticBoundary,
        s.permitLimit,
        s.permitIssuedAt,
        s.attemptId != null,
        s.recalculationPaused,
        s.reconciled,
        s.lastError,
        values.numerator(),
        values.denominator(),
        s.failedAttempts);
  }

  public Status status() {
    return tx.execute(_ -> view(em.find(ControlState.class, 1)));
  }

  public Gate gate() {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          establishBoundary(s);
          time.bound()
              .ifPresent(
                  bound -> {
                    if (running(s)
                        && (s.completedDue == null
                            || s.completedDue.isBefore(latestDue(bound.earliest()))))
                      s.recalculationPaused = true;
                  });
          var values = candidates.accepted(s);
          return new Gate(
              values.coefficient(),
              values.numerator() == null
                  ? null
                  : new Rational(values.numerator(), values.denominator()),
              s.resumeBoundary == null ? s.observationBoundary : s.resumeBoundary,
              running(s),
              s.recalculationPaused,
              readiness.ready() && s.reconciled && (s.observationBoundary == null || s.feedReady),
              s.mode,
              s.permitIssuedAt,
              s.permitLimit,
              s.automaticBoundary,
              s.firstAcceptance,
              s.liveAccepted,
              s.permitId,
              s.permitEpoch,
              s.generation);
        });
  }

  public boolean consumeTradingPermission(
      Instant executedAt, long lots, String currency, BigDecimal totalRub) {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          if (!readiness.ready()
              || !running(s)
              || !s.reconciled
              || !s.feedReady
              || s.recalculationPaused
              || !properties.marginDisabledConfirmed()
              || !shadowQualified(s)) return false;
          if ("AUTOMATIC".equals(s.mode)
              && s.automaticBoundary != null
              && executedAt.isAfter(s.automaticBoundary)) return true;
          if (s.permitIssuedAt == null
              || !executedAt.isAfter(s.permitIssuedAt)
              || totalRub == null
              || totalRub.compareTo(s.permitLimit) > 0) return false;
          if (s.firstAcceptance && (lots != 1 || !"RUB".equalsIgnoreCase(currency))) return false;
          s.permitIssuedAt = null;
          s.permitLimit = null;
          s.permitId = null;
          return true;
        });
  }

  public List<InstrumentStatus> instruments() {
    return tx.execute(
        _ ->
            em
                .createQuery("from InstrumentState order by instrumentId", InstrumentState.class)
                .getResultList()
                .stream()
                .map(
                    s ->
                        new InstrumentStatus(
                            s.instrumentId, s.paused, s.waiting, s.reconciled, s.reason))
                .toList());
  }

  public boolean tradingReady() {
    return readiness.ready();
  }

  public Status start() {
    return recalculate("START");
  }

  public Status recalculate() {
    return recalculate("MANUAL");
  }

  public Status mandatoryRecalculation() {
    return recalculate("MANDATORY");
  }

  /** Only terminal broker-read failures register here; a concurrent live attempt is never swept. */
  public void recoverAbandonedCapitalAttempts() {
    for (AbandonedCapital abandoned : abandonedCapital.values()) {
      tx.executeWithoutResult(
          _ -> {
            ControlState state = lock();
            if (!current(state, abandoned.attempt())) return;
            state.reconciled = false;
            failedCapital(abandoned.attempt(), abandoned.reason());
          });
      // Keep the marker across an uncertain COMMIT. A retry matches identity or observes that
      // cleanup already committed; neither case can clear an unrelated later attempt.
      abandonedCapital.remove(abandoned.attempt().id(), abandoned);
    }
  }

  private Status recalculate(String kind) {
    recoverAbandonedCapitalAttempts();
    Attempt attempt =
        tx.execute(
            _ -> {
              ControlState state = lock();
              if (state.attemptId != null || "STOPPING".equals(state.mode)) return null;
              if ("MANDATORY".equals(kind)
                  && (!running(state)
                      || state.retryAt != null && clock.instant().isBefore(state.retryAt)))
                return null;
              if ("START".equals(kind) && running(state)) return null;
              state.attemptId = UUID.randomUUID();
              state.attemptKind = kind;
              if ("MANDATORY".equals(kind)) state.recalculationPaused = true;
              em.persist(
                  new JournalEntry(
                      clock.instant(),
                      "RECALCULATION_STARTED",
                      null,
                      kind + " " + state.attemptId));
              return new Attempt(state.attemptId, state.generation, kind, 0, 0, 0, null);
            });
    if (attempt == null) return status();
    CapitalEvidence source = null, mirror = null;
    String error = null;
    long firstNanos = 0, sourceNanos = 0, mirrorNanos = 0;
    Instant firstAt = null;
    try {
      if (!properties.accountsConfigured())
        throw new IllegalStateException("Не настроена пара различных собственных счетов");
      BrokerGateway gateway = broker.getIfAvailable();
      if (gateway == null) throw new IllegalStateException("Не настроено соединение с брокером");
      firstAt = clock.instant();
      firstNanos = System.nanoTime();
      try {
        source = gateway.capital(properties.sourceAccount());
        sourceNanos = capitalReceiptNanos(source, firstNanos, System.nanoTime());
      } catch (RuntimeException failure) {
        throw new CapitalReadFailure("источника", failure);
      }
      try {
        long mirrorRequestNanos = System.nanoTime();
        mirror = gateway.capital(properties.mirrorAccount());
        mirrorNanos = capitalReceiptNanos(mirror, mirrorRequestNanos, System.nanoTime());
      } catch (RuntimeException failure) {
        throw new CapitalReadFailure("повторения", failure);
      }
    } catch (RuntimeException failure) {
      try {
        PersistenceBoundary.rethrowBrokerPersistence(failure);
      } catch (RuntimeException persistenceFailure) {
        abandonedCapital.put(
            attempt.id(),
            new AbandonedCapital(
                attempt, "Не удалось сохранить состояние связи с брокером; требуется сверка"));
        try {
          recoverAbandonedCapitalAttempts();
        } catch (RuntimeException cleanupFailure) {
          persistenceFailure.addSuppressed(cleanupFailure);
        }
        // Successful cleanup never authorizes continuing this failed trading tick.
        throw persistenceFailure;
      }
      error = safeError(failure);
    }
    return finish(
        new Attempt(
            attempt.id(),
            attempt.generation(),
            attempt.kind(),
            firstNanos,
            sourceNanos,
            mirrorNanos,
            firstAt),
        source,
        mirror,
        error);
  }

  public Status finish(
      Attempt attempt, CapitalEvidence source, CapitalEvidence mirror, String failure) {
    try {
      tx.executeWithoutResult(
          _ -> {
            lock();
            finishLocked(attempt, source, mirror, failure);
          });
    } catch (RuntimeException persistenceFailure) {
      // Cleanup uses a fresh transaction after the singleton transaction releases its lock.
      if (abandonedCapital.containsKey(attempt.id())) {
        try {
          recoverAbandonedCapitalAttempts();
        } catch (RuntimeException cleanupFailure) {
          persistenceFailure.addSuppressed(cleanupFailure);
        }
      }
      throw persistenceFailure;
    }
    return status();
  }

  private Status finishLocked(
      Attempt attempt, CapitalEvidence source, CapitalEvidence mirror, String failure) {
    Status interrupted =
        tx.execute(
            _ -> {
              ControlState state = lock();
              return current(state, attempt) ? null : view(state);
            });
    if (interrupted != null) return interrupted;
    String error = failure != null ? failure : validatePair(source, mirror, clock.instant());
    if (error == null)
      error = capitalTimingError(attempt, System.nanoTime() - attempt.firstRequestNanos());
    if (error != null) return failedCapital(attempt, error);
    CapitalCandidateService.Receipt receipt;
    try {
      receipt = candidates.stage(attempt, source, mirror);
    } catch (RuntimeException failedCommit) {
      // A returned but late Receipt is an ordinary qualification failure below. An exception
      // while staging is an infrastructure failure and must end the current trading tick.
      abandonedCapital.put(
          attempt.id(),
          new AbandonedCapital(
              attempt, "Не удалось подтвердить сохранение оценок капитала; требуется сверка"));
      throw failedCommit;
    }
    error = capitalTimingError(attempt, receipt.commitAckElapsedNanos());
    if (error == null
        && (receipt.acknowledgedAt().isBefore(attempt.firstRequestAt())
            || Duration.between(attempt.firstRequestAt(), receipt.acknowledgedAt())
                    .compareTo(Duration.ofSeconds(5))
                > 0)) error = "Получение и сохранение оценок превысило 5 секунд";
    if (error == null) error = capitalChronologyError(receipt);
    if (error != null) return failedCapital(attempt, error, receipt);
    // The entire captured numeric-persistence interval proves the same Moscow due date.
    LocalDate qualifiedDue = latestDue(receipt.persistenceEarliest());
    tx.execute(
        _ -> {
          ControlState state = lock();
          if (!current(state, attempt)) {
            candidates.reject(attempt.id(), "Попытка прервана командой владельца или перезапуском");
            return view(state);
          }
          candidates.qualify(attempt, receipt);
          state.activeCapitalCandidateId = receipt.id();
          state.attemptId = null;
          state.attemptKind = null;
          if (qualifiedDue != null) state.completedDue = qualifiedDue;
          state.retryAt = null;
          state.lastError = null;
          Instant now = clock.instant();
          if (state.recalculationPaused && state.failedAttempts > 0) {
            Duration pauseDuration = Duration.between(state.pauseStartedAt, now);
            String recoveryText =
                "Пересчёт восстановлен; начало "
                    + NotificationService.formatTime(state.pauseStartedAt)
                    + ", окончание "
                    + NotificationService.formatTime(now)
                    + ", длительность "
                    + (pauseDuration.isNegative() ? "не подтверждена" : pauseDuration)
                    + ", ошибок "
                    + state.failedAttempts;
            if (state.recalculationErrorNotificationId != null)
              notificationService.annotateUndelivered(
                  state.recalculationErrorNotificationId, now, recoveryText);
            notifications.publish("RECALCULATION_RECOVERED", null, now, recoveryText);
          }
          state.recalculationPaused = false;
          state.pauseStartedAt = null;
          state.recalculationErrorNotificationId = null;
          state.lastRecalculationNotificationReason = null;
          state.failedAttempts = 0;
          if ("START".equals(attempt.kind())) {
            if (!state.reconciled) {
              state.lastError = "Требуется успешная сверка с брокером до включения наблюдения";
              return view(state);
            }
            var bound = time.bound();
            Instant boundary = bound.map(ClockEvidence.Bound::latest).orElse(now);
            if (state.observationBoundary == null) {
              state.observationBoundary = boundary;
              state.observationBoundaryTrusted = bound.isPresent();
            }
            state.resumeBoundary = boundary;
            state.resumeBoundaryTrusted = bound.isPresent();
            state.mode = "SHADOW";
            state.automaticBoundary = null;
            if (!state.observationActivated) state.feedReady = false;
          }
          em.persist(
              new JournalEntry(
                  now,
                  "COEFFICIENT_SAVED",
                  null,
                  receipt.values().coefficient().toPlainString()
                      + " due="
                      + state.completedDue
                      + " candidate="
                      + receipt.id()));
          return view(state);
        });
    return status();
  }

  private Status failedCapital(Attempt attempt, String error) {
    return failedCapital(attempt, error, null);
  }

  private Status failedCapital(
      Attempt attempt, String error, CapitalCandidateService.Receipt receipt) {
    return tx.execute(
        _ -> {
          ControlState state = lock();
          if (receipt == null) candidates.reject(attempt.id(), error);
          else candidates.reject(receipt, error);
          if (!current(state, attempt)) return view(state);
          state.attemptId = null;
          state.attemptKind = null;
          state.lastError = error;
          Instant now = clock.instant();
          if ("MANDATORY".equals(attempt.kind()) || state.recalculationPaused) {
            state.recalculationPaused = true;
            state.retryAt = now.plusSeconds(60);
            state.failedAttempts++;
            if (state.pauseStartedAt == null) {
              state.pauseStartedAt = now;
              state.recalculationErrorNotificationId = null;
              state.lastRecalculationNotificationReason = null;
            }
            if (!Objects.equals(state.lastRecalculationNotificationReason, error)) {
              UUID notificationId =
                  notifications.publish(
                      "RECALCULATION_ERROR",
                      null,
                      now,
                      error + "; обязательный пересчёт приостановлен");
              if (state.recalculationErrorNotificationId == null)
                state.recalculationErrorNotificationId = notificationId;
              state.lastRecalculationNotificationReason = error;
            }
          }
          em.persist(new JournalEntry(now, "RECALCULATION_FAILED", null, error));
          return view(state);
        });
  }

  private static boolean current(ControlState state, Attempt attempt) {
    return Objects.equals(state.attemptId, attempt.id())
        && state.generation == attempt.generation();
  }

  private static String capitalTimingError(Attempt attempt, long elapsed) {
    long gap = attempt.mirrorReplyNanos() - attempt.sourceReplyNanos();
    if (gap < 0 || gap > Duration.ofSeconds(3).toNanos())
      return "Разрыв получения полных ответов превысил 3 секунды";
    if (elapsed < 0 || elapsed > Duration.ofSeconds(5).toNanos())
      return "Получение и сохранение оценок превысило 5 секунд";
    return null;
  }

  private static long capitalReceiptNanos(CapitalEvidence evidence, long requested, long returned) {
    if (evidence == null) return returned;
    Long receipt = evidence.receivedMonotonicNanos();
    String domain = evidence.receiptClockDomain();
    // Simple synchronous gateways historically return the full response directly.
    if (receipt == null && domain == null) return returned;
    if (receipt == null
        || !ru.orbita.broker.BrokerReceiptClock.domain().equals(domain)
        || receipt - requested < 0
        || returned - receipt < 0)
      throw new IllegalStateException("Не подтверждена монотонная метка полного ответа капитала");
    return receipt;
  }

  private static String capitalChronologyError(CapitalCandidateService.Receipt receipt) {
    if (receipt.persistenceEarliest() == null
        || receipt.persistenceLatest() == null
        || receipt.persistenceLatest().isBefore(receipt.persistenceEarliest()))
      return "Не подтверждён UTC-интервал долговечного сохранения коэффициента";
    if (!latestDue(receipt.persistenceEarliest()).equals(latestDue(receipt.persistenceLatest())))
      return "Интервал сохранения коэффициента пересекает 02:00 МСК; порядок относительно срока не подтверждён";
    return null;
  }

  private String validatePair(CapitalEvidence source, CapitalEvidence mirror, Instant now) {
    if (source == null
        || mirror == null
        || !source.compositionConfirmed()
        || !mirror.compositionConfirmed()) return "Не подтверждён состав рублёвой оценки капитала";
    if ((source.receivedMonotonicNanos() == null) != (mirror.receivedMonotonicNanos() == null))
      return "Не подтверждена единая монотонная шкала ответов капитала";
    if (!properties.sourceAccount().equals(source.accountId())
        || !properties.mirrorAccount().equals(mirror.accountId()))
      return "Оценки относятся к неверным счетам";
    if (source.totalRub() == null || source.totalRub().signum() <= 0)
      return "Капитал источника должен быть положительным";
    if (mirror.totalRub() == null || mirror.totalRub().signum() < 0)
      return "Капитал повторения отрицателен или неизвестен";
    if (source.requestStarted() == null
        || mirror.requestStarted() == null
        || source.receivedAt() == null
        || mirror.receivedAt() == null)
      return "Не подтверждены метки получения полных оценок капитала";
    Instant first =
        source.requestStarted().isBefore(mirror.requestStarted())
            ? source.requestStarted()
            : mirror.requestStarted();
    if (now.isBefore(first) || Duration.between(first, now).compareTo(Duration.ofSeconds(5)) > 0)
      return "Получение и сохранение оценок превысило 5 секунд";
    if (Duration.between(source.receivedAt(), mirror.receivedAt())
            .abs()
            .compareTo(Duration.ofSeconds(3))
        > 0) return "Разрыв получения оценок превысил 3 секунды";
    return null;
  }

  public Status stop() {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          if (!"RESTORED".equals(s.mode)) s.mode = "STOPPING";
          s.generation++;
          s.attemptId = null;
          s.attemptKind = null;
          s.automaticBoundary = null;
          s.permitIssuedAt = null;
          s.permitLimit = null;
          s.permitId = null;
          s.permitEpoch++;
          s.retryAt = null;
          s.recalculationPaused = false;
          s.pauseStartedAt = null;
          s.recalculationErrorNotificationId = null;
          s.lastRecalculationNotificationReason = null;
          s.failedAttempts = 0;
          unsentIntents.getObject().discardUnsent("Общая ручная остановка до начала отправки");
          em.persist(
              new JournalEntry(clock.instant(), "MANUAL_STOP", null, "Остановить повторение"));
          return view(s);
        });
  }

  public Status stoppedAfterReconciliation() {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          if ("STOPPING".equals(s.mode)) s.mode = "STOPPED";
          return view(s);
        });
  }

  public Status markReconciled() {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          s.reconciled = true;
          return view(s);
        });
  }

  public void markFeedReady(boolean ready) {
    tx.executeWithoutResult(
        _ -> {
          ControlState s = lock();
          if (ready && !s.observationActivated && s.observationBoundary != null) {
            var bound = time.bound();
            Instant activated = bound.map(ClockEvidence.Bound::latest).orElse(clock.instant());
            s.observationBoundary = activated;
            s.resumeBoundary = activated;
            s.observationBoundaryTrusted = bound.isPresent();
            s.resumeBoundaryTrusted = bound.isPresent();
            s.observationActivated = true;
            em.persist(
                new JournalEntry(
                    clock.instant(),
                    "OBSERVATION_ACTIVATED",
                    null,
                    "Успешное включение наблюдения; граница " + activated));
          }
          s.feedReady = ready;
        });
  }

  public Status recoverStartup() {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          candidates.rejectPending();
          invalidateInstrumentReconciliation();
          s.reconciled = false;
          s.feedReady = false;
          s.attemptId = null;
          s.attemptKind = null;
          em.persist(
              new JournalEntry(
                  clock.instant(),
                  "PROCESS_STARTED",
                  null,
                  "До торговых действий требуется сверка"));
          return view(s);
        });
  }

  public Status requireReconciliation(String reason) {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          s.reconciled = false;
          s.lastError = reason;
          return view(s);
        });
  }

  public Status markRestored() {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          s.mode = "RESTORED";
          s.generation++;
          s.attemptId = null;
          s.reconciled = false;
          s.automaticBoundary = null;
          s.permitIssuedAt = null;
          s.permitLimit = null;
          s.permitId = null;
          s.permitEpoch++;
          s.retryAt = null;
          notificationService.ignoreRestoredQueue();
          connectivity.restoredCopyOpened();
          invalidateInstrumentReconciliation();
          em.persist(
              new JournalEntry(clock.instant(), "BACKUP_RESTORED", null, "Только чтение и сверка"));
          return view(s);
        });
  }

  public Status issuePermit(BigDecimal limit) {
    if (limit != null
        && (limit.stripTrailingZeros().scale() > 9
            || (long) limit.precision() - limit.scale() > 29))
      throw new IllegalArgumentException(
          "Рублёвый предел допуска должен содержать не более 9 знаков после запятой и 29 до неё");
    return tx.execute(
        _ -> {
          ControlState s = lock();
          if (!running(s)
              || !s.reconciled
              || !shadowQualified(s)
              || limit == null
              || limit.signum() <= 0)
            throw new IllegalStateException(
                "Допуск требует работающего теневого режима, успешных проверок покупки и продажи, сверки и положительного предела RUB");
          if (em.createQuery(
                      "select count(i) from CopyIntent i where i.acceptanceAttempt=true and i.requestedLots>0 and i.acceptanceReconciled=false",
                      Long.class)
                  .getSingleResult()
              > 0)
            throw new IllegalStateException(
                "Предыдущая приёмочная попытка требует завершения и сверки");
          establishBoundary(s);
          Instant issued =
              time.bound()
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "Не подтверждено текущее время для выдачи допуска"))
                  .latest();
          s.permitLimit = limit;
          s.permitIssuedAt = issued;
          s.permitId = UUID.randomUUID();
          s.permitEpoch++;
          em.persist(
              new JournalEntry(clock.instant(), "PERMIT_ISSUED", null, limit.toPlainString()));
          return view(s);
        });
  }

  private void invalidateInstrumentReconciliation() {
    for (var instrument :
        em.createQuery("from InstrumentState order by instrumentId", InstrumentState.class)
            .getResultList()) instrument.reconciled = false;
  }

  public Status revokePermit() {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          s.permitLimit = null;
          s.permitIssuedAt = null;
          s.permitId = null;
          s.permitEpoch++;
          return view(s);
        });
  }

  public void restoreKnownUnsentPermit(
      UUID permitId, long epoch, long generation, Instant issuedAt, BigDecimal limit) {
    tx.executeWithoutResult(
        _ -> {
          ControlState s = lock();
          if (permitId != null
              && s.generation == generation
              && s.permitEpoch == epoch
              && s.permitId == null
              && running(s)) {
            s.permitId = permitId;
            s.permitIssuedAt = issuedAt;
            s.permitLimit = limit;
            em.persist(
                new JournalEntry(
                    clock.instant(), "UNSENT_PERMIT_RESTORED", null, permitId.toString()));
          }
        });
  }

  public Status enableAutomatic() {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          if (!running(s) || !s.reconciled || !s.liveAccepted)
            throw new IllegalStateException("Требуется подтверждённая живая приёмка и сверка");
          establishBoundary(s);
          s.mode = "AUTOMATIC";
          s.automaticBoundary =
              time.bound()
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "Не подтверждено текущее время для активации автоотправки"))
                  .latest();
          return view(s);
        });
  }

  public void confirmLiveAcceptance() {
    tx.executeWithoutResult(
        _ -> {
          ControlState s = lock();
          s.liveAccepted = true;
          s.firstAcceptance = false;
        });
  }

  public void shadowChecked(String version, String side, boolean success) {
    tx.executeWithoutResult(
        _ -> {
          ControlState s = lock();
          if (!Objects.equals(s.shadowVersion, version)) {
            s.shadowVersion = version;
            s.shadowBuyChecked = false;
            s.shadowSellChecked = false;
          }
          if ("BUY".equals(side)) s.shadowBuyChecked = success;
          else if ("SELL".equals(side)) s.shadowSellChecked = success;
        });
  }

  public boolean shadowQualified() {
    return tx.execute(_ -> shadowQualified(lock()));
  }

  private boolean shadowQualified(ControlState s) {
    return algorithms.current().equals(s.shadowVersion)
        && s.shadowBuyChecked
        && s.shadowSellChecked;
  }

  private void establishBoundary(ControlState s) {
    if (!running(s) || (s.observationBoundaryTrusted && s.resumeBoundaryTrusted)) return;
    var bound = time.bound();
    if (bound.isEmpty()) return;
    Instant confirmed = bound.get().latest();
    if (!s.observationBoundaryTrusted) {
      s.observationBoundary = confirmed;
      s.observationBoundaryTrusted = true;
    }
    if (!s.resumeBoundaryTrusted) {
      s.resumeBoundary = confirmed;
      s.resumeBoundaryTrusted = true;
    }
    em.persist(
        new JournalEntry(
            clock.instant(),
            "BOUNDARY_TIME_CONFIRMED",
            null,
            "После восстановления времени подтверждена граница "
                + confirmed
                + "; более ранние исполнения не допускаются"));
  }

  public void pauseInstrument(String instrument, String reason) {
    tx.executeWithoutResult(
        _ -> {
          lock();
          InstrumentState s = instrument(instrument);
          if (s.paused && Objects.equals(s.reason, reason)) return;
          s.paused = true;
          s.reconciled = false;
          s.reason = reason;
          notifications.publish(
              "INSTRUMENT_PAUSED", instrument, clock.instant(), instrument + ": " + reason);
          em.persist(new JournalEntry(clock.instant(), "INSTRUMENT_PAUSED", instrument, reason));
        });
  }

  public void resumeInstrument(String instrument) {
    tx.executeWithoutResult(
        _ -> {
          lock();
          InstrumentState s = instrument(instrument);
          if (!s.reconciled || s.waiting)
            throw new IllegalStateException("Сначала требуется успешная сверка инструмента");
          s.paused = false;
          s.reason = null;
        });
  }

  public void instrumentReconciled(
      String instrument, boolean matches, boolean orderProven, String reason) {
    tx.executeWithoutResult(
        _ -> {
          lock();
          InstrumentState s = instrument(instrument);
          s.reconciled = matches && orderProven;
          s.waiting = matches && !orderProven;
          if (!matches) {
            s.paused = true;
            s.reason = reason;
            notifications.publish(
                "POSITION_MISMATCH", instrument, clock.instant(), instrument + ": " + reason);
          } else if (!s.paused) s.reason = s.waiting ? reason : null;
        });
  }

  InstrumentState instrument(String id) {
    InstrumentState s = em.find(InstrumentState.class, id, LockModeType.PESSIMISTIC_WRITE);
    if (s == null) {
      s = new InstrumentState(id);
      em.persist(s);
    }
    return s;
  }

  static boolean running(ControlState s) {
    return "SHADOW".equals(s.mode) || "AUTOMATIC".equals(s.mode);
  }

  public static LocalDate latestDue(Instant now) {
    var local = now.atZone(MOSCOW);
    return local.toLocalTime().isBefore(LocalTime.of(2, 0))
        ? local.toLocalDate().minusDays(1)
        : local.toLocalDate();
  }

  public boolean mandatoryDue() {
    return tx.execute(
        _ -> {
          ControlState s = lock();
          var bound = time.bound();
          if (bound.isEmpty()) return false;
          return running(s)
              && (s.completedDue == null
                  || s.completedDue.isBefore(latestDue(bound.get().earliest())))
              && (s.retryAt == null || !clock.instant().isBefore(s.retryAt));
        });
  }

  private static final class CapitalReadFailure extends RuntimeException {
    private final String ownerText;

    private CapitalReadFailure(String role, RuntimeException cause) {
      super(cause);
      var code = io.grpc.Status.fromThrowable(cause).getCode();
      String kind =
          code == io.grpc.Status.Code.UNKNOWN ? cause.getClass().getSimpleName() : code.name();
      ownerText = "Ошибка чтения капитала " + role + ": " + kind;
    }
  }

  private static String safeError(RuntimeException e) {
    if (e instanceof CapitalReadFailure failure) return failure.ownerText;
    return e instanceof IllegalStateException
        ? e.getMessage()
        : "Не удалось получить подтверждённые оценки брокера";
  }
}
