package ru.orbita.copy;

import jakarta.persistence.*;
import java.math.*;
import java.time.*;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.BrokerGateway.SourceExecution;
import ru.orbita.control.ControlService;
import ru.orbita.control.OrbitaProperties;
import ru.orbita.control.TimeAuthority;
import ru.orbita.journal.JournalEntry;
import ru.orbita.notifications.NotificationPublisher;

/**
 * Durable execution-input seam: receipt, acceptance and each lot decision are independent facts.
 */
@Service
public class CopyService {
  private final EntityManager em;
  private final TransactionTemplate tx;
  private final ControlService control;
  private final Clock clock;
  private final OrbitaProperties properties;
  private final NotificationPublisher notifications;
  private final TimeAuthority time;

  public CopyService(
      EntityManager em,
      TransactionTemplate tx,
      ControlService control,
      Clock clock,
      OrbitaProperties properties,
      NotificationPublisher notifications,
      TimeAuthority time) {
    this.em = em;
    this.tx = tx;
    this.control = control;
    this.clock = clock;
    this.properties = properties;
    this.notifications = notifications;
    this.time = time;
  }

  public record Decision(
      String executionId, String disposition, BigDecimal acceptedCoefficient, long newLots) {}

  public Decision accept(SourceExecution source) {
    return accept(source, false);
  }

  public Decision acceptRecovered(SourceExecution source) {
    return accept(source, true);
  }

  private Decision accept(SourceExecution source, boolean recoveryWindowProven) {
    validate(source);
    return tx.execute(
        _ -> {
          var gate =
              control
                  .gate(); // single account lock also serializes coefficient acceptance and global
          // resource allocation
          String id = key(source.accountId(), source.tradeId());
          SourceExecutionEntity event = em.find(SourceExecutionEntity.class, id);
          if (event != null) {
            if (event.executedAt == null
                && source.executedAt() != null
                && sameExceptTime(event, source)) {
              if ("RECEIVED".equals(event.disposition)) {
                var receipt = em.find(IncomingReceipt.class, id);
                var current = time.bound();
                if (recoveryWindowProven
                    || (receipt != null
                        && receipt.firstEarliest != null
                        && current.isPresent()
                        && !current
                            .get()
                            .latest()
                            .isAfter(receipt.firstEarliest.plusSeconds(60)))) {
                  event.executedAt = source.executedAt();
                  calculate(event, gate);
                }
              } else event.executedAt = source.executedAt();
              return decision(event);
            }
            if (!same(event, source)) {
              control.pauseInstrument(
                  source.instrumentId(), "Противоречивые сведения об одном исходном исполнении");
            }
            return decision(event);
          }
          event = new SourceExecutionEntity();
          event.id = id;
          event.accountId = source.accountId();
          event.tradeId = source.tradeId();
          event.sourceOrderId = source.sourceOrderId();
          event.instrumentId = source.instrumentId();
          event.side = source.side().name();
          event.lots = source.lots();
          event.price = source.price();
          event.currency = source.currency();
          event.executedAt = source.executedAt();
          event.receivedAt = source.receivedAt();
          if (event.executedAt != null) {
            Instant latest =
                em.createQuery(
                        "select max(e.executedAt) from SourceExecutionEntity e where e.instrumentId=:instrument",
                        Instant.class)
                    .setParameter("instrument", event.instrumentId)
                    .getSingleResult();
            event.requiresRecovery = latest != null && event.executedAt.isBefore(latest);
          }
          em.persist(event);
          if (event.requiresRecovery)
            control.instrumentReconciled(
                event.instrumentId,
                true,
                false,
                "Нарушен порядок доставки исходных исполнений; требуется восстановление истории");
          var receipt =
              new JournalEntry(clock.instant(), "SOURCE_RECEIVED", event.instrumentId, id);
          em.persist(receipt);
          event.receivedSequence = receipt.id();
          calculate(event, gate);
          return decision(event);
        });
  }

  public void retryPending() {
    tx.executeWithoutResult(
        _ -> {
          var gate = control.gate();
          for (var event :
              em.createQuery(
                      "from SourceExecutionEntity where disposition='RECEIVED' order by receivedSequence,id",
                      SourceExecutionEntity.class)
                  .setMaxResults(100)
                  .getResultList()) calculate(event, gate);
        });
  }

  private void calculate(SourceExecutionEntity event, ControlService.Gate gate) {
    Instant now = clock.instant();
    if (event.executedAt == null) return;
    if (gate.boundary() == null || !event.executedAt.isAfter(gate.boundary())) {
      skip(event, "Исполнение до границы наблюдения или возобновления");
      return;
    }
    if (!gate.running()) {
      skip(event, "Исполнение во время ручной остановки");
      return;
    }
    if (gate.exactCoefficient() == null || gate.recalculationPaused() || !gate.reconciled()) return;
    TradePolicy.Age age =
        time.bound()
            .map(bound -> TradePolicy.ageRange(event.executedAt, bound.earliest(), bound.latest()))
            .orElse(TradePolicy.Age.WAIT_FOR_TIME);
    if (age == TradePolicy.Age.WAIT_FOR_TIME) return;
    if (age != TradePolicy.Age.ALLOWED) {
      skip(
          event,
          age == TradePolicy.Age.EXPIRED
              ? "Возраст исполнения превышает 60 секунд"
              : "Метка исполнения находится в будущем");
      return;
    }
    event.coefficient = gate.coefficient();
    event.ratioNumerator = gate.exactCoefficient().numerator();
    event.ratioDenominator = gate.exactCoefficient().denominator();
    event.contribution = gate.exactCoefficient().multiply(event.lots).decimal(100);
    Rational previous =
        em
            .createQuery(
                "from SourceExecutionEntity e where e.sourceOrderId=:order and e.accountId=:account and e.id<>:id and e.acceptedSequence>0",
                SourceExecutionEntity.class)
            .setParameter("order", event.sourceOrderId)
            .setParameter("account", event.accountId)
            .setParameter("id", event.id)
            .getResultList()
            .stream()
            .map(e -> exactCoefficient(e).multiply(e.lots))
            .reduce(Rational.ZERO, Rational::add);
    event.newLots = CopyCalculation.increment(previous, event.lots, gate.exactCoefficient());
    event.disposition = "ACCEPTED";
    var accepted =
        new JournalEntry(
            now,
            "SOURCE_ACCEPTED",
            event.instrumentId,
            event.id
                + " coefficient="
                + event.ratioNumerator
                + "/"
                + event.ratioDenominator
                + " display="
                + event.coefficient
                + " contribution="
                + event.contribution
                + " increment="
                + event.newLots);
    em.persist(accepted);
    event.acceptedSequence = accepted.id();
    if (event.newLots == 0) return;
    var intent = new CopyIntent();
    intent.id = UUID.randomUUID();
    intent.executionId = event.id;
    intent.instrumentId = event.instrumentId;
    intent.sourceOrderId = event.sourceOrderId;
    intent.side = event.side;
    intent.currency = event.currency;
    intent.lots = event.newLots;
    intent.referencePrice = event.price;
    intent.executedAt = event.executedAt;
    intent.createdAt = now;
    intent.prioritySequence = event.receivedSequence;
    em.persist(intent);
  }

  private void skip(SourceExecutionEntity event, String reason) {
    event.disposition = "SKIPPED";
    em.persist(
        new JournalEntry(
            clock.instant(), "SOURCE_SKIPPED", event.instrumentId, event.id + " " + reason));
    notifications.publish(
        "SOURCE_SKIPPED",
        event.instrumentId,
        clock.instant(),
        TradeNotificationText.sourceSkip(event.instrumentId, event.side, event.lots, reason));
  }

  public List<CopyIntent.View> intents() {
    return tx.execute(
        _ ->
            em
                .createQuery("from CopyIntent order by createdAt desc,id", CopyIntent.class)
                .setMaxResults(200)
                .getResultList()
                .stream()
                .map(CopyIntent::view)
                .toList());
  }

  public List<MirrorPosition.View> positions() {
    return tx.execute(
        _ ->
            em
                .createQuery("from MirrorPosition order by instrumentId", MirrorPosition.class)
                .getResultList()
                .stream()
                .map(MirrorPosition::view)
                .toList());
  }

  public List<HoldingAllocation.View> holdingAllocations() {
    return tx.execute(
        _ ->
            em
                .createQuery(
                    "from HoldingAllocation order by sourceExecutionId", HoldingAllocation.class)
                .getResultList()
                .stream()
                .map(HoldingAllocation::view)
                .toList());
  }

  public List<JournalEntry.View> journal(long beforeId, int limit) {
    return tx.execute(
        _ ->
            em
                .createQuery(
                    "from JournalEntry where id<:before order by id desc", JournalEntry.class)
                .setParameter("before", beforeId <= 0 ? Long.MAX_VALUE : beforeId)
                .setMaxResults(Math.max(1, Math.min(200, limit)))
                .getResultList()
                .stream()
                .map(JournalEntry::view)
                .toList());
  }

  public Optional<String> sourceOrderId(String account, String trade) {
    return tx.execute(
        _ ->
            Optional.ofNullable(em.find(SourceExecutionEntity.class, key(account, trade)))
                .map(e -> e.sourceOrderId));
  }

  public Optional<SourceExecution> remembered(String account, String trade) {
    return tx.execute(
        _ ->
            Optional.ofNullable(em.find(SourceExecutionEntity.class, key(account, trade)))
                .map(
                    e ->
                        new SourceExecution(
                            e.accountId,
                            e.tradeId,
                            e.sourceOrderId,
                            e.instrumentId,
                            ru.orbita.broker.BrokerGateway.Side.valueOf(e.side),
                            e.lots,
                            e.price,
                            e.currency,
                            e.executedAt,
                            e.receivedAt)));
  }

  public void skipSource(SourceExecution source, String reason) {
    accept(source);
    tx.executeWithoutResult(
        _ -> {
          control.gate();
          var row = em.find(SourceExecutionEntity.class, key(source.accountId(), source.tradeId()));
          if ("RECEIVED".equals(row.disposition)) skip(row, reason);
        });
  }

  static String key(String account, String trade) {
    return account.length() + ":" + account + trade;
  }

  static Rational exactCoefficient(SourceExecutionEntity e) {
    return new Rational(e.ratioNumerator, e.ratioDenominator);
  }

  private static Decision decision(SourceExecutionEntity event) {
    return new Decision(event.id, event.disposition, event.coefficient, event.newLots);
  }

  private void validate(SourceExecution e) {
    if (!Objects.equals(properties.sourceAccount(), e.accountId())
        || e.tradeId() == null
        || e.tradeId().isBlank()
        || e.sourceOrderId() == null
        || e.sourceOrderId().isBlank()
        || e.instrumentId() == null
        || e.instrumentId().isBlank()
        || e.side() == null
        || e.lots() == null
        || e.lots().signum() <= 0
        || e.price() == null
        || e.price().signum() <= 0
        || e.receivedAt() == null
        || e.currency() == null)
      throw new IllegalArgumentException("Incomplete execution identity or wrong source account");
  }

  private static boolean same(SourceExecutionEntity e, SourceExecution other) {
    return sameExceptTime(e, other) && Objects.equals(e.executedAt, other.executedAt());
  }

  private static boolean sameExceptTime(SourceExecutionEntity e, SourceExecution other) {
    return e.sourceOrderId.equals(other.sourceOrderId())
        && e.instrumentId.equals(other.instrumentId())
        && e.side.equals(other.side().name())
        && e.lots.compareTo(other.lots()) == 0
        && e.price.compareTo(other.price()) == 0
        && e.currency.equals(other.currency());
  }
}
