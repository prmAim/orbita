package ru.orbita.copy;

import jakarta.persistence.*;
import java.math.*;
import java.time.*;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.BrokerGateway.SourceExecution;
import ru.orbita.broker.SourceFeedJournal;
import ru.orbita.control.ControlService;
import ru.orbita.control.OrbitaProperties;
import ru.orbita.control.TimeAuthority;
import ru.orbita.journal.JournalEntry;
import ru.orbita.notifications.NotificationPublisher;
import ru.orbita.persistence.PostgresTimestampPrecision;

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

  public boolean acceptRecovered(SourceExecution raw, SourceExecution canonical) {
    return acceptRecovered(raw, canonical, canonical.tradeId());
  }

  public boolean acceptRecovered(
      SourceExecution raw, SourceExecution canonical, String canonicalTradeId) {
    validate(canonical);
    if (!aliasPairMatches(raw, canonical) || canonicalTradeId == null || canonicalTradeId.isBlank())
      return false;
    return tx.execute(
        _ -> {
          control.lockForSourceRecovery();
          String fingerprint = aliasFingerprint(raw);
          var rawAlias = findAlias(raw.accountId(), raw.tradeId());
          var stageAlias = findAlias(raw.accountId(), canonicalTradeId);
          if (!aliasMatches(rawAlias, canonicalTradeId, fingerprint)
              || !aliasMatches(stageAlias, canonicalTradeId, fingerprint)) return false;

          var rawEvent = em.find(SourceExecutionEntity.class, key(raw.accountId(), raw.tradeId()));
          var stageEvent =
              em.find(SourceExecutionEntity.class, key(raw.accountId(), canonicalTradeId));
          if (rawAlias == null && stageAlias == null && rawEvent == null && stageEvent == null) {
            if (!canonical.tradeId().equals(canonicalTradeId)
                || !aliasTargetsAgree(raw.accountId(), canonicalTradeId, canonicalTradeId))
              return false;
            accept(canonical, true);
            saveRecoveryAliases(raw, canonicalTradeId, canonicalTradeId, fingerprint);
            return true;
          }
          String targetTradeId =
              rawAlias != null
                  ? rawAlias.executionTradeId
                  : stageAlias != null
                      ? stageAlias.executionTradeId
                      : rawEvent != null
                              && "RECEIVED".equals(rawEvent.disposition)
                              && stageEvent != null
                          ? stageEvent.tradeId
                          : rawEvent != null ? rawEvent.tradeId : stageEvent.tradeId;
          var targetEvent =
              em.find(SourceExecutionEntity.class, key(raw.accountId(), targetTradeId));
          if (targetEvent == null
              || !(canonical.tradeId().equals(canonicalTradeId)
                  || (rawAlias != null && canonical.tradeId().equals(targetTradeId)))
              || !aliasEventMatches(targetEvent, canonical)
              || !aliasTargetsAgree(raw.accountId(), canonicalTradeId, targetTradeId)) return false;

          boolean mergeRawEvent = rawEvent != null && rawEvent != targetEvent;
          if (mergeRawEvent
              && (!"RECEIVED".equals(rawEvent.disposition)
                  || rawEvent.acceptedSequence != 0
                  || !aliasEventMatches(rawEvent, canonical))) return false;
          if (stageEvent != null && stageEvent != targetEvent) return false;
          if ("SKIPPED".equals(targetEvent.disposition))
            return !mergeRawEvent
                && ((rawAlias != null && stageAlias != null)
                    || (targetTradeId.equals(raw.tradeId())
                        && targetTradeId.equals(canonicalTradeId)))
                && targetEvent.acceptedSequence == 0
                && targetEvent.executedAt != null
                && same(targetEvent, canonical);
          if (!"ACCEPTED".equals(targetEvent.disposition)
              && !"RECEIVED".equals(targetEvent.disposition)) return false;

          if (mergeRawEvent) {
            mergeReceiptId(
                key(raw.accountId(), rawEvent.tradeId),
                key(targetEvent.accountId, targetEvent.tradeId));
            em.remove(rawEvent);
            em.flush();
          }
          if ("RECEIVED".equals(targetEvent.disposition) && targetEvent.sourceOrderId.isBlank())
            targetEvent.sourceOrderId = canonical.sourceOrderId();
          var acceptedExecution =
              new SourceExecution(
                  canonical.accountId(),
                  targetEvent.tradeId,
                  canonical.sourceOrderId(),
                  canonical.instrumentId(),
                  canonical.side(),
                  canonical.lots(),
                  canonical.price(),
                  canonical.currency(),
                  canonical.executedAt(),
                  targetEvent.receivedAt);
          accept(acceptedExecution, true);
          saveRecoveryAliases(raw, targetTradeId, canonicalTradeId, fingerprint);
          return true;
        });
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
            event.currency = normalizeCurrency(event.currency);
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
                        && !PostgresTimestampPrecision.normalize(current.get().latest())
                            .isAfter(
                                PostgresTimestampPrecision.normalize(
                                    receipt.firstEarliest.plusSeconds(60))))) {
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
          event.currency = normalizeCurrency(source.currency());
          event.executedAt = source.executedAt();
          event.receivedAt = source.receivedAt();
          if (event.executedAt != null) {
            Instant latest =
                em.createQuery(
                        "select max(e.executedAt) from SourceExecutionEntity e where e.instrumentId=:instrument",
                        Instant.class)
                    .setParameter("instrument", event.instrumentId)
                    .getSingleResult();
            event.requiresRecovery =
                latest != null
                    && PostgresTimestampPrecision.normalize(event.executedAt)
                        .isBefore(PostgresTimestampPrecision.normalize(latest));
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
    Instant executedAt = PostgresTimestampPrecision.normalize(event.executedAt);
    Instant boundary = PostgresTimestampPrecision.normalize(gate.boundary());
    if (boundary == null || !executedAt.isAfter(boundary)) {
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
            .map(
                bound ->
                    TradePolicy.ageRange(
                        executedAt,
                        PostgresTimestampPrecision.normalize(bound.earliest()),
                        PostgresTimestampPrecision.normalize(bound.latest())))
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
                .map(CopyService::sourceExecution));
  }

  public SourceFeedJournal.AliasLookup sourceExecutionAlias(SourceExecution raw) {
    return tx.execute(
        _ -> {
          var alias =
              em.find(
                  SourceExecutionAliasEntity.class,
                  new SourceExecutionAliasId(raw.accountId(), raw.tradeId()));
          if (alias == null)
            return new SourceFeedJournal.AliasLookup(
                SourceFeedJournal.AliasStatus.NONE, null, null);
          if (!alias.fingerprint.equals(aliasFingerprint(raw)))
            return new SourceFeedJournal.AliasLookup(
                SourceFeedJournal.AliasStatus.CONFLICT, null, null);
          var stageAlias = findAlias(raw.accountId(), alias.canonicalTradeId);
          if (stageAlias == null
              || !stageAlias.canonicalTradeId.equals(alias.canonicalTradeId)
              || !stageAlias.executionTradeId.equals(alias.executionTradeId)
              || !stageAlias.fingerprint.equals(alias.fingerprint)
              || !aliasTargetsAgree(
                  raw.accountId(), alias.canonicalTradeId, alias.executionTradeId))
            return new SourceFeedJournal.AliasLookup(
                SourceFeedJournal.AliasStatus.CONFLICT, null, null);
          var canonical =
              em.find(SourceExecutionEntity.class, key(raw.accountId(), alias.executionTradeId));
          if (canonical == null
              || canonical.sourceOrderId == null
              || canonical.sourceOrderId.isBlank()
              || !aliasEventMatches(canonical, raw)
              || !("ACCEPTED".equals(canonical.disposition)
                  || "RECEIVED".equals(canonical.disposition)
                  || ("SKIPPED".equals(canonical.disposition)
                      && canonical.acceptedSequence == 0
                      && canonical.executedAt != null)))
            return new SourceFeedJournal.AliasLookup(
                SourceFeedJournal.AliasStatus.CONFLICT, null, null);
          return new SourceFeedJournal.AliasLookup(
              SourceFeedJournal.AliasStatus.MATCH,
              sourceExecution(canonical),
              alias.canonicalTradeId);
        });
  }

  private SourceExecutionAliasEntity findAlias(String account, String aliasTradeId) {
    return em.find(
        SourceExecutionAliasEntity.class, new SourceExecutionAliasId(account, aliasTradeId));
  }

  private boolean aliasMatches(
      SourceExecutionAliasEntity alias, String canonicalTradeId, String fingerprint) {
    return alias == null
        || (alias.canonicalTradeId.equals(canonicalTradeId)
            && alias.fingerprint.equals(fingerprint));
  }

  private boolean aliasTargetsAgree(
      String account, String canonicalTradeId, String executionTradeId) {
    var targets =
        em.createQuery(
                "select distinct a.executionTradeId from SourceExecutionAliasEntity a where a.id.accountId=:account and a.canonicalTradeId=:canonicalTradeId",
                String.class)
            .setParameter("account", account)
            .setParameter("canonicalTradeId", canonicalTradeId)
            .setMaxResults(2)
            .getResultList();
    return targets.stream().allMatch(executionTradeId::equals);
  }

  private void saveRecoveryAliases(
      SourceExecution raw, String targetTradeId, String canonicalTradeId, String fingerprint) {
    saveAlias(raw.accountId(), raw.tradeId(), targetTradeId, canonicalTradeId, fingerprint);
    if (!canonicalTradeId.equals(raw.tradeId()))
      saveAlias(raw.accountId(), canonicalTradeId, targetTradeId, canonicalTradeId, fingerprint);
  }

  private void saveAlias(
      String account,
      String aliasTradeId,
      String executionTradeId,
      String canonicalTradeId,
      String fingerprint) {
    if (findAlias(account, aliasTradeId) != null) return;
    em.persist(
        new SourceExecutionAliasEntity(
            account, aliasTradeId, executionTradeId, canonicalTradeId, fingerprint));
  }

  private void mergeReceiptId(String fromId, String toId) {
    if (fromId.equals(toId)) return;
    var from = em.find(IncomingReceipt.class, fromId);
    if (from == null) return;
    var to = em.find(IncomingReceipt.class, toId);
    if (to == null) {
      var replacement = new IncomingReceipt(toId, from.firstReceived);
      replacement.firstEarliest = from.firstEarliest;
      replacement.firstLatest = from.firstLatest;
      em.remove(from);
      em.flush();
      em.persist(replacement);
      return;
    }
    if (from.firstReceived.isBefore(to.firstReceived)) {
      to.firstReceived = from.firstReceived;
      to.firstEarliest = from.firstEarliest;
      to.firstLatest = from.firstLatest;
    }
    em.remove(from);
  }

  private static boolean aliasEventMatches(SourceExecutionEntity event, SourceExecution execution) {
    return event.accountId.equals(execution.accountId())
        && (knownOrderMatches(execution.sourceOrderId(), event.sourceOrderId)
            || ("RECEIVED".equals(event.disposition) && event.sourceOrderId.isBlank()))
        && event.instrumentId.equals(execution.instrumentId())
        && event.side.equals(execution.side().name())
        && event.lots.compareTo(execution.lots()) == 0
        && event.price.compareTo(execution.price()) == 0
        && event.currency.equalsIgnoreCase(execution.currency())
        && (event.executedAt == null
            || Objects.equals(
                PostgresTimestampPrecision.normalize(event.executedAt),
                PostgresTimestampPrecision.normalize(execution.executedAt())));
  }

  private static boolean aliasPairMatches(SourceExecution raw, SourceExecution canonical) {
    return raw != null
        && raw.accountId().equals(canonical.accountId())
        && !raw.tradeId().isBlank()
        && knownOrderMatches(raw.sourceOrderId(), canonical.sourceOrderId())
        && raw.instrumentId().equals(canonical.instrumentId())
        && raw.side() == canonical.side()
        && raw.lots().compareTo(canonical.lots()) == 0
        && raw.price().compareTo(canonical.price()) == 0
        && raw.currency().equalsIgnoreCase(canonical.currency())
        && Objects.equals(
            PostgresTimestampPrecision.normalize(raw.executedAt()),
            PostgresTimestampPrecision.normalize(canonical.executedAt()));
  }

  private static boolean knownOrderMatches(String incoming, String stored) {
    return incoming != null && (incoming.isBlank() || incoming.equals(stored));
  }

  private static String aliasFingerprint(SourceExecution execution) {
    try {
      var digest = java.security.MessageDigest.getInstance("SHA-256");
      var values =
          List.of(
              execution.accountId(),
              execution.instrumentId(),
              execution.side().name(),
              execution.lots().stripTrailingZeros().toPlainString(),
              execution.price().stripTrailingZeros().toPlainString(),
              execution.currency().toUpperCase(Locale.ROOT),
              execution.executedAt() == null ? "" : execution.executedAt().toString());
      for (String value : values) {
        byte[] encoded = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        digest.update(java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(encoded.length).array());
        digest.update(encoded);
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
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

  private static SourceExecution sourceExecution(SourceExecutionEntity e) {
    return new SourceExecution(
        e.accountId,
        e.tradeId,
        e.sourceOrderId,
        e.instrumentId,
        ru.orbita.broker.BrokerGateway.Side.valueOf(e.side),
        e.lots,
        e.price,
        e.currency,
        e.executedAt,
        e.receivedAt);
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
    return sameExceptTime(e, other)
        && Objects.equals(
            PostgresTimestampPrecision.normalize(e.executedAt),
            PostgresTimestampPrecision.normalize(other.executedAt()));
  }

  private static boolean sameExceptTime(SourceExecutionEntity e, SourceExecution other) {
    return e.sourceOrderId.equals(other.sourceOrderId())
        && e.instrumentId.equals(other.instrumentId())
        && e.side.equals(other.side().name())
        && e.lots.compareTo(other.lots()) == 0
        && e.price.compareTo(other.price()) == 0
        && e.currency.equalsIgnoreCase(other.currency());
  }

  private static String normalizeCurrency(String currency) {
    return currency.toUpperCase(Locale.ROOT);
  }
}
