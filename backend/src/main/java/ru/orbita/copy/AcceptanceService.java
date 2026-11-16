package ru.orbita.copy;

import jakarta.persistence.EntityManager;
import java.math.*;
import java.time.*;
import java.util.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.control.*;
import ru.orbita.journal.JournalEntry;
import tools.jackson.databind.json.JsonMapper;

/** Automatically replays saved natural executions using retained independent broker evidence. */
@Service
public class AcceptanceService {
  private final EntityManager em;
  private final TransactionTemplate tx;
  private final ControlService control;
  private final ObjectProvider<BrokerGateway> gateways;
  private final AlgorithmVersion algorithms;
  private final Clock clock;
  private final OrbitaProperties properties;
  private final ru.orbita.valuation.RubValuation rubValuation;
  private final JsonMapper json;

  public AcceptanceService(
      EntityManager em,
      TransactionTemplate tx,
      ControlService control,
      ObjectProvider<BrokerGateway> gateways,
      AlgorithmVersion algorithms,
      Clock clock,
      OrbitaProperties properties,
      ru.orbita.valuation.RubValuation rubValuation,
      JsonMapper json) {
    this.em = em;
    this.tx = tx;
    this.control = control;
    this.gateways = gateways;
    this.algorithms = algorithms;
    this.clock = clock;
    this.properties = properties;
    this.rubValuation = rubValuation;
    this.json = json;
  }

  public void verifySavedShadow() {
    BrokerGateway gateway = gateways.getIfAvailable();
    for (String side : List.of("BUY", "SELL")) {
      SourceExecutionEntity event =
          tx.execute(
              _ ->
                  em.createQuery(
                          "from SourceExecutionEntity where side=:side and coefficient is not null order by acceptedSequence,id",
                          SourceExecutionEntity.class)
                      .setParameter("side", side)
                      .setMaxResults(1)
                      .getResultStream()
                      .findFirst()
                      .orElse(null));
      if (event == null) continue;
      boolean exists =
          tx.execute(
                  _ ->
                      em.createQuery(
                              "select count(c) from ShadowCheck c where c.executionId=:id and c.algorithmVersion=:version and c.executionMatches=true and c.calculationMatches=true",
                              Long.class)
                          .setParameter("id", event.id)
                          .setParameter("version", algorithms.current())
                          .getSingleResult())
              > 0;
      if (exists) {
        control.shadowChecked(algorithms.current(), side, true);
        continue;
      }
      History history = savedHistory(event.id);
      if (history == null) {
        // Legacy protocols retain their text unchanged. A fresh read supplies the structured
        // evidence that later corrected artifacts can replay without another broker request.
        if (gateway == null) continue;
        try {
          history =
              gateway.history(event.executedAt.minusSeconds(1), event.executedAt.plusSeconds(1));
        } catch (RuntimeException e) {
          PersistenceBoundary.rethrowBrokerPersistence(e);
          continue;
        }
      }
      History evidence = history;
      List<SourceExecution> exact =
          history.executions().stream()
              .filter(
                  e -> e.accountId().equals(event.accountId) && e.tradeId().equals(event.tradeId))
              .toList();
      boolean identity =
          history.complete() && exact.size() == 1 && matches(event, exact.getFirst());
      tx.executeWithoutResult(
          _ -> {
            var row = em.find(SourceExecutionEntity.class, event.id);
            Rational previous = prefix(row);
            Rational ratio = CopyService.exactCoefficient(row);
            long expected = previous.add(ratio.multiply(row.lots)).floor() - previous.floor();
            // Run the installed calculation on the retained source inputs. A former artifact's
            // incorrect output is audit history, not the oracle for a corrected calculation.
            long replayLots = CopyCalculation.increment(previous, row.lots, ratio);
            boolean calculation = replayLots == expected;
            ShadowCheck check = new ShadowCheck();
            check.id = UUID.randomUUID();
            check.executionId = row.id;
            check.algorithmVersion = algorithms.current();
            check.side = side;
            check.checkedAt = clock.instant();
            check.executionMatches = identity;
            check.calculationMatches = calculation;
            check.independentHistory = json.writeValueAsString(evidence);
            check.replayContribution = ratio.multiply(row.lots).decimal(100);
            check.replayNewLots = replayLots;
            check.detail =
                "Независимая история: "
                    + exact
                    + "; исходный коэффициент="
                    + row.coefficient
                    + "; ожидаемый прирост="
                    + expected
                    + "; повторный расчёт="
                    + replayLots
                    + "; исторически сохранённый="
                    + row.newLots;
            em.persist(check);
            control.shadowChecked(algorithms.current(), side, identity && calculation);
            em.persist(
                new JournalEntry(
                    clock.instant(), "SHADOW_CHECK", row.instrumentId, check.view().toString()));
          });
    }
  }

  private History savedHistory(String executionId) {
    String saved =
        tx.execute(
            _ ->
                em.createQuery(
                        "select c.independentHistory from ShadowCheck c where c.executionId=:id and c.executionMatches=true and c.independentHistory is not null order by c.checkedAt,c.id",
                        String.class)
                    .setParameter("id", executionId)
                    .setMaxResults(1)
                    .getResultStream()
                    .findFirst()
                    .orElse(null));
    return saved == null ? null : json.readValue(saved, History.class);
  }

  public void verifyLive() {
    BrokerGateway gateway = gateways.getIfAvailable();
    if (gateway == null) return;
    var candidates =
        tx.execute(
            _ ->
                em.createQuery(
                        "from CopyIntent where acceptanceAttempt=true and status='FINAL' and executedLots>0 and actualCommission is not null",
                        CopyIntent.class)
                    .getResultList());
    for (var intent : candidates) {
      var request =
          new OrderRequest(
              properties.mirrorAccount(),
              intent.id,
              intent.instrumentId,
              Side.valueOf(intent.side),
              intent.requestedLots,
              intent.limitPrice);
      OrderEvidence independent;
      try {
        independent = gateway.reconcile(request);
      } catch (RuntimeException e) {
        PersistenceBoundary.rethrowBrokerPersistence(e);
        continue;
      }
      boolean matches =
          independent.known()
              && properties.mirrorAccount().equals(independent.accountId())
              && intent.instrumentId.equals(independent.instrumentId())
              && intent.executedLots == independent.lotsExecuted()
              && independent.side() == Side.valueOf(intent.side)
              && independent.fills().stream()
                      .map(Fill::lots)
                      .reduce(BigDecimal.ZERO, BigDecimal::add)
                      .compareTo(BigDecimal.valueOf(intent.executedLots))
                  == 0;
      if (!matches || intent.permittedRubLimit == null) continue;
      Instrument instrument = gateway.instrument(intent.instrumentId);
      BigDecimal executedPrincipal =
          independent.fills().stream()
              .map(
                  f ->
                      f.lots()
                          .multiply(BigDecimal.valueOf(instrument.lotSize()))
                          .multiply(f.price()))
              .reduce(BigDecimal.ZERO, BigDecimal::add);
      CommissionEvidence fee = gateway.commission(request);
      Instant from =
          independent.fills().stream().map(Fill::executedAt).min(Instant::compareTo).orElse(null);
      Instant through =
          independent.fills().stream().map(Fill::executedAt).max(Instant::compareTo).orElse(null);
      var actual =
          rubValuation.finalCost(
              properties.mirrorAccount(),
              intent.instrumentId,
              intent.currency,
              intent.id,
              executedPrincipal,
              fee,
              from,
              through);
      if (!actual.within(intent.permittedRubLimit)) continue;
      tx.executeWithoutResult(
          _ -> {
            control.confirmLiveAcceptance();
            long recorded =
                em.createQuery(
                        "select count(j) from JournalEntry j where category='LIVE_ACCEPTED' and detail=:id",
                        Long.class)
                    .setParameter("id", intent.id.toString())
                    .getSingleResult();
            if (recorded == 0) {
              em.persist(
                  new JournalEntry(
                      clock.instant(), "LIVE_ACCEPTED", intent.instrumentId, intent.id.toString()));
              em.persist(
                  new JournalEntry(
                      clock.instant(),
                      "LIVE_COST_VERIFIED",
                      intent.instrumentId,
                      intent.id
                          + " RUB="
                          + actual.rub()
                          + " rate="
                          + actual.reference()
                          + " actualFee="
                          + fee.amount()
                          + " "
                          + fee.currency()));
            }
          });
    }
  }

  public List<ShadowCheck.View> checks() {
    return tx.execute(
        _ ->
            em
                .createQuery("from ShadowCheck order by checkedAt desc,id", ShadowCheck.class)
                .setMaxResults(100)
                .getResultList()
                .stream()
                .map(ShadowCheck::view)
                .toList());
  }

  public record Replay(
      String executionId,
      String accountId,
      String tradeId,
      String sourceOrderId,
      String instrumentId,
      String side,
      BigDecimal sourceLots,
      BigDecimal price,
      String currency,
      Instant executedAt,
      BigDecimal coefficient,
      BigDecimal contribution,
      long newLots,
      String algorithmVersion,
      java.math.BigInteger ratioNumerator,
      java.math.BigInteger ratioDenominator,
      boolean executionMatches,
      boolean calculationMatches,
      String independentEvidence) {}

  /** Read-only server-transfer proof; this never accepts input or creates a trading intent. */
  public Replay replayVerified(String executionId) {
    SourceExecutionEntity e = tx.execute(_ -> em.find(SourceExecutionEntity.class, executionId));
    if (e == null || e.coefficient == null || e.executedAt == null)
      throw new IllegalArgumentException("Нет принятого исходного исполнения для теневой проверки");
    BrokerGateway gateway = gateways.getIfAvailable();
    if (gateway == null)
      throw new IllegalStateException("Нет соединения для независимого чтения истории");
    History history = gateway.history(e.executedAt.minusSeconds(1), e.executedAt.plusSeconds(1));
    List<SourceExecution> exact =
        history.executions().stream()
            .filter(row -> e.accountId.equals(row.accountId()) && e.tradeId.equals(row.tradeId()))
            .toList();
    boolean identity = history.complete() && exact.size() == 1 && matches(e, exact.getFirst());
    Rational previous = tx.execute(_ -> prefix(e));
    Rational ratio = CopyService.exactCoefficient(e);
    long expected = CopyCalculation.increment(previous, e.lots, ratio);
    boolean calculation =
        e.contribution.compareTo(ratio.multiply(e.lots).decimal(100)) == 0 && e.newLots == expected;
    return new Replay(
        e.id,
        e.accountId,
        e.tradeId,
        e.sourceOrderId,
        e.instrumentId,
        e.side,
        e.lots,
        e.price,
        e.currency,
        e.executedAt,
        e.coefficient,
        e.contribution,
        e.newLots,
        algorithms.current(),
        e.ratioNumerator,
        e.ratioDenominator,
        identity,
        calculation,
        exact.toString());
  }

  private Rational prefix(SourceExecutionEntity e) {
    return prefixRows(e).stream()
        .map(row -> CopyService.exactCoefficient(row).multiply(row.lots))
        .reduce(Rational.ZERO, Rational::add);
  }

  private List<SourceExecutionEntity> prefixRows(SourceExecutionEntity e) {
    return em.createQuery(
            "from SourceExecutionEntity e where e.accountId=:account and e.sourceOrderId=:order and e.acceptedSequence<:sequence and e.acceptedSequence>0 order by e.acceptedSequence,e.id",
            SourceExecutionEntity.class)
        .setParameter("account", e.accountId)
        .setParameter("order", e.sourceOrderId)
        .setParameter("sequence", e.acceptedSequence)
        .getResultList();
  }

  private static boolean matches(SourceExecutionEntity e, SourceExecution actual) {
    return (actual.sourceOrderId() == null
            || actual.sourceOrderId().isBlank()
            || e.sourceOrderId.equals(actual.sourceOrderId()))
        && e.instrumentId.equals(actual.instrumentId())
        && e.side.equals(actual.side().name())
        && e.lots.compareTo(actual.lots()) == 0
        && e.price.compareTo(actual.price()) == 0
        && e.currency.equalsIgnoreCase(actual.currency())
        && e.executedAt.equals(actual.executedAt());
  }
}
