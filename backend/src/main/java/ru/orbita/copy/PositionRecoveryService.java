package ru.orbita.copy;

import jakarta.persistence.EntityManager;
import java.math.*;
import java.time.Clock;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.control.ControlService;
import ru.orbita.journal.JournalEntry;

/** Connects the confirmed recovery cases to the same durable intents used for ordinary trading. */
@Service
public class PositionRecoveryService {
  private final EntityManager em;
  private final TransactionTemplate tx;
  private final ControlService control;
  private final Clock clock;
  private final ru.orbita.control.TimeAuthority time;

  public PositionRecoveryService(
      EntityManager em,
      TransactionTemplate tx,
      ControlService control,
      Clock clock,
      ru.orbita.control.TimeAuthority time) {
    this.em = em;
    this.tx = tx;
    this.control = control;
    this.clock = clock;
    this.time = time;
  }

  public void recover() {
    tx.executeWithoutResult(
        _ -> {
          if (!control.gate().reconciled()) return;
          var rows =
              em.createQuery(
                      "from SourceExecutionEntity where executedAt is not null order by instrumentId,executedAt,id",
                      SourceExecutionEntity.class)
                  .getResultList();
          Map<String, List<SourceExecutionEntity>> groups = new LinkedHashMap<>();
          rows.forEach(e -> groups.computeIfAbsent(e.instrumentId, _ -> new ArrayList<>()).add(e));
          for (var entry : groups.entrySet()) recoverInstrument(entry.getKey(), entry.getValue());
        });
  }

  private void recoverInstrument(String instrument, List<SourceExecutionEntity> events) {
    if (em.createQuery(
                "select count(r) from MirrorTradeReceipt r where r.instrumentId=:instrument and r.copiedLotsApplied<>0 and r.sourceCycleId is null",
                Long.class)
            .setParameter("instrument", instrument)
            .getSingleResult()
        > 0) {
      control.instrumentReconciled(
          instrument,
          true,
          false,
          "Не доказана принадлежность ручного уменьшения одному исходному циклу");
      return;
    }
    boolean reordered =
        events.stream().anyMatch(e -> e.requiresRecovery)
            || control.instruments().stream()
                .anyMatch(s -> s.instrumentId().equals(instrument) && s.waiting());
    boolean proven = true;
    List<SourceExecutionEntity> cycle = new ArrayList<>();
    BigDecimal balance = BigDecimal.ZERO;
    for (var event : events) {
      if (!cycle.isEmpty()
          && event.executedAt.equals(cycle.getLast().executedAt)
          && !event.side.equals(cycle.getLast().side)) {
        control.instrumentReconciled(
            instrument,
            true,
            false,
            "Не доказан порядок разнонаправленных исполнений с одной меткой времени");
        return;
      }
      balance =
          balance.add(event.lots.multiply(BigDecimal.valueOf("BUY".equals(event.side) ? 1 : -1)));
      cycle.add(event);
      if (balance.signum() < 0) {
        control.instrumentReconciled(
            instrument, true, false, "Ожидание предшествующей покупки из истории");
        return;
      }
      if (balance.signum() == 0) {
        proven = processCycle(instrument, cycle, true, reordered) && proven;
        cycle = new ArrayList<>();
      }
    }
    if (!cycle.isEmpty() && reordered)
      proven = processCycle(instrument, cycle, false, true) && proven;
    if (reordered) {
      var position = em.find(MirrorPosition.class, instrument);
      boolean snapshot =
          position != null && position.brokerLots != null && position.brokerInventoryQualified;
      boolean matches =
          snapshot
              && position.brokerLots.compareTo(
                      BigDecimal.valueOf(position.copiedLots + position.manualLots))
                  == 0;
      if (snapshot && !matches)
        control.instrumentReconciled(
            instrument,
            false,
            false,
            "Расхождение позиции: программа "
                + (position.copiedLots + position.manualLots)
                + ", брокер "
                + position.brokerLots
                + " лотов");
      else
        control.instrumentReconciled(
            instrument,
            true,
            proven && matches,
            proven
                ? "Ожидание независимого снимка позиции"
                : "Ожидание доказанного порядка и окончательных заявок");
    }
  }

  private boolean processCycle(
      String instrument, List<SourceExecutionEntity> events, boolean closed, boolean reordered) {
    List<String> ids = events.stream().map(e -> e.id).toList();
    var intents =
        em.createQuery(
                "from CopyIntent where executionId in :ids order by createdAt,id", CopyIntent.class)
            .setParameter("ids", ids)
            .getResultList();
    if (intents.stream()
        .anyMatch(
            i ->
                Set.of("ACTIVE", "UNKNOWN", "SEND_STARTED", "CANCEL_STARTED").contains(i.status))) {
      if (reordered || closed)
        control.instrumentReconciled(
            instrument, true, false, "Сначала отмена и окончательная сверка прежних заявок");
      return false;
    }
    List<SourceExecutionEntity> buys = events.stream().filter(e -> "BUY".equals(e.side)).toList();
    List<SourceExecutionEntity> sells = events.stream().filter(e -> "SELL".equals(e.side)).toList();
    if (sells.isEmpty()) return true;
    if (!closed && buys.stream().anyMatch(e -> e.coefficient == null)) return false;
    SourceExecutionEntity sale = sells.getLast();
    long actual =
        intents.stream().mapToLong(i -> i.executedLots * ("BUY".equals(i.side) ? 1 : -1)).sum();
    actual +=
        em.createQuery(
                "select coalesce(sum(r.copiedLotsApplied),0) from MirrorTradeReceipt r where r.sourceCycleId=:cycle",
                Long.class)
            .setParameter("cycle", events.getFirst().id)
            .getSingleResult();
    if (closed
        && actual > 0
        && "SKIPPED".equals(sale.disposition)
        && intents.stream().noneMatch(i -> i.executionId.equals(sale.id))) {
      var gate = control.gate();
      boolean expired =
          time.bound()
              .map(bound -> bound.latest().isAfter(sale.executedAt.plusSeconds(60)))
              .orElse(false);
      if (gate.running()
          && gate.boundary() != null
          && sale.executedAt.isAfter(gate.boundary())
          && expired) {
        control.pauseInstrument(
            instrument,
            "Источник полностью закрыл позицию, исходная продажа устарела; в повторении осталось "
                + actual
                + " лотов");
        return true;
      }
    }
    if (actual < 0) {
      control.pauseInstrument(
          instrument, "Неоднозначная связь фактических остатков с исходным циклом");
      return false;
    }
    boolean allBuysUnsent =
        intents.stream().filter(i -> "BUY".equals(i.side)).allMatch(i -> i.executedLots == 0);
    if (!closed && !allBuysUnsent && (buys.size() > 2 || sells.size() > 1)) {
      control.instrumentReconciled(
          instrument,
          true,
          false,
          "История вне согласованного узкого случая пропорционального восстановления");
      return false;
    }
    var retained =
        PositionRecovery.retainedSequentially(
            events.stream()
                .map(
                    e ->
                        new PositionRecovery.Step(
                            e.id,
                            e.sourceOrderId,
                            "BUY".equals(e.side),
                            e.lots,
                            "BUY".equals(e.side) && e.coefficient != null
                                ? CopyService.exactCoefficient(e)
                                : Rational.ZERO))
                .toList());
    var retainedLots = PositionRecovery.allocateRetainedLots(retained);
    long target = retainedLots.values().stream().mapToLong(Long::longValue).sum();
    var result =
        new PositionRecovery.Result(
            target,
            Math.max(0, target - actual),
            Math.max(0, actual - target),
            retained.stream().map(r -> r.contribution().decimal(100)).toList(),
            closed ? "CLOSED" : "PROPORTIONAL_REMAINDER");
    if (closed
        && actual > 0
        && "ACCEPTED".equals(sale.disposition)
        && intents.stream().noneMatch(i -> i.executionId.equals(sale.id))) {
      var fullClose = CopyIntent.fromExecution(sale, actual, clock.instant());
      fullClose.fullClose = true;
      em.persist(fullClose);
      intents.add(fullClose);
    }
    // Only a new reordered input or an unsent full-close decision may change an intent.
    if (!reordered
        && intents.stream()
            .noneMatch(i -> i.executionId.equals(sale.id) && "WAITING".equals(i.status)))
      return true;
    if (result.sellLots() > 0) {
      var candidate =
          intents.stream()
              .filter(i -> i.executionId.equals(sale.id) && "WAITING".equals(i.status))
              .findFirst();
      if (candidate.isEmpty()) {
        control.instrumentReconciled(
            instrument,
            true,
            false,
            "Нет разрешённого неотправленного намерения для корректировки");
        return false;
      }
      CopyIntent correction = candidate.get();
      correction.lots = result.sellLots();
      correction.skippedLots = 0;
      correction.fullClose = closed;
      if (buys.size() == 2 && !closed) {
        correction.recoveryFirstBuy = buys.getFirst().id;
        correction.recoverySecondBuy = buys.getLast().id;
        correction.recoveryFirstTarget =
            result
                .retainedContributions()
                .getFirst()
                .setScale(0, RoundingMode.FLOOR)
                .longValueExact();
        correction.recoverySecondTarget =
            result
                .retainedContributions()
                .getLast()
                .setScale(0, RoundingMode.FLOOR)
                .longValueExact();
      }
      correction.reason =
          closed
              ? "Полное закрытие связанного прежнего остатка"
              : "Подтверждённая корректировка фактического остатка к исторической цели";
      for (var i : intents)
        if ("WAITING".equals(i.status) && i != correction)
          finalSkip(i, "Объём учтён единой корректирующей продажей");
    } else {
      for (int index = 0; index < buys.size(); index++) {
        SourceExecutionEntity buy = buys.get(index);
        long retainedQuantity = retainedLots.getOrDefault(buy.id, 0L);
        if (retainedQuantity > 0
            && "ACCEPTED".equals(buy.disposition)
            && intents.stream().noneMatch(i -> i.executionId.equals(buy.id))) {
          var newWhole = CopyIntent.fromExecution(buy, retainedQuantity, clock.instant());
          em.persist(newWhole);
          intents.add(newWhole);
        }
        for (var i : intents)
          if (i.executionId.equals(buy.id) && "WAITING".equals(i.status)) {
            long remaining = Math.max(0, retainedQuantity - i.executedLots);
            i.skippedLots = Math.max(i.skippedLots, i.lots - remaining);
            if (remaining == 0)
              finalSkip(i, "Подтверждённое закрытие или проданная доля исходной покупки");
          }
      }
      for (var i : intents)
        if ("SELL".equals(i.side) && "WAITING".equals(i.status))
          finalSkip(i, "Проданная исходная доля уже учтена в уменьшенных покупках");
    }
    events.forEach(e -> e.requiresRecovery = false);
    em.persist(
        new JournalEntry(
            clock.instant(),
            "POSITION_RECOVERY",
            instrument,
            "source="
                + ids
                + " actual="
                + actual
                + " target="
                + result.targetLots()
                + " sell="
                + result.sellLots()
                + " shares="
                + result.retainedContributions()));
    return true;
  }

  private static void finalSkip(CopyIntent i, String reason) {
    i.skippedLots = i.lots - i.executedLots;
    i.status = "FINAL";
    i.reason = reason;
  }
}
