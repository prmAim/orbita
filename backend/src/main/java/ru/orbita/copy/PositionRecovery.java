package ru.orbita.copy;

import java.math.BigDecimal;
import java.util.List;

/**
 * The confirmed proportional-history cases R70–R92. Ambiguous history does not authorize
 * correction.
 */
public final class PositionRecovery {
  private PositionRecovery() {}

  public record Step(
      String executionId,
      String sourceOrderId,
      boolean buy,
      BigDecimal sourceLots,
      Rational coefficient) {}

  public record Retained(String executionId, String sourceOrderId, Rational contribution) {}

  public static List<Retained> retainedSequentially(List<Step> history) {
    var balances = new java.util.LinkedHashMap<Step, Rational>();
    BigDecimal source = BigDecimal.ZERO;
    for (var step : history) {
      if (step.buy()) {
        balances.put(step, Rational.of(step.sourceLots()));
        source = source.add(step.sourceLots());
      } else {
        if (source.signum() <= 0 || step.sourceLots().compareTo(source) > 0)
          throw new IllegalArgumentException("Incomplete source history");
        Rational remaining = Rational.ratio(source.subtract(step.sourceLots()), source);
        balances.replaceAll((_, quantity) -> quantity.multiply(remaining));
        source = source.subtract(step.sourceLots());
      }
    }
    return balances.entrySet().stream()
        .map(
            entry ->
                new Retained(
                    entry.getKey().executionId(),
                    entry.getKey().sourceOrderId(),
                    entry.getValue().multiply(entry.getKey().coefficient())))
        .toList();
  }

  public static java.util.Map<String, Long> allocateRetainedLots(List<Retained> retained) {
    var prefixes = new java.util.HashMap<String, Rational>();
    var result = new java.util.LinkedHashMap<String, Long>();
    for (var value : retained) {
      Rational before = prefixes.getOrDefault(value.sourceOrderId(), Rational.ZERO),
          after = before.add(value.contribution());
      result.put(value.executionId(), after.floor() - before.floor());
      prefixes.put(value.sourceOrderId(), after);
    }
    return java.util.Map.copyOf(result);
  }

  public record Buy(
      String executionId,
      String sourceOrderId,
      BigDecimal sourceLots,
      Rational acceptedCoefficient) {
    public Buy(String executionId, BigDecimal sourceLots, BigDecimal acceptedCoefficient) {
      this(executionId, executionId, sourceLots, Rational.of(acceptedCoefficient));
    }

    public Buy(
        String executionId,
        String sourceOrderId,
        BigDecimal sourceLots,
        BigDecimal acceptedCoefficient) {
      this(executionId, sourceOrderId, sourceLots, Rational.of(acceptedCoefficient));
    }
  }

  public record Result(
      long targetLots,
      long buyLots,
      long sellLots,
      List<BigDecimal> retainedContributions,
      String disposition) {
    public Result {
      retainedContributions = List.copyOf(retainedContributions);
    }
  }

  public static Result recover(
      List<Buy> buys,
      BigDecimal sourceSold,
      long actualMirrorLots,
      boolean sequenceProven,
      boolean allOrdersFinal) {
    if (!sequenceProven || !allOrdersFinal)
      return new Result(0, 0, 0, List.of(), "WAIT_FOR_RECONCILIATION");
    BigDecimal total = buys.stream().map(Buy::sourceLots).reduce(BigDecimal.ZERO, BigDecimal::add);
    if (total.signum() <= 0
        || sourceSold.signum() < 0
        || sourceSold.compareTo(total) > 0
        || actualMirrorLots < 0)
      throw new IllegalArgumentException("History is outside confirmed recovery cases");
    BigDecimal remaining = total.subtract(sourceSold);
    List<BigDecimal> shares =
        buys.stream()
            .map(
                b ->
                    b.acceptedCoefficient()
                        .multiply(b.sourceLots())
                        .multiply(remaining)
                        .divide(Rational.of(total))
                        .decimal(100)
                        .stripTrailingZeros())
            .toList();
    var orders = new java.util.HashMap<String, Rational>();
    buys.forEach(
        b ->
            orders.merge(
                b.sourceOrderId(),
                b.acceptedCoefficient().multiply(b.sourceLots()),
                Rational::add));
    long target =
        orders.values().stream()
            .mapToLong(value -> value.multiply(remaining).divide(Rational.of(total)).floor())
            .sum();
    return new Result(
        target,
        Math.max(0, target - actualMirrorLots),
        Math.max(0, actualMirrorLots - target),
        shares,
        total.compareTo(sourceSold) == 0 ? "CLOSED" : "PROPORTIONAL_REMAINDER");
  }

  /** R88–R92: unexecuted correction remains in the first old holding, never counted twice. */
  public static List<Long> allocateFinalCorrection(
      long firstTarget, long secondTarget, long plannedSale, long executedSale) {
    if (executedSale < 0 || executedSale > plannedSale)
      throw new IllegalArgumentException("Invalid final fill quantity");
    return List.of(Math.addExact(firstTarget, plannedSale - executedSale), secondTarget);
  }
}
