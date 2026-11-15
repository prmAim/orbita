package ru.orbita.copy;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** An isolated source-order calculation. Persistence serializes acceptance before calling it. */
public final class CopyCalculation {
  private final Map<String, Rational> contributions = new HashMap<>();
  private final Map<String, String> executions = new HashMap<>();

  public record Increment(
      long newLots,
      long targetLots,
      BigDecimal referencePrice,
      Instant referenceTime,
      boolean duplicate) {}

  public Increment accept(
      String tradeId,
      String sourceOrderId,
      BigDecimal lots,
      BigDecimal coefficient,
      BigDecimal price,
      Instant executedAt) {
    return accept(tradeId, sourceOrderId, lots, Rational.of(coefficient), price, executedAt);
  }

  public Increment accept(
      String tradeId,
      String sourceOrderId,
      BigDecimal lots,
      Rational coefficient,
      BigDecimal price,
      Instant executedAt) {
    if (executions.containsKey(tradeId))
      return new Increment(0, targetLots(sourceOrderId), price, executedAt, true);
    if (lots.signum() <= 0 || coefficient.numerator().signum() < 0 || price.signum() <= 0)
      throw new IllegalArgumentException("Invalid execution or coefficient");
    long before = targetLots(sourceOrderId);
    contributions.merge(sourceOrderId, coefficient.multiply(lots), Rational::add);
    executions.put(tradeId, sourceOrderId);
    long after = targetLots(sourceOrderId);
    return new Increment(after - before, after, price, executedAt, false);
  }

  public long targetLots(String sourceOrderId) {
    return contributions.getOrDefault(sourceOrderId, Rational.ZERO).floor();
  }

  public static BigDecimal contribution(BigDecimal lots, BigDecimal coefficient) {
    if (lots.signum() <= 0 || coefficient.signum() < 0)
      throw new IllegalArgumentException("Invalid lots/coefficient");
    return lots.multiply(coefficient);
  }

  public static long floorTarget(BigDecimal contribution) {
    return contribution.setScale(0, RoundingMode.FLOOR).longValueExact();
  }

  public static long increment(BigDecimal previous, BigDecimal lots, BigDecimal coefficient) {
    return floorTarget(previous.add(contribution(lots, coefficient))) - floorTarget(previous);
  }

  public static long increment(Rational previous, BigDecimal lots, Rational coefficient) {
    return previous.add(coefficient.multiply(lots)).floor() - previous.floor();
  }
}
