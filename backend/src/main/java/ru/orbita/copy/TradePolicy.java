package ru.orbita.copy;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import ru.orbita.broker.BrokerGateway.Side;

/** Price, age and own-resource checks shared by normal and recovery decisions. */
public final class TradePolicy {
  private TradePolicy() {}

  public static BigDecimal limit(Side side, BigDecimal reference, BigDecimal step) {
    if (reference.signum() <= 0 || step.signum() <= 0)
      throw new IllegalArgumentException("Invalid price");
    BigDecimal multiplier = new BigDecimal(side == Side.BUY ? "1.005" : "0.995");
    return reference
        .multiply(multiplier)
        .divide(step, 0, side == Side.BUY ? RoundingMode.FLOOR : RoundingMode.CEILING)
        .multiply(step);
  }

  public enum Age {
    ALLOWED,
    WAIT_FOR_TIME,
    FUTURE_TIMESTAMP,
    EXPIRED
  }

  public static Age age(Instant executedAt, Instant now, Duration uncertainty) {
    if (uncertainty == null
        || uncertainty.isNegative()
        || uncertainty.compareTo(Duration.ofMillis(500)) > 0) return Age.WAIT_FOR_TIME;
    if (executedAt.isAfter(now.plus(uncertainty))) return Age.FUTURE_TIMESTAMP;
    if (now.plus(uncertainty).isAfter(executedAt.plusSeconds(60))) return Age.EXPIRED;
    if (now.minus(uncertainty).isBefore(executedAt)) return Age.WAIT_FOR_TIME;
    return Age.ALLOWED;
  }

  public static Age ageRange(Instant executedAt, Instant earliest, Instant latest) {
    if (earliest == null || latest == null || latest.isBefore(earliest)) return Age.WAIT_FOR_TIME;
    if (executedAt.isAfter(latest)) return Age.FUTURE_TIMESTAMP;
    if (latest.isAfter(executedAt.plusSeconds(60))) return Age.EXPIRED;
    if (earliest.isBefore(executedAt)) return Age.WAIT_FOR_TIME;
    return Age.ALLOWED;
  }

  /** Returns the most lots affordable when the commission quote covers the full request. */
  public static long affordable(
      long requested,
      int lotSize,
      BigDecimal limit,
      BigDecimal money,
      BigDecimal totalCommission,
      BigDecimal additionalReserve) {
    if (requested < 0
        || lotSize <= 0
        || limit == null
        || limit.signum() <= 0
        || money == null
        || totalCommission == null
        || additionalReserve == null
        || totalCommission.signum() < 0
        || additionalReserve.signum() < 0)
      throw new IllegalArgumentException("Unqualified resources");
    if (requested == 0) return 0;

    BigDecimal principalPerLot = limit.multiply(BigDecimal.valueOf(lotSize));
    BigDecimal principalBudget = money.subtract(additionalReserve);
    if (principalBudget.signum() <= 0) return 0;
    BigDecimal principalLotLimit = principalBudget.divide(principalPerLot, 0, RoundingMode.FLOOR);
    long high =
        principalLotLimit.compareTo(BigDecimal.valueOf(requested)) >= 0
            ? requested
            : principalLotLimit.longValueExact();
    long low = 0;
    while (low < high) {
      long distance = high - low;
      long candidate = low + distance / 2 + distance % 2;
      BigDecimal candidateCost =
          principalPerLot
              .multiply(BigDecimal.valueOf(candidate))
              .add(commissionForLots(requested, candidate, totalCommission))
              .add(additionalReserve);
      if (candidateCost.compareTo(money) <= 0) low = candidate;
      else high = candidate - 1;
    }
    return low;
  }

  /** Scales a total quote to selected lots, rounding up to the supported 9 decimal places. */
  public static BigDecimal commissionForLots(
      long requested, long selected, BigDecimal totalCommission) {
    if (requested <= 0
        || selected < 0
        || selected > requested
        || totalCommission == null
        || totalCommission.signum() < 0)
      throw new IllegalArgumentException("Unqualified commission quote");
    if (selected == 0) return BigDecimal.ZERO;
    if (selected == requested) return totalCommission;
    return totalCommission
        .multiply(BigDecimal.valueOf(selected))
        .divide(BigDecimal.valueOf(requested), 9, RoundingMode.CEILING);
  }
}
