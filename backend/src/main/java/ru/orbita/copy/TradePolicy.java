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

}
