package ru.orbita.valuation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/** Exact rational money used for limit comparisons; display rounding never authorizes an order. */
public record ExactRub(BigInteger numerator, BigInteger denominator) {
  public ExactRub {
    if (numerator == null
        || denominator == null
        || numerator.signum() < 0
        || denominator.signum() <= 0)
      throw new IllegalArgumentException("Nonnegative exact RUB amount required");
    var gcd = numerator.gcd(denominator);
    numerator = numerator.divide(gcd);
    denominator = denominator.divide(gcd);
  }

  public static ExactRub of(BigDecimal amount) {
    if (amount == null
        || amount.signum() < 0
        || Math.abs((long) amount.scale()) > 1000
        || amount.precision() > 2000)
      throw new IllegalArgumentException("Finite nonnegative settlement amount required");
    return amount.scale() >= 0
        ? new ExactRub(amount.unscaledValue(), BigInteger.TEN.pow(amount.scale()))
        : new ExactRub(
            amount.unscaledValue().multiply(BigInteger.TEN.pow(-amount.scale())), BigInteger.ONE);
  }

  public ExactRub multiply(BigInteger rateNumerator, BigInteger rateDenominator) {
    if (rateNumerator == null
        || rateNumerator.signum() <= 0
        || rateDenominator == null
        || rateDenominator.signum() <= 0)
      throw new IllegalArgumentException("Positive exact conversion ratio required");
    return new ExactRub(numerator.multiply(rateNumerator), denominator.multiply(rateDenominator));
  }

  public boolean atMost(BigDecimal limit) {
    var cap = of(limit);
    return numerator.multiply(cap.denominator).compareTo(cap.numerator.multiply(denominator)) <= 0;
  }

  public BigDecimal ceiling(int scale) {
    if (scale < 0 || scale > 1000)
      throw new IllegalArgumentException("Bounded decimal scale required");
    return new BigDecimal(numerator)
        .divide(new BigDecimal(denominator), scale, RoundingMode.CEILING);
  }
}
