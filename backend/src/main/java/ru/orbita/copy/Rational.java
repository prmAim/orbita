package ru.orbita.copy;

import java.math.*;

/** Exact finite-input ratio. Decimal rendering never determines a whole-lot boundary. */
public record Rational(BigInteger numerator, BigInteger denominator) {
  public static final Rational ZERO = new Rational(BigInteger.ZERO, BigInteger.ONE);

  public Rational {
    if (denominator.signum() == 0) throw new IllegalArgumentException("Zero denominator");
    if (denominator.signum() < 0) {
      numerator = numerator.negate();
      denominator = denominator.negate();
    }
    BigInteger gcd = numerator.gcd(denominator);
    numerator = numerator.divide(gcd);
    denominator = denominator.divide(gcd);
  }

  public static Rational of(BigDecimal value) {
    return value.scale() >= 0
        ? new Rational(value.unscaledValue(), BigInteger.TEN.pow(value.scale()))
        : new Rational(
            value.unscaledValue().multiply(BigInteger.TEN.pow(-value.scale())), BigInteger.ONE);
  }

  public static Rational ratio(BigDecimal numerator, BigDecimal denominator) {
    return of(numerator).divide(of(denominator));
  }

  public Rational add(Rational other) {
    return new Rational(
        numerator.multiply(other.denominator).add(other.numerator.multiply(denominator)),
        denominator.multiply(other.denominator));
  }

  public Rational multiply(Rational other) {
    return new Rational(
        numerator.multiply(other.numerator), denominator.multiply(other.denominator));
  }

  public Rational multiply(BigDecimal value) {
    return multiply(of(value));
  }

  public Rational divide(Rational other) {
    return new Rational(
        numerator.multiply(other.denominator), denominator.multiply(other.numerator));
  }

  public long floor() {
    BigInteger[] quotient = numerator.divideAndRemainder(denominator);
    return (numerator.signum() < 0 && quotient[1].signum() != 0
            ? quotient[0].subtract(BigInteger.ONE)
            : quotient[0])
        .longValueExact();
  }

  public BigDecimal decimal(int scale) {
    return new BigDecimal(numerator)
        .divide(new BigDecimal(denominator), scale, RoundingMode.HALF_EVEN);
  }
}
