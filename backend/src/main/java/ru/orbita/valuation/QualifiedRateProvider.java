package ru.orbita.valuation;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Optional;
import ru.orbita.control.ClockEvidence;

/** Independently qualified RUB cost per one settlement-currency unit, never a guessed FX quote. */
public interface QualifiedRateProvider {
  enum Purpose {
    PRE_SEND,
    FINAL_COST
  }

  record Scope(String accountId, String instrumentId, String currency, Purpose purpose) {
    public Scope {
      if (accountId == null
          || accountId.isBlank()
          || accountId.length() > 256
          || accountId.chars().anyMatch(Character::isISOControl)
          || instrumentId == null
          || instrumentId.isBlank()
          || instrumentId.length() > 256
          || instrumentId.chars().anyMatch(Character::isISOControl)
          || currency == null
          || !currency.matches("[A-Za-z]{3}")
          || purpose == null)
        throw new IllegalArgumentException(
            "Complete account/instrument/currency/purpose scope required");
      currency = currency.toUpperCase(java.util.Locale.ROOT);
    }
  }

  record Rate(
      Scope scope,
      BigInteger rubNumerator,
      BigInteger settlementDenominator,
      Instant validFrom,
      Instant validThrough,
      Instant evidenceAsOf,
      Instant evidenceExpiresAt,
      String reference) {
    public Rate {
      if (scope == null
          || rubNumerator == null
          || rubNumerator.signum() <= 0
          || rubNumerator.bitLength() > 4096
          || settlementDenominator == null
          || settlementDenominator.signum() <= 0
          || settlementDenominator.bitLength() > 4096
          || validFrom == null
          || validThrough == null
          || validThrough.isBefore(validFrom)
          || evidenceAsOf == null
          || evidenceExpiresAt == null
          || evidenceExpiresAt.isBefore(evidenceAsOf)
          || reference == null
          || reference.isBlank())
        throw new IllegalArgumentException("Complete positive qualified rate evidence required");
    }
  }

  Optional<Rate> current(
      Scope scope, Instant valuationFrom, Instant valuationThrough, ClockEvidence.Bound currentUtc);
}
