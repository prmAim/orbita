package ru.orbita.broker;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;
import ru.orbita.control.OrbitaProperties;

/**
 * Immutable operator attestation; binding a file does not perform or invent account qualification.
 */
@ConfigurationProperties(prefix = "orbita.broker.qualification", ignoreUnknownFields = false)
public record BrokerQualificationSettings(
    boolean enabled,
    boolean capitalCompositionConfirmed,
    boolean ownResourceInterpretationConfirmed,
    boolean commissionInterpretationConfirmed,
    boolean marginDisabledConfirmed,
    BigDecimal additionalCommissionReserve,
    String evidenceReference,
    Set<String> capitalAccountIds,
    String mirrorAccountId,
    Set<String> instrumentIds,
    String reserveCurrency,
    Instant validUntil,
    boolean actualCommissionTotalConfirmed,
    BrokerQualifications.OwnMoneyBasis ownMoneyBasis) {
  public BrokerQualificationSettings {
    capitalAccountIds = capitalAccountIds == null ? Set.of() : Set.copyOf(capitalAccountIds);
    instrumentIds = instrumentIds == null ? Set.of() : Set.copyOf(instrumentIds);
    reserveCurrency =
        reserveCurrency == null || reserveCurrency.isBlank()
            ? null
            : reserveCurrency.toUpperCase(Locale.ROOT);
    ownMoneyBasis =
        ownMoneyBasis == null ? BrokerQualifications.OwnMoneyBasis.UNQUALIFIED : ownMoneyBasis;
  }

  BrokerQualifications qualifiedProfile(OrbitaProperties accounts) {
    if (!enabled) return BrokerQualifications.unconfirmed();
    require(
        accounts.accountsConfigured(),
        "Distinct configured accounts required for qualification profile");
    require(
        accounts.mirrorAccount().equals(mirrorAccountId),
        "Qualification profile mirror account mismatch");
    require(
        capitalAccountIds.equals(Set.of(accounts.sourceAccount(), accounts.mirrorAccount())),
        "Qualification profile capital accounts must match the configured pair exactly");
    require(text(evidenceReference, 2000), "Qualification evidence reference required");
    require(
        validUntil != null && !Instant.MAX.equals(validUntil) && !Instant.MIN.equals(validUntil),
        "Finite qualification expiry required");
    boolean resources =
        ownResourceInterpretationConfirmed
            || commissionInterpretationConfirmed
            || marginDisabledConfirmed;
    boolean instrumentScope = resources || actualCommissionTotalConfirmed;
    if (instrumentScope) require(!instrumentIds.isEmpty(), "Qualified instrument scope required");
    require(
        instrumentIds.stream().allMatch(id -> text(id, 256)),
        "Invalid qualified instrument identifier");
    if (instrumentScope || reserveCurrency != null)
      require(
          reserveCurrency != null && reserveCurrency.matches("[A-Z]{3}"),
          "Qualified settlement/reserve currency required");
    if (resources) {
      require(
          additionalCommissionReserve != null, "Explicit additional commission reserve required");
      require(
          ownMoneyBasis != BrokerQualifications.OwnMoneyBasis.UNQUALIFIED,
          "Explicit qualified own-money basis required");
    }
    if (additionalCommissionReserve != null) {
      var amount = additionalCommissionReserve.stripTrailingZeros();
      require(
          amount.signum() >= 0
              && amount.scale() <= 28
              && (long) amount.precision() - amount.scale() <= 36,
          "Additional reserve must be nonnegative and fit exact persisted money precision");
    }
    return new BrokerQualifications(
        capitalCompositionConfirmed,
        ownResourceInterpretationConfirmed,
        commissionInterpretationConfirmed,
        marginDisabledConfirmed,
        additionalCommissionReserve,
        evidenceReference,
        capitalAccountIds,
        mirrorAccountId,
        instrumentIds,
        reserveCurrency,
        validUntil,
        actualCommissionTotalConfirmed,
        ownMoneyBasis);
  }

  private static boolean text(String value, int maximum) {
    return value != null
        && !value.isBlank()
        && value.length() <= maximum
        && !value.chars().anyMatch(Character::isISOControl);
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new IllegalArgumentException(message);
  }
}
