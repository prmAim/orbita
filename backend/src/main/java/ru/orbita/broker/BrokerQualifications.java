package ru.orbita.broker;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

/** Explicit evidence from final account qualification. No tariff or reserve defaults. */
public record BrokerQualifications(
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
    OwnMoneyBasis ownMoneyBasis) {
  public enum OwnMoneyBasis {
    UNQUALIFIED,
    GROSS_BEFORE_PROGRAM_RESERVES,
    NET_REQUIRES_EXACT_BANK_RESERVE_EVIDENCE
  }

  public BrokerQualifications {
    capitalAccountIds = Set.copyOf(capitalAccountIds);
    instrumentIds = Set.copyOf(instrumentIds);
  }

  public static BrokerQualifications unconfirmed() {
    return new BrokerQualifications(
        false,
        false,
        false,
        false,
        null,
        "not qualified",
        Set.of(),
        null,
        Set.of(),
        null,
        null,
        false,
        OwnMoneyBasis.UNQUALIFIED);
  }

  public boolean current(Instant now) {
    return validUntil != null
        && !now.isAfter(validUntil)
        && evidenceReference != null
        && !evidenceReference.isBlank();
  }

  public boolean capitalQualified(String accountId, Instant now) {
    return current(now) && capitalCompositionConfirmed && capitalAccountIds.contains(accountId);
  }

  public boolean resourcesQualified(
      String accountId, String instrumentId, String currency, Instant now) {
    return ownResourceInterpretationConfirmed
        && commissionInterpretationConfirmed
        && marginDisabledConfirmed
        && additionalCommissionReserve != null
        && additionalCommissionReserve.signum() >= 0
        && current(now)
        && scopeMatches(accountId, instrumentId, currency, now)
        && ownMoneyBasis == OwnMoneyBasis.GROSS_BEFORE_PROGRAM_RESERVES;
  }

  public boolean scopeMatches(String accountId, String instrumentId, String currency, Instant now) {
    return current(now)
        && accountId.equals(mirrorAccountId)
        && instrumentIds.contains(instrumentId)
        && currency.equalsIgnoreCase(reserveCurrency);
  }

  public java.util.List<String> resourceProblems(
      String accountId, String instrumentId, String currency, Instant now) {
    var problems = new java.util.ArrayList<String>();
    if (!current(now)) problems.add("qualification evidence absent/expired");
    if (!accountId.equals(mirrorAccountId)) problems.add("mirror account not qualified");
    if (!instrumentIds.contains(instrumentId))
      problems.add("instrument price/resource semantics not qualified");
    if (!ownResourceInterpretationConfirmed)
      problems.add("own cash and settlement availability not qualified");
    if (!commissionInterpretationConfirmed)
      problems.add("total-minus-initial commission interpretation not qualified");
    if (!marginDisabledConfirmed) problems.add("disabled margin not confirmed for account");
    if (additionalCommissionReserve == null || additionalCommissionReserve.signum() < 0)
      problems.add("additional commission reserve absent/invalid");
    if (!currency.equalsIgnoreCase(reserveCurrency))
      problems.add("reserve settlement currency not qualified");
    if (ownMoneyBasis != OwnMoneyBasis.GROSS_BEFORE_PROGRAM_RESERVES)
      problems.add("bank reservation coverage/gross own funds not qualified");
    return java.util.List.copyOf(problems);
  }
}
