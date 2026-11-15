package ru.orbita.valuation;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import ru.orbita.broker.BrokerGateway.CommissionEvidence;
import ru.orbita.control.TimeAuthority;
import ru.orbita.valuation.QualifiedRateProvider.Purpose;

@Service
public final class RubValuation {
  public record Result(
      boolean qualified,
      ExactRub rub,
      String diagnostic,
      String reference,
      Instant evidenceAsOf,
      Instant evidenceExpiresAt) {
    public boolean within(BigDecimal limit) {
      try {
        return qualified && rub != null && rub.atMost(limit);
      } catch (IllegalArgumentException invalidLimit) {
        return false;
      }
    }

    public BigDecimal rubCeiling() {
      return qualified && rub != null ? rub.ceiling(28) : null;
    }
  }

  private final ObjectProvider<QualifiedRateProvider> providers;
  private final TimeAuthority time;

  public RubValuation(ObjectProvider<QualifiedRateProvider> providers, TimeAuthority time) {
    this.providers = providers;
    this.time = time;
  }

  public Result value(
      String accountId,
      String instrumentId,
      String currency,
      BigDecimal amount,
      Purpose purpose,
      Instant at) {
    return value(accountId, instrumentId, currency, amount, purpose, at, at);
  }

  public Result value(
      String accountId,
      String instrumentId,
      String currency,
      BigDecimal amount,
      Purpose purpose,
      Instant from,
      Instant through) {
    try {
      var scope = new QualifiedRateProvider.Scope(accountId, instrumentId, currency, purpose);
      var principal = ExactRub.of(amount);
      if (from == null || through == null || through.isBefore(from))
        return unknown("Ordered valuation interval required");
      if (scope.currency().equals("RUB"))
        return new Result(true, principal, "RUB identity", "RUB identity", null, null);
      var bound = time.bound();
      if (bound.isEmpty())
        return unknown("Current UTC evidence unavailable for currency valuation");
      var provider = providers.getIfAvailable();
      if (provider == null) return unknown("Qualified currency rate provider not configured");
      var quote = provider.current(scope, from, through, bound.get());
      if (quote.isEmpty()) return unknown("Scoped fresh conversion evidence unavailable");
      var afterRead = time.bound();
      if (afterRead.isEmpty())
        return unknown("Current UTC evidence unavailable after currency provider read");
      var rate = quote.get();
      if (!rate.scope().equals(scope)
          || from.isBefore(rate.validFrom())
          || through.isAfter(rate.validThrough())
          || afterRead.get().earliest().isBefore(rate.evidenceAsOf())
          || afterRead.get().latest().isAfter(rate.evidenceExpiresAt()))
        return unknown("Conversion scope, valuation interval or evidence freshness mismatch");
      if (purpose == Purpose.PRE_SEND
          && (afterRead.get().earliest().isBefore(rate.validFrom())
              || afterRead.get().latest().isAfter(rate.validThrough())))
        return unknown("Pre-send conversion period expired during provider read");
      Instant expiresAt =
          purpose == Purpose.PRE_SEND && rate.validThrough().isBefore(rate.evidenceExpiresAt())
              ? rate.validThrough()
              : rate.evidenceExpiresAt();
      return new Result(
          true,
          principal.multiply(rate.rubNumerator(), rate.settlementDenominator()),
          "Qualified exact RUB valuation",
          rate.reference(),
          rate.evidenceAsOf(),
          expiresAt);
    } catch (RuntimeException invalid) {
      return unknown("Currency valuation unavailable: " + invalid.getClass().getSimpleName());
    }
  }

  public Result preSend(
      String accountId,
      String instrumentId,
      String currency,
      BigDecimal principal,
      BigDecimal preliminaryCommission,
      BigDecimal additionalReserve,
      Instant at) {
    if (!nonnegative(principal)
        || !nonnegative(preliminaryCommission)
        || !nonnegative(additionalReserve))
      return unknown("Qualified principal, preliminary commission and additional reserve required");
    return value(
        accountId,
        instrumentId,
        currency,
        principal.add(preliminaryCommission).add(additionalReserve),
        Purpose.PRE_SEND,
        at);
  }

  public Result finalCost(
      String accountId,
      String instrumentId,
      String currency,
      UUID requestId,
      BigDecimal executedPrincipal,
      CommissionEvidence actualCommission,
      Instant from,
      Instant through) {
    if (!nonnegative(executedPrincipal)
        || actualCommission == null
        || !actualCommission.qualified()
        || !nonnegative(actualCommission.amount())
        || requestId == null
        || !requestId.equals(actualCommission.requestId())
        || !java.util.Objects.equals(accountId, actualCommission.accountId())
        || currency == null
        || !currency.equalsIgnoreCase(actualCommission.currency())
        || actualCommission.observedAt() == null)
      return unknown("Matching independent actual commission evidence required");
    return value(
        accountId,
        instrumentId,
        currency,
        executedPrincipal.add(actualCommission.amount()),
        Purpose.FINAL_COST,
        from,
        through);
  }

  private static boolean nonnegative(BigDecimal value) {
    return value != null && value.signum() >= 0;
  }

  private static Result unknown(String diagnostic) {
    return new Result(false, null, diagnostic, null, null, null);
  }
}
