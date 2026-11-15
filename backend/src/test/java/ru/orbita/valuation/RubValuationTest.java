package ru.orbita.valuation;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import ru.orbita.control.ClockEvidence;
import ru.orbita.control.TimeAuthority;
import ru.orbita.valuation.QualifiedRateProvider.*;

class RubValuationTest {
  private static final Instant NOW = Instant.parse("2026-11-15T12:00:00Z");

  @Test
  void laterPermitComparesA190TwoCnyLotsAndCommissionWithoutRoundingOrShrinking() {
    var scope = new Scope("mirror", "CNYRUB", "CNY", Purpose.PRE_SEND);
    var rate =
        new Rate(
            scope,
            BigInteger.valueOf(12),
            BigInteger.ONE,
            NOW.minusSeconds(1),
            NOW.plusSeconds(1),
            NOW,
            NOW.plusSeconds(1),
            "synthetic qualified 12 RUB/CNY");
    var valuation = valuation((_, _, _, _) -> Optional.of(rate));
    var result =
        valuation.value("mirror", "CNYRUB", "CNY", new BigDecimal("201"), Purpose.PRE_SEND, NOW);
    assertThat(result.qualified()).isTrue();
    assertThat(result.within(new BigDecimal("2412"))).isTrue();
    assertThat(result.within(new BigDecimal("2411"))).isFalse();
    assertThat(result.rubCeiling()).isEqualByComparingTo("2412");
  }

  @Test
  void additionalReserveIsCountedOnceAndFinalProofUsesActualCommissionWithoutAReserve() {
    var valuation =
        valuation(
            (scope, _, _, _) ->
                Optional.of(
                    new Rate(
                        scope,
                        BigInteger.valueOf(12),
                        BigInteger.ONE,
                        NOW.minusSeconds(30),
                        NOW,
                        NOW,
                        NOW.plusSeconds(1),
                        "synthetic exact rate")));
    var before =
        valuation.preSend(
            "mirror",
            "CNYRUB",
            "CNY",
            new BigDecimal("200"),
            BigDecimal.ONE,
            new BigDecimal("2"),
            NOW);
    assertThat(before.rubCeiling()).isEqualByComparingTo("2436");
    var request = java.util.UUID.randomUUID();
    var fee =
        new ru.orbita.broker.BrokerGateway.CommissionEvidence(
            "mirror",
            request,
            new BigDecimal("1.5"),
            "CNY",
            true,
            NOW,
            "independent actual charge");
    var after =
        valuation.finalCost(
            "mirror",
            "CNYRUB",
            "CNY",
            request,
            new BigDecimal("200"),
            fee,
            NOW.minusSeconds(10),
            NOW);
    assertThat(after.rubCeiling()).isEqualByComparingTo("2418");
    assertThat(
            valuation
                .finalCost(
                    "mirror",
                    "CNYRUB",
                    "CNY",
                    java.util.UUID.randomUUID(),
                    new BigDecimal("200"),
                    fee,
                    NOW,
                    NOW)
                .qualified())
        .isFalse();
    var unqualified =
        new ru.orbita.broker.BrokerGateway.CommissionEvidence(
            "mirror", request, new BigDecimal("1.5"), "CNY", false, NOW, "not final");
    assertThat(
            valuation
                .finalCost(
                    "mirror",
                    "CNYRUB",
                    "CNY",
                    request,
                    new BigDecimal("200"),
                    unqualified,
                    NOW,
                    NOW)
                .qualified())
        .isFalse();
  }

  @Test
  void repeatingRationalRateCannotBeRoundedDownToFitACap() {
    var valuation =
        valuation(
            (scope, _, _, _) ->
                Optional.of(
                    new Rate(
                        scope,
                        BigInteger.ONE,
                        BigInteger.valueOf(3),
                        NOW,
                        NOW,
                        NOW,
                        NOW.plusSeconds(1),
                        "synthetic one third")));
    var result = valuation.value("mirror", "asset", "CNY", BigDecimal.ONE, Purpose.PRE_SEND, NOW);
    assertThat(result.within(new BigDecimal("0.3333333333333333333333333333"))).isFalse();
    assertThat(result.within(new BigDecimal("0.3333333333333333333333333334"))).isTrue();
  }

  @Test
  void nativeRubNeedsNoRateButUnknownForeignConversionNeverBecomesZero() {
    var valuation = valuation(null);
    assertThat(
            valuation
                .value("mirror", "asset", "rub", new BigDecimal("2412"), Purpose.PRE_SEND, NOW)
                .within(new BigDecimal("2412")))
        .isTrue();
    var foreign =
        valuation.value("mirror", "asset", "CNY", new BigDecimal("201"), Purpose.PRE_SEND, NOW);
    assertThat(foreign.qualified()).isFalse();
    assertThat(foreign.rub()).isNull();
  }

  @Test
  void wrongRatePurposeAndIncompleteHistoricalExecutionIntervalFailClosed() {
    var wrong =
        valuation(
            (scope, _, _, _) ->
                Optional.of(
                    new Rate(
                        new Scope(
                            scope.accountId(),
                            scope.instrumentId(),
                            scope.currency(),
                            Purpose.FINAL_COST),
                        BigInteger.TEN,
                        BigInteger.ONE,
                        NOW,
                        NOW,
                        NOW,
                        NOW.plusSeconds(1),
                        "different purpose")));
    assertThat(
            wrong
                .value("mirror", "asset", "CNY", BigDecimal.ONE, Purpose.PRE_SEND, NOW)
                .qualified())
        .isFalse();
    var narrow =
        valuation(
            (scope, _, _, _) ->
                Optional.of(
                    new Rate(
                        scope,
                        BigInteger.TEN,
                        BigInteger.ONE,
                        NOW.minusSeconds(1),
                        NOW,
                        NOW,
                        NOW.plusSeconds(1),
                        "narrow history")));
    assertThat(
            narrow
                .value(
                    "mirror",
                    "asset",
                    "CNY",
                    BigDecimal.ONE,
                    Purpose.FINAL_COST,
                    NOW.minusSeconds(10),
                    NOW)
                .qualified())
        .isFalse();
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"expired", "time-lost", "rate-period"})
  void providerDelayOrLostUtcTrustCannotAuthorizeUsingThePreReadTimeBound(String failure) {
    var now = new java.util.concurrent.atomic.AtomicReference<>(NOW);
    var trusted = new java.util.concurrent.atomic.AtomicBoolean(true);
    var clock =
        new Clock() {
          @Override
          public ZoneId getZone() {
            return ZoneOffset.UTC;
          }

          @Override
          public Clock withZone(ZoneId zone) {
            return this;
          }

          @Override
          public Instant instant() {
            return now.get();
          }
        };
    var beans = new DefaultListableBeanFactory();
    beans.registerSingleton(
        "time",
        (ClockEvidence)
            () ->
                trusted.get()
                    ? Optional.of(
                        new ClockEvidence.Bound(now.get(), now.get(), "synthetic current UTC"))
                    : Optional.empty());
    beans.registerSingleton(
        "rates",
        (QualifiedRateProvider)
            (scope, _, _, _) -> {
              var rate =
                  new Rate(
                      scope,
                      BigInteger.valueOf(12),
                      BigInteger.ONE,
                      NOW.minusSeconds(1),
                      NOW.plusSeconds(1),
                      NOW,
                      NOW.plusSeconds(failure.equals("rate-period") ? 5 : 1),
                      "delayed provider proof");
              if (!failure.equals("time-lost")) now.set(NOW.plusSeconds(2));
              else trusted.set(false);
              return Optional.of(rate);
            });
    var valuation =
        new RubValuation(
            beans.getBeanProvider(QualifiedRateProvider.class),
            new TimeAuthority(beans.getBeanProvider(ClockEvidence.class), clock));
    var result =
        valuation.value("mirror", "asset", "CNY", new BigDecimal("201"), Purpose.PRE_SEND, NOW);
    assertThat(result.qualified()).isFalse();
    assertThat(result.within(new BigDecimal("2412"))).isFalse();
    assertThat(result.rub()).isNull();
  }

  private RubValuation valuation(QualifiedRateProvider provider) {
    var beans = new DefaultListableBeanFactory();
    if (provider != null) beans.registerSingleton("rates", provider);
    beans.registerSingleton(
        "time",
        (ClockEvidence) () -> Optional.of(new ClockEvidence.Bound(NOW, NOW, "synthetic UTC")));
    var time =
        new TimeAuthority(
            beans.getBeanProvider(ClockEvidence.class), Clock.fixed(NOW, ZoneOffset.UTC));
    return new RubValuation(beans.getBeanProvider(QualifiedRateProvider.class), time);
  }
}
