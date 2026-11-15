package ru.orbita.valuation;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import ru.orbita.control.ClockEvidence;
import ru.orbita.control.TimeAuthority;
import ru.orbita.valuation.QualifiedRateProvider.Purpose;

class QualifiedRateFileProviderTest {
  @TempDir Path directory;
  private static final Instant NOW = Instant.parse("2026-11-15T12:00:00Z");
  private final UUID session = UUID.randomUUID();

  @Test
  void ephemeralQualifiedFileEnablesA190AndAnExpiredFileCannotAuthorizeTheNextOrder()
      throws Exception {
    var provider =
        new QualifiedRateFileProvider(
            directory,
            "mirror",
            "collector",
            Duration.ofSeconds(5),
            Duration.ofMinutes(1),
            session);
    var context = provider.context("CNYRUB", "CNY", Purpose.PRE_SEND);
    Files.writeString(
        directory.resolve(context.fileName()),
        evidence(context, NOW.minusMillis(1), NOW.plusSeconds(1)));
    var current =
        valuation(provider, NOW)
            .value("mirror", "CNYRUB", "CNY", new BigDecimal("201"), Purpose.PRE_SEND, NOW);
    assertThat(current.within(new BigDecimal("2412"))).isTrue();
    assertThat(current.within(new BigDecimal("2411"))).isFalse();
    assertThat(
            valuation(provider, NOW.plusSeconds(2))
                .value("mirror", "CNYRUB", "CNY", new BigDecimal("201"), Purpose.PRE_SEND, NOW)
                .qualified())
        .isFalse();
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "account",
        "instrument",
        "currency",
        "purpose",
        "session",
        "collector",
        "basis",
        "ratio",
        "unbounded",
        "provenance",
        "duplicate"
      })
  void unqualifiedOrMismatchedCurrencyEvidenceNeverAuthorizesAPermit(String corruption)
      throws Exception {
    var provider =
        new QualifiedRateFileProvider(
            directory,
            "mirror",
            "collector",
            Duration.ofSeconds(5),
            Duration.ofMinutes(1),
            session);
    var context = provider.context("CNYRUB", "CNY", Purpose.PRE_SEND);
    String content = evidence(context, NOW.minusMillis(1), NOW.plusSeconds(1));
    content =
        switch (corruption) {
          case "account" -> content.replace("accountId=mirror", "accountId=other");
          case "instrument" -> content.replace("instrumentId=CNYRUB", "instrumentId=other");
          case "currency" -> content.replace("currency=CNY", "currency=USD");
          case "purpose" -> content.replace("purpose=PRE_SEND", "purpose=FINAL_COST");
          case "session" -> content.replace(session.toString(), UUID.randomUUID().toString());
          case "collector" -> content.replace("collectorId=collector", "collectorId=other");
          case "basis" ->
              content.replace(
                  "basis=QUALIFIED_RUB_COST_PER_SETTLEMENT_UNIT",
                  "basis=UNQUALIFIED_INVERSE_PRICE");
          case "ratio" -> content.replace("rateNumerator=12", "rateNumerator=0");
          case "unbounded" ->
              content.replace(
                  "evidenceExpiresAt=" + NOW.plusSeconds(1),
                  "evidenceExpiresAt=" + NOW.plusSeconds(500));
          case "provenance" ->
              content.replace("qualificationReference=synthetic-qualified-conversion\n", "");
          default -> content + "sequence=1\n";
        };
    Files.writeString(directory.resolve(context.fileName()), content);
    assertThat(
            valuation(provider, NOW)
                .value("mirror", "CNYRUB", "CNY", new BigDecimal("201"), Purpose.PRE_SEND, NOW)
                .qualified())
        .isFalse();
  }

  @Test
  void aFreshHistoricalFinalQuoteMustCoverEveryExecutionTimestamp() throws Exception {
    var provider =
        new QualifiedRateFileProvider(
            directory,
            "mirror",
            "collector",
            Duration.ofSeconds(5),
            Duration.ofMinutes(1),
            session);
    var context = provider.context("CNYRUB", "CNY", Purpose.FINAL_COST);
    String content =
        evidence(context, NOW.minusMillis(1), NOW.plusSeconds(1))
            .replace(
                "rateValidFrom=" + NOW.minusSeconds(1), "rateValidFrom=" + NOW.minusSeconds(60))
            .replace(
                "rateValidThrough=" + NOW.plusSeconds(1),
                "rateValidThrough=" + NOW.minusSeconds(10));
    Files.writeString(directory.resolve(context.fileName()), content);
    var valuation = valuation(provider, NOW);
    assertThat(
            valuation
                .value(
                    "mirror",
                    "CNYRUB",
                    "CNY",
                    new BigDecimal("201"),
                    Purpose.FINAL_COST,
                    NOW.minusSeconds(50),
                    NOW.minusSeconds(20))
                .within(new BigDecimal("2412")))
        .isTrue();
    assertThat(
            valuation
                .value(
                    "mirror",
                    "CNYRUB",
                    "CNY",
                    new BigDecimal("201"),
                    Purpose.FINAL_COST,
                    NOW.minusSeconds(70),
                    NOW.minusSeconds(20))
                .qualified())
        .isFalse();
    assertThat(
            valuation
                .value("mirror", "CNYRUB", "CNY", new BigDecimal("201"), Purpose.PRE_SEND, NOW)
                .qualified())
        .isFalse();
  }

  @Test
  void changedSameSequenceCannotReplaceAQuoteButFreshHigherSequenceCan() throws Exception {
    var provider =
        new QualifiedRateFileProvider(
            directory,
            "mirror",
            "collector",
            Duration.ofSeconds(5),
            Duration.ofMinutes(1),
            session);
    var context = provider.context("CNYRUB", "CNY", Purpose.PRE_SEND);
    Path file = directory.resolve(context.fileName());
    String original = evidence(context, NOW.minusMillis(1), NOW.plusSeconds(1));
    Files.writeString(file, original);
    var valuation = valuation(provider, NOW);
    assertThat(
            valuation
                .value("mirror", "CNYRUB", "CNY", BigDecimal.ONE, Purpose.PRE_SEND, NOW)
                .rubCeiling())
        .isEqualByComparingTo("12");
    String changed = original.replace("rateNumerator=12", "rateNumerator=13");
    Files.writeString(file, changed);
    assertThat(
            valuation
                .value("mirror", "CNYRUB", "CNY", BigDecimal.ONE, Purpose.PRE_SEND, NOW)
                .qualified())
        .isFalse();
    Files.writeString(file, changed.replace("sequence=1", "sequence=2"));
    assertThat(
            valuation
                .value("mirror", "CNYRUB", "CNY", BigDecimal.ONE, Purpose.PRE_SEND, NOW)
                .rubCeiling())
        .isEqualByComparingTo("13");
  }

  private String evidence(
      QualifiedRateFileProvider.Context context, Instant asOf, Instant expires) {
    return "format=orbita-qualified-rub-rate-v1\nsessionId="
        + session
        + "\ncollectorId=collector\naccountId=mirror\ninstrumentId=CNYRUB\ncurrency=CNY\npurpose="
        + context.scope().purpose()
        + "\nbasis=QUALIFIED_RUB_COST_PER_SETTLEMENT_UNIT\nqualificationReference=synthetic-qualified-conversion\nsequence=1\nrateNumerator=12\nrateDenominator=1\nrateValidFrom="
        + NOW.minusSeconds(1)
        + "\nrateValidThrough="
        + NOW.plusSeconds(1)
        + "\nevidenceAsOf="
        + asOf
        + "\nevidenceExpiresAt="
        + expires
        + "\n";
  }

  private RubValuation valuation(QualifiedRateProvider provider, Instant now) {
    var beans = new DefaultListableBeanFactory();
    beans.registerSingleton("rates", provider);
    beans.registerSingleton(
        "utc",
        (ClockEvidence) () -> Optional.of(new ClockEvidence.Bound(now, now, "synthetic UTC")));
    return new RubValuation(
        beans.getBeanProvider(QualifiedRateProvider.class),
        new TimeAuthority(
            beans.getBeanProvider(ClockEvidence.class), Clock.fixed(now, ZoneOffset.UTC)));
  }
}
