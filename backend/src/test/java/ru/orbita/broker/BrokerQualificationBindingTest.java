package ru.orbita.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import ru.orbita.control.OrbitaProperties;

class BrokerQualificationBindingTest {
  @TempDir Path privateDirectory;
  private static final Instant NOW = Instant.parse("2026-11-15T12:00:00Z");
  private final ApplicationContextRunner context =
      new ApplicationContextRunner()
          .withUserConfiguration(BrokerConfiguration.class, AccountConfiguration.class)
          .withPropertyValues(
              "orbita.source-account=source",
              "orbita.mirror-account=mirror",
              "orbita.broker.enabled=false");

  @Test
  void absentOperatorProfileStaysUnconfirmedWithoutAZeroReserve() {
    context.run(
        app -> {
          assertThat(app).hasNotFailed();
          var profile = app.getBean(BrokerQualifications.class);
          assertThat(profile.capitalQualified("source", NOW)).isFalse();
          assertThat(profile.resourcesQualified("mirror", "asset", "RUB", NOW)).isFalse();
          assertThat(profile.additionalCommissionReserve()).isNull();
          assertThat(profile.buyTotalCostNondecreasingQualified("mirror", "asset", "RUB", NOW))
              .isFalse();
        });
  }

  @Test
  void explicitScopedProfileBindsIntoTheProductionQualificationBean() {
    context
        .withPropertyValues(validProfile())
        .run(
            app -> {
              assertThat(app).hasNotFailed();
              var profile = app.getBean(BrokerQualifications.class);
              assertThat(profile.capitalQualified("source", NOW)).isTrue();
              assertThat(profile.capitalQualified("mirror", NOW)).isTrue();
              assertThat(profile.resourcesQualified("mirror", "asset", "RUB", NOW)).isTrue();
              assertThat(profile.resourcesQualified("source", "asset", "RUB", NOW)).isFalse();
              assertThat(profile.resourcesQualified("mirror", "other", "RUB", NOW)).isFalse();
              assertThat(profile.resourcesQualified("mirror", "asset", "USD", NOW)).isFalse();
              assertThat(profile.additionalCommissionReserve()).isEqualByComparingTo("1.000000009");
              assertThat(profile.buyTotalCostNondecreasingQualified("mirror", "asset", "RUB", NOW))
                  .isFalse();
            });
  }

  @Test
  void explicitTotalBuyCostConfirmationIsScopedToTheQualifiedProfile() {
    context
        .withPropertyValues(validProfile())
        .withPropertyValues(
            "orbita.broker.qualification.total-buy-cost-nondecreasing-confirmed=true")
        .run(
            app -> {
              assertThat(app).hasNotFailed();
              var profile = app.getBean(BrokerQualifications.class);
              assertThat(profile.buyTotalCostNondecreasingQualified("mirror", "asset", "RUB", NOW))
                  .isTrue();
              assertThat(profile.buyTotalCostNondecreasingQualified("source", "asset", "RUB", NOW))
                  .isFalse();
              assertThat(profile.buyTotalCostNondecreasingQualified("mirror", "other", "RUB", NOW))
                  .isFalse();
              assertThat(profile.buyTotalCostNondecreasingQualified("mirror", "asset", "USD", NOW))
                  .isFalse();
            });
  }

  @Test
  void nativePrivateConfigImportLoadsTheSameValidatedProfile() throws Exception {
    var file = privateDirectory.resolve("broker.properties");
    Files.writeString(file, String.join(System.lineSeparator(), validProfile()));
    context
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withPropertyValues("spring.config.import=" + file.toUri())
        .run(
            app -> {
              assertThat(app).hasNotFailed();
              assertThat(
                      app.getBean(BrokerQualifications.class)
                          .resourcesQualified("mirror", "asset", "RUB", NOW))
                  .isTrue();
            });
  }

  @Test
  void expiredProfileKeepsReadsAvailableButCannotQualifyCapitalResourcesOrFinalFees() {
    profile("valid-until=2026-11-14T12:00:00Z")
        .run(
            app -> {
              assertThat(app).hasNotFailed();
              var qualification = app.getBean(BrokerQualifications.class);
              assertThat(qualification.capitalQualified("source", NOW)).isFalse();
              assertThat(qualification.resourcesQualified("mirror", "asset", "RUB", NOW)).isFalse();
              assertThat(qualification.scopeMatches("mirror", "asset", "RUB", NOW)).isFalse();
            });
  }

  @Test
  void capitalOnlyAttestationDoesNotInventAReserveOrFundingInterpretation() {
    context
        .withPropertyValues(
            "orbita.broker.qualification.enabled=true",
            "orbita.broker.qualification.capital-composition-confirmed=true",
            "orbita.broker.qualification.capital-account-ids=source,mirror",
            "orbita.broker.qualification.mirror-account-id=mirror",
            "orbita.broker.qualification.evidence-reference=synthetic-capital-only-profile",
            "orbita.broker.qualification.valid-until=2026-11-16T12:00:00Z")
        .run(
            app -> {
              assertThat(app).hasNotFailed();
              var qualification = app.getBean(BrokerQualifications.class);
              assertThat(qualification.capitalQualified("source", NOW)).isTrue();
              assertThat(qualification.additionalCommissionReserve()).isNull();
              assertThat(qualification.resourcesQualified("mirror", "asset", "RUB", NOW)).isFalse();
            });
  }

  @Test
  void netFundsAttestationDoesNotPretendToSupplyExactBankReservationCoverage() {
    profile("own-money-basis=NET_REQUIRES_EXACT_BANK_RESERVE_EVIDENCE")
        .run(
            app -> {
              assertThat(app).hasNotFailed();
              assertThat(
                      app.getBean(BrokerQualifications.class)
                          .resourcesQualified("mirror", "asset", "RUB", NOW))
                  .isFalse();
            });
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "mirror-account-id=other",
        "capital-account-ids=source,other",
        "instrument-ids=",
        "reserve-currency=",
        "reserve-currency=RUBLE",
        "evidence-reference=",
        "valid-until=",
        "valid-until=not-an-instant",
        "additional-commission-reserve=",
        "additional-commission-reserve=-0.01",
        "additional-commission-reserve=0.00000000000000000000000000001",
        "additional-commission-reserve=1000000000000000000000000000000000000",
        "own-money-basis=UNQUALIFIED",
        "unrecognised-confirmation=true"
      })
  void invalidExplicitProfileFailsStartupInsteadOfBroadeningScope(String property) {
    profile(property).run(app -> assertThat(app).hasFailed());
  }

  private ApplicationContextRunner profile(String override) {
    return context
        .withPropertyValues(validProfile())
        .withPropertyValues("orbita.broker.qualification." + override);
  }

  private static String[] validProfile() {
    return new String[] {
      "orbita.broker.qualification.enabled=true",
      "orbita.broker.qualification.capital-composition-confirmed=true",
      "orbita.broker.qualification.own-resource-interpretation-confirmed=true",
      "orbita.broker.qualification.commission-interpretation-confirmed=true",
      "orbita.broker.qualification.margin-disabled-confirmed=true",
      "orbita.broker.qualification.actual-commission-total-confirmed=true",
      "orbita.broker.qualification.additional-commission-reserve=1.000000009",
      "orbita.broker.qualification.evidence-reference=synthetic-qualified-account-profile",
      "orbita.broker.qualification.capital-account-ids=source,mirror",
      "orbita.broker.qualification.mirror-account-id=mirror",
      "orbita.broker.qualification.instrument-ids=asset",
      "orbita.broker.qualification.reserve-currency=RUB",
      "orbita.broker.qualification.valid-until=2026-11-16T12:00:00Z",
      "orbita.broker.qualification.own-money-basis=GROSS_BEFORE_PROGRAM_RESERVES"
    };
  }

  @Configuration(proxyBeanMethods = false)
  @EnableConfigurationProperties(OrbitaProperties.class)
  static class AccountConfiguration {}
}
