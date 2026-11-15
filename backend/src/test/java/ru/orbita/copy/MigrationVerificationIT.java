package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.OrbitaApplication;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.control.AlgorithmVersion;
import ru.orbita.control.ControlService;
import ru.orbita.notifications.NotificationService;
import ru.orbita.operations.MigrationService;
import ru.orbita.web.OwnerCredentials;

@SpringBootTest(
    properties = {
      "orbita.source-account=source",
      "orbita.mirror-account=mirror",
      "orbita.clock-uncertainty-millis=0",
      "orbita.margin-disabled-confirmed=true",
      "orbita.worker.enabled=false"
    })
@Import(CopyLifecycleIT.Fixture.class)
@org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
class MigrationVerificationIT {
  @Autowired MigrationService migration;
  @Autowired ControlService control;
  @Autowired CopyService copies;
  @Autowired CopyLifecycleIT.Simulator broker;
  @Autowired CopyLifecycleIT.MutableClock clock;
  @Autowired org.springframework.test.web.servlet.MockMvc http;
  @Autowired JdbcTemplate database;
  @Autowired PostgreSQLContainer postgres;
  @Autowired AlgorithmVersion algorithms;
  @Autowired OwnerCredentials credentials;

  @BeforeEach
  void resetFixture() {
    CopyLifecycleIT.reset(database, broker, clock);
    database.execute("truncate migration_verification");
  }

  @Test
  void actualDatabaseRestorePreservesMigrationProofAfterRuntimeAccessAndPermitAreRevoked()
      throws Exception {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    clock.now = clock.now.plusSeconds(1);
    var execution =
        new BrokerGateway.SourceExecution(
            "source",
            "restored-proof",
            "restored-order",
            "instrument",
            BrokerGateway.Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    var accepted = copies.accept(execution);
    broker.historyExecutions.add(execution);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    control.confirmLiveAcceptance();
    control.issuePermit(new BigDecimal("1000"));
    credentials.replace("synthetic-migration-password".toCharArray());
    long originalCredentialGeneration = credentials.generation();
    control.pauseInstrument("another-instrument", "Проверка очереди восстановленной копии");
    var manifest = migration.exportManifest(accepted.executionId());
    int originalIntentCount = copies.intents().size();

    String suffix = UUID.randomUUID().toString().replace("-", "");
    String restoredDatabase = "migration_restore_" + suffix;
    String archive = "/tmp/migration-" + suffix + ".dump";
    try {
      assertSuccess(
          postgres.execInContainer(
              "pg_dump",
              "--format=custom",
              "--no-owner",
              "--no-acl",
              "--username=" + postgres.getUsername(),
              "--dbname=" + postgres.getDatabaseName(),
              "--file=" + archive));
      assertSuccess(
          postgres.execInContainer(
              "createdb", "--username=" + postgres.getUsername(), restoredDatabase));
      assertSuccess(
          postgres.execInContainer(
              "pg_restore",
              "--exit-on-error",
              "--no-owner",
              "--no-acl",
              "--username=" + postgres.getUsername(),
              "--dbname=" + restoredDatabase,
              archive));

      try (var restored =
          new SpringApplicationBuilder(OrbitaApplication.class)
              .web(WebApplicationType.NONE)
              .registerShutdownHook(false)
              .initializers(
                  context -> {
                    var registry = (GenericApplicationContext) context;
                    registry.registerBean(
                        "restoredProofBroker",
                        BrokerGateway.class,
                        () -> broker,
                        definition -> definition.setPrimary(true));
                    registry.registerBean(
                        "restoredProofClock",
                        Clock.class,
                        () -> clock,
                        definition -> definition.setPrimary(true));
                  })
              .run(
                  "--spring.datasource.url=jdbc:postgresql://"
                      + postgres.getHost()
                      + ":"
                      + postgres.getMappedPort(5432)
                      + "/"
                      + restoredDatabase,
                  "--spring.datasource.username=" + postgres.getUsername(),
                  "--spring.datasource.password=" + postgres.getPassword(),
                  "--orbita.source-account=source",
                  "--orbita.mirror-account=mirror",
                  "--orbita.clock-uncertainty-millis=0",
                  "--orbita.margin-disabled-confirmed=true",
                  "--orbita.worker.enabled=false",
                  "--orbita.broker.enabled=false",
                  "--orbita.telegram.enabled=false",
                  "--orbita.backup.enabled=false",
                  "--spring.main.banner-mode=off")) {
        var restoredControl = restored.getBean(ControlService.class);
        var restoredCredentials = restored.getBean(OwnerCredentials.class);
        var restoredCopies = restored.getBean(CopyService.class);
        restoredControl.markRestored();
        restoredCredentials.lock();

        assertThat(restoredControl.status().mode()).isEqualTo("RESTORED");
        assertThat(restoredControl.status().reconciled()).isFalse();
        assertThat(restoredControl.status().permitLimit()).isNull();
        assertThat(restoredControl.status().automaticBoundary()).isNull();
        assertThat(restoredCredentials.generation()).isGreaterThan(originalCredentialGeneration);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> restoredCredentials.loadUserByUsername("owner"))
            .isInstanceOf(
                org.springframework.security.core.userdetails.UsernameNotFoundException.class);
        assertThat(restored.getBean(NotificationService.class).list(200))
            .allSatisfy(event -> assertThat(event.status()).isEqualTo("IGNORED"));

        var verification = restored.getBean(MigrationService.class).compare(manifest);
        assertThat(verification.matches()).isTrue();
        assertThat(verification.journalMatches()).isTrue();
        assertThat(verification.settingsMatches()).isTrue();
        assertThat(verification.executionMatches()).isTrue();
        assertThat(verification.calculationMatches()).isTrue();
        assertThat(restored.getBean(MigrationService.class).verifications())
            .singleElement()
            .extracting(MigrationService.Verification::id)
            .isEqualTo(verification.id());
        assertThat(restoredCopies.intents()).hasSize(originalIntentCount);
        assertThat(restoredControl.status().mode()).isEqualTo("RESTORED");
        assertThat(restoredControl.status().reconciled()).isFalse();
        assertThat(restoredControl.status().permitLimit()).isNull();
      }
      assertThat(control.status().mode()).isEqualTo("SHADOW");
      assertThat(control.status().permitLimit()).isEqualByComparingTo("1000");
      assertThat(credentials.generation()).isEqualTo(originalCredentialGeneration);
      assertThat(credentials.loadUserByUsername("owner").getUsername()).isEqualTo("owner");
      assertThat(broker.posts).isZero();
      assertThat(broker.cancels).isZero();
      assertThat(migration.verifications()).isEmpty();
    } finally {
      postgres.execInContainer(
          "dropdb", "--if-exists", "--username=" + postgres.getUsername(), restoredDatabase);
      postgres.execInContainer("rm", "-f", archive);
    }
  }

  private static void assertSuccess(org.testcontainers.containers.Container.ExecResult command) {
    assertThat(command.getExitCode()).describedAs(command.getStderr()).isZero();
  }

  @Test
  void httpStatusPreservesEverySavedCoefficientDigit() throws Exception {
    broker.mirrorCapital = new BigDecimal("123456.78901234567890123456789");
    control.markReconciled();
    control.start();
    control.recalculate();
    http.perform(
            org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get("/api/status")
                .with(
                    org.springframework.security.test.web.servlet.request
                        .SecurityMockMvcRequestPostProcessors.user("owner")
                        .roles("OWNER")))
        .andExpect(
            org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk())
        .andExpect(
            org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath(
                    "$.coefficient")
                .isString())
        .andExpect(
            org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath(
                    "$.coefficient")
                .value("1.2345678901234567890123456789"));
  }

  @Test
  void serverComparisonReplaysTheVerifiedExecutionWithoutCreatingTradingActions() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    clock.now = clock.now.plusSeconds(1);
    var execution =
        new BrokerGateway.SourceExecution(
            "source",
            "migration-proof",
            "migration-order",
            "instrument",
            BrokerGateway.Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    var accepted = copies.accept(execution);
    broker.historyExecutions.add(execution);
    control.confirmLiveAcceptance();
    String id = accepted.executionId();
    var manifest = migration.exportManifest(id);
    int intents = copies.intents().size();
    assertThat(migration.compare(manifest).matches()).isTrue();
    assertThat(copies.intents()).hasSize(intents);
    assertThat(broker.posts).isZero();
    var changed =
        new MigrationService.Manifest(
            manifest.formatVersion(),
            manifest.executionId(),
            manifest.algorithmVersion(),
            manifest.journalBoundary(),
            "0".repeat(64),
            manifest.settingsHash(),
            manifest.executionHash(),
            manifest.calculationHash(),
            manifest.recordedAt());
    var proof = migration.compare(changed);
    assertThat(proof.matches()).isFalse();
    assertThat(proof.journalMatches()).isFalse();
    assertThat(migration.verifications()).hasSize(2);
  }
}
