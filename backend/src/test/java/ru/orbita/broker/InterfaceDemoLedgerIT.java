package ru.orbita.broker;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.OrbitaApplication;
import ru.orbita.account.AccountQuery;
import ru.orbita.account.AccountReadService;
import ru.orbita.control.ControlService;
import ru.orbita.control.OrbitaProperties;

/** Exercises the shipped teardown guards; complete domain generation is InterfaceDemoIT's job. */
@Testcontainers
class InterfaceDemoLedgerIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine");

  @BeforeEach
  void freshIsolatedDatabase() {
    database().execute("drop schema if exists orbita_demo_preview cascade");
    var flyway =
        Flyway.configure()
            .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
            .cleanDisabled(false)
            .load();
    flyway.clean();
    flyway.migrate();
  }

  @Test
  void compositeReadOwnershipPreservesBaselineAndDeletesOnlyCapturedPreviewAccount() {
    var db = database();
    try (var baseline = application("", false)) {
      baseline.getBean(AccountReadService.class).page("assets", AccountQuery.defaults());
      baseline
          .getBean(ru.orbita.web.OwnerCredentials.class)
          .replace("synthetic-ledger-owner-password".toCharArray());
    }
    String credential =
        db.queryForObject("select to_jsonb(c)::text from owner_credential c", String.class);
    String offlineRevision =
        db.queryForObject(
            "select local_revision from account_read_revision where account_id='unconfigured'",
            String.class);
    try (var baseline = application("baseline/Ω", true)) {
      baseline.getBean(AccountReadService.class).refresh();
    }
    var before =
        db.queryForList(
            "select to_jsonb(d)::text from account_read_document d order by account_id,kind,id",
            String.class);
    var ledger = InterfaceDemoLedger.begin(db, false);
    try (var preview = application("preview/Ω", true)) {
      preview.getBean(AccountReadService.class).refresh();
      assertThat(
              preview
                  .getBean(AccountReadService.class)
                  .page("orders", AccountQuery.defaults())
                  .total())
          .isEqualTo(2);
    }
    ledger.captureOwnedFacts();
    ledger.restoreControl();
    String key =
        db.queryForObject(
            "select row_id from orbita_demo_preview.owned where relation='account_read_document' and row_snapshot->>'kind'='BANK_ORDER' and row_snapshot->>'id'='[ТЕСТ]/один'",
            String.class);
    var parts = tools.jackson.databind.json.JsonMapper.builder().build().readTree(key);
    assertThat(parts.size()).isEqualTo(3);
    assertThat(parts.get(0).asText()).isEqualTo("preview/Ω");
    assertThat(parts.get(1).asText()).isEqualTo("BANK_ORDER");
    assertThat(parts.get(2).asText()).isEqualTo("[ТЕСТ]/один");
    ledger.cleanup();
    ledger.cleanup();
    assertThat(
            db.queryForList(
                "select to_jsonb(d)::text from account_read_document d order by account_id,kind,id",
                String.class))
        .isEqualTo(before);
    assertThat(
            db.queryForObject(
                "select local_revision from account_read_revision where account_id='unconfigured'",
                String.class))
        .isEqualTo(offlineRevision);
    assertThat(
            db.queryForObject(
                "select count(*) from account_read_freshness where account_id='preview/Ω'",
                Integer.class))
        .isZero();
    assertThat(
            db.queryForObject(
                "select count(*) from account_read_revision where account_id='preview/Ω'",
                Integer.class))
        .isZero();
    assertThat(db.queryForObject("select to_jsonb(c)::text from owner_credential c", String.class))
        .isEqualTo(credential);
  }

  @Test
  void laterOwnerPauseAndOfflineProjectionAreRemovedButUnrelatedStartupJournalSurvives() {
    var db = database();
    var ledger = InterfaceDemoLedger.begin(db, false);
    try (var preview = application("preview", true)) {
      preview.getBean(AccountReadService.class).refresh();
    }
    ledger.captureOwnedFacts();
    ledger.restoreControl();
    long capturedJournal =
        db.queryForObject("select coalesce(max(id),0) from journal_entry", Long.class);
    try (var offline = application("preview", false)) {
      offline.getBean(ControlService.class).pauseInstrument("security", "[ТЕСТ] Later owner pause");
      assertThat(
              offline
                  .getBean(AccountReadService.class)
                  .page("assets", AccountQuery.defaults())
                  .items()
                  .getFirst()
                  .get("state"))
          .isEqualTo("PAUSED");
    }
    ledger.cleanup();
    assertThat(db.queryForObject("select count(*) from account_read_document", Integer.class))
        .isZero();
    assertThat(db.queryForObject("select count(*) from account_read_freshness", Integer.class))
        .isZero();
    assertThat(db.queryForObject("select count(*) from account_read_revision", Integer.class))
        .isZero();
    assertThat(db.queryForObject("select count(*) from instrument_state", Integer.class)).isZero();
    assertThat(
            db.queryForObject(
                "select count(*) from journal_entry where id>? and category='PROCESS_STARTED'",
                Integer.class,
                capturedJournal))
        .isPositive();
    assertThat(
            db.queryForObject(
                "select count(*) from journal_entry where detail='[ТЕСТ] Later owner pause'",
                Integer.class))
        .isZero();
  }

  @Test
  void cleanupRefusesBrokerObservationsArrivingAfterOwnershipCaptureWithoutDeletingAnything() {
    var db = database();
    var ledger = InterfaceDemoLedger.begin(db, false);
    try (var preview = application("preview", true)) {
      preview.getBean(AccountReadService.class).refresh();
    }
    ledger.captureOwnedFacts();
    ledger.restoreControl();
    try (var external = application("preview", true)) {
      var bank = external.getBean(AccountReadIT.Bank.class);
      bank.orders.put("outside-preview", AccountReadIT.order("outside-preview"));
      external.getBean(AccountReadService.class).refresh();
    }
    var before =
        db.queryForList(
            "select to_jsonb(t)::text from account_read_document t order by account_id,kind,id",
            String.class);
    assertThatThrownBy(ledger::cleanup)
        .hasMessageContaining("Demo cleanup failed")
        .rootCause()
        .hasMessageContaining("Demo cleanup");
    assertThat(
            db.queryForList(
                "select to_jsonb(t)::text from account_read_document t order by account_id,kind,id",
                String.class))
        .isEqualTo(before);
    assertThat(
            db.queryForObject(
                "select to_regnamespace('orbita_demo_preview') is not null", Boolean.class))
        .isTrue();
  }

  @Test
  void legacyVersionOneRemovesOnSchemaFifteenWithoutResolvingReadTablesAndRefusesUpgrade() {
    var flyway =
        Flyway.configure()
            .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
            .cleanDisabled(false)
            .target("15")
            .load();
    flyway.clean();
    flyway.migrate();
    var db = database();
    assertThat(
            db.queryForObject(
                "select to_regclass('public.account_read_document') is null", Boolean.class))
        .isTrue();
    legacyManifest(db);
    InterfaceDemoLedger.cleanup(db);
    InterfaceDemoLedger.cleanup(db);
    assertThat(
            db.queryForObject(
                "select to_regnamespace('orbita_demo_preview') is null", Boolean.class))
        .isTrue();
    legacyManifest(db);
    Flyway.configure()
        .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
        .load()
        .migrate();
    assertThatThrownBy(() -> InterfaceDemoLedger.cleanup(db))
        .rootCause()
        .hasMessageContaining("before upgrading");
    assertThat(
            db.queryForObject(
                "select to_regnamespace('orbita_demo_preview') is not null", Boolean.class))
        .isTrue();
  }

  private void legacyManifest(JdbcTemplate db) {
    // Ownership metadata only; no trading facts are inserted by SQL.
    db.execute("create schema orbita_demo_preview");
    db.execute(
        "create table orbita_demo_preview.batch(singleton boolean primary key,version integer,captured boolean,restored_at timestamptz,control_before jsonb,connectivity_before jsonb)");
    db.execute(
        "create table orbita_demo_preview.baseline(relation text,row_id text,primary key(relation,row_id))");
    db.execute(
        "create table orbita_demo_preview.owned(relation text,row_id text,primary key(relation,row_id))");
    db.execute("create table orbita_demo_preview.instrument(instrument_id text primary key)");
    db.execute(
        "insert into orbita_demo_preview.batch select true,1,true,clock_timestamp(),to_jsonb(c),to_jsonb(b) from control_state c cross join broker_connectivity b");
  }

  private JdbcTemplate database() {
    return new JdbcTemplate(
        new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
  }

  private JdbcTemplate database(String zone) {
    var source =
        new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()) {
          @Override
          public java.sql.Connection getConnection() throws java.sql.SQLException {
            var connection = super.getConnection();
            try (var statement =
                connection.prepareStatement("select set_config('TimeZone',?,false)")) {
              statement.setString(1, zone);
              statement.execute();
            } catch (java.sql.SQLException failure) {
              connection.close();
              throw failure;
            }
            return connection;
          }
        };
    return new JdbcTemplate(source);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Asia/Yekaterinburg", "UTC"})
  void psqlRemovalComparesTypedInstantsAcrossCaptureTimezones(String captureZone) throws Exception {
    try (var baseline = application("baseline", true)) {
      baseline.getBean(AccountReadService.class).refresh();
    }
    var db = database();
    var baseline =
        db.queryForList(
            "select to_jsonb(d)::text from account_read_document d order by account_id,kind,id",
            String.class);
    var capture = database(captureZone);
    assertThat(capture.queryForObject("select current_setting('TimeZone')", String.class))
        .isEqualTo(captureZone);
    var ledger = InterfaceDemoLedger.begin(capture, false);
    try (var preview = application("preview", true)) {
      preview.getBean(AccountReadService.class).refresh();
    }
    ledger.captureOwnedFacts();
    ledger.restoreControl();
    var result = psqlCleanup(captureZone.equals("UTC") ? "Asia/Yekaterinburg" : "UTC");
    assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isZero();
    assertThat(
            db.queryForList(
                "select to_jsonb(d)::text from account_read_document d order by account_id,kind,id",
                String.class))
        .isEqualTo(baseline);
    assertThat(
            db.queryForObject(
                "select to_regnamespace('orbita_demo_preview') is null", Boolean.class))
        .isTrue();
  }

  private org.testcontainers.containers.Container.ExecResult psqlCleanup(String zone)
      throws Exception {
    String script =
        "select set_config('orbita.demo.expected_database',current_database(),false),set_config('orbita.demo.expected_role',current_user,false),set_config('orbita.demo.offline_confirmed','broker+telegram+worker+backup-disabled;accounts-empty',false);\n"
            + Files.readString(
                Path.of(System.getProperty("basedir"))
                    .resolve("../deploy/demo/cleanup.sql")
                    .normalize());
    POSTGRES.copyFileToContainer(Transferable.of(script, 0644), "/tmp/interface-demo-cleanup.sql");
    return POSTGRES.execInContainer(
        "env",
        "PGOPTIONS=-c TimeZone=" + zone,
        "psql",
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        POSTGRES.getUsername(),
        "-d",
        POSTGRES.getDatabaseName(),
        "-f",
        "/tmp/interface-demo-cleanup.sql");
  }

  @Test
  void psqlRemovalStillRefusesChangedTypedTimestampWithIdenticalRawDocuments() throws Exception {
    var fixture = capturedPartial();
    var db = database();
    var documents =
        db.queryForList(
            "select to_jsonb(d)::text from account_read_document d order by account_id,kind,id",
            String.class);
    var attempt =
        db.queryForObject(
            "select last_attempt_at from account_read_freshness where account_id='preview' and section='cash'",
            java.sql.Timestamp.class);
    try (var writer = application("preview", false)) {
      writer
          .getBean(AccountReadService.class)
          .acceptCash(
              partialSnapshot(
                  "rub", fixture.at().plusSeconds(1), fixture.at().minusSeconds(4), "900"));
    }
    assertThat(
            db.queryForList(
                "select to_jsonb(d)::text from account_read_document d order by account_id,kind,id",
                String.class))
        .isEqualTo(documents);
    assertThat(
            db.queryForObject(
                "select last_attempt_at from account_read_freshness where account_id='preview' and section='cash'",
                java.sql.Timestamp.class))
        .isNotEqualTo(attempt);
    var freshness =
        db.queryForList(
            "select to_jsonb(f)::text from account_read_freshness f order by account_id,section",
            String.class);
    var result = psqlCleanup("UTC");
    assertThat(result.getExitCode()).isNotZero();
    assertThat(result.getStderr())
        .contains("changed captured broker observations: account_read_freshness");
    assertThat(
            db.queryForList(
                "select to_jsonb(f)::text from account_read_freshness f order by account_id,section",
                String.class))
        .isEqualTo(freshness);
    assertThat(
            db.queryForList(
                "select to_jsonb(d)::text from account_read_document d order by account_id,kind,id",
                String.class))
        .isEqualTo(documents);
    assertThat(
            db.queryForObject(
                "select to_regnamespace('orbita_demo_preview') is not null", Boolean.class))
        .isTrue();
  }

  @Test
  void unchangedCapturedPartialIsOwnedAndRemoved() {
    var data = capturedPartial();
    var db = database();
    assertThat(
            db.queryForObject(
                "select count(*) from orbita_demo_preview.owned where relation='account_read_document' and row_snapshot->>'kind'='CASH_PARTIAL'",
                Integer.class))
        .isEqualTo(1);
    data.ledger().cleanup();
    assertThat(db.queryForObject("select count(*) from account_read_document", Integer.class))
        .isZero();
    assertThat(db.queryForObject("select count(*) from account_read_freshness", Integer.class))
        .isZero();
  }

  @Test
  void changedCapturedPartialRefusesEvenWhenFreshnessAndCoherentCashAreUnchanged() {
    partialMutationMustRefuse("rub");
  }

  @Test
  void newPartialCurrencyRefusesEvenWhenFreshnessAndCoherentCashAreUnchanged() {
    partialMutationMustRefuse("usd");
  }

  private record PartialFixture(InterfaceDemoLedger ledger, Instant at) {}

  private PartialFixture capturedPartial() {
    var ledger = InterfaceDemoLedger.begin(database(), false);
    Instant at = Instant.now().minusSeconds(10);
    try (var preview = application("preview", false)) {
      var reads = preview.getBean(AccountReadService.class);
      reads.acceptCash(
          new AccountRead.Snapshot<>(
              List.of(
                  new AccountRead.Cash(
                      "rub",
                      new BigDecimal("1000"),
                      new BigDecimal("800"),
                      new BigDecimal("200"),
                      null,
                      at.minusSeconds(5),
                      "[ТЕСТ] cash stream",
                      null),
                  new AccountRead.Cash(
                      "usd",
                      new BigDecimal("10"),
                      new BigDecimal("10"),
                      BigDecimal.ZERO,
                      null,
                      at.minusSeconds(5),
                      "[ТЕСТ] cash stream",
                      null)),
              at,
              at,
              false,
              List.of("[ТЕСТ] partial currency inventory")));
      reads.acceptCash(partialSnapshot("rub", at, at.minusSeconds(4), "900"));
    }
    ledger.captureOwnedFacts();
    ledger.restoreControl();
    return new PartialFixture(ledger, at);
  }

  private AccountRead.Snapshot<AccountRead.Cash> partialSnapshot(
      String currency, Instant received, Instant bank, String available) {
    return new AccountRead.Snapshot<>(
        List.of(
            new AccountRead.Cash(
                currency,
                null,
                new BigDecimal(available),
                null,
                null,
                bank,
                "[ТЕСТ] cash stream",
                null)),
        received,
        received,
        false,
        List.of("[ТЕСТ] partial currency inventory"));
  }

  private void partialMutationMustRefuse(String currency) {
    var fixture = capturedPartial();
    var db = database();
    var freshness =
        db.queryForList(
            "select to_jsonb(f)::text from account_read_freshness f order by account_id,section",
            String.class);
    var coherent =
        db.queryForList(
            "select to_jsonb(d)::text from account_read_document d where kind='CASH' order by account_id,kind,id",
            String.class);
    try (var writer = application("preview", false)) {
      // Public read-service seam: two bank observations can share one receive timestamp.
      // No SQL seeds or freshness edits; isolate the raw partial-kind guard itself.
      writer
          .getBean(AccountReadService.class)
          .acceptCash(partialSnapshot(currency, fixture.at(), fixture.at().minusSeconds(3), "901"));
    }
    assertThat(
            db.queryForList(
                "select to_jsonb(f)::text from account_read_freshness f order by account_id,section",
                String.class))
        .isEqualTo(freshness);
    assertThat(
            db.queryForList(
                "select to_jsonb(d)::text from account_read_document d where kind='CASH' order by account_id,kind,id",
                String.class))
        .isEqualTo(coherent);
    var all =
        db.queryForList(
            "select to_jsonb(d)::text from account_read_document d order by account_id,kind,id",
            String.class);
    assertThatThrownBy(fixture.ledger()::cleanup).rootCause().hasMessageContaining("Demo cleanup");
    assertThat(
            db.queryForList(
                "select to_jsonb(d)::text from account_read_document d order by account_id,kind,id",
                String.class))
        .isEqualTo(all);
    assertThat(
            db.queryForObject(
                "select to_regnamespace('orbita_demo_preview') is not null", Boolean.class))
        .isTrue();
  }

  private ConfigurableApplicationContext application(String account, boolean bank) {
    return new SpringApplicationBuilder(
            bank
                ? new Class<?>[] {OrbitaApplication.class, ReadFixture.class}
                : new Class<?>[] {OrbitaApplication.class})
        .web(WebApplicationType.NONE)
        .run(
            "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
            "--spring.datasource.username=" + POSTGRES.getUsername(),
            "--spring.datasource.password=" + POSTGRES.getPassword(),
            "--orbita.broker.enabled=false",
            "--orbita.telegram.enabled=false",
            "--orbita.worker.enabled=false",
            "--orbita.backup.enabled=false",
            "--orbita.account-read.enabled=false",
            "--orbita.mirror-account=" + account);
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class ReadFixture {
    @Bean
    AccountReadIT.Bank bank() {
      var bank = new AccountReadIT.Bank();
      bank.reset();
      return bank;
    }

    @Bean(destroyMethod = "close")
    BrokerTlsTestSupport.Simulator tls(AccountReadIT.Bank bank) throws Exception {
      return new AccountReadIT.Fixture().tls(bank);
    }

    @Bean(destroyMethod = "close")
    BrokerGateway gateway(BrokerTlsTestSupport.Simulator tls, OrbitaProperties properties)
        throws Exception {
      return BrokerTlsTestSupport.gateway(
          tls.port(),
          Clock.systemUTC(),
          BrokerTlsTestSupport.NO_EVENTS,
          BrokerQualifications.unconfirmed(),
          "source",
          properties.mirrorAccount());
    }
  }

  @Test
  void teardownRefusesUncapturedOwnershipAndBecomesANoOpAfterSuccessfulRemoval() {
    Flyway.configure()
        .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
        .load()
        .migrate();
    var database =
        new JdbcTemplate(
            new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    String control =
        database.queryForObject("select to_jsonb(c)::text from control_state c", String.class);
    String credential =
        database.queryForObject("select to_jsonb(c)::text from owner_credential c", String.class);
    var ledger = InterfaceDemoLedger.begin(database, false);
    assertThatThrownBy(ledger::cleanup)
        .isInstanceOf(IllegalStateException.class)
        .hasCauseInstanceOf(java.sql.SQLException.class)
        .cause()
        .hasMessageContaining("Demo ownership must be captured");
    assertThat(
            database.queryForObject(
                "select to_regnamespace('orbita_demo_preview') is not null", Boolean.class))
        .isTrue();
    assertThatThrownBy(() -> InterfaceDemoLedger.begin(database, false))
        .hasMessageContaining("Remove before regeneration");
    ledger.captureOwnedFacts();
    ledger.restoreControl();
    ledger.cleanup();
    ledger.cleanup();
    assertThat(
            database.queryForObject(
                "select to_regnamespace('orbita_demo_preview') is null", Boolean.class))
        .isTrue();
    assertThat(
            database.queryForObject("select to_jsonb(c)::text from control_state c", String.class))
        .isEqualTo(control);
    assertThat(
            database.queryForObject(
                "select to_jsonb(c)::text from owner_credential c", String.class))
        .isEqualTo(credential);
  }
}
