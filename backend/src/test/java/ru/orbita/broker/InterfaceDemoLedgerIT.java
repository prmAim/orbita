package ru.orbita.broker;

import static org.assertj.core.api.Assertions.*;

import java.time.Clock;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
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
