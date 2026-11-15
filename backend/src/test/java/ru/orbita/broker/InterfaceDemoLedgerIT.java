package ru.orbita.broker;

import static org.assertj.core.api.Assertions.*;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Exercises the shipped teardown guards; complete domain generation is InterfaceDemoIT's job. */
@Testcontainers
class InterfaceDemoLedgerIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine");

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
