package ru.orbita.operations;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Executes the exact pre-Flyway restore artifact against genuine historical schemas. */
@Testcontainers
class RestoreStateIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine");

  @ParameterizedTest
  @ValueSource(strings = {"4", "15"})
  void restorationInvalidatesAccessPermitsAndConnectivityWithoutAssumingNewColumns(String version)
      throws Exception {
    String database = "restore_state_" + UUID.randomUUID().toString().replace("-", "");
    String url =
        "jdbc:postgresql://"
            + POSTGRES.getHost()
            + ":"
            + POSTGRES.getMappedPort(5432)
            + "/"
            + database;
    try (Connection admin = connection(POSTGRES.getJdbcUrl());
        Statement command = admin.createStatement()) {
      command.execute("create database " + database);
      try {
        Flyway.configure()
            .dataSource(url, POSTGRES.getUsername(), POSTGRES.getPassword())
            .target(version)
            .load()
            .migrate();
        try (Connection restored = connection(url);
            Statement sql = restored.createStatement()) {
          UUID notice = UUID.randomUUID(), attempt = UUID.randomUUID();
          sql.executeUpdate(
              "update control_state set mode='AUTOMATIC',generation=5,reconciled=true,feed_ready=true,automatic_boundary=now(),permit_issued_at=now(),permit_limit=1000");
          sql.executeUpdate(
              "update owner_credential set password_hash='synthetic-only-hash',generation=7");
          long journal;
          try (ResultSet row =
              sql.executeQuery(
                  "insert into journal_entry(occurred_at,category,detail) values(now(),'TEST','Synthetic old message') returning id")) {
            row.next();
            journal = row.getLong(1);
          }
          try (PreparedStatement insert =
              restored.prepareStatement(
                  "insert into notification(id,category,instrument,occurred_at,status,journal_id,next_attempt_at,claim_id,claim_started_at,claim_kind) values(?,'TEST','TEST',now(),'SENDING',?,now(),?,now(),'AUTOMATIC')")) {
            insert.setObject(1, notice);
            insert.setLong(2, journal);
            insert.setObject(3, attempt);
            insert.executeUpdate();
          }
          try (PreparedStatement insert =
              restored.prepareStatement(
                  "insert into notification_attempt(id,notification_id,kind,started_at) values(?,?,'AUTOMATIC',now())")) {
            insert.setObject(1, attempt);
            insert.setObject(2, notice);
            insert.executeUpdate();
          }
          if (version.equals("15")) {
            sql.executeUpdate(
                "update control_state set permit_id=gen_random_uuid(),permit_epoch=9");
            try (PreparedStatement update =
                restored.prepareStatement(
                    "update broker_connectivity set loss_notification_id=?,lost_at=now(),last_recovered_at=now(),restore_session=gen_random_uuid() where singleton")) {
              update.setObject(1, notice);
              update.executeUpdate();
            }
          }
          sql.execute(Files.readString(Path.of("..", "deploy", "restore-state.sql")));
          try (ResultSet row =
              sql.executeQuery(
                  "select mode,generation,reconciled,feed_ready,permit_limit,permit_issued_at,automatic_boundary from control_state where id=1")) {
            row.next();
            assertThat(row.getString(1)).isEqualTo("RESTORED");
            assertThat(row.getLong(2)).isEqualTo(6);
            assertThat(row.getBoolean(3)).isFalse();
            assertThat(row.getBoolean(4)).isFalse();
            assertThat(row.getObject(5)).isNull();
            assertThat(row.getObject(6)).isNull();
            assertThat(row.getObject(7)).isNull();
          }
          try (ResultSet row =
              sql.executeQuery("select password_hash,generation from owner_credential")) {
            row.next();
            assertThat(row.getString(1)).isNull();
            assertThat(row.getLong(2)).isEqualTo(8);
          }
          try (ResultSet row =
              sql.executeQuery("select status,claim_id,next_attempt_at from notification")) {
            row.next();
            assertThat(row.getString(1)).isEqualTo("IGNORED");
            assertThat(row.getObject(2)).isNull();
            assertThat(row.getObject(3)).isNull();
          }
          try (ResultSet row =
              sql.executeQuery("select result,completed_at from notification_attempt")) {
            row.next();
            assertThat(row.getString(1)).isEqualTo("RESTORED_COPY");
            assertThat(row.getObject(2)).isNotNull();
          }
          if (version.equals("15")) {
            try (ResultSet row =
                sql.executeQuery("select permit_id,permit_epoch from control_state")) {
              row.next();
              assertThat(row.getObject(1)).isNull();
              assertThat(row.getLong(2)).isEqualTo(10);
            }
            try (ResultSet row =
                sql.executeQuery(
                    "select loss_notification_id,lost_at,last_recovered_at,restore_session from broker_connectivity")) {
              row.next();
              for (int column = 1; column <= 4; column++)
                assertThat(row.getObject(column)).isNull();
            }
          }
        }
      } finally {
        command.execute("drop database " + database);
      }
    }
  }

  private static Connection connection(String url) throws SQLException {
    return DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword());
  }
}
