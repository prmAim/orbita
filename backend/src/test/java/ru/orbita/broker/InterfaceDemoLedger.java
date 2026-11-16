package ru.orbita.broker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test-only ownership bookkeeping. Domain facts are produced solely by the running application. */
public final class InterfaceDemoLedger {
  private static final Map<String, String> KEYS = new LinkedHashMap<>();

  static {
    KEYS.put("incoming_receipt", "id");
    KEYS.put("source_execution", "id");
    KEYS.put("copy_intent", "id");
    KEYS.put("broker_fill", "id");
    KEYS.put("holding_allocation", "source_execution_id");
    KEYS.put("shadow_check", "id");
    KEYS.put("mirror_position", "instrument_id");
    KEYS.put("instrument_state", "instrument_id");
    KEYS.put("mirror_trade_receipt", "id");
    KEYS.put("capital_candidate", "id");
    KEYS.put("migration_verification", "id");
    KEYS.put("journal_entry", "id");
    KEYS.put("notification", "id");
    KEYS.put("notification_attempt", "id");
    KEYS.put("account_read_document", "account_id,kind,id");
    KEYS.put("account_read_freshness", "account_id,section");
    KEYS.put("account_read_revision", "account_id");
  }

  private final JdbcTemplate database;

  private InterfaceDemoLedger(JdbcTemplate database) {
    this.database = database;
  }

  /** Call before starting the sole harness application, with the owner UI already stopped. */
  public static InterfaceDemoLedger begin(JdbcTemplate database, boolean retained) {
    var ledger = new InterfaceDemoLedger(database);
    ledger.transaction(
        connection -> {
          try (Statement sql = connection.createStatement()) {
            try (ResultSet row =
                sql.executeQuery("select to_regnamespace('orbita_demo_preview') is not null")) {
              row.next();
              if (row.getBoolean(1))
                throw new IllegalStateException("Demo already present: Remove before regeneration");
            }
            try (ResultSet row =
                sql.executeQuery(
                    "select pg_get_userbyid(datdba)=current_user and current_user=session_user from pg_database where datname=current_database()")) {
              row.next();
              if (!row.getBoolean(1))
                throw new IllegalStateException(
                    "Demo requires the database owner without role impersonation");
            }
            try (ResultSet row =
                sql.executeQuery(
                    "select version from public.flyway_schema_history where success order by installed_rank desc limit 1")) {
              if (!row.next() || !"17".equals(row.getString(1)))
                throw new IllegalStateException("Demo requires the qualified schema version 17");
            }
            for (var key : KEYS.entrySet())
              if (readRelation(key.getKey())) {
                try (PreparedStatement check =
                    connection.prepareStatement(
                        "select string_agg(a.attname,',' order by k.ordinality) from pg_index i cross join lateral unnest(i.indkey) with ordinality k(attnum,ordinality) join pg_attribute a on a.attrelid=i.indrelid and a.attnum=k.attnum where i.indrelid=?::regclass and i.indisprimary")) {
                  check.setString(1, "public." + key.getKey());
                  try (ResultSet row = check.executeQuery()) {
                    if (!row.next() || !key.getValue().equals(row.getString(1)))
                      throw new IllegalStateException(
                          "Unexpected read-model primary key: " + key.getKey());
                  }
                }
              }
            try (ResultSet row =
                sql.executeQuery(
                    "select mode='NOT_STARTED' and not live_accepted and not shadow_buy_checked and not shadow_sell_checked and shadow_version is null and automatic_boundary is null and permit_limit is null and permit_issued_at is null and permit_id is null and attempt_id is null and active_capital_candidate_id is null from public.control_state where id=1 for update")) {
              if (!row.next() || !row.getBoolean(1))
                throw new IllegalStateException(
                    "Demo requires a NOT_STARTED baseline without trading qualifications or an active attempt");
            }
            for (String relation : KEYS.keySet()) {
              if (relation.equals("journal_entry")
                  || relation.equals("notification")
                  || relation.equals("notification_attempt")
                  || readRelation(relation)) continue;
              try (ResultSet row =
                  sql.executeQuery("select exists(select 1 from public." + relation + ")")) {
                row.next();
                if (row.getBoolean(1))
                  throw new IllegalStateException(
                      "Demo requires pristine trading facts: " + relation);
              }
            }
            try (ResultSet row =
                sql.executeQuery(
                    "select exists(select 1 from public.notification where status in ('PENDING','SENDING'))")) {
              row.next();
              if (row.getBoolean(1))
                throw new IllegalStateException(
                    "Demo refuses an existing deliverable notification queue");
            }
            sql.execute("create schema orbita_demo_preview");
            sql.execute(
                "create table orbita_demo_preview.batch(singleton boolean primary key check(singleton),version integer not null check(version=2),created_at timestamptz not null default clock_timestamp(),retained boolean not null,captured boolean not null default false,restored_at timestamptz,control_before jsonb not null,connectivity_before jsonb not null)");
            sql.execute(
                "create table orbita_demo_preview.baseline(relation text not null,row_id text not null,row_snapshot jsonb,primary key(relation,row_id))");
            sql.execute(
                "create table orbita_demo_preview.owned(relation text not null,row_id text not null,row_snapshot jsonb,primary key(relation,row_id))");
            sql.execute(
                "create table orbita_demo_preview.instrument(instrument_id text primary key)");
          }
          try (PreparedStatement insert =
              connection.prepareStatement(
                  "insert into orbita_demo_preview.batch(singleton,version,retained,control_before,connectivity_before) select true,2,?,to_jsonb(c),to_jsonb(b) from public.control_state c cross join public.broker_connectivity b where c.id=1 and b.singleton")) {
            insert.setBoolean(1, retained);
            if (insert.executeUpdate() != 1)
              throw new IllegalStateException("Demo baseline singleton is missing");
          }
          try (Statement sql = connection.createStatement()) {
            for (var key : KEYS.entrySet())
              sql.executeUpdate(
                  "insert into orbita_demo_preview.baseline(relation,row_id,row_snapshot) select '"
                      + key.getKey()
                      + "',"
                      + rowKey(key.getKey(), "t")
                      + ","
                      + (readRelation(key.getKey()) ? "to_jsonb(t)" : "null::jsonb")
                      + " from public."
                      + key.getKey()
                      + " t");
          }
        });
    return ledger;
  }

  /** Capture every read-document kind, including CASH_PARTIAL, only after full harness drain. */
  public void captureOwnedFacts() {
    transaction(
        connection -> {
          try (Statement sql = connection.createStatement()) {
            try (ResultSet row =
                sql.executeQuery(
                    "select captured from orbita_demo_preview.batch where singleton for update")) {
              if (!row.next()) throw new IllegalStateException("Demo ledger is missing");
              if (row.getBoolean(1)) return;
            }
            for (var key : KEYS.entrySet())
              sql.executeUpdate(
                  "insert into orbita_demo_preview.owned(relation,row_id,row_snapshot) select '"
                      + key.getKey()
                      + "',"
                      + rowKey(key.getKey(), "t")
                      + ","
                      + (readRelation(key.getKey()) ? "to_jsonb(t)" : "null::jsonb")
                      + " from public."
                      + key.getKey()
                      + " t where not exists(select 1 from orbita_demo_preview.baseline b where b.relation='"
                      + key.getKey()
                      + "' and b.row_id="
                      + rowKey(key.getKey(), "t")
                      + ") on conflict do nothing");
            sql.executeUpdate(
                "insert into orbita_demo_preview.instrument select distinct e.instrument_id from public.source_execution e join orbita_demo_preview.owned o on o.relation='source_execution' and o.row_id=e.id union select p.instrument_id from public.mirror_position p join orbita_demo_preview.owned o on o.relation='mirror_position' and o.row_id=p.instrument_id union select i.instrument_id from public.instrument_state i join orbita_demo_preview.owned o on o.relation='instrument_state' and o.row_id=i.instrument_id on conflict do nothing");
            sql.executeUpdate(
                "insert into orbita_demo_preview.instrument select distinct d.instrument_id from public.account_read_document d join orbita_demo_preview.owned o on o.relation='account_read_document' and o.row_id="
                    + rowKey("account_read_document", "d")
                    + " where d.instrument_id is not null on conflict do nothing");
            sql.executeUpdate("update orbita_demo_preview.batch set captured=true where singleton");
          }
        });
  }

  private static boolean readRelation(String relation) {
    return relation.startsWith("account_read_");
  }

  private static String rowKey(String relation, String alias) {
    String columns =
        java.util.Arrays.stream(KEYS.get(relation).split(","))
            .map(column -> alias + "." + column)
            .collect(java.util.stream.Collectors.joining(","));
    return readRelation(relation) ? "jsonb_build_array(" + columns + ")::text" : columns + "::text";
  }

  /** Teardown only; never changes owner credentials and never replays the snapshot after UI use. */
  public void restoreControl() {
    transaction(
        connection -> {
          try (Statement sql = connection.createStatement();
              ResultSet row =
                  sql.executeQuery(
                      "select captured,restored_at from orbita_demo_preview.batch where singleton for update")) {
            if (!row.next() || !row.getBoolean(1))
              throw new IllegalStateException("Capture demo ownership before restoring control");
            if (row.getObject(2) != null) return;
          }
          restore(connection, "control_state", "control_before", "id=1");
          restore(connection, "broker_connectivity", "connectivity_before", "singleton");
          try (Statement sql = connection.createStatement()) {
            sql.executeUpdate(
                "update orbita_demo_preview.batch set restored_at=clock_timestamp() where singleton");
          }
        });
  }

  public void cleanup() {
    try (Connection connection = database.getDataSource().getConnection();
        Statement sql = connection.createStatement()) {
      sql.execute(
          "select set_config('orbita.demo.expected_database',current_database(),false),set_config('orbita.demo.expected_role',current_user,false),set_config('orbita.demo.offline_confirmed','broker+telegram+worker+backup-disabled;accounts-empty',false)");
      String basedir = System.getProperty("basedir");
      if (basedir == null)
        throw new IllegalStateException("Run the scenario through Maven with its explicit basedir");
      sql.execute(
          Files.readString(Path.of(basedir).resolve("../deploy/demo/cleanup.sql").normalize()));
    } catch (Exception failure) {
      throw new IllegalStateException("Demo cleanup failed", failure);
    }
  }

  /** Executes the shipped Remove artifact without creating or upgrading a manifest. */
  public static void cleanup(JdbcTemplate database) {
    new InterfaceDemoLedger(database).cleanup();
  }

  private static void restore(
      Connection connection, String relation, String snapshot, String predicate)
      throws SQLException {
    String columns;
    try (PreparedStatement query =
        connection.prepareStatement(
            "select string_agg(quote_ident(attname),',' order by attnum) from pg_attribute where attrelid=?::regclass and attnum>0 and not attisdropped")) {
      query.setString(1, "public." + relation);
      try (ResultSet row = query.executeQuery()) {
        row.next();
        columns = row.getString(1);
      }
    }
    try (Statement sql = connection.createStatement()) {
      sql.executeUpdate(
          "update public."
              + relation
              + " set ("
              + columns
              + ")=(select "
              + columns
              + " from jsonb_populate_record(null::public."
              + relation
              + ",(select "
              + snapshot
              + " from orbita_demo_preview.batch where singleton))) where "
              + predicate);
    }
  }

  private void transaction(Operation operation) {
    try (Connection connection = database.getDataSource().getConnection()) {
      connection.setAutoCommit(false);
      try {
        try (Statement sql = connection.createStatement()) {
          sql.execute("set local lock_timeout='5s'");
          sql.execute("select pg_advisory_xact_lock(hashtextextended('orbita.interface-demo',0))");
        }
        operation.run(connection);
        connection.commit();
      } catch (Exception failure) {
        connection.rollback();
        throw failure;
      }
    } catch (Exception failure) {
      throw new IllegalStateException(
          "Demo ownership transaction failed: " + failure.getMessage(), failure);
    }
  }

  @FunctionalInterface
  private interface Operation {
    void run(Connection connection) throws Exception;
  }
}
