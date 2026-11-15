package ru.orbita.operations;

import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/** PostgreSQL snapshot export, restore verification, and independent USB transfer retries. */
@Service
public class BackupService {
  private static final ZoneId MOSCOW = ZoneId.of("Europe/Moscow");
  private final BackupSettings settings;
  private final JdbcTemplate db;
  private final Clock clock;
  private final Environment environment;

  public BackupService(
      BackupSettings settings, JdbcTemplate db, Clock clock, Environment environment) {
    this.settings = settings;
    this.db = db;
    this.clock = clock;
    this.environment = environment;
  }

  public record View(
      LocalDate month,
      String status,
      Instant createdAt,
      Instant verifiedAt,
      Instant transferredAt,
      String lastError) {}

  public List<View> list() {
    return db.query(
        "select month,status,created_at,verified_at,transferred_at,last_error from backup_version order by month desc",
        (rs, n) ->
            new View(
                rs.getDate(1).toLocalDate(),
                rs.getString(2),
                time(rs, 3),
                time(rs, 4),
                time(rs, 5),
                rs.getString(6)));
  }

  public void tick() {
    if (!settings.enabled()) return;
    validateSettings();
    Instant now = clock.instant();
    YearMonth current = YearMonth.from(now.atZone(MOSCOW));
    Instant due = current.atDay(1).atTime(3, 0).atZone(MOSCOW).toInstant();
    if (!now.isBefore(due)) {
      db.update(
          "insert into backup_version(month,status,next_attempt_at) values(?,'WAITING',?) on conflict do nothing",
          java.sql.Date.valueOf(current.atDay(1)),
          Timestamp.from(now));
      var pending =
          db.query(
              "select month from backup_version where month=? and verified_at is null and (next_attempt_at is null or next_attempt_at<=?)",
              (rs, n) -> rs.getDate(1).toLocalDate(),
              java.sql.Date.valueOf(current.atDay(1)),
              Timestamp.from(now));
      if (!pending.isEmpty()) create(current);
    }
    transfer();
    retain(current);
  }

  private void validateSettings() {
    if (settings.localDirectory() == null
        || settings.usbDirectory() == null
        || settings.database() == null
        || settings.host() == null
        || settings.username() == null
        || settings.port() < 1)
      throw new IllegalStateException("Не настроены параметры резервного копирования");
  }

  private void create(YearMonth month) {
    Path file = settings.localDirectory().resolve(name(month));
    Path existingUsb = settings.usbDirectory().resolve(name(month));
    if (Files.exists(file)
        || Files.exists(existingUsb)
        || Files.exists(existingUsb.resolveSibling(existingUsb.getFileName() + ".sha256"))) {
      db.update(
          "update backup_version set status='EXISTING_COPY_UNCONFIRMED',next_attempt_at=?,last_error=? where month=?",
          Timestamp.from(clock.instant().plusSeconds(3600)),
          "Месячная версия уже существует. Автоматическое создание остановлено до ручной сверки архива и состояния после восстановления.",
          java.sql.Date.valueOf(month.atDay(1)));
      return;
    }
    Path temporary = settings.localDirectory().resolve(name(month) + ".partial");
    String verification = "orbita_verify_" + UUID.randomUUID().toString().replace("-", "");
    boolean created = false;
    try {
      Files.createDirectories(settings.localDirectory());
      db.update(
          "update backup_version set status='CREATING',last_error=null where month=?",
          java.sql.Date.valueOf(month.atDay(1)));
      Map<String, String> expected;
      try (Connection source = connection(settings.database())) {
        source.setAutoCommit(false);
        source.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        source.setReadOnly(true);
        String snapshot;
        try (var statement = source.createStatement();
            var result = statement.executeQuery("select pg_export_snapshot()")) {
          result.next();
          snapshot = result.getString(1);
        }
        expected = tableFingerprints(source);
        run(
            List.of(
                "pg_dump",
                "--format=custom",
                "--no-owner",
                "--no-acl",
                "--snapshot=" + snapshot,
                "--file=" + temporary,
                "--dbname=" + settings.database()));
        source.commit();
      }
      run(List.of("createdb", verification));
      created = true;
      run(
          List.of(
              "pg_restore",
              "--exit-on-error",
              "--no-owner",
              "--no-acl",
              "--dbname=" + verification,
              temporary.toString()));
      try (Connection restored = connection(verification)) {
        restored.setAutoCommit(false);
        restored.setReadOnly(true);
        if (!expected.equals(tableFingerprints(restored)))
          throw new IOException("Восстановленная копия не соответствует снимку");
        try (var statement = restored.createStatement();
            var integrity =
                statement.executeQuery(
                    "select count(*) from notification_attempt a left join notification n on a.notification_id=n.id where n.id is null")) {
          integrity.next();
          if (integrity.getLong(1) != 0) throw new IOException("Нарушена целостность очереди");
        }
      }
      Files.move(
          temporary, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      Instant completed = clock.instant();
      db.update(
          "update backup_version set status='VERIFIED',created_at=?,verified_at=?,local_path=?,sha256=?,next_attempt_at=?,last_error=null where month=?",
          Timestamp.from(completed),
          Timestamp.from(completed),
          file.toAbsolutePath().toString(),
          digest(file),
          Timestamp.from(completed),
          java.sql.Date.valueOf(month.atDay(1)));
    } catch (Exception failure) {
      db.update(
          "update backup_version set status='CREATE_FAILED',next_attempt_at=?,last_error=? where month=?",
          Timestamp.from(clock.instant().plusSeconds(3600)),
          "Не удалось создать и проверить копию: " + failure.getClass().getSimpleName(),
          java.sql.Date.valueOf(month.atDay(1)));
    } finally {
      if (created)
        try {
          run(List.of("dropdb", "--if-exists", verification));
        } catch (Exception ignored) {
          /* Isolated verification DB may be removed by the operator; never target the source. */
        }
      try {
        Files.deleteIfExists(temporary);
      } catch (IOException ignored) {
        /* The next attempt replaces only this month's partial file. */
      }
    }
  }

  private void transfer() {
    var pending =
        db.query(
            "select month,local_path,sha256 from backup_version where verified_at is not null and transferred_at is null and (next_attempt_at is null or next_attempt_at<=?) order by month desc",
            (rs, n) ->
                new Pending(rs.getDate(1).toLocalDate(), Path.of(rs.getString(2)), rs.getString(3)),
            Timestamp.from(clock.instant()));
    for (var version : pending) {
      Path target = settings.usbDirectory().resolve(name(YearMonth.from(version.month())));
      Path partial = target.resolveSibling(target.getFileName() + ".partial");
      try {
        if (!Files.isDirectory(settings.usbDirectory()))
          throw new IOException("Выбранная USB-папка недоступна");
        if (!digest(version.file()).equals(version.sha()))
          throw new IOException("Изменилась проверенная локальная копия");
        Files.copy(version.file(), partial, StandardCopyOption.REPLACE_EXISTING);
        if (!digest(partial).equals(version.sha()))
          throw new IOException("Копия на USB не совпадает");
        Files.move(
            partial, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        Files.writeString(
            target.resolveSibling(target.getFileName() + ".sha256"),
            version.sha() + "\n",
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING);
        db.update(
            "update backup_version set status='TRANSFERRED',transferred_at=?,next_attempt_at=null,last_error=null where month=?",
            Timestamp.from(clock.instant()),
            java.sql.Date.valueOf(version.month()));
      } catch (Exception failure) {
        db.update(
            "update backup_version set status='WAITING_TRANSFER',next_attempt_at=?,last_error=? where month=?",
            Timestamp.from(clock.instant().plusSeconds(3600)),
            "Копия создана и проверена, ожидает переноса",
            java.sql.Date.valueOf(version.month()));
      }
    }
  }

  private void retain(YearMonth month) {
    LocalDate earliest = month.minusMonths(2).atDay(1);
    for (Path directory : List.of(settings.localDirectory(), settings.usbDirectory())) {
      if (!Files.isDirectory(directory)) continue;
      try (var paths = Files.list(directory)) {
        for (var file : paths.toList()) {
          String filename = file.getFileName().toString();
          if (!filename.matches("orbita-\\d{4}-\\d{2}\\.dump(?:\\.sha256)?")) continue;
          LocalDate created = YearMonth.parse(filename.substring(7, 14)).atDay(1);
          if (created.isBefore(earliest)) Files.delete(file);
        }
      } catch (IOException failure) {
        /* Retention retries with the next tick; no unrelated file is removed. */
      }
    }
    db.update("delete from backup_version where month<?", java.sql.Date.valueOf(earliest));
  }

  private Connection connection(String database) throws SQLException {
    return DriverManager.getConnection(
        "jdbc:postgresql://" + settings.host() + ":" + settings.port() + "/" + database,
        settings.username(),
        environment.getProperty("spring.datasource.password", ""));
  }

  private void run(List<String> command) throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder(command);
    builder
        .environment()
        .putAll(
            Map.of(
                "PGHOST",
                settings.host(),
                "PGPORT",
                Integer.toString(settings.port()),
                "PGUSER",
                settings.username(),
                "PGPASSWORD",
                environment.getProperty("spring.datasource.password", "")));
    builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
    builder.redirectError(ProcessBuilder.Redirect.DISCARD);
    Process process = builder.start();
    if (!process.waitFor(5, TimeUnit.MINUTES)) {
      process.destroyForcibly();
      throw new IOException("Истекло время PostgreSQL backup");
    }
    if (process.exitValue() != 0)
      throw new IOException(
          "Этап " + command.getFirst() + " завершился с кодом " + process.exitValue());
  }

  private static Map<String, String> tableFingerprints(Connection connection) throws Exception {
    Map<String, String> fingerprints = new TreeMap<>();
    List<String> tables = new ArrayList<>();
    try (var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "select tablename from pg_tables where schemaname='public' order by tablename")) {
      while (rows.next()) tables.add(rows.getString(1));
    }
    for (String table : tables) {
      if (!table.matches("[a-zA-Z_][a-zA-Z_0-9]*"))
        throw new SQLException("Unsupported table identifier");
      var hash = MessageDigest.getInstance("SHA-256");
      long count = 0;
      try (var statement = connection.createStatement()) {
        statement.setFetchSize(1000);
        try (var rows =
            statement.executeQuery(
                "select row_to_json(t)::text as row_data from public.\""
                    + table
                    + "\" t order by row_data")) {
          while (rows.next()) {
            hash.update(rows.getString(1).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            hash.update((byte) '\n');
            count++;
          }
        }
      }
      fingerprints.put(table, count + ":" + HexFormat.of().formatHex(hash.digest()));
    }
    return fingerprints;
  }

  private static String digest(Path path) throws Exception {
    var hash = MessageDigest.getInstance("SHA-256");
    try (var stream = Files.newInputStream(path)) {
      byte[] buffer = new byte[65536];
      int count;
      while ((count = stream.read(buffer)) >= 0) hash.update(buffer, 0, count);
    }
    return HexFormat.of().formatHex(hash.digest());
  }

  private static String name(YearMonth month) {
    return "orbita-" + month + ".dump";
  }

  private static Instant time(ResultSet rs, int column) throws SQLException {
    var time = rs.getTimestamp(column);
    return time == null ? null : time.toInstant();
  }

  private record Pending(LocalDate month, Path file, String sha) {}
}
