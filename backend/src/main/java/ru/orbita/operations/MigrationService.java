package ru.orbita.operations;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.control.OrbitaProperties;
import ru.orbita.copy.AcceptanceService;

/** R127: independently read the saved execution and compare a journal prefix without trading. */
@Service
public class MigrationService {
  private final JdbcTemplate db;
  private final AcceptanceService acceptance;
  private final OrbitaProperties properties;
  private final Clock clock;
  private final TransactionTemplate snapshot;

  public MigrationService(
      JdbcTemplate db,
      AcceptanceService acceptance,
      OrbitaProperties properties,
      Clock clock,
      PlatformTransactionManager manager) {
    this.db = db;
    this.acceptance = acceptance;
    this.properties = properties;
    this.clock = clock;
    this.snapshot = new TransactionTemplate(manager);
    snapshot.setReadOnly(true);
    snapshot.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
  }

  public record Manifest(
      int formatVersion,
      String executionId,
      String algorithmVersion,
      long journalBoundary,
      String journalHash,
      String settingsHash,
      String executionHash,
      String calculationHash,
      Instant recordedAt) {}

  public record Verification(
      UUID id,
      Instant checkedAt,
      String manifestHash,
      boolean journalMatches,
      boolean settingsMatches,
      boolean executionMatches,
      boolean calculationMatches,
      String detail) {
    public boolean matches() {
      return journalMatches && settingsMatches && executionMatches && calculationMatches;
    }
  }

  public Manifest exportManifest(String executionId) {
    if (!Boolean.TRUE.equals(
        db.queryForObject("select live_accepted from control_state where id=1", Boolean.class)))
      throw new IllegalStateException("Сначала требуется живая приёмка на ПК");
    var replay = acceptance.replayVerified(executionId);
    if (!replay.executionMatches() || !replay.calculationMatches())
      throw new IllegalStateException("История и теневой расчёт не подтверждены");
    return snapshot.execute(
        status -> {
          long boundary =
              db.queryForObject("select coalesce(max(id),0) from journal_entry", Long.class);
          return new Manifest(
              1,
              replay.executionId(),
              replay.algorithmVersion(),
              boundary,
              journalHash(boundary),
              settingsHash(),
              executionHash(replay),
              calculationHash(replay),
              clock.instant());
        });
  }

  public Verification compare(Manifest source) {
    validate(source);
    AcceptanceService.Replay replay;
    try {
      replay = acceptance.replayVerified(source.executionId());
    } catch (RuntimeException failure) {
      replay = null;
    }
    final var independentlyRead = replay;
    var proof =
        snapshot.execute(
            status -> {
              boolean journal = journalHash(source.journalBoundary()).equals(source.journalHash());
              boolean settings = settingsHash().equals(source.settingsHash());
              boolean execution =
                  independentlyRead != null
                      && independentlyRead.executionMatches()
                      && executionHash(independentlyRead).equals(source.executionHash());
              boolean calculation =
                  independentlyRead != null
                      && independentlyRead.calculationMatches()
                      && independentlyRead.algorithmVersion().equals(source.algorithmVersion())
                      && calculationHash(independentlyRead).equals(source.calculationHash());
              String detail =
                  "Журнал до записи "
                      + source.journalBoundary()
                      + ": "
                      + result(journal)
                      + ". Настройки: "
                      + result(settings)
                      + ". Исполнение по независимой истории брокера: "
                      + result(execution)
                      + ". Теневой расчёт той же версии: "
                      + result(calculation)
                      + ". Торговые действия не выполнялись.";
              return new Verification(
                  UUID.randomUUID(),
                  clock.instant(),
                  fingerprint(
                      source.formatVersion(),
                      source.executionId(),
                      source.algorithmVersion(),
                      source.journalBoundary(),
                      source.journalHash(),
                      source.settingsHash(),
                      source.executionHash(),
                      source.calculationHash(),
                      source.recordedAt()),
                  journal,
                  settings,
                  execution,
                  calculation,
                  detail);
            });
    Objects.requireNonNull(proof);
    db.update(
        "insert into migration_verification(id,checked_at,manifest_hash,execution_id,journal_matches,settings_match,execution_matches,calculation_matches,detail) values(?,?,?,?,?,?,?,?,?)",
        proof.id(),
        Timestamp.from(proof.checkedAt()),
        proof.manifestHash(),
        source.executionId(),
        proof.journalMatches(),
        proof.settingsMatches(),
        proof.executionMatches(),
        proof.calculationMatches(),
        proof.detail());
    return proof;
  }

  public List<Verification> verifications() {
    return db.query(
        "select id,checked_at,manifest_hash,journal_matches,settings_match,execution_matches,calculation_matches,detail from migration_verification order by checked_at desc,id desc limit 100",
        (rs, n) ->
            new Verification(
                rs.getObject(1, UUID.class),
                rs.getTimestamp(2).toInstant(),
                rs.getString(3),
                rs.getBoolean(4),
                rs.getBoolean(5),
                rs.getBoolean(6),
                rs.getBoolean(7),
                rs.getString(8)));
  }

  private String journalHash(long boundary) {
    return db.query(
        "select id,occurred_at,category,instrument,detail from journal_entry where id<=? order by id",
        rs -> {
          var hash = sha256();
          while (rs.next())
            for (Object field :
                new Object[] {
                  rs.getLong(1),
                  rs.getTimestamp(2).toInstant(),
                  rs.getString(3),
                  rs.getString(4),
                  rs.getString(5)
                }) update(hash, field);
          return HexFormat.of().formatHex(hash.digest());
        },
        boundary);
  }

  private String settingsHash() {
    return fingerprint(
        properties.sourceAccount(),
        properties.mirrorAccount(),
        properties.clockUncertaintyMillis(),
        properties.marginDisabledConfirmed());
  }

  private static String executionHash(AcceptanceService.Replay replay) {
    return fingerprint(
        replay.accountId(),
        replay.tradeId(),
        replay.sourceOrderId(),
        replay.instrumentId(),
        replay.side(),
        number(replay.sourceLots()),
        number(replay.price()),
        replay.currency().toUpperCase(Locale.ROOT),
        replay.executedAt());
  }

  private static String calculationHash(AcceptanceService.Replay replay) {
    return fingerprint(
        number(replay.coefficient()),
        replay.ratioNumerator(),
        replay.ratioDenominator(),
        number(replay.contribution()),
        replay.newLots(),
        replay.algorithmVersion());
  }

  private static String number(BigDecimal value) {
    return value.stripTrailingZeros().toPlainString();
  }

  private static String result(boolean matches) {
    return matches ? "совпадает" : "не подтверждено";
  }

  private static String fingerprint(Object... fields) {
    var hash = sha256();
    for (var field : fields) update(hash, field);
    return HexFormat.of().formatHex(hash.digest());
  }

  private static void update(MessageDigest hash, Object field) {
    if (field == null) {
      hash.update((byte) 0);
      return;
    }
    hash.update((byte) 1);
    byte[] text = field.toString().getBytes(StandardCharsets.UTF_8);
    hash.update(ByteBuffer.allocate(4).putInt(text.length).array());
    hash.update(text);
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static void validate(Manifest source) {
    if (source == null
        || source.formatVersion() != 1
        || source.executionId() == null
        || source.executionId().isBlank()
        || source.executionId().length() > 500
        || source.algorithmVersion() == null
        || !source.algorithmVersion().matches("[0-9a-f]{64}")
        || source.journalBoundary() < 0
        || source.recordedAt() == null)
      throw new IllegalArgumentException("Некорректный протокол переноса");
    for (String hash :
        List.of(
            Objects.requireNonNullElse(source.journalHash(), ""),
            Objects.requireNonNullElse(source.settingsHash(), ""),
            Objects.requireNonNullElse(source.executionHash(), ""),
            Objects.requireNonNullElse(source.calculationHash(), "")))
      if (!hash.matches("[0-9a-f]{64}"))
        throw new IllegalArgumentException("Некорректный отпечаток протокола");
  }
}
