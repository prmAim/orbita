package ru.orbita.control;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.BrokerGateway.CapitalEvidence;
import ru.orbita.copy.Rational;

/** Timed durable capital values are distinct from their later accepted reference. */
@Service
public class CapitalCandidateService {
  private final JdbcTemplate database;
  private final Clock clock;
  private final TimeAuthority time;
  private final TransactionTemplate staging;
  private final UUID processId = UUID.randomUUID();

  public CapitalCandidateService(
      JdbcTemplate database, Clock clock, TimeAuthority time, PlatformTransactionManager manager) {
    this.database = database;
    this.clock = clock;
    this.time = time;
    staging = new TransactionTemplate(manager);
    staging.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  public record Values(BigDecimal coefficient, BigInteger numerator, BigInteger denominator) {}

  public record Receipt(
      UUID id,
      Values values,
      long commitAckElapsedNanos,
      Instant acknowledgedAt,
      Instant persistenceEarliest,
      Instant persistenceLatest) {}

  /** This is the only write of the numeric candidate. It never changes the accepted pointer. */
  public Receipt stage(
      ControlService.Attempt attempt, CapitalEvidence source, CapitalEvidence mirror) {
    Rational ratio = Rational.ratio(mirror.totalRub(), source.totalRub());
    Values values = new Values(ratio.decimal(50), ratio.numerator(), ratio.denominator());
    Instant lower =
        staging.execute(
            status -> {
              database.update(
                  "insert into capital_candidate(id,generation,kind,process_id,source_capital,mirror_capital,coefficient,ratio_numerator,ratio_denominator,first_request_at,first_request_nanos,source_reply_nanos,mirror_reply_nanos) values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  attempt.id(),
                  attempt.generation(),
                  attempt.kind(),
                  processId,
                  source.totalRub(),
                  mirror.totalRub(),
                  values.coefficient(),
                  new BigDecimal(values.numerator()),
                  new BigDecimal(values.denominator()),
                  Timestamp.from(attempt.firstRequestAt()),
                  attempt.firstRequestNanos(),
                  attempt.sourceReplyNanos(),
                  attempt.mirrorReplyNanos());
              return time.bound().map(ClockEvidence.Bound::earliest).orElse(null);
            });
    long acknowledgedNanos = System.nanoTime();
    Instant acknowledgedAt = clock.instant();
    Instant upper = time.bound().map(ClockEvidence.Bound::latest).orElse(null);
    return new Receipt(
        attempt.id(),
        values,
        acknowledgedNanos - attempt.firstRequestNanos(),
        acknowledgedAt,
        lower,
        upper);
  }

  @Transactional(propagation = Propagation.MANDATORY)
  public Values accepted(ControlState state) {
    if (state.activeCapitalCandidateId == null)
      return new Values(state.coefficient, state.ratioNumerator, state.ratioDenominator);
    return database
        .query(
            "select coefficient,ratio_numerator,ratio_denominator from capital_candidate where id=? and status='QUALIFIED'",
            (row, index) ->
                new Values(
                    row.getBigDecimal(1),
                    row.getBigDecimal(2).toBigIntegerExact(),
                    row.getBigDecimal(3).toBigIntegerExact()),
            state.activeCapitalCandidateId)
        .stream()
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Принятый коэффициент не подтверждён долговечной записью"));
  }

  /** Qualify only an acknowledged write from this process, in the pointer-promotion transaction. */
  @Transactional(propagation = Propagation.MANDATORY)
  public void qualify(ControlService.Attempt attempt, Receipt receipt) {
    if (receipt.commitAckElapsedNanos() < 0
        || receipt.commitAckElapsedNanos() > java.time.Duration.ofSeconds(5).toNanos())
      throw new IllegalStateException("Запись кандидата не подтверждена за 5 секунд");
    int updated =
        database.update(
            "update capital_candidate set status='QUALIFIED',commit_ack_elapsed_nanos=?,qualified_at=?,persistence_earliest=?,persistence_latest=?,persistence_earliest_seconds=?,persistence_earliest_nano=?,persistence_latest_seconds=?,persistence_latest_nano=? where id=? and generation=? and process_id=? and status='UNACCEPTED'",
            receipt.commitAckElapsedNanos(),
            Timestamp.from(receipt.acknowledgedAt()),
            timestamp(receipt.persistenceEarliest()),
            timestamp(receipt.persistenceLatest()),
            seconds(receipt.persistenceEarliest()),
            nanos(receipt.persistenceEarliest()),
            seconds(receipt.persistenceLatest()),
            nanos(receipt.persistenceLatest()),
            receipt.id(),
            attempt.generation(),
            processId);
    if (updated != 1)
      throw new IllegalStateException(
          "Кандидат коэффициента уже отклонён или относится к другой попытке");
  }

  @Transactional(propagation = Propagation.MANDATORY)
  public void reject(UUID id, String reason) {
    database.update(
        "update capital_candidate set status='REJECTED',qualification_error=? where id=? and status='UNACCEPTED'",
        reason,
        id);
  }

  @Transactional(propagation = Propagation.MANDATORY)
  public void reject(Receipt receipt, String reason) {
    database.update(
        "update capital_candidate set status='REJECTED',qualification_error=?,commit_ack_elapsed_nanos=?,persistence_earliest=?,persistence_latest=?,persistence_earliest_seconds=?,persistence_earliest_nano=?,persistence_latest_seconds=?,persistence_latest_nano=? where id=? and status='UNACCEPTED'",
        reason,
        receipt.commitAckElapsedNanos(),
        timestamp(receipt.persistenceEarliest()),
        timestamp(receipt.persistenceLatest()),
        seconds(receipt.persistenceEarliest()),
        nanos(receipt.persistenceEarliest()),
        seconds(receipt.persistenceLatest()),
        nanos(receipt.persistenceLatest()),
        receipt.id());
  }

  @Transactional(propagation = Propagation.MANDATORY)
  public void rejectPending() {
    database.update(
        "update capital_candidate set status='REJECTED',qualification_error='Перезапуск до подтверждения своевременности долговечной записи' where status='UNACCEPTED'");
  }

  private static Timestamp timestamp(Instant value) {
    return value == null ? null : Timestamp.from(value);
  }

  private static Long seconds(Instant value) {
    return value == null ? null : value.getEpochSecond();
  }

  private static Integer nanos(Instant value) {
    return value == null ? null : value.getNano();
  }
}
