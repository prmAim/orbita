package ru.orbita.copy;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "copy_intent")
public class CopyIntent {
  @Id UUID id;
  @Version long version;

  @Column(nullable = false)
  String executionId;

  @Column(nullable = false)
  String instrumentId;

  @Column(nullable = false)
  String sourceOrderId;

  @Column(nullable = false)
  String side;

  @Column(nullable = false)
  String currency;

  @Column(nullable = false)
  long lots;

  @Column(nullable = false, precision = 38, scale = 9)
  BigDecimal referencePrice;

  @Column(nullable = false)
  Instant executedAt;

  @Column(nullable = false)
  Instant createdAt;

  @Column(nullable = false)
  String status = "WAITING";

  @Column(precision = 38, scale = 9)
  BigDecimal limitPrice;

  @Column(precision = 38, scale = 9)
  BigDecimal reservedMoney = BigDecimal.ZERO;

  @Column(precision = 38, scale = 9)
  BigDecimal commissionReserve = BigDecimal.ZERO;

  long reservedLots;
  long executedLots;
  long skippedLots;
  String brokerOrderId;
  Instant sentAt;
  Instant deadline;
  boolean deadlineReliable;
  String reason;
  boolean fullClose;
  String recoveryFirstBuy;
  String recoverySecondBuy;
  long recoveryFirstTarget;
  long recoverySecondTarget;
  boolean acceptanceAttempt;
  long requestedLots;
  long prioritySequence;
  boolean cancellationPending;
  java.util.UUID unknownNotification;

  @Column(precision = 38, scale = 9)
  BigDecimal permittedRubLimit;

  @Column(precision = 38, scale = 9)
  BigDecimal firstBestOpposite;

  UUID retryOf;
  UUID consumedPermitId;
  long consumedPermitEpoch;
  long controlGeneration;
  Instant permittedIssuedAt;

  @Column(precision = 38, scale = 9)
  BigDecimal actualCommission;

  protected CopyIntent() {}

  static CopyIntent fromExecution(SourceExecutionEntity event, long lots, Instant now) {
    if (lots <= 0) throw new IllegalArgumentException("An intent requires positive lots");
    var intent = new CopyIntent();
    intent.id = UUID.randomUUID();
    intent.executionId = event.id;
    intent.instrumentId = event.instrumentId;
    intent.sourceOrderId = event.sourceOrderId;
    intent.side = event.side;
    intent.currency = event.currency;
    intent.lots = lots;
    intent.referencePrice = event.price;
    intent.executedAt = event.executedAt;
    intent.createdAt = now;
    intent.prioritySequence = event.receivedSequence;
    return intent;
  }

  public record View(
      UUID id,
      String instrumentId,
      String sourceOrderId,
      String side,
      String currency,
      long lots,
      long executedLots,
      long skippedLots,
      String status,
      BigDecimal limitPrice,
      BigDecimal reservedMoney,
      Instant deadline,
      String reason,
      boolean deadlineReliable) {}

  public View view() {
    return new View(
        id,
        instrumentId,
        sourceOrderId,
        side,
        currency,
        lots,
        executedLots,
        skippedLots,
        status,
        limitPrice,
        reservedMoney,
        deadline,
        reason,
        deadlineReliable);
  }
}
