package ru.orbita.copy;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(
    name = "source_execution",
    uniqueConstraints = @UniqueConstraint(columnNames = {"account_id", "trade_id"}))
public class SourceExecutionEntity {
  @Id String id;

  @Column(nullable = false)
  String accountId;

  @Column(nullable = false)
  String tradeId;

  @Column(nullable = false)
  String sourceOrderId;

  @Column(nullable = false)
  String instrumentId;

  @Column(nullable = false)
  String side;

  @Column(nullable = false, precision = 38, scale = 9)
  BigDecimal lots;

  @Column(nullable = false, precision = 38, scale = 9)
  BigDecimal price;

  @Column(nullable = false)
  String currency;

  Instant executedAt;

  @Column(nullable = false)
  Instant receivedAt;

  @Column(precision = 100, scale = 50)
  BigDecimal coefficient;

  @Column(precision = 200, scale = 0)
  java.math.BigInteger ratioNumerator;

  @Column(precision = 200, scale = 0)
  java.math.BigInteger ratioDenominator;

  @Column(precision = 160, scale = 100)
  BigDecimal contribution;

  @Column(nullable = false)
  String disposition = "RECEIVED";

  long newLots;
  boolean requiresRecovery;
  long acceptedSequence;
  long receivedSequence;

  protected SourceExecutionEntity() {}
}
