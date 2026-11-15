package ru.orbita.copy;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
    name = "broker_fill",
    uniqueConstraints = @UniqueConstraint(columnNames = {"intent_id", "trade_id"}))
public class BrokerFillEntity {
  @Id String id;

  @Column(nullable = false)
  UUID intentId;

  @Column(nullable = false)
  String tradeId;

  @Column(nullable = false, precision = 38, scale = 9)
  BigDecimal lots;

  @Column(nullable = false, precision = 38, scale = 9)
  BigDecimal price;

  @Column(nullable = false)
  String currency;

  @Column(nullable = false)
  Instant executedAt;

  protected BrokerFillEntity() {}
}
