package ru.orbita.copy;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "mirror_trade_receipt")
public class MirrorTradeReceipt {
  @Id String id;

  @Column(nullable = false)
  String instrumentId;

  @Column(nullable = false)
  String side;

  @Column(nullable = false)
  long lots;

  @Column(nullable = false)
  Instant executedAt;

  long manualLotsApplied;
  long copiedLotsApplied;
  String sourceCycleId;

  protected MirrorTradeReceipt() {}
}
