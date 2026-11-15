package ru.orbita.copy;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "mirror_position")
public class MirrorPosition {
  @Id String instrumentId;
  @Version long version;
  long copiedLots;
  long manualLots;

  @Column(precision = 38, scale = 9)
  BigDecimal brokerLots;

  Instant brokerReadAt;
  boolean brokerInventoryQualified;

  protected MirrorPosition() {}

  MirrorPosition(String id) {
    instrumentId = id;
  }

  public record View(
      String instrumentId,
      long copiedLots,
      long manualLots,
      @com.fasterxml.jackson.annotation.JsonFormat(
              shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
          BigDecimal brokerLots,
      Instant brokerReadAt,
      boolean brokerInventoryQualified) {}

  public View view() {
    return new View(
        instrumentId, copiedLots, manualLots, brokerLots, brokerReadAt, brokerInventoryQualified);
  }
}
