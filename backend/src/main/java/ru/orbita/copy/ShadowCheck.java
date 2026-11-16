package ru.orbita.copy;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "shadow_check")
public class ShadowCheck {
  @Id UUID id;

  @Column(nullable = false)
  String executionId;

  @Column(nullable = false)
  String algorithmVersion;

  @Column(nullable = false)
  String side;

  @Column(nullable = false)
  Instant checkedAt;

  boolean executionMatches;
  boolean calculationMatches;

  @Column(nullable = false, columnDefinition = "text")
  String detail;

  @Column(columnDefinition = "text")
  String independentHistory;

  @Column(precision = 150, scale = 100)
  java.math.BigDecimal replayContribution;

  Long replayNewLots;

  protected ShadowCheck() {}

  public record View(
      UUID id,
      String executionId,
      String algorithmVersion,
      String side,
      Instant checkedAt,
      boolean executionMatches,
      boolean calculationMatches,
      String detail,
      java.math.BigDecimal replayContribution,
      Long replayNewLots) {}

  public View view() {
    return new View(
        id,
        executionId,
        algorithmVersion,
        side,
        checkedAt,
        executionMatches,
        calculationMatches,
        detail,
        replayContribution,
        replayNewLots);
  }
}
