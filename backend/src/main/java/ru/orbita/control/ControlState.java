package ru.orbita.control;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

@Entity
@Table(name = "control_state")
public class ControlState {
  @Id int id = 1;
  @Version long version;

  @Column(nullable = false)
  String mode = "NOT_STARTED";

  @Column(precision = 100, scale = 50)
  BigDecimal coefficient;

  @Column(precision = 200, scale = 0)
  java.math.BigInteger ratioNumerator;

  @Column(precision = 200, scale = 0)
  java.math.BigInteger ratioDenominator;

  Instant observationBoundary;
  Instant resumeBoundary;
  boolean observationBoundaryTrusted;
  boolean resumeBoundaryTrusted;
  Instant automaticBoundary;

  @Column(precision = 38, scale = 9)
  BigDecimal permitLimit;

  Instant permitIssuedAt;
  UUID permitId;
  long permitEpoch;
  boolean observationActivated;
  UUID activeCapitalCandidateId;
  boolean firstAcceptance = true;
  boolean liveAccepted;
  boolean reconciled;
  boolean feedReady;
  String shadowVersion;
  boolean shadowBuyChecked;
  boolean shadowSellChecked;
  boolean recalculationPaused;
  UUID attemptId;
  String attemptKind;
  long generation;
  LocalDate completedDue;
  Instant retryAt;
  String lastError;
  Instant pauseStartedAt;
  UUID recalculationErrorNotificationId;
  Long recalculationPauseJournalBoundary;
  int failedAttempts;

  protected ControlState() {}
}
