package ru.orbita.control;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Host qualification supplies a current bounded UTC observation, including sleep/clock-step
 * invalidation.
 */
public interface ClockEvidence {
  record Bound(Instant earliest, Instant latest, String evidenceReference) {
    public Bound {
      if (earliest == null
          || latest == null
          || latest.isBefore(earliest)
          || evidenceReference == null
          || evidenceReference.isBlank())
        throw new IllegalArgumentException("Incomplete UTC evidence");
    }

    public Duration uncertainty() {
      return Duration.between(earliest, latest).dividedBy(2);
    }
  }

  Optional<Bound> current();

  /** Historical UTC is qualified only over a proven uninterrupted monotonic interval. */
  default Optional<Bound> atReceipt(long receivedMonotonicNanos) {
    return Optional.empty();
  }
}
