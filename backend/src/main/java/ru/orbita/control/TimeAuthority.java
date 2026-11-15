package ru.orbita.control;

import java.time.*;
import java.util.Optional;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * A configured number is not evidence of host time accuracy. Missing evidence always blocks new
 * orders.
 */
@Component
public class TimeAuthority {
  private final ObjectProvider<ClockEvidence> evidence;
  private final Clock clock;

  public TimeAuthority(ObjectProvider<ClockEvidence> evidence, Clock clock) {
    this.evidence = evidence;
    this.clock = clock;
  }

  public Duration uncertainty() {
    var provider = evidence.getIfAvailable();
    if (provider == null) return null;
    var value = provider.current();
    if (value.isEmpty()) return null;
    var bound = value.get();
    Instant now = clock.instant();
    Duration left = Duration.between(bound.earliest(), now).abs(),
        right = Duration.between(now, bound.latest()).abs();
    return left.compareTo(right) > 0 ? left : right;
  }

  public boolean trusted() {
    Duration uncertainty = uncertainty();
    return uncertainty != null && uncertainty.compareTo(Duration.ofMillis(500)) <= 0;
  }

  public Optional<ClockEvidence.Bound> bound() {
    var provider = evidence.getIfAvailable();
    if (provider == null) return Optional.empty();
    var result = provider.current();
    if (result.isEmpty()) return result;
    var value = result.get();
    Instant now = clock.instant();
    if (Duration.between(value.earliest(), now).abs().compareTo(Duration.ofMillis(500)) > 0
        || Duration.between(now, value.latest()).abs().compareTo(Duration.ofMillis(500)) > 0)
      return Optional.empty();
    return result;
  }

  public Optional<ClockEvidence.Bound> boundAtReceipt(
      long receivedMonotonicNanos, String receiptClockDomain) {
    if (!ru.orbita.broker.BrokerReceiptClock.domain().equals(receiptClockDomain))
      return Optional.empty();
    var provider = evidence.getIfAvailable();
    if (provider == null) return Optional.empty();
    try {
      if (Math.subtractExact(ru.orbita.broker.BrokerReceiptClock.nanoTime(), receivedMonotonicNanos)
          < 0) return Optional.empty();
      return provider
          .atReceipt(receivedMonotonicNanos)
          .filter(
              bound ->
                  Duration.between(bound.earliest(), bound.latest())
                          .compareTo(Duration.ofSeconds(1))
                      <= 0);
    } catch (ArithmeticException invalidInterval) {
      return Optional.empty();
    }
  }
}
