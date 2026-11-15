package ru.orbita.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import ru.orbita.control.ClockEvidence;
import ru.orbita.control.TimeAuthority;
import ru.orbita.copy.TradePolicy;

class QualifiedClockEvidenceFileTest {
  @TempDir Path directory;
  private final MutableClock clock = new MutableClock();
  private final AtomicLong nanos = new AtomicLong(1_000_000_000L);
  private final UUID session = UUID.randomUUID();

  @Test
  void executionAgeGateRequiresFreshProcessBoundQualifiedEvidence() throws Exception {
    Path file = directory.resolve("clock.properties");
    var provider =
        new QualifiedClockEvidenceFile(
            file, "host-1", "collector-1", clock, nanos::get, session, nanos.get());
    var beans = new DefaultListableBeanFactory();
    beans.registerSingleton("clockEvidence", provider);
    var authority = new TimeAuthority(beans.getBeanProvider(ClockEvidence.class), clock);
    Instant tradeTime = clock.instant().minusSeconds(30);
    assertThat(TradePolicy.age(tradeTime, clock.instant(), authority.uncertainty()))
        .isEqualTo(TradePolicy.Age.WAIT_FOR_TIME);
    nanos.incrementAndGet();
    publish(file, 1);
    assertThat(TradePolicy.age(tradeTime, clock.instant(), authority.uncertainty()))
        .isEqualTo(TradePolicy.Age.ALLOWED);
    clock.advance(Duration.ofSeconds(3));
    nanos.addAndGet(3_000_000_000L);
    assertThat(TradePolicy.age(tradeTime, clock.instant(), authority.uncertainty()))
        .isEqualTo(TradePolicy.Age.WAIT_FOR_TIME);
    nanos.incrementAndGet();
    publish(file, 2);
    assertThat(TradePolicy.age(tradeTime, clock.instant(), authority.uncertainty()))
        .isEqualTo(TradePolicy.Age.ALLOWED);
  }

  @Test
  void clockStepAndPollingGapBlockExecutionUntilANewerPostBreakObservation() throws Exception {
    Path file = directory.resolve("clock.properties");
    var provider =
        new QualifiedClockEvidenceFile(
            file, "host-1", "collector-1", clock, nanos::get, session, nanos.get());
    var beans = new DefaultListableBeanFactory();
    beans.registerSingleton("clockEvidence", provider);
    var authority = new TimeAuthority(beans.getBeanProvider(ClockEvidence.class), clock);
    Instant trade = clock.instant().minusSeconds(10);
    nanos.incrementAndGet();
    publish(file, 1);
    assertThat(TradePolicy.age(trade, clock.instant(), authority.uncertainty()))
        .isEqualTo(TradePolicy.Age.ALLOWED);
    clock.advance(Duration.ofMillis(300));
    nanos.addAndGet(100_000_000);
    assertThat(TradePolicy.age(trade, clock.instant(), authority.uncertainty()))
        .isEqualTo(TradePolicy.Age.WAIT_FOR_TIME);
    nanos.incrementAndGet();
    publish(file, 2);
    assertThat(TradePolicy.age(trade, clock.instant(), authority.uncertainty()))
        .isEqualTo(TradePolicy.Age.ALLOWED);
    clock.advance(Duration.ofMillis(1500));
    nanos.addAndGet(1_500_000_000);
    publish(
        file,
        3); // A sample captured before the gap was detected cannot establish restored continuity.
    assertThat(TradePolicy.age(trade, clock.instant(), authority.uncertainty()))
        .isEqualTo(TradePolicy.Age.WAIT_FOR_TIME);
    nanos.incrementAndGet();
    publish(file, 4);
    assertThat(TradePolicy.age(trade, clock.instant(), authority.uncertainty()))
        .isEqualTo(TradePolicy.Age.ALLOWED);
  }

  @Test
  void oldProcessHostMismatchAndUnqualifiedDriftNeverAuthorizeExecution() throws Exception {
    for (String corruption : new String[] {"session", "host", "drift", "duplicate", "missing"}) {
      Path file = directory.resolve(corruption + ".properties");
      var provider =
          new QualifiedClockEvidenceFile(
              file, "host-1", "collector-1", clock, nanos::get, session, nanos.get());
      nanos.incrementAndGet();
      publish(file, 1);
      String content = Files.readString(file);
      content =
          switch (corruption) {
            case "session" -> content.replace(session.toString(), UUID.randomUUID().toString());
            case "host" -> content.replace("hostId=host-1", "hostId=another-host");
            case "drift" -> content.replace("maxDriftPpb=100", "maxDriftPpb=-1");
            case "duplicate" -> content + "sequence=1\n";
            default -> content.replace("reference=offline-qualified-simulator\n", "");
          };
      Files.writeString(file, content);
      var beans = new DefaultListableBeanFactory();
      beans.registerSingleton("clockEvidence", provider);
      var authority = new TimeAuthority(beans.getBeanProvider(ClockEvidence.class), clock);
      assertThat(
              TradePolicy.age(
                  clock.instant().minusSeconds(10), clock.instant(), authority.uncertainty()))
          .isEqualTo(TradePolicy.Age.WAIT_FOR_TIME);
    }
  }

  @Test
  void blockingCurrentSampleCannotAuthorizeAnExecutionThatCrossedSixtySeconds() throws Exception {
    Path file = directory.resolve("delayed-clock.properties");
    Instant before = clock.instant();
    long started = nanos.get();
    nanos.incrementAndGet();
    publish(file, 1);
    String content =
        Files.readString(file)
            .replace("earliestUtc=" + before.minusMillis(100), "earliestUtc=" + before)
            .replace("latestUtc=" + before.plusMillis(100), "latestUtc=" + before)
            .replace(
                "maximumWallMonotonicSkewNanos=1000000", "maximumWallMonotonicSkewNanos=500000000");
    Files.writeString(file, content);
    var delay = new java.util.concurrent.atomic.AtomicBoolean(true);
    Clock delayed =
        new Clock() {
          public ZoneId getZone() {
            return ZoneOffset.UTC;
          }

          public Clock withZone(ZoneId zone) {
            return this;
          }

          public Instant instant() {
            if (delay.getAndSet(false)) {
              clock.advance(Duration.ofMillis(400));
              nanos.addAndGet(400_000_000L);
            }
            return clock.instant();
          }
        };
    var provider =
        new QualifiedClockEvidenceFile(
            file, "host-1", "collector-1", delayed, nanos::get, session, started);
    var beans = new DefaultListableBeanFactory();
    beans.registerSingleton("clockEvidence", provider);
    var authority = new TimeAuthority(beans.getBeanProvider(ClockEvidence.class), delayed);
    var bound = authority.bound();
    var decision =
        bound
            .map(b -> TradePolicy.ageRange(before.minusMillis(59900), b.earliest(), b.latest()))
            .orElse(TradePolicy.Age.WAIT_FOR_TIME);
    assertThat(clock.instant()).isEqualTo(before.plusMillis(400));
    assertThat(decision).isNotEqualTo(TradePolicy.Age.ALLOWED);
    bound.ifPresent(b -> assertThat(b.latest()).isAfterOrEqualTo(clock.instant()));
  }

  @Test
  void historicalReceiptDoesNotMoveToTheEndOfBlockingValidation() throws Exception {
    Path file = directory.resolve("receipt-clock.properties");
    long started = nanos.get();
    nanos.incrementAndGet();
    publish(file, 1);
    var delay = new java.util.concurrent.atomic.AtomicBoolean(false);
    Clock delayed =
        new Clock() {
          public ZoneId getZone() {
            return ZoneOffset.UTC;
          }

          public Clock withZone(ZoneId zone) {
            return this;
          }

          public Instant instant() {
            if (delay.getAndSet(false)) {
              clock.advance(Duration.ofMillis(400));
              nanos.addAndGet(400_000_000L);
            }
            return clock.instant();
          }
        };
    var provider =
        new QualifiedClockEvidenceFile(
            file, "host-1", "collector-1", delayed, nanos::get, session, started);
    assertThat(provider.current()).isPresent();
    clock.advance(Duration.ofMillis(100));
    nanos.addAndGet(100_000_000L);
    long receipt = nanos.get();
    Instant receivedAt = clock.instant();
    delay.set(true);
    var bound = provider.atReceipt(receipt).orElseThrow();
    assertThat(bound.earliest()).isBeforeOrEqualTo(receivedAt);
    assertThat(bound.latest()).isAfterOrEqualTo(receivedAt).isBefore(receivedAt.plusMillis(101));
    assertThat(clock.instant()).isEqualTo(receivedAt.plusMillis(400));
  }

  @Test
  void overlappingPreviouslyQualifiedSampleCanAnchorReceiptButFreshPostStepSampleCannot()
      throws Exception {
    Path file = directory.resolve("refresh-clock.properties");
    long started = nanos.get();
    nanos.incrementAndGet();
    publish(file, 1);
    var provider =
        new QualifiedClockEvidenceFile(
            file, "host-1", "collector-1", clock, nanos::get, session, started);
    assertThat(provider.current()).isPresent();
    clock.advance(Duration.ofMillis(100));
    nanos.addAndGet(100_000_000L);
    long receipt = nanos.get();
    clock.advance(Duration.ofMillis(100));
    nanos.addAndGet(100_000_000L);
    publish(file, 2);
    assertThat(provider.atReceipt(receipt)).isPresent();
    clock.advance(Duration.ofSeconds(2));
    nanos.addAndGet(100_000_000L);
    publish(file, 3);
    assertThat(provider.atReceipt(receipt)).isEmpty();
    var fresh =
        new QualifiedClockEvidenceFile(
            file, "host-1", "collector-1", clock, nanos::get, session, started);
    assertThat(fresh.atReceipt(receipt)).isEmpty();
  }

  private void publish(Path file, long sequence) throws Exception {
    Files.writeString(
        file,
        "format=orbita-clock-evidence-v1\n"
            + "sessionId="
            + session
            + "\nhostId=host-1\ncollectorId=collector-1\nreference=offline-qualified-simulator\n"
            + "sequence="
            + sequence
            + "\nsampleMonotonicNanos="
            + nanos.get()
            + "\n"
            + "sampleLocalUtc="
            + clock.instant()
            + "\nearliestUtc="
            + clock.instant().minusMillis(100)
            + "\nlatestUtc="
            + clock.instant().plusMillis(100)
            + "\n"
            + "validForNanos=2000000000\nmaxDriftPpb=100\nmaximumPollingGapNanos=1000000000\nmaximumWallMonotonicSkewNanos=1000000\n");
  }

  private static final class MutableClock extends Clock {
    Instant now = Instant.parse("2026-11-15T12:00:00Z");

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }
}
