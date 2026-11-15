package ru.orbita.time;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import ru.orbita.control.ClockEvidence;

/** Consumes a qualified external observation; it does not measure or assert NTP/host accuracy. */
public final class QualifiedClockEvidenceFile implements ClockEvidence {
  private static final Set<String> FIELDS =
      Set.of(
          "format",
          "sessionId",
          "hostId",
          "collectorId",
          "reference",
          "sequence",
          "sampleMonotonicNanos",
          "sampleLocalUtc",
          "earliestUtc",
          "latestUtc",
          "validForNanos",
          "maxDriftPpb",
          "maximumPollingGapNanos",
          "maximumWallMonotonicSkewNanos");
  private final Path file;
  private final String hostId;
  private final String collectorId;
  private final Clock clock;
  private final LongSupplier monotonic;
  private final UUID sessionId;
  private final long startedNanos;
  private long invalidatedAfterNanos;
  private Long lastPollNanos;
  private Long lastMaximumGap;
  private long lastSequence = -1;
  private String lastContent;
  private QualifiedSample lastQualifiedSample;
  private QualifiedSample previousQualifiedSample;
  private long validationBeforeWall;
  private long validationAfterWall;
  private Instant validationWall;

  private record QualifiedSample(
      long monotonic,
      Instant localUtc,
      Instant earliest,
      Instant latest,
      BigDecimal driftPpb,
      long validity,
      long maximumSkew,
      String reference) {}

  public QualifiedClockEvidenceFile(
      Path file,
      String hostId,
      String collectorId,
      Clock clock,
      LongSupplier monotonic,
      UUID sessionId,
      long startedNanos) {
    if (file == null
        || hostId == null
        || hostId.isBlank()
        || collectorId == null
        || collectorId.isBlank())
      throw new IllegalArgumentException(
          "Clock file and expected host/collector identities required");
    this.file = file;
    this.hostId = hostId;
    this.collectorId = collectorId;
    this.clock = clock;
    this.monotonic = monotonic;
    this.sessionId = sessionId;
    this.startedNanos = startedNanos;
    invalidatedAfterNanos = startedNanos;
  }

  public record Context(
      UUID sessionId, String hostId, String collectorId, Instant localUtc, long monotonicNanos) {}

  public Context context() {
    return new Context(sessionId, hostId, collectorId, clock.instant(), monotonic.getAsLong());
  }

  @Override
  public synchronized Optional<Bound> current() {
    try {
      byte[] bytes;
      try (var input = Files.newInputStream(file)) {
        bytes = input.readNBytes(16385);
      }
      if (bytes.length > 16384) return invalidateCurrent();
      String content =
          java.nio.charset.StandardCharsets.UTF_8
              .newDecoder()
              .decode(java.nio.ByteBuffer.wrap(bytes))
              .toString();
      Map<String, String> data = parse(content);
      if (!"orbita-clock-evidence-v1".equals(data.get("format"))
          || !sessionId.toString().equals(data.get("sessionId"))
          || !hostId.equals(data.get("hostId"))
          || !collectorId.equals(data.get("collectorId"))) return invalidateCurrent();
      long sequence = Long.parseLong(data.get("sequence"));
      long sample = Long.parseLong(data.get("sampleMonotonicNanos"));
      long validity = positive(data, "validForNanos");
      long maximumGap = positive(data, "maximumPollingGapNanos");
      long maximumSkew = positive(data, "maximumWallMonotonicSkewNanos");
      BigDecimal driftPpb = new BigDecimal(data.get("maxDriftPpb"));
      if (driftPpb.signum() < 0
          || driftPpb.compareTo(BigDecimal.valueOf(1_000_000_000L)) > 0
          || driftPpb.precision() > 30
          || Math.abs((long) driftPpb.scale()) > 18
          || sequence < 0
          || sequence < lastSequence
          || sequence == lastSequence && !content.equals(lastContent)) return invalidateCurrent();
      Instant localSample = Instant.parse(data.get("sampleLocalUtc"));
      Instant earliest = Instant.parse(data.get("earliestUtc"));
      Instant latest = Instant.parse(data.get("latestUtc"));
      if (latest.isBefore(earliest)) return invalidateCurrent();
      // All blocking file I/O and parsing precede current-time sampling.
      // The wall read is bracketed because even Clock.instant() can take time.
      long beforeWall = monotonic.getAsLong();
      Instant wall = clock.instant();
      long afterWall = monotonic.getAsLong();
      if (afterWall < beforeWall) return invalidateCurrent();
      long gapLimit = lastMaximumGap == null ? maximumGap : Math.min(lastMaximumGap, maximumGap);
      if (lastPollNanos != null
          && (afterWall < lastPollNanos || Math.subtractExact(afterWall, lastPollNanos) > gapLimit))
        return invalidateCurrent();
      if (sample <= invalidatedAfterNanos || sample < startedNanos || sample > beforeWall)
        return invalidateCurrent();
      long elapsedBefore = Math.subtractExact(beforeWall, sample);
      long elapsedAfter = Math.subtractExact(afterWall, sample);
      long wallElapsed = Duration.between(localSample, wall).toNanos();
      if (wallElapsed < Math.subtractExact(elapsedBefore, maximumSkew)
          || wallElapsed > Math.addExact(elapsedAfter, maximumSkew)) return invalidateCurrent();
      long completed = monotonic.getAsLong();
      if (completed < afterWall) return invalidateCurrent();
      long elapsed = Math.subtractExact(completed, sample);
      if (elapsed > validity
          || lastPollNanos != null && Math.subtractExact(completed, lastPollNanos) > gapLimit)
        return invalidateCurrent();
      long growth =
          driftPpb
              .multiply(BigDecimal.valueOf(elapsed))
              .movePointLeft(9)
              .setScale(0, RoundingMode.CEILING)
              .longValueExact();
      var bound =
          new Bound(
              earliest.plusNanos(elapsedBefore).minusNanos(growth),
              latest.plusNanos(elapsed).plusNanos(growth),
              data.get("reference"));
      lastSequence = sequence;
      lastContent = content;
      lastMaximumGap = maximumGap;
      lastPollNanos = completed;
      var sampleEvidence =
          new QualifiedSample(
              sample,
              localSample,
              earliest,
              latest,
              driftPpb,
              validity,
              maximumSkew,
              data.get("reference"));
      if (lastQualifiedSample != null && !lastQualifiedSample.equals(sampleEvidence))
        previousQualifiedSample = lastQualifiedSample;
      lastQualifiedSample = sampleEvidence;
      validationBeforeWall = beforeWall;
      validationAfterWall = afterWall;
      validationWall = wall;
      return Optional.of(bound);
    } catch (Exception invalidOrUnavailable) {
      return invalidateCurrent();
    }
  }

  @Override
  public synchronized Optional<Bound> atReceipt(long receiptNanos) {
    if (current().isEmpty()) return Optional.empty();
    try {
      var sample =
          lastQualifiedSample.monotonic() <= receiptNanos
              ? lastQualifiedSample
              : previousQualifiedSample;
      if (sample == null || receiptNanos < sample.monotonic() || receiptNanos > lastPollNanos)
        return Optional.empty();
      long elapsed = Math.subtractExact(receiptNanos, sample.monotonic());
      if (elapsed > sample.validity()) return Optional.empty();
      // A newly qualified post-step sample cannot establish continuity for an older receipt.
      // Even a retained sample must still agree with the current wall/monotonic bracket.
      long wallElapsed = Duration.between(sample.localUtc(), validationWall).toNanos();
      long before = Math.subtractExact(validationBeforeWall, sample.monotonic());
      long after = Math.subtractExact(validationAfterWall, sample.monotonic());
      if (wallElapsed < Math.subtractExact(before, sample.maximumSkew())
          || wallElapsed > Math.addExact(after, sample.maximumSkew())) return Optional.empty();
      long growth =
          sample
              .driftPpb()
              .multiply(BigDecimal.valueOf(elapsed))
              .movePointLeft(9)
              .setScale(0, RoundingMode.CEILING)
              .longValueExact();
      return Optional.of(
          new Bound(
              sample.earliest().plusNanos(elapsed).minusNanos(growth),
              sample.latest().plusNanos(elapsed).plusNanos(growth),
              sample.reference()));
    } catch (ArithmeticException invalid) {
      return Optional.empty();
    }
  }

  private Optional<Bound> invalidateCurrent() {
    return invalidate(monotonic.getAsLong());
  }

  private Optional<Bound> invalidate(long now) {
    lastQualifiedSample = null;
    previousQualifiedSample = null;
    invalidatedAfterNanos = now;
    lastPollNanos = now;
    return Optional.empty();
  }

  private static long positive(Map<String, String> data, String field) {
    long value = Long.parseLong(data.get(field));
    if (value <= 0) throw new IllegalArgumentException("Positive qualified interval required");
    return value;
  }

  private static Map<String, String> parse(String content) {
    var fields = new HashMap<String, String>();
    for (String line : content.split("\\R")) {
      if (line.isBlank() || line.stripLeading().startsWith("#")) continue;
      int separator = line.indexOf('=');
      if (separator <= 0 || separator == line.length() - 1)
        throw new IllegalArgumentException("Invalid clock observation field");
      String key = line.substring(0, separator).strip(),
          value = line.substring(separator + 1).strip();
      if (!FIELDS.contains(key) || value.isBlank() || fields.putIfAbsent(key, value) != null)
        throw new IllegalArgumentException("Unknown/duplicate/empty clock observation field");
    }
    if (!fields.keySet().equals(FIELDS))
      throw new IllegalArgumentException("Incomplete clock observation");
    return fields;
  }
}
