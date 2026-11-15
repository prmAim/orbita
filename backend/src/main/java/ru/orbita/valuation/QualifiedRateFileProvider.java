package ru.orbita.valuation;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import ru.orbita.control.ClockEvidence;

/** Production consumer for independently qualified, ephemeral conversion evidence. */
public final class QualifiedRateFileProvider implements QualifiedRateProvider {
  private static final Set<String> FIELDS =
      Set.of(
          "format",
          "sessionId",
          "collectorId",
          "accountId",
          "instrumentId",
          "currency",
          "purpose",
          "basis",
          "qualificationReference",
          "sequence",
          "rateNumerator",
          "rateDenominator",
          "rateValidFrom",
          "rateValidThrough",
          "evidenceAsOf",
          "evidenceExpiresAt");

  private record Seen(long sequence, String content) {}

  public record Context(UUID sessionId, String collectorId, Scope scope, String fileName) {}

  private final Path directory;
  private final String accountId;
  private final String collectorId;
  private final Duration maximumEvidenceAge;
  private final Duration maximumRateWindow;
  private final UUID sessionId;
  private final Map<Scope, Seen> seen = new HashMap<>();

  public QualifiedRateFileProvider(
      Path directory,
      String accountId,
      String collectorId,
      Duration maximumEvidenceAge,
      Duration maximumRateWindow,
      UUID sessionId) {
    if (directory == null
        || accountId == null
        || accountId.isBlank()
        || collectorId == null
        || collectorId.isBlank()
        || sessionId == null
        || maximumEvidenceAge == null
        || maximumEvidenceAge.isZero()
        || maximumEvidenceAge.isNegative()
        || maximumEvidenceAge.compareTo(Duration.ofSeconds(5)) > 0
        || maximumRateWindow == null
        || maximumRateWindow.isZero()
        || maximumRateWindow.isNegative())
      throw new IllegalArgumentException(
          "Explicit account/collector identity and bounded evidence/rate windows required");
    this.directory = directory;
    this.accountId = accountId;
    this.collectorId = collectorId;
    this.maximumEvidenceAge = maximumEvidenceAge;
    this.maximumRateWindow = maximumRateWindow;
    this.sessionId = sessionId;
  }

  public Context context(String instrumentId, String currency, Purpose purpose) {
    var scope = new Scope(accountId, instrumentId, currency, purpose);
    return new Context(sessionId, collectorId, scope, fileName(scope));
  }

  private static String fileName(Scope scope) {
    try {
      var bytes =
          MessageDigest.getInstance("SHA-256")
              .digest(
                  (scope.accountId()
                          + "\n"
                          + scope.instrumentId()
                          + "\n"
                          + scope.currency()
                          + "\n"
                          + scope.purpose())
                      .getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(bytes) + ".properties";
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 unavailable", impossible);
    }
  }

  @Override
  public synchronized Optional<Rate> current(
      Scope scope, Instant from, Instant through, ClockEvidence.Bound now) {
    try {
      if (!accountId.equals(scope.accountId())) return Optional.empty();
      Path file = directory.resolve(fileName(scope));
      byte[] bytes;
      try (var input = Files.newInputStream(file)) {
        bytes = input.readNBytes(16385);
      }
      if (bytes.length > 16384) return Optional.empty();
      String content =
          StandardCharsets.UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(bytes)).toString();
      var data = parse(content);
      if (!data.get("format").equals("orbita-qualified-rub-rate-v1")
          || !data.get("sessionId").equals(sessionId.toString())
          || !data.get("collectorId").equals(collectorId)
          || !data.get("basis").equals("QUALIFIED_RUB_COST_PER_SETTLEMENT_UNIT"))
        return Optional.empty();
      var actualScope =
          new Scope(
              data.get("accountId"),
              data.get("instrumentId"),
              data.get("currency"),
              Purpose.valueOf(data.get("purpose")));
      if (!actualScope.equals(scope)) return Optional.empty();
      long sequence = Long.parseLong(data.get("sequence"));
      var previous = seen.get(scope);
      if (sequence < 0
          || previous != null
              && (sequence < previous.sequence()
                  || sequence == previous.sequence() && !content.equals(previous.content())))
        return Optional.empty();
      var rate =
          new Rate(
              actualScope,
              integer(data, "rateNumerator"),
              integer(data, "rateDenominator"),
              Instant.parse(data.get("rateValidFrom")),
              Instant.parse(data.get("rateValidThrough")),
              Instant.parse(data.get("evidenceAsOf")),
              Instant.parse(data.get("evidenceExpiresAt")),
              data.get("qualificationReference")
                  + ";collector="
                  + collectorId
                  + ";session="
                  + sessionId
                  + ";sequence="
                  + sequence);
      if (Duration.between(rate.evidenceAsOf(), rate.evidenceExpiresAt())
                  .compareTo(maximumEvidenceAge)
              > 0
          || Duration.between(rate.validFrom(), rate.validThrough()).compareTo(maximumRateWindow)
              > 0
          || now.earliest().isBefore(rate.evidenceAsOf())
          || now.latest().isAfter(rate.evidenceExpiresAt())
          || from.isBefore(rate.validFrom())
          || through.isAfter(rate.validThrough())
          || through.isBefore(from)) return Optional.empty();
      seen.put(scope, new Seen(sequence, content));
      return Optional.of(rate);
    } catch (Exception invalidOrUnavailable) {
      return Optional.empty();
    }
  }

  private static BigInteger integer(Map<String, String> data, String key) {
    String value = data.get(key);
    if (!value.matches("[0-9]{1,1234}"))
      throw new IllegalArgumentException("Bounded positive integer ratio required");
    return new BigInteger(value);
  }

  private static Map<String, String> parse(String content) {
    var data = new HashMap<String, String>();
    for (String line : content.split("\\R")) {
      if (line.isBlank() || line.stripLeading().startsWith("#")) continue;
      int split = line.indexOf('=');
      if (split <= 0 || split == line.length() - 1)
        throw new IllegalArgumentException("Invalid conversion evidence field");
      String key = line.substring(0, split).strip(), value = line.substring(split + 1).strip();
      if (!FIELDS.contains(key) || value.isBlank() || data.putIfAbsent(key, value) != null)
        throw new IllegalArgumentException("Unknown/duplicate/empty conversion evidence field");
    }
    if (!data.keySet().equals(FIELDS))
      throw new IllegalArgumentException("Incomplete conversion evidence");
    return data;
  }
}
