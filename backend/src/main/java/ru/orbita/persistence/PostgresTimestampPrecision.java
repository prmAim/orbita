package ru.orbita.persistence;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** PostgreSQL timestamp(6) stores instants rounded to the nearest microsecond. */
public final class PostgresTimestampPrecision {
  private PostgresTimestampPrecision() {}

  public static Instant normalize(Instant value) {
    return value == null ? null : value.plusNanos(500).truncatedTo(ChronoUnit.MICROS);
  }
}
