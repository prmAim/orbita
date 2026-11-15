package ru.orbita.web;

import java.math.BigDecimal;
import java.time.Instant;
import ru.orbita.control.ControlService;

/** Decimal strings preserve the saved coefficient and RUB limit through JavaScript JSON parsing. */
record StatusResponse(
    String mode,
    String coefficient,
    Instant observationBoundary,
    Instant automaticBoundary,
    String permitLimit,
    Instant permitIssuedAt,
    boolean recalculationRunning,
    boolean recalculationPaused,
    boolean reconciled,
    String lastError,
    String ratioNumerator,
    String ratioDenominator) {
  static StatusResponse from(ControlService.Status status) {
    return new StatusResponse(
        status.mode(),
        decimal(status.coefficient()),
        status.observationBoundary(),
        status.automaticBoundary(),
        decimal(status.permitLimit()),
        status.permitIssuedAt(),
        status.recalculationRunning(),
        status.recalculationPaused(),
        status.reconciled(),
        status.lastError(),
        status.ratioNumerator() == null ? null : status.ratioNumerator().toString(),
        status.ratioDenominator() == null ? null : status.ratioDenominator().toString());
  }

  static String decimal(BigDecimal value) {
    return value == null ? null : value.stripTrailingZeros().toPlainString();
  }
}
