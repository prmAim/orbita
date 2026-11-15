package ru.orbita.time;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Authenticated local context for an external collector; this endpoint never qualifies the host.
 */
@RestController
@ConditionalOnProperty(prefix = "orbita.clock-evidence", name = "file")
@ConditionalOnProperty(
    name = "orbita.admin.reset-password",
    havingValue = "false",
    matchIfMissing = true)
public class ClockEvidenceContextController {
  private final QualifiedClockEvidenceFile evidence;

  public ClockEvidenceContextController(QualifiedClockEvidenceFile evidence) {
    this.evidence = evidence;
  }

  @GetMapping("/api/clock/context")
  public QualifiedClockEvidenceFile.Context context() {
    return evidence.context();
  }
}
