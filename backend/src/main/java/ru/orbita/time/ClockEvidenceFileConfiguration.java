package ru.orbita.time;

import java.time.Clock;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "orbita.clock-evidence", name = "file")
@ConditionalOnProperty(
    name = "orbita.admin.reset-password",
    havingValue = "false",
    matchIfMissing = true)
public class ClockEvidenceFileConfiguration {
  @Bean
  QualifiedClockEvidenceFile qualifiedClockEvidenceFile(
      ClockEvidenceFileSettings settings, Clock clock) {
    long started = System.nanoTime();
    return new QualifiedClockEvidenceFile(
        settings.file(),
        settings.hostId(),
        settings.collectorId(),
        clock,
        System::nanoTime,
        UUID.randomUUID(),
        started);
  }
}
