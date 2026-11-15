package ru.orbita.valuation;

import java.nio.file.Path;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("orbita.rub-valuation")
public record RubValuationSettings(
    Path evidenceDirectory,
    String collectorId,
    Duration maximumEvidenceAge,
    Duration maximumRateWindow) {}
