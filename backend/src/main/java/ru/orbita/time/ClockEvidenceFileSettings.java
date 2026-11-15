package ru.orbita.time;

import java.nio.file.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("orbita.clock-evidence")
public record ClockEvidenceFileSettings(Path file, String hostId, String collectorId) {}
