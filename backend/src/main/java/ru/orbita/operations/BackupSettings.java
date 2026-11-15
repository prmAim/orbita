package ru.orbita.operations;

import java.nio.file.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("orbita.backup")
public record BackupSettings(
    boolean enabled,
    Path localDirectory,
    Path usbDirectory,
    String database,
    String host,
    int port,
    String username) {}
