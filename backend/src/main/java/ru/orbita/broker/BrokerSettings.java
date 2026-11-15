package ru.orbita.broker;

import java.nio.file.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("orbita.broker")
public record BrokerSettings(boolean enabled, Path tokenFile) {}
