package ru.orbita.notifications;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("orbita.telegram")
public record TelegramSettings(boolean enabled, String token, String chatId) {}
