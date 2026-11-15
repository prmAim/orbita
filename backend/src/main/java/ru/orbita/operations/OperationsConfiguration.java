package ru.orbita.operations;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import ru.orbita.notifications.TelegramSettings;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties({TelegramSettings.class, BackupSettings.class})
class OperationsConfiguration {}
