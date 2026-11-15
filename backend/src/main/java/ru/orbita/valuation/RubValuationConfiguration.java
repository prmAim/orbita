package ru.orbita.valuation;

import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.orbita.control.OrbitaProperties;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "orbita.rub-valuation", name = "evidence-directory")
@ConditionalOnProperty(
    name = "orbita.admin.reset-password",
    havingValue = "false",
    matchIfMissing = true)
public class RubValuationConfiguration {
  @Bean
  QualifiedRateFileProvider qualifiedRateProvider(
      RubValuationSettings settings, OrbitaProperties accounts) {
    if (!accounts.accountsConfigured())
      throw new IllegalStateException(
          "Configured distinct account pair required for RUB valuation");
    return new QualifiedRateFileProvider(
        settings.evidenceDirectory(),
        accounts.mirrorAccount(),
        settings.collectorId(),
        settings.maximumEvidenceAge(),
        settings.maximumRateWindow(),
        UUID.randomUUID());
  }
}
