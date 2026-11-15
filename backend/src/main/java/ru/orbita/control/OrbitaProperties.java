package ru.orbita.control;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("orbita")
public record OrbitaProperties(
    String sourceAccount,
    String mirrorAccount,
    Long clockUncertaintyMillis,
    boolean restored,
    boolean marginDisabledConfirmed,
    boolean shadowAcceptanceConfirmed) {
  public boolean accountsConfigured() {
    return sourceAccount != null
        && !sourceAccount.isBlank()
        && mirrorAccount != null
        && !mirrorAccount.isBlank()
        && !sourceAccount.equals(mirrorAccount);
  }
}
