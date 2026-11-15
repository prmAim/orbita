package ru.orbita.runtime;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.orbita.control.ControlService;
import ru.orbita.control.OrbitaProperties;

/**
 * Recovery is mandatory even when trading is disabled, and excluded from the password-reset CLI.
 */
@Component
@ConditionalOnProperty(
    name = "orbita.admin.reset-password",
    havingValue = "false",
    matchIfMissing = true)
public final class ApplicationInitialization {
  private final ControlService control;
  private final OrbitaProperties properties;
  private final ApplicationReadiness readiness;

  public ApplicationInitialization(
      ControlService control, OrbitaProperties properties, ApplicationReadiness readiness) {
    this.control = control;
    this.properties = properties;
    this.readiness = readiness;
  }

  @EventListener(ApplicationReadyEvent.class)
  public synchronized void initialize() {
    if (readiness.initialized()) return;
    control.recoverStartup();
    if (properties.restored()) control.markRestored();
    readiness.initializationCompleted();
  }
}
