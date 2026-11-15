package ru.orbita.runtime;

import org.springframework.stereotype.Component;

/**
 * Process-local barriers supplement durable recovery gates; a failed startup never opens workers.
 */
@Component
public final class ApplicationReadiness {
  private volatile boolean initialized;
  private volatile boolean brokerPersistenceAvailable = true;

  public boolean initialized() {
    return initialized;
  }

  public boolean ready() {
    return initialized && brokerPersistenceAvailable;
  }

  void initializationCompleted() {
    initialized = true;
  }

  public void brokerPersistenceUnavailable() {
    brokerPersistenceAvailable = false;
  }

  public void brokerPersistenceRecovered() {
    brokerPersistenceAvailable = true;
  }
}
