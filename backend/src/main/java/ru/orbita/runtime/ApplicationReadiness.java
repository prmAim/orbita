package ru.orbita.runtime;

import org.springframework.stereotype.Component;

/**
 * Process-local barriers supplement durable recovery gates; a failed startup never opens workers.
 */
@Component
public final class ApplicationReadiness {
  private volatile boolean initialized;
  private volatile boolean brokerPersistenceAvailable = true;
  private volatile boolean mirrorPersistenceAvailable = true;

  public boolean initialized() {
    return initialized;
  }

  public boolean ready() {
    return initialized && brokerPersistenceAvailable && mirrorPersistenceAvailable;
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

  public void mirrorPersistenceUnavailable() {
    mirrorPersistenceAvailable = false;
  }

  public void mirrorPersistenceRecovered() {
    mirrorPersistenceAvailable = true;
  }
}
