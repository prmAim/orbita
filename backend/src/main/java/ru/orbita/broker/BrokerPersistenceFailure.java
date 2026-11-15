package ru.orbita.broker;

/** A broker observation could not be durably recorded; callers must stop this trading tick. */
public final class BrokerPersistenceFailure extends RuntimeException {
  public BrokerPersistenceFailure(String operation, RuntimeException cause) {
    super("Broker observation persistence failed: " + operation, cause);
  }
}
