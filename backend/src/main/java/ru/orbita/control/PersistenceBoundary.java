package ru.orbita.control;

/** A failed durable broker observation is not a recoverable transport-data absence. */
public final class PersistenceBoundary {
  private PersistenceBoundary() {}

  public static void rethrowBrokerPersistence(RuntimeException failure) {
    var seen =
        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
    for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause())
      if (cause instanceof ru.orbita.broker.BrokerPersistenceFailure) throw failure;
  }
}
