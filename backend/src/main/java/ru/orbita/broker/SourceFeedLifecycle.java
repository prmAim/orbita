package ru.orbita.broker;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Observation starts only after the durable first-start boundary exists, and continues during
 * stops.
 */
@Component
@ConditionalOnProperty(
    name = "orbita.admin.reset-password",
    havingValue = "false",
    matchIfMissing = true)
public final class SourceFeedLifecycle {
  private final ObjectProvider<BrokerGateway> gateways;
  private final SourceFeedJournal journal;
  private final ru.orbita.runtime.ApplicationReadiness readiness;
  private AutoCloseable subscription;

  public SourceFeedLifecycle(
      ObjectProvider<BrokerGateway> gateways,
      SourceFeedJournal journal,
      ru.orbita.runtime.ApplicationReadiness readiness) {
    this.gateways = gateways;
    this.journal = journal;
    this.readiness = readiness;
  }

  @Scheduled(fixedDelay = 1000)
  public synchronized void ensureObservation() {
    if (!readiness.initialized()) return;
    var gateway = gateways.getIfAvailable();
    if (subscription == null && gateway != null && journal.observationBoundary() != null) {
      try {
        subscription = gateway.observe(journal);
      } catch (RuntimeException failure) {
        ru.orbita.control.PersistenceBoundary.rethrowBrokerPersistence(failure);
        journal.gap(
            "Broker account/subscription setup failed: " + failure.getClass().getSimpleName());
      }
    }
  }

  @PreDestroy
  public synchronized void close() throws Exception {
    if (subscription != null) {
      subscription.close();
      subscription = null;
    }
  }
}
