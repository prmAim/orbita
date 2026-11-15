package ru.orbita.notifications;

import java.time.Instant;
import java.util.UUID;

/** The caller's state change and this durable outbox insert share one transaction. */
public interface NotificationPublisher {
  UUID publish(String category, String instrument, Instant occurredAt, String ownerText);
}
