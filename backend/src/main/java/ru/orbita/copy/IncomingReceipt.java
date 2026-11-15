package ru.orbita.copy;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "incoming_receipt")
public class IncomingReceipt {
  @Id String id;

  @Column(nullable = false)
  Instant firstReceived;

  Instant firstEarliest;
  Instant firstLatest;

  protected IncomingReceipt() {}

  IncomingReceipt(String id, Instant at) {
    this.id = id;
    firstReceived = at;
  }
}
