package ru.orbita.journal;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "journal_entry")
public class JournalEntry {
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "journal_seq")
  @SequenceGenerator(name = "journal_seq", sequenceName = "journal_seq", allocationSize = 1)
  long id;

  @Column(nullable = false)
  Instant occurredAt;

  @Column(nullable = false)
  String category;

  String instrument;

  @Column(nullable = false, columnDefinition = "text")
  String detail;

  protected JournalEntry() {}

  public JournalEntry(Instant time, String category, String instrument, String detail) {
    this.occurredAt = time;
    this.category = category;
    this.instrument = instrument;
    this.detail = detail;
  }

  public record View(
      long id, Instant occurredAt, String category, String instrument, String detail) {}

  public View view() {
    return new View(id, occurredAt, category, instrument, detail);
  }

  public long id() {
    return id;
  }
}
