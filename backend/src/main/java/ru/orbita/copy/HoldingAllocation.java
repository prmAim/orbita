package ru.orbita.copy;

import jakarta.persistence.*;

@Entity
@Table(name = "holding_allocation")
public class HoldingAllocation {
  @Id String sourceExecutionId;

  @Column(nullable = false)
  String instrumentId;

  long lots;

  protected HoldingAllocation() {}

  HoldingAllocation(String source, String instrument, long quantity) {
    sourceExecutionId = source;
    instrumentId = instrument;
    lots = quantity;
  }

  public record View(String sourceExecutionId, String instrumentId, long lots) {}

  public View view() {
    return new View(sourceExecutionId, instrumentId, lots);
  }
}
