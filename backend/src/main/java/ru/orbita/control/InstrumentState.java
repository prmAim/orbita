package ru.orbita.control;

import jakarta.persistence.*;

@Entity
@Table(name = "instrument_state")
public class InstrumentState {
  @Id String instrumentId;
  @Version long version;
  boolean paused;
  boolean waiting;
  boolean reconciled;
  String reason;

  protected InstrumentState() {}

  public InstrumentState(String id) {
    instrumentId = id;
  }
}
