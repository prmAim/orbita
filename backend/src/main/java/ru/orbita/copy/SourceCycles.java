package ru.orbita.copy;

import java.math.BigDecimal;
import java.util.*;

/** Source cycles are distinguished by a proven flat position between purchases. */
public final class SourceCycles {
  private SourceCycles() {}

  static final class UnprovenHistory extends IllegalStateException {}

  static Map<String, String> map(List<SourceExecutionEntity> events) {
    var result = new HashMap<String, String>();
    BigDecimal balance = BigDecimal.ZERO;
    String cycle = null;
    SourceExecutionEntity previous = null;
    for (var event : events) {
      if (previous != null
          && event.executedAt.equals(previous.executedAt)
          && !event.side.equals(previous.side)) throw new UnprovenHistory();
      if (balance.signum() == 0) cycle = event.id;
      result.put(event.id, cycle);
      balance =
          balance.add(event.lots.multiply(BigDecimal.valueOf("BUY".equals(event.side) ? 1 : -1)));
      if (balance.signum() < 0) throw new UnprovenHistory();
      previous = event;
    }
    return result;
  }
}
