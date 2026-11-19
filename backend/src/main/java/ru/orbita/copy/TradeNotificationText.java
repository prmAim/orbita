package ru.orbita.copy;

import java.math.BigDecimal;

/** Builds owner-facing copy-trade notices from the same persisted trade facts as the journal. */
public final class TradeNotificationText {
  private TradeNotificationText() {}

  public static String finalSkip(
      String instrument,
      String side,
      long executedLots,
      long totalLots,
      long skippedLots,
      String reason) {
    if (instrument == null
        || instrument.isBlank()
        || executedLots < 0
        || totalLots < 0
        || skippedLots < 0
        || executedLots > totalLots
        || skippedLots > totalLots - executedLots
        || reason == null
        || reason.isBlank()) throw new IllegalArgumentException("Incomplete trade notification");
    return instrument
        + " — "
        + direction(side)
        + "; исполнено "
        + executedLots
        + "/"
        + totalLots
        + ", пропущено "
        + skippedLots
        + " "
        + lotWord(skippedLots)
        + "; причина: "
        + reason;
  }

  public static String sourceSkip(
      String instrument, String side, BigDecimal sourceLots, String reason) {
    if (instrument == null
        || instrument.isBlank()
        || sourceLots == null
        || sourceLots.signum() <= 0
        || reason == null
        || reason.isBlank()) throw new IllegalArgumentException("Incomplete source notification");
    return instrument
        + " — "
        + direction(side)
        + "; исходное исполнение "
        + sourceLots.stripTrailingZeros().toPlainString()
        + " лотов не скопировано; причина: "
        + reason;
  }

  public static String unknown(
      String instrument,
      String side,
      long executedLots,
      long totalLots,
      long skippedLots,
      String reason) {
    if (instrument == null
        || instrument.isBlank()
        || executedLots < 0
        || totalLots < 0
        || skippedLots < 0
        || executedLots > totalLots
        || skippedLots > totalLots - executedLots
        || reason == null
        || reason.isBlank()) throw new IllegalArgumentException("Incomplete trade notification");
    return instrument
        + " — "
        + direction(side)
        + "; исполнено "
        + executedLots
        + "/"
        + totalLots
        + ", подтверждённо пропущено "
        + skippedLots
        + ", под сверкой "
        + (totalLots - executedLots - skippedLots)
        + " "
        + lotWord(totalLots - executedLots - skippedLots)
        + "; результат заявки неизвестен; причина: "
        + reason;
  }

  private static String direction(String side) {
    if (side == null) throw new IllegalArgumentException("Unknown trade direction");
    return switch (side) {
      case "BUY" -> "Покупка";
      case "SELL" -> "Продажа";
      default -> throw new IllegalArgumentException("Unknown trade direction");
    };
  }

  private static String lotWord(long lots) {
    long lastTwo = lots % 100;
    if (lastTwo >= 11 && lastTwo <= 14) return "лотов";
    return switch ((int) (lots % 10)) {
      case 1 -> "лот";
      case 2, 3, 4 -> "лота";
      default -> "лотов";
    };
  }
}
