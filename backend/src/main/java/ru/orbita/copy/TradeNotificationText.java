package ru.orbita.copy;

import java.math.BigDecimal;
import ru.orbita.broker.BrokerGateway.OrderStatus;
import ru.orbita.broker.BrokerGateway.RejectionReason;

/** Builds owner-facing copy-trade notices from the same persisted trade facts as the journal. */
public final class TradeNotificationText {
  static final String PARTIAL_RECOVERY_SKIP_REASON =
      "Проданная доля исходной покупки учтена при восстановлении";
  static final String RESOURCE_LIMIT_SKIP_REASON = "Объём ограничен доступными ресурсами";
  static final String RESOURCE_UNAVAILABLE_SKIP_REASON =
      "Недостаточно собственных средств или бумаг с комиссией";
  static final String CONFIRMED_CANCEL_UNKNOWN_REASON =
      "Подтверждённая отмена заявки; причина брокера неизвестна";
  static final String CONFIRMED_PARTIAL_CANCEL_UNKNOWN_REASON =
      "Часть заявки исполнена, остаток подтверждённо отменён; причина брокера неизвестна";
  static final String CONFIRMED_REJECTION_UNKNOWN_REASON =
      "Подтверждённый отказ брокера; причина отказа неизвестна";
  static final String CONFIRMED_PARTIAL_REJECTION_UNKNOWN_REASON =
      "Часть заявки исполнена; остаток подтверждённо отклонён брокером; причина отказа неизвестна";
  static final String CONFIRMED_HALT_REJECTION_REASON =
      "Подтверждённый отказ из-за остановки торгов";
  static final String UNSENT_FINAL_SKIP_REASON = "Объём не отправлялся до заявки";
  static final String UNQUALIFIED_FINAL_SKIP_REASON =
      "Причина остатка заявки не квалифицирована";

  private TradeNotificationText() {}

  static String preserveSkipReasons(String currentReason, String nextReason) {
    String preserved = preservedSkipReasons(currentReason);
    if (nextReason != null) {
      if (nextReason.contains(PARTIAL_RECOVERY_SKIP_REASON))
        preserved = appendReason(preserved, PARTIAL_RECOVERY_SKIP_REASON);
      if (nextReason.contains(RESOURCE_LIMIT_SKIP_REASON))
        preserved = appendReason(preserved, RESOURCE_LIMIT_SKIP_REASON);
      if (nextReason.contains(RESOURCE_UNAVAILABLE_SKIP_REASON))
        preserved = appendReason(preserved, RESOURCE_UNAVAILABLE_SKIP_REASON);
      String remaining = nextReason;
      remaining = removeSkipReason(remaining, PARTIAL_RECOVERY_SKIP_REASON);
      remaining = removeSkipReason(remaining, RESOURCE_LIMIT_SKIP_REASON);
      remaining = removeSkipReason(remaining, RESOURCE_UNAVAILABLE_SKIP_REASON);
      return appendReason(preserved, remaining);
    }
    return preserved;
  }

  private static String preservedSkipReasons(String persistedReason) {
    String preserved = null;
    if (persistedReason != null) {
      if (persistedReason.contains(PARTIAL_RECOVERY_SKIP_REASON))
        preserved = appendReason(preserved, PARTIAL_RECOVERY_SKIP_REASON);
      if (persistedReason.contains(RESOURCE_LIMIT_SKIP_REASON))
        preserved = appendReason(preserved, RESOURCE_LIMIT_SKIP_REASON);
      if (persistedReason.contains(RESOURCE_UNAVAILABLE_SKIP_REASON))
        preserved = appendReason(preserved, RESOURCE_UNAVAILABLE_SKIP_REASON);
    }
    return preserved;
  }

  static String finalOrderSkipReason(
      String persistedReason,
      boolean qualifiedReasonBoundToAttempt,
      OrderStatus status,
      RejectionReason rejectionReason,
      long executedLots,
      long skippedBeforeSend,
      long skippedAtFinal) {
    String reason = preservedSkipReasons(persistedReason);
    if (skippedAtFinal > skippedBeforeSend) {
      String attemptReason =
          qualifiedReasonBoundToAttempt ? qualifiedAttemptReason(persistedReason) : null;
      reason =
          appendReason(
              reason,
              attemptReason != null
                  ? attemptReason
                  : terminalSkipReason(status, rejectionReason, executedLots));
    }
    return reason == null ? UNSENT_FINAL_SKIP_REASON : reason;
  }

  private static String qualifiedAttemptReason(String persistedReason) {
    String reason = persistedReason;
    reason = removeSkipReason(reason, PARTIAL_RECOVERY_SKIP_REASON);
    reason = removeSkipReason(reason, RESOURCE_LIMIT_SKIP_REASON);
    reason = removeSkipReason(reason, RESOURCE_UNAVAILABLE_SKIP_REASON);
    return reason == null || reason.isBlank() ? RESOURCE_UNAVAILABLE_SKIP_REASON : reason;
  }

  private static String removeSkipReason(String currentReason, String skipReason) {
    if (currentReason == null) return null;
    String result = currentReason.replace("; " + skipReason, "");
    result = result.replace(skipReason + "; ", "");
    return result.equals(skipReason) || result.isBlank() ? null : result;
  }

  private static String terminalSkipReason(
      OrderStatus status, RejectionReason rejectionReason, long executedLots) {
    if (status == OrderStatus.CANCELLED)
      return executedLots > 0
          ? CONFIRMED_PARTIAL_CANCEL_UNKNOWN_REASON
          : CONFIRMED_CANCEL_UNKNOWN_REASON;
    if (status == OrderStatus.REJECTED) {
      if (rejectionReason == RejectionReason.HALTED) return CONFIRMED_HALT_REJECTION_REASON;
      return executedLots > 0
          ? CONFIRMED_PARTIAL_REJECTION_UNKNOWN_REASON
          : CONFIRMED_REJECTION_UNKNOWN_REASON;
    }
    return UNQUALIFIED_FINAL_SKIP_REASON;
  }

  private static String appendReason(String currentReason, String nextReason) {
    if (nextReason == null || nextReason.isBlank()) return currentReason;
    if (currentReason == null || currentReason.isBlank()) return nextReason;
    if (currentReason.contains(nextReason)) return currentReason;
    return currentReason + "; " + nextReason;
  }

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
