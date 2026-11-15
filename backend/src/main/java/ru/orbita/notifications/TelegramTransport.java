package ru.orbita.notifications;

import java.time.Duration;

/** External boundary. A timeout is an unknown delivery result, never a confirmed failure. */
public interface TelegramTransport {
  Result send(String text);

  enum Outcome {
    SUCCESS,
    FAILURE,
    NO_RESPONSE
  }

  record Result(Outcome outcome, String detail, Duration retryAfter) {
    public static Result success() {
      return new Result(Outcome.SUCCESS, "Доставлено", Duration.ZERO);
    }

    public static Result failure(String reason, Duration wait) {
      return new Result(Outcome.FAILURE, reason, wait);
    }

    public static Result noResponse() {
      return new Result(Outcome.NO_RESPONSE, "результат не дождался ответа", Duration.ZERO);
    }
  }
}
