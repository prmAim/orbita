package ru.orbita.notifications;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
final class HttpTelegramTransport implements TelegramTransport {
  private final TelegramSettings settings;
  private final JsonMapper json;
  private final HttpClient client;
  private final URI testEndpoint;

  @Autowired
  HttpTelegramTransport(TelegramSettings settings, JsonMapper json) {
    this(
        settings,
        json,
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build(),
        null);
  }

  HttpTelegramTransport(
      TelegramSettings settings, JsonMapper json, HttpClient client, URI testEndpoint) {
    this.settings = settings;
    this.json = json;
    this.client = client;
    this.testEndpoint = testEndpoint;
  }

  @Override
  public Result send(String text) {
    if (!settings.enabled()) return Result.failure("Доставка Telegram отключена", Duration.ZERO);
    if (settings.token() == null
        || settings.token().isBlank()
        || settings.chatId() == null
        || !settings.chatId().matches("[1-9][0-9]*"))
      return Result.failure("Не настроен личный чат Telegram", Duration.ZERO);
    try {
      String body = "chat_id=" + encode(settings.chatId()) + "&text=" + encode(text);
      var request =
          HttpRequest.newBuilder(
                  testEndpoint == null
                      ? URI.create(
                          "https://api.telegram.org/bot" + settings.token() + "/sendMessage")
                      : testEndpoint)
              .timeout(Duration.ofSeconds(10))
              .header("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();
      var response =
          client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      var root = json.readTree(response.body());
      if (response.statusCode() == 200 && root.path("ok").asBoolean()) return Result.success();
      long retry = Math.max(0, root.path("parameters").path("retry_after").asLong(0));
      String reason =
          response.statusCode() == 403
              ? "Пользователь заблокировал бота"
              : response.statusCode() == 429
                  ? "Telegram ограничил частоту отправки"
                  : "Telegram подтвердил ошибку доставки (HTTP " + response.statusCode() + ")";
      return Result.failure(reason, Duration.ofSeconds(retry));
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return Result.noResponse();
    } catch (IOException | RuntimeException exception) {
      return Result.noResponse();
    }
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
