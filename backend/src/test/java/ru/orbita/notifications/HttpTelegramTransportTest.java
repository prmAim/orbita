package ru.orbita.notifications;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class HttpTelegramTransportTest {
  @Test
  void oneTenSecondBudgetIncludesWaitingForTheResponse() throws Exception {
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    var executor = Executors.newVirtualThreadPerTaskExecutor();
    server.setExecutor(executor);
    var received = new java.util.concurrent.CountDownLatch(1);
    server.createContext(
        "/sendMessage",
        exchange -> {
          received.countDown();
          try {
            Thread.sleep(11000);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      var sender =
          new HttpTelegramTransport(
              new TelegramSettings(true, "synthetic-token", "123456"),
              JsonMapper.builder().build(),
              HttpClient.newHttpClient(),
              URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/sendMessage"));
      long started = System.nanoTime();
      var result = sender.send("Synthetic timeout probe");
      Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
      assertThat(received.getCount()).isZero();
      assertThat(result.outcome()).isEqualTo(TelegramTransport.Outcome.NO_RESPONSE);
      assertThat(elapsed).isBetween(Duration.ofMillis(9500), Duration.ofSeconds(13));
    } finally {
      server.stop(0);
      executor.shutdownNow();
    }
  }

  @Test
  void telegramHttpFailurePreservesRetryAfterAndSendsUnmodifiedUtf8Text() throws Exception {
    var body = new AtomicReference<String>();
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/sendMessage",
        exchange -> {
          body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] response =
              "{\"ok\":false,\"parameters\":{\"retry_after\":90}}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(429, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      var sender =
          new HttpTelegramTransport(
              new TelegramSettings(true, "synthetic-token", "123456"),
              JsonMapper.builder().build(),
              HttpClient.newHttpClient(),
              URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/sendMessage"));
      var result =
          sender.send(
              "Покупка SBER. 13.11.2026 12:00:00 МСК\nИдентификатор уведомления: stable-id");
      assertThat(result.outcome()).isEqualTo(TelegramTransport.Outcome.FAILURE);
      assertThat(result.retryAfter()).isEqualTo(Duration.ofSeconds(90));
      assertThat(URLDecoder.decode(body.get(), StandardCharsets.UTF_8))
          .isEqualTo(
              "chat_id=123456&text=Покупка SBER. 13.11.2026 12:00:00 МСК\nИдентификатор уведомления: stable-id");
    } finally {
      server.stop(0);
    }
  }
}
