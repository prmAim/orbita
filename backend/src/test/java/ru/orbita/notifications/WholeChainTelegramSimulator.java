package ru.orbita.notifications;

import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import tools.jackson.databind.json.JsonMapper;

/** Real production HTTP transport directed exclusively to a local synthetic Telegram endpoint. */
public final class WholeChainTelegramSimulator implements AutoCloseable {
  private final HttpServer server;
  private final Queue<Boolean> failures = new ConcurrentLinkedQueue<>();
  private final List<String> payloads = new CopyOnWriteArrayList<>();

  public WholeChainTelegramSimulator() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/sendMessage",
        exchange -> {
          if (!exchange.getRemoteAddress().getAddress().isLoopbackAddress())
            throw new IllegalStateException("Loopback only");
          String body =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          String text =
              Arrays.stream(body.split("&"))
                  .filter(p -> p.startsWith("text="))
                  .map(p -> URLDecoder.decode(p.substring(5), StandardCharsets.UTF_8))
                  .findFirst()
                  .orElseThrow();
          payloads.add(text);
          boolean failed = Boolean.TRUE.equals(failures.poll());
          byte[] response =
              (failed
                      ? "{\"ok\":false,\"description\":\"[ТЕСТ] локальная ошибка доставки\"}"
                      : "{\"ok\":true,\"result\":{\"message_id\":1}}")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(failed ? 500 : 200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
  }

  public void failNext() {
    failures.add(true);
  }

  public List<String> payloads() {
    return List.copyOf(payloads);
  }

  public TelegramTransport transport(TelegramSettings settings, JsonMapper json) {
    return new HttpTelegramTransport(
        settings,
        json,
        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build(),
        URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/sendMessage"));
  }

  public void close() {
    server.stop(0);
  }
}
