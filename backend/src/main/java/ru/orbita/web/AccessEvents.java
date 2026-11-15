package ru.orbita.web;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** LISTEN/NOTIFY conveys local command revocations to every open tab after commit. */
@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
final class AccessEvents {
  private final DataSource datasource;
  private final OwnerCredentials credentials;
  private final Map<SseEmitter, Long> clients = new ConcurrentHashMap<>();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final CountDownLatch ready = new CountDownLatch(1);
  private volatile Connection connection;
  private Thread listener;

  AccessEvents(DataSource datasource, OwnerCredentials credentials) {
    this.datasource = datasource;
    this.credentials = credentials;
  }

  @PostConstruct
  void start() {
    listener = Thread.ofVirtual().name("orbita-access-revocation").start(this::listen);
  }

  @GetMapping(value = "/api/access-events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  SseEmitter events(@AuthenticationPrincipal OwnerCredentials.Owner owner)
      throws IOException, InterruptedException {
    if (owner == null || !ready.await(5, TimeUnit.SECONDS))
      throw new IllegalStateException("Канал доступа недоступен");
    var emitter = new SseEmitter(0L);
    clients.put(emitter, owner.generation());
    emitter.onCompletion(() -> clients.remove(emitter));
    emitter.onTimeout(() -> clients.remove(emitter));
    emitter.onError(error -> clients.remove(emitter));
    if (owner.generation() != credentials.generation()) revoke(emitter);
    else emitter.send(SseEmitter.event().name("ready").data("connected"));
    return emitter;
  }

  private void listen() {
    while (running.get()) {
      try (Connection current = datasource.getConnection()) {
        connection = current;
        try (var statement = current.createStatement()) {
          statement.execute("LISTEN orbita_access_changed");
        }
        ready.countDown();
        PGConnection postgres = current.unwrap(PGConnection.class);
        long heartbeat = System.nanoTime();
        while (running.get()) {
          var notifications = postgres.getNotifications(1000);
          if (notifications != null)
            for (var notification : notifications) {
              long generation = Long.parseLong(notification.getParameter());
              clients.forEach(
                  (emitter, original) -> {
                    if (original != generation) revoke(emitter);
                  });
            }
          if (System.nanoTime() - heartbeat >= TimeUnit.SECONDS.toNanos(10)) {
            heartbeat = System.nanoTime();
            clients.forEach(
                (emitter, generation) -> {
                  try {
                    emitter.send(SseEmitter.event().comment("access connection alive"));
                  } catch (IOException | RuntimeException failure) {
                    clients.remove(emitter);
                    emitter.completeWithError(failure);
                  }
                });
          }
        }
      } catch (Exception failure) {
        clients.forEach(
            (emitter, generation) ->
                emitter.completeWithError(new IOException("Канал доступа закрыт")));
        clients.clear();
        if (running.get())
          try {
            Thread.sleep(1000);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
          }
      } finally {
        connection = null;
      }
    }
  }

  private void revoke(SseEmitter emitter) {
    try {
      emitter.send(SseEmitter.event().name("access-revoked").data("locked"));
      emitter.complete();
    } catch (IOException | RuntimeException failure) {
      emitter.completeWithError(failure);
    } finally {
      clients.remove(emitter);
    }
  }

  @PreDestroy
  void close() {
    running.set(false);
    listener.interrupt();
    var current = connection;
    if (current != null)
      try {
        current.close();
      } catch (java.sql.SQLException ignored) {
      }
    clients.forEach((emitter, generation) -> emitter.complete());
    clients.clear();
  }
}
