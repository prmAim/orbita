package ru.orbita.operations;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.OrbitaApplication;
import ru.orbita.control.ControlService;
import ru.orbita.runtime.ApplicationReadiness;
import ru.orbita.web.OwnerCredentials;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
class InitializationHttpIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine");

  @Test
  void authenticatedCsrfCommandCannotClearPreservedPauseBeforeRecoveryButRuntimeStopStaysAvailable()
      throws Exception {
    var probe = new Probe();
    var launch =
        CompletableFuture.supplyAsync(
            () ->
                new SpringApplicationBuilder(OrbitaApplication.class, Configuration.class)
                    .initializers(
                        context ->
                            context
                                .getBeanFactory()
                                .registerSingleton("httpInitializationProbe", probe))
                    .run(
                        "--server.port=0",
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--orbita.worker.enabled=false",
                        "--orbita.telegram.enabled=false",
                        "--orbita.backup.enabled=false"));
    var client =
        HttpClient.newBuilder()
            .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ALL))
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    try {
      assertThat(probe.seeded.await(45, TimeUnit.SECONDS)).isTrue();
      JsonNode token = csrf(client, probe.base);
      var login =
          client.send(
              HttpRequest.newBuilder(probe.base.resolve("/api/login"))
                  .header(token.get("headerName").asText(), token.get("token").asText())
                  .header("Content-Type", "application/x-www-form-urlencoded")
                  .POST(
                      HttpRequest.BodyPublishers.ofString(
                          "username=owner&password=synthetic-initialization-password"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(login.statusCode()).isEqualTo(204);
      token = csrf(client, probe.base);
      assertThat(
              command(client, probe.base, "/api/instruments/instrument/resume", null).statusCode())
          .isEqualTo(403);
      assertThat(
              command(client, probe.base, "/api/instruments/instrument/resume", token).statusCode())
          .isEqualTo(503);
      assertThat(instruments(client, probe.base).get(0).get("paused").asBoolean()).isTrue();
      probe.release.countDown();
      var context = launch.get(30, TimeUnit.SECONDS);
      var instrument = instruments(client, probe.base).get(0);
      assertThat(instrument.get("paused").asBoolean()).isTrue();
      assertThat(instrument.get("reconciled").asBoolean()).isFalse();
      context.getBean(ApplicationReadiness.class).brokerPersistenceUnavailable();
      assertThat(command(client, probe.base, "/api/control/stop", token).statusCode())
          .isEqualTo(200);
      assertThat(command(client, probe.base, "/api/logout", token).statusCode()).isEqualTo(204);
    } finally {
      probe.release.countDown();
      launch.get(30, TimeUnit.SECONDS).close();
    }
  }

  private static JsonNode csrf(HttpClient client, URI base) throws Exception {
    var response =
        client.send(
            HttpRequest.newBuilder(base.resolve("/api/csrf")).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    return JsonMapper.builder().build().readTree(response.body());
  }

  private static JsonNode instruments(HttpClient client, URI base) throws Exception {
    var response =
        client.send(
            HttpRequest.newBuilder(base.resolve("/api/instruments")).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    return JsonMapper.builder().build().readTree(response.body());
  }

  private static HttpResponse<String> command(
      HttpClient client, URI base, String path, JsonNode csrf) throws Exception {
    var request =
        HttpRequest.newBuilder(base.resolve(path)).POST(HttpRequest.BodyPublishers.noBody());
    if (csrf != null) request.header(csrf.get("headerName").asText(), csrf.get("token").asText());
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  static class Probe {
    final CountDownLatch seeded = new CountDownLatch(1), release = new CountDownLatch(1);
    URI base;
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class Configuration {
    @Bean
    ApplicationRunner pausedPreviousProcess(
        Probe probe,
        OwnerCredentials credentials,
        ControlService control,
        ConfigurableApplicationContext context) {
      return args -> {
        credentials.replace("synthetic-initialization-password".toCharArray());
        control.pauseInstrument("instrument", "Сохранённая пауза прежнего процесса");
        control.instrumentReconciled("instrument", true, true, null);
        probe.base =
            URI.create(
                "http://127.0.0.1:"
                    + context.getEnvironment().getRequiredProperty("local.server.port"));
        probe.seeded.countDown();
        if (!probe.release.await(20, TimeUnit.SECONDS))
          throw new IllegalStateException("HTTP startup test barrier timed out");
      };
    }
  }
}
