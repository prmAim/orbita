package ru.orbita.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.stub.StreamObserver;
import java.net.*;
import java.net.http.*;
import java.nio.file.Files;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.account.*;
import ru.orbita.control.ControlService;
import ru.orbita.web.OwnerCredentials;
import ru.tinkoff.piapi.contract.v1.*;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "orbita.source-account=source",
      "orbita.mirror-account=mirror",
      "orbita.worker.enabled=false",
      "orbita.account-read.enabled=false"
    })
@Import(AccountReadIT.Fixture.class)
class AccountReadIT {
  @Autowired AccountReadService reads;
  @Autowired Bank bank;
  @Autowired JdbcTemplate db;
  @Autowired ControlService control;
  @Autowired OwnerCredentials credentials;
  @Autowired BrokerGateway gateway;
  @Autowired PostgreSQLContainer postgres;
  @Autowired org.springframework.beans.factory.ObjectProvider<BrokerGateway> gateways;
  @Autowired ru.orbita.runtime.ApplicationReadiness readiness;
  @LocalServerPort int port;

  @BeforeEach
  void reset() {
    db.execute(
        "truncate account_read_document,account_read_freshness,account_read_revision,instrument_state,journal_entry,notification_attempt,notification restart identity cascade");
    bank.reset();
  }

  @Test
  void realSdkFactsPersistWithWeightedPriceAndGlobalPagination() {
    reads.refresh();
    var page = reads.page("orders", AccountQuery.defaults());
    assertThat(page.total()).isEqualTo(2);
    assertThat(page.items().getFirst().get("origin")).isEqualTo("UNKNOWN");
    assertThat(price(page.items().getFirst()).get("value")).isEqualTo("106");
    reads.refresh();
    assertThat(reads.page("orders", AccountQuery.defaults()).total()).isEqualTo(2);
    assertThat(bank.mutations).isZero();
  }

  @Test
  void moreThanOneHundredRowsAreFilteredSortedPagedAndExactIdsIgnorePage() {
    bank.orders.clear();
    for (int i = 0; i < 123; i++) {
      var o = order("bulk/" + i).toBuilder().setTicker("SYN-" + i).build();
      bank.orders.put(o.getOrderId(), o);
    }
    reads.refresh();
    var q =
        AccountQuery.parse(
            Map.of("page", "2", "size", "50", "sort", "instrument", "direction", "asc"));
    var page = reads.page("orders", q);
    assertThat(page.total()).isEqualTo(123);
    assertThat(page.items()).hasSize(23);
    var again = reads.page("orders", q);
    assertThat(again.items()).isEqualTo(page.items());
    var filtered =
        reads.page(
            "orders",
            AccountQuery.parse(Map.of("q", "SYN-122", "origin", "UNKNOWN", "side", "SELL")));
    assertThat(filtered.total()).isEqualTo(1);
    var exact = reads.exact("ORDER", filtered.items().getFirst().get("id").toString());
    assertThat(exact.get("brokerOrderId")).isEqualTo("bulk/122");
    assertThat(
            reads
                .exact(
                    "FILL",
                    ((List<Map<String, Object>>) exact.get("fills"))
                        .getFirst()
                        .get("id")
                        .toString())
                .get("orderId"))
        .isEqualTo(exact.get("id"));
  }

  @Test
  void partialFailuresRetainRowsAndLaterActualFillUpdates106To107() {
    reads.refresh();
    String id =
        reads.page("orders", AccountQuery.defaults()).items().stream()
            .filter(o -> o.get("brokerOrderId").equals("[ТЕСТ]/один"))
            .findFirst()
            .orElseThrow()
            .get("id")
            .toString();
    var old = bank.orders.get("[ТЕСТ]/один");
    bank.orders.put(
        old.getOrderId(),
        old.toBuilder().setLotsExecuted(6).addStages(stage("c", 1, 112, bank.at + 1)).build());
    reads.refresh();
    assertThat(price(reads.exact("ORDER", id)).get("value")).isEqualTo("107");
    bank.fail = true;
    reads.refresh();
    assertThat(reads.page("orders", AccountQuery.defaults()).total()).isEqualTo(2);
    assertThat(price(reads.exact("ORDER", id)).get("value")).isEqualTo("107");
    assertThat(
            ((Map<?, ?>) reads.page("orders", AccountQuery.defaults()).freshness().get("orders"))
                .get("status"))
        .isEqualTo("ERROR");
    bank.fail = false;
    bank.orders.put(old.getOrderId(), old.toBuilder().setLotsExecuted(3).build());
    reads.refresh();
    var retained = reads.exact("ORDER", id);
    assertThat(retained.get("executedLots")).isEqualTo("6");
    assertThat((List<?>) retained.get("fills")).hasSize(3);
    assertThat(price(retained).get("status")).isEqualTo("UNCONFIRMED");
  }

  @Test
  void latestSaleOrderPriceAndOfflinePauseReasonUseActualReadModel() {
    bank.orders.clear();
    bank.orders.put(
        "sale",
        order("sale").toBuilder()
            .setOrderDate(timestamp(bank.at - 20))
            .clearStages()
            .addStages(stage("a", 2, 100, bank.at))
            .addStages(stage("b", 3, 110, bank.at + 1))
            .build());
    reads.refresh();
    var asset = reads.page("assets", AccountQuery.defaults()).items().getFirst();
    assertThat(price(asset).get("value")).isEqualTo("106");
    assertThat(asset.get("lastOrderId"))
        .isEqualTo(reads.page("orders", AccountQuery.defaults()).items().getFirst().get("id"));
    control.pauseInstrument("security", "Synthetic full pause reason");
    bank.fail = true;
    asset = reads.page("assets", AccountQuery.defaults()).items().getFirst();
    assertThat(asset.get("state")).isEqualTo("PAUSED");
    assertThat(asset.get("reason")).isEqualTo("Synthetic full pause reason");
    assertThat(reads.journal(Long.parseLong(asset.get("journalId").toString())).get("detail"))
        .isEqualTo("Synthetic full pause reason");
    assertThat(bank.mutations).isZero();
  }

  @Test
  void ownerHttpAuthenticationValidationExactLinksAndSessionRevocation() throws Exception {
    reads.refresh();
    var client =
        HttpClient.newBuilder()
            .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ALL))
            .build();
    assertThat(get(client, "/api/account/orders").statusCode()).isEqualTo(401);
    credentials.replace("synthetic-account-read-password".toCharArray());
    login(client, "synthetic-account-read-password");
    var response = get(client, "/api/account/orders");
    assertThat(response.statusCode()).isEqualTo(200);
    var json = JsonMapper.builder().build().readTree(response.body());
    String id = json.get("items").get(0).get("id").asText();
    assertThat(
            get(
                    client,
                    "/api/account/order?id="
                        + URLEncoder.encode(id, java.nio.charset.StandardCharsets.UTF_8))
                .statusCode())
        .isEqualTo(200);
    assertThat(get(client, "/api/account/orders?size=7").statusCode()).isEqualTo(400);
    assertThat(get(client, "/api/account/orders?sort=unsafe").statusCode()).isEqualTo(400);
    assertThat(get(client, "/api/account/order?id=missing").statusCode()).isEqualTo(404);
    captureHttpExamples(client, id);
    credentials.replace("synthetic-revoked-account-password".toCharArray());
    assertThat(get(client, "/api/account/orders").statusCode()).isEqualTo(401);
    assertThat(bank.mutations).isZero();
  }

  @Test
  void positiveSdkCashUpdatesPersistWithoutInventingOrDeletingCurrencies() throws Exception {
    reads.refresh();
    try (var subscription = gateway.observeCash(reads::acceptCash)) {
      long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (reads.page("assets", AccountQuery.defaults()).cash().size() < 2
          && System.nanoTime() < until) Thread.sleep(20);
      var cash = reads.page("assets", AccountQuery.defaults()).cash();
      assertThat(cash).hasSize(2);
      var rub =
          cash.stream().filter(c -> c.get("currency").equals("rub")).findFirst().orElseThrow();
      assertThat(rub.get("total")).isEqualTo("1000");
      assertThat(rub.get("available")).isEqualTo("800");
    }
    var before = reads.page("assets", AccountQuery.defaults());
    var stamp =
        before.cash().stream()
            .filter(c -> c.get("currency").equals("rub"))
            .findFirst()
            .orElseThrow()
            .get("updatedAt");
    var success = ((Map<?, ?>) before.freshness().get("cash")).get("lastSuccessAt");
    Thread.sleep(20);
    reads.refresh();
    var after = reads.page("assets", AccountQuery.defaults());
    var cash = after.cash();
    assertThat(
            cash.stream()
                .filter(c -> c.get("currency").equals("rub"))
                .findFirst()
                .orElseThrow()
                .get("updatedAt"))
        .isEqualTo(stamp);
    assertThat(((Map<?, ?>) after.freshness().get("cash")).get("lastSuccessAt")).isEqualTo(success);
    assertThat(((Map<?, ?>) after.freshness().get("cash")).get("status")).isNotEqualTo("FRESH");
    assertThat(cash).hasSize(2);
    assertThat(
            cash.stream()
                .filter(c -> c.get("currency").equals("usd"))
                .findFirst()
                .orElseThrow()
                .get("available"))
        .isEqualTo("1.234567891");
  }

  @Test
  void newerUnlinkedActualExecutionMakesAssetPriceUnknownAndCompleteEmptyRemovesOnlyAsset() {
    bank.orders.clear();
    bank.orders.put("sale", order("sale"));
    reads.refresh();
    assertThat(price(reads.page("assets", AccountQuery.defaults()).items().getFirst()).get("value"))
        .isEqualTo("106");
    bank.unlinked =
        OperationItem.newBuilder()
            .setBrokerAccountId("mirror")
            .setId("unlinked-operation")
            .setInstrumentUid("security")
            .setType(OperationType.OPERATION_TYPE_SELL)
            .setState(OperationState.OPERATION_STATE_EXECUTED)
            .setTradesInfo(
                OperationItemTrades.newBuilder()
                    .addTrades(
                        OperationItemTrade.newBuilder()
                            .setNum("unlinked-trade")
                            .setQuantity(10)
                            .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(112))
                            .setDate(timestamp(Instant.now().getEpochSecond() - 1))))
            .build();
    reads.refresh();
    var asset = reads.page("assets", AccountQuery.defaults()).items().getFirst();
    assertThat(price(asset).get("status")).isEqualTo("UNCONFIRMED");
    assertThat(asset.get("lastOrderId")).isNull();
    bank.fail = true;
    reads.refresh();
    assertThat(reads.page("assets", AccountQuery.defaults()).total()).isEqualTo(1);
    bank.fail = false;
    bank.emptyPortfolio = true;
    reads.refresh();
    assertThat(reads.page("assets", AccountQuery.defaults()).total()).isZero();
    assertThat(reads.page("operations", AccountQuery.defaults()).total()).isGreaterThan(0);
  }

  @Test
  void restartWithoutGatewayOrConfiguredAccountRetainsExactRowsAndOwnership() {
    reads.refresh();
    var before = reads.page("orders", AccountQuery.defaults());
    String id = before.items().getFirst().get("id").toString();
    try (var offline =
        new org.springframework.boot.builder.SpringApplicationBuilder(
                ru.orbita.OrbitaApplication.class)
            .run(
                "--server.port=0",
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--orbita.worker.enabled=false",
                "--orbita.broker.enabled=false",
                "--orbita.account-read.enabled=false",
                "--orbita.source-account=",
                "--orbita.mirror-account=")) {
      var restored = offline.getBean(AccountReadService.class);
      assertThat(restored.page("orders", AccountQuery.defaults()).total())
          .isEqualTo(before.total());
      assertThat(restored.exact("ORDER", id).get("brokerOrderId"))
          .isEqualTo(before.items().getFirst().get("brokerOrderId"));
      restored.refresh();
      assertThat(restored.page("orders", AccountQuery.defaults()).total())
          .isEqualTo(before.total());
    }
  }

  @Test
  void cashPersistenceFailureClosesAndRecreatesDedicatedSubscriptionAfterDatabaseRecovery()
      throws Exception {
    db.execute(
        "alter table account_read_document add constraint synthetic_cash_failure check(kind<>'CASH' or payload->>'bankAt' is null) not valid");
    try (var lifecycle = new AccountReadLifecycle(gateways, reads, readiness, Clock.systemUTC())) {
      lifecycle.refresh();
      long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (bank.cashConnections < 1 && System.nanoTime() < until) Thread.sleep(20);
      assertThat(bank.cashConnections).isEqualTo(1);
      // Ensure the asynchronous failing callback has completed before removing the fault.
      until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (bank.cashCancelled.get() < 1 && System.nanoTime() < until) Thread.sleep(20);
      assertThat(bank.cashCancelled.get()).isEqualTo(1);
      db.execute("alter table account_read_document drop constraint synthetic_cash_failure");
      lifecycle.refresh();
      until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while ((bank.cashConnections < 2
              || reads.page("assets", AccountQuery.defaults()).cash().size() < 2)
          && System.nanoTime() < until) Thread.sleep(20);
      assertThat(bank.cashConnections).isEqualTo(2);
      assertThat(reads.page("assets", AccountQuery.defaults()).cash()).hasSize(2);
    } finally {
      db.execute(
          "alter table account_read_document drop constraint if exists synthetic_cash_failure");
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2})
  void partialCashStreamRetainsCoherentTupleAcrossOfflineReadAndFullUpdateRestoresIt(int mode)
      throws Exception {
    reads.refresh();
    bank.cashOnlyRub = true;
    credentials.replace("synthetic-partial-cash-password".toCharArray());
    var client =
        HttpClient.newBuilder()
            .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ALL))
            .build();
    login(client, "synthetic-partial-cash-password");
    String attempt =
        JsonMapper.builder()
            .build()
            .readTree(get(client, "/api/account/assets").body())
            .get("freshness")
            .get("cash")
            .get("lastAttemptAt")
            .asText();
    try (var feed = gateway.observeCash(reads::acceptCash)) {
      awaitCashAttempt(client, attempt);
    }
    var before = JsonMapper.builder().build().readTree(get(client, "/api/account/assets").body());
    var coherent = before.get("cash").get(0);
    assertThat(coherent.get("total").asText()).isEqualTo("1000");
    bank.cashMode = mode;
    bank.cashAt++;
    bank.cashAvailable = 900;
    bank.cashBlocked = 300;
    attempt = before.get("freshness").get("cash").get("lastAttemptAt").asText();
    try (var feed = gateway.observeCash(reads::acceptCash)) {
      awaitCashAttempt(client, attempt);
    }
    var partial =
        JsonMapper.builder()
            .build()
            .readTree(get(client, "/api/account/assets").body())
            .get("cash")
            .get(0);
    assertThat(partial.get("total").asText()).isEqualTo("1000");
    assertThat(partial.get("available").asText()).isEqualTo("800");
    assertThat(partial.get("blocked").asText()).isEqualTo("200");
    assertThat(partial.get("bankAt")).isEqualTo(coherent.get("bankAt"));
    assertThat(partial.get("updatedAt")).isEqualTo(coherent.get("updatedAt"));
    assertThat(partial.get("freshness").asText()).isEqualTo("UNCONFIRMED");
    assertThat(partial.get("limitation").asText()).contains("Partial currency observation");
    captureExample(
        "assets-partial-cash-" + mode + ".json", get(client, "/api/account/assets").body());
    try (var offline =
        new org.springframework.boot.builder.SpringApplicationBuilder(
                ru.orbita.OrbitaApplication.class)
            .run(
                "--server.port=0",
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--orbita.worker.enabled=false",
                "--orbita.broker.enabled=false",
                "--orbita.account-read.enabled=false",
                "--orbita.source-account=",
                "--orbita.mirror-account=")) {
      int offlinePort =
          ((org.springframework.boot.web.server.context.WebServerApplicationContext) offline)
              .getWebServer()
              .getPort();
      var offlineClient =
          HttpClient.newBuilder()
              .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ALL))
              .build();
      login(offlineClient, "synthetic-partial-cash-password", offlinePort);
      var saved =
          JsonMapper.builder()
              .build()
              .readTree(get(offlineClient, offlinePort, "/api/account/assets").body())
              .get("cash")
              .get(0);
      assertThat(saved.get("total")).isEqualTo(partial.get("total"));
      assertThat(saved.get("bankAt")).isEqualTo(partial.get("bankAt"));
      assertThat(saved.get("updatedAt")).isEqualTo(partial.get("updatedAt"));
      assertThat(saved.get("freshness").asText()).isEqualTo("UNCONFIRMED");
    }
    attempt =
        JsonMapper.builder()
            .build()
            .readTree(get(client, "/api/account/assets").body())
            .get("freshness")
            .get("cash")
            .get("lastAttemptAt")
            .asText();
    bank.cashMode = 0;
    bank.cashAt++;
    try (var feed = gateway.observeCash(reads::acceptCash)) {
      awaitCashAttempt(client, attempt);
    }
    var restored =
        JsonMapper.builder()
            .build()
            .readTree(get(client, "/api/account/assets").body())
            .get("cash")
            .get(0);
    assertThat(restored.get("total").asText()).isEqualTo("1200");
    assertThat(restored.get("available").asText()).isEqualTo("900");
    assertThat(restored.get("blocked").asText()).isEqualTo("300");
    assertThat(restored.get("freshness").asText()).isEqualTo("FRESH");
    assertThat(restored.get("limitation").asText()).doesNotContain("Partial currency observation");
    captureExample(
        "assets-restored-cash-" + mode + ".json", get(client, "/api/account/assets").body());
  }

  private void awaitCashAttempt(HttpClient client, String before) throws Exception {
    long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < until) {
      var page = JsonMapper.builder().build().readTree(get(client, "/api/account/assets").body());
      if (!page.get("freshness").get("cash").get("lastAttemptAt").asText().equals(before)) return;
      Thread.sleep(20);
    }
    throw new AssertionError("Cash observation was not persisted");
  }

  private void captureExample(String name, String body) throws Exception {
    String target = System.getProperty("orbita.http.examples.dir");
    if (target == null) return;
    var directory = java.nio.file.Path.of(target);
    Files.createDirectories(directory);
    Files.writeString(directory.resolve(name), body + "\n");
  }

  private void captureHttpExamples(HttpClient client, String orderId) throws Exception {
    String target = System.getProperty("orbita.http.examples.dir");
    if (target == null) return;
    var directory = java.nio.file.Path.of(target);
    Files.createDirectories(directory);
    var mapper = JsonMapper.builder().build();
    try (var feed = gateway.observeCash(reads::acceptCash)) {
      long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (mapper.readTree(get(client, "/api/account/assets").body()).get("cash").size() < 2
          && System.nanoTime() < until) Thread.sleep(20);
    }
    control.pauseInstrument("security", "[ТЕСТ] Пример полной причины паузы");
    for (String route : List.of("assets", "orders", "operations")) {
      var reply = get(client, "/api/account/" + route);
      assertThat(reply.statusCode()).isEqualTo(200);
      Files.writeString(directory.resolve(route + ".json"), reply.body() + "\n");
    }
    var detail =
        get(
            client,
            "/api/account/order?id="
                + URLEncoder.encode(orderId, java.nio.charset.StandardCharsets.UTF_8));
    Files.writeString(directory.resolve("order.json"), detail.body() + "\n");
    String fill = mapper.readTree(detail.body()).get("fills").get(0).get("id").asText();
    var reply =
        get(
            client,
            "/api/account/fill?id="
                + URLEncoder.encode(fill, java.nio.charset.StandardCharsets.UTF_8));
    Files.writeString(directory.resolve("fill.json"), reply.body() + "\n");
    var assets = mapper.readTree(Files.readString(directory.resolve("assets.json")));
    Files.writeString(
        directory.resolve("cash.json"), mapper.writeValueAsString(assets.get("cash")) + "\n");
    String journal = assets.get("items").get(0).get("journalId").asText();
    Files.writeString(
        directory.resolve("journal.json"),
        get(client, "/api/account/journal?id=" + journal).body() + "\n");
    Files.writeString(
        directory.resolve("README.txt"),
        "SYNTHETIC ONLY. Actual owner HTTP response bodies from AccountReadIT, backed by local official SDK/TLS simulator and disposable PostgreSQL. Captured "
            + Instant.now()
            + ". cash.json is the cash array extracted from assets.json. No live broker/Telegram.\n");
  }

  private HttpResponse<String> get(HttpClient client, int targetPort, String path)
      throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + targetPort + path)).GET().build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private void login(HttpClient client, String password, int targetPort) throws Exception {
    var csrf = JsonMapper.builder().build().readTree(get(client, targetPort, "/api/csrf").body());
    var reply =
        client.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + targetPort + "/api/login"))
                .header(csrf.get("headerName").asText(), csrf.get("token").asText())
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("username=owner&password=" + password))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(reply.statusCode()).isEqualTo(204);
  }

  private HttpResponse<String> get(HttpClient client, String path) throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private void login(HttpClient client, String password) throws Exception {
    var csrf = JsonMapper.builder().build().readTree(get(client, "/api/csrf").body());
    var reply =
        client.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/login"))
                .header(csrf.get("headerName").asText(), csrf.get("token").asText())
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("username=owner&password=" + password))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(reply.statusCode()).isEqualTo(204);
  }

  private static Map<?, ?> price(Map<String, Object> row) {
    return (Map<?, ?>) row.get("executionPrice");
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class Fixture {
    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
      return new PostgreSQLContainer("postgres:17.11-alpine");
    }

    @Bean
    Bank bank() {
      return new Bank();
    }

    @Bean(destroyMethod = "close")
    BrokerTlsTestSupport.Simulator tls(Bank bank) throws Exception {
      return BrokerTlsTestSupport.open(
          Files.createTempDirectory("orbita-read-it"),
          bank.ordersService(),
          bank.operationsService(),
          bank.instrumentsService(),
          bank.cashService());
    }

    @Bean(destroyMethod = "close")
    BrokerGateway gateway(BrokerTlsTestSupport.Simulator tls) throws Exception {
      return BrokerTlsTestSupport.gateway(
          tls.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS);
    }
  }

  static final class Bank {
    final Map<String, OrderState> orders = new LinkedHashMap<>();
    volatile boolean fail;
    boolean emptyPortfolio;
    OperationItem unlinked;
    int mutations;
    volatile int cashConnections;
    final java.util.concurrent.atomic.AtomicInteger cashCancelled =
        new java.util.concurrent.atomic.AtomicInteger();
    long at;
    int cashMode;
    long cashAt, cashAvailable, cashBlocked;
    boolean cashOnlyRub;

    void reset() {
      orders.clear();
      at = Instant.now().getEpochSecond() - 100;
      fail = false;
      emptyPortfolio = false;
      unlinked = null;
      mutations = 0;
      cashConnections = 0;
      cashMode = 0;
      cashAt = Instant.now().getEpochSecond() - 5;
      cashAvailable = 800;
      cashBlocked = 200;
      cashOnlyRub = false;
      cashCancelled.set(0);
      orders.put("[ТЕСТ]/один", order("[ТЕСТ]/один"));
      orders.put("two", order("two"));
    }

    OrdersServiceGrpc.OrdersServiceImplBase ordersService() {
      return new OrdersServiceGrpc.OrdersServiceImplBase() {
        public void getOrders(GetOrdersRequest request, StreamObserver<GetOrdersResponse> out) {
          if (fail) {
            out.onError(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());
            return;
          }
          out.onNext(
              request.hasAdvancedFilters()
                  ? GetOrdersResponse.getDefaultInstance()
                  : GetOrdersResponse.newBuilder().addAllOrders(orders.values()).build());
          out.onCompleted();
        }

        public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
          if (fail || !orders.containsKey(request.getOrderId())) {
            out.onError(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());
            return;
          }
          out.onNext(orders.get(request.getOrderId()));
          out.onCompleted();
        }

        public void postOrder(PostOrderRequest request, StreamObserver<PostOrderResponse> out) {
          mutations++;
          out.onError(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());
        }

        public void cancelOrder(
            CancelOrderRequest request, StreamObserver<CancelOrderResponse> out) {
          mutations++;
          out.onError(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());
        }
      };
    }

    OperationsServiceGrpc.OperationsServiceImplBase operationsService() {
      return new OperationsServiceGrpc.OperationsServiceImplBase() {
        public void getPortfolio(PortfolioRequest request, StreamObserver<PortfolioResponse> out) {
          if (fail) {
            out.onError(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());
            return;
          }
          if (emptyPortfolio) {
            out.onNext(PortfolioResponse.newBuilder().setAccountId(request.getAccountId()).build());
            out.onCompleted();
            return;
          }
          out.onNext(
              PortfolioResponse.newBuilder()
                  .setAccountId(request.getAccountId())
                  .addPositions(
                      PortfolioPosition.newBuilder()
                          .setInstrumentUid("security")
                          .setTicker("SYN")
                          .setInstrumentType("share")
                          .setQuantity(Quotation.newBuilder().setUnits(30))
                          .setQuantityLots(Quotation.newBuilder().setUnits(3)))
                  .build());
          out.onCompleted();
        }

        public void getPositions(PositionsRequest request, StreamObserver<PositionsResponse> out) {
          out.onNext(
              PositionsResponse.newBuilder()
                  .setAccountId(request.getAccountId())
                  .addMoney(MoneyValue.newBuilder().setCurrency("rub").setUnits(1000))
                  .build());
          out.onCompleted();
        }

        public void getOperationsByCursor(
            GetOperationsByCursorRequest request,
            StreamObserver<GetOperationsByCursorResponse> out) {
          out.onNext(
              unlinked == null
                  ? GetOperationsByCursorResponse.getDefaultInstance()
                  : GetOperationsByCursorResponse.newBuilder().addItems(unlinked).build());
          out.onCompleted();
        }
      };
    }

    OperationsStreamServiceGrpc.OperationsStreamServiceImplBase cashService() {
      return new OperationsStreamServiceGrpc.OperationsStreamServiceImplBase() {
        public void positionsStream(
            PositionsStreamRequest request, StreamObserver<PositionsStreamResponse> out) {
          cashConnections++;
          ((io.grpc.stub.ServerCallStreamObserver<PositionsStreamResponse>) out)
              .setOnCancelHandler(cashCancelled::incrementAndGet);
          var money = PositionsMoney.newBuilder();
          if (cashMode != 2)
            money.setAvailableValue(
                MoneyValue.newBuilder().setCurrency("rub").setUnits(cashAvailable));
          if (cashMode != 1)
            money.setBlockedValue(MoneyValue.newBuilder().setCurrency("rub").setUnits(cashBlocked));
          var data =
              PositionData.newBuilder()
                  .setAccountId("mirror")
                  .setDate(timestamp(cashAt))
                  .addMoney(money);
          if (!cashOnlyRub)
            data.addMoney(
                PositionsMoney.newBuilder()
                    .setAvailableValue(
                        MoneyValue.newBuilder().setCurrency("usd").setUnits(1).setNano(234567891))
                    .setBlockedValue(MoneyValue.newBuilder().setCurrency("usd")));
          out.onNext(PositionsStreamResponse.newBuilder().setPosition(data).build());
        }
      };
    }

    InstrumentsServiceGrpc.InstrumentsServiceImplBase instrumentsService() {
      return new InstrumentsServiceGrpc.InstrumentsServiceImplBase() {
        public void getInstrumentBy(
            InstrumentRequest request, StreamObserver<InstrumentResponse> out) {
          out.onNext(
              InstrumentResponse.newBuilder()
                  .setInstrument(
                      Instrument.newBuilder()
                          .setUid(request.getId())
                          .setTicker("SYN")
                          .setName("Synthetic security")
                          .setLot(10)
                          .setCurrency("rub")
                          .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                  .build());
          out.onCompleted();
        }
      };
    }
  }

  static OrderState order(String id) {
    long at = Instant.now().getEpochSecond() - 100;
    return OrderState.newBuilder()
        .setOrderId(id)
        .setInstrumentUid("security")
        .setTicker("SYN")
        .setDirection(OrderDirection.ORDER_DIRECTION_SELL)
        .setOrderType(OrderType.ORDER_TYPE_LIMIT)
        .setInitialSecurityPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(120))
        .setLotsRequested(10)
        .setLotsExecuted(5)
        .setOrderDate(timestamp(at - 10))
        .setExecutionReportStatus(OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL)
        .addStages(stage("a", 2, 100, at))
        .addStages(stage("b", 3, 110, at + 1))
        .build();
  }

  static com.google.protobuf.Timestamp timestamp(long seconds) {
    return com.google.protobuf.Timestamp.newBuilder().setSeconds(seconds).build();
  }

  static OrderStage stage(String id, long qty, long price, long at) {
    return OrderStage.newBuilder()
        .setTradeId(id)
        .setQuantity(qty)
        .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(price))
        .setExecutionTime(timestamp(at))
        .build();
  }
}
