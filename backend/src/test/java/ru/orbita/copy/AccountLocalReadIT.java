package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.orbita.account.*;
import ru.orbita.broker.*;
import ru.orbita.control.*;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "orbita.source-account=source",
      "orbita.mirror-account=mirror",
      "orbita.clock-uncertainty-millis=0",
      "orbita.margin-disabled-confirmed=true",
      "orbita.worker.enabled=false",
      "orbita.account-read.enabled=false"
    })
@Import({CopyLifecycleIT.Fixture.class, AccountLocalReadIT.ReadFixture.class})
class AccountLocalReadIT {
  @Autowired AccountReadService reads;
  @Autowired ControlService control;
  @Autowired CopyService copies;
  @Autowired TradingService trading;
  @Autowired ReadSimulator broker;
  @Autowired CopyLifecycleIT.MutableClock clock;
  @Autowired JdbcTemplate db;
  @Autowired AlgorithmVersion algorithms;
  @Autowired ManualReconciliationService manual;
  @Autowired ru.orbita.web.OwnerCredentials credentials;
  @org.springframework.boot.test.web.server.LocalServerPort int port;

  @BeforeEach
  void reset() {
    CopyLifecycleIT.reset(db, broker, clock);
    db.execute("truncate account_read_document,account_read_freshness,account_read_revision");
    broker.displayOrders = List.of();
  }

  @Test
  void unknownOwnIntentHasStableIdentityWithoutBankIdAndFinalMovesToHistory() throws Exception {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    var decision =
        copies.accept(
            new BrokerGateway.SourceExecution(
                "source",
                "local-source",
                "local-source-order",
                "instrument",
                BrokerGateway.Side.BUY,
                BigDecimal.ONE,
                new BigDecimal("100"),
                "RUB",
                clock.instant(),
                clock.instant()));
    assertThat(reads.page("orders", AccountQuery.defaults()).items()).isEmpty();
    var operation = reads.page("operations", AccountQuery.defaults()).items().getFirst();
    var waiting = reads.exact("ORDER", ((List<?>) operation.get("orderIds")).getFirst().toString());
    assertThat(waiting.get("origin")).isEqualTo("ORBITA");
    assertThat(waiting.get("brokerOrderId")).isNull();
    trading.tick();
    var unknown = reads.page("orders", AccountQuery.defaults()).items().getFirst();
    assertThat(unknown.get("status")).isEqualTo("UNKNOWN");
    assertThat(unknown.get("id")).isEqualTo(waiting.get("id"));
    trading.tick();
    trading.tick();
    assertThat(reads.page("orders", AccountQuery.defaults()).total()).isZero();
    var group =
        reads.page("operations", AccountQuery.defaults()).items().stream()
            .filter(o -> decision.executionId().equals(o.get("sourceExecutionId")))
            .findFirst()
            .orElseThrow();
    assertThat(group.get("executedLots")).isEqualTo("1");
    assertThat(((Map<?, ?>) group.get("executionPrice")).get("value")).isEqualTo("100.5");
    assertThat(((List<?>) group.get("fills"))).hasSize(1);
    var http = ownerClient();
    var linkedOrder = httpJson(http, "/api/account/order?id=" + encodedQuery(waiting.get("id")));
    var fillId = ((Map<?, ?>) ((List<?>) group.get("fills")).getFirst()).get("id");
    var linkedFill = httpJson(http, "/api/account/fill?id=" + encodedQuery(fillId));
    assertThat(linkedOrder.get("operationIds").get(0).asText()).isEqualTo(group.get("id"));
    assertThat(linkedFill.get("operationIds").get(0).asText()).isEqualTo(group.get("id"));
  }

  @Test
  void manualOriginRequiresExactDurableReceiptAndNeverInventsSourceOrCoefficient()
      throws Exception {
    control.markReconciled();
    control.start();
    control.pauseInstrument("instrument", "Synthetic manual reconciliation");
    clock.now = clock.now.plusSeconds(1);
    var trade =
        new BrokerGateway.SourceExecution(
            "mirror",
            "manual-fill",
            "",
            "instrument",
            BrokerGateway.Side.BUY,
            new BigDecimal("2"),
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    broker.mirrorExecutions.add(trade);
    var fill =
        new AccountRead.Execution(
            "manual-fill",
            "manual-order",
            "instrument",
            BrokerGateway.Side.BUY,
            new BigDecimal("2"),
            "lots",
            new AccountRead.Price(new BigDecimal("100"), "RUB", "currency per instrument unit"),
            clock.instant(),
            null,
            "Synthetic bank port");
    broker.displayOrders =
        List.of(
            new AccountRead.Order(
                "manual-order",
                "",
                "instrument",
                "SYN",
                BrokerGateway.Side.BUY,
                2,
                2,
                BrokerGateway.OrderStatus.FILLED,
                "LIMIT",
                clock.instant(),
                null,
                List.of(fill),
                true,
                fill.price(),
                AccountRead.Origin.UNKNOWN,
                List.of()));
    reads.refresh();
    assertThat(reads.page("operations", AccountQuery.defaults()).items().getFirst().get("origin"))
        .isEqualTo("UNKNOWN");
    manual.reconcilePaused();
    var operation = reads.page("operations", AccountQuery.defaults()).items().getFirst();
    assertThat(operation.get("origin")).isEqualTo("MANUAL");
    assertThat(operation.get("sourceExecutionId")).isNull();
    assertThat(operation.get("coefficient")).isNull();
    var http = ownerClient();
    var linkedOrder =
        httpJson(
            http,
            "/api/account/order?id="
                + encodedQuery(((List<?>) operation.get("orderIds")).getFirst()));
    var linkedFill =
        httpJson(
            http,
            "/api/account/fill?id="
                + encodedQuery(
                    ((Map<?, ?>) ((List<?>) operation.get("fills")).getFirst()).get("id")));
    assertThat(linkedOrder.get("origin").asText()).isEqualTo("MANUAL");
    assertThat(linkedOrder.get("operationIds").get(0).asText()).isEqualTo(operation.get("id"));
    assertThat(linkedFill.get("operationIds").get(0).asText()).isEqualTo(operation.get("id"));
    assertThat(broker.posts).isZero();
  }

  @Test
  void repeatedOwnOrdersKeepOneSourceTargetAndUniqueExecutionTotal() {
    broker.fillEveryPost = true;
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    control.confirmLiveAcceptance();
    control.enableAutomatic();
    clock.now = clock.now.plusSeconds(1);
    broker.rejectNext = BrokerGateway.RejectionReason.HALTED;
    var event =
        new BrokerGateway.SourceExecution(
            "source",
            "retry-source",
            "retry-source-order",
            "instrument",
            BrokerGateway.Side.BUY,
            BigDecimal.TEN,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    var source = copies.accept(event);
    trading.tick();
    trading.tick();
    var group =
        reads.page("operations", AccountQuery.defaults()).items().stream()
            .filter(o -> source.executionId().equals(o.get("sourceExecutionId")))
            .findFirst()
            .orElseThrow();
    assertThat((List<?>) group.get("orderIds")).hasSize(2);
    assertThat(group.get("targetLots")).isEqualTo("10");
    assertThat(group.get("executedLots")).isEqualTo("10");
    assertThat((List<?>) group.get("fills")).hasSize(1);
    assertThat(((Map<?, ?>) group.get("executionPrice")).get("value")).isEqualTo("100.5");
    assertThat(
            reads
                .page("operations", AccountQuery.defaults())
                .items()
                .getFirst()
                .get("executedLots"))
        .isEqualTo("10");
  }

  @Test
  void activeLocalIntentCanBeFilteredBeforeAnyBankReadImport() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    broker.partialNext = 0;
    copies.accept(
        new BrokerGateway.SourceExecution(
            "source",
            "active-before-import",
            "active-source-order",
            "instrument",
            BrokerGateway.Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    trading.tick();
    assertThat(reads.page("orders", AccountQuery.defaults()).items().getFirst().get("status"))
        .isEqualTo("ACTIVE");
    var query = AccountQuery.parse(Map.of("status", "ACTIVE"));
    assertThat(reads.page("orders", query).total()).isEqualTo(1);
    assertThat(reads.page("operations", query).total()).isEqualTo(1);
  }

  @Test
  void skippedSourceWithoutIntentRetainsExactReasonAndJournalAssociation() throws Exception {
    var source =
        copies.accept(
            new BrokerGateway.SourceExecution(
                "source",
                "[ТЕСТ] skipped/source",
                "source-order",
                "instrument",
                BrokerGateway.Side.BUY,
                BigDecimal.ONE,
                new BigDecimal("100"),
                "RUB",
                clock.instant(),
                clock.instant()));
    assertThat(copies.intents()).isEmpty();
    var group = reads.page("operations", AccountQuery.defaults()).items().getFirst();
    assertThat(group.get("result")).isEqualTo("SKIPPED");
    assertThat(group.get("reason")).isEqualTo("Исполнение до границы наблюдения или возобновления");
    var journal = reads.journal(Long.parseLong(group.get("journalId").toString()));
    assertThat(journal.get("category")).isEqualTo("SOURCE_SKIPPED");
    var linked = httpJson(ownerClient(), "/api/account/journal?id=" + group.get("journalId"));
    assertThat(linked.get("operationIds").get(0).asText()).isEqualTo(group.get("id"));
    assertThat(linked.get("orderIds").isEmpty()).isTrue();
    assertThat(linked.get("fillIds").isEmpty()).isTrue();
    assertThat(journal.get("detail"))
        .isEqualTo(source.executionId() + " Исполнение до границы наблюдения или возобновления");
  }

  @Test
  void sourceIdsWithSpacesNeverBorrowAnotherSkippedSourcesJournalReason() {
    control.markReconciled();
    control.start();
    clock.now = clock.now.plusSeconds(1);
    clock.evidenceAvailable = false;
    var shortEvent =
        new BrokerGateway.SourceExecution(
            "source",
            "[ТЕСТ] prefix",
            "order-short",
            "instrument",
            BrokerGateway.Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    var longEvent =
        new BrokerGateway.SourceExecution(
            "source",
            "[ТЕСТ] prefix suffix",
            "order-long",
            "instrument",
            BrokerGateway.Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    var shortSource = copies.accept(shortEvent);
    var longSource = copies.accept(longEvent);
    copies.skipSource(shortEvent, "SHORT persisted reason");
    copies.skipSource(longEvent, "LONG persisted reason");
    var groups = reads.page("operations", AccountQuery.defaults()).items();
    var shortGroup =
        groups.stream()
            .filter(g -> g.get("sourceExecutionId").equals(shortSource.executionId()))
            .findFirst()
            .orElseThrow();
    var longGroup =
        groups.stream()
            .filter(g -> g.get("sourceExecutionId").equals(longSource.executionId()))
            .findFirst()
            .orElseThrow();
    assertThat(shortGroup.get("reason")).isEqualTo("SHORT persisted reason");
    assertThat(reads.journal(Long.parseLong(shortGroup.get("journalId").toString())).get("detail"))
        .isEqualTo(shortSource.executionId() + " SHORT persisted reason");
    assertThat(longGroup.get("reason").toString()).contains("не подтверждена");
    assertThat(longGroup.get("journalId")).isNull();
    assertThat(longGroup.get("complete")).isEqualTo(false);
  }

  private java.net.http.HttpClient ownerClient() throws Exception {
    credentials.replace("synthetic-local-links-password".toCharArray());
    var client =
        java.net.http.HttpClient.newBuilder()
            .cookieHandler(new java.net.CookieManager(null, java.net.CookiePolicy.ACCEPT_ALL))
            .build();
    var token = httpJson(client, "/api/csrf");
    var login =
        client.send(
            java.net.http.HttpRequest.newBuilder(
                    java.net.URI.create("http://127.0.0.1:" + port + "/api/login"))
                .header(token.get("headerName").asText(), token.get("token").asText())
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(
                    java.net.http.HttpRequest.BodyPublishers.ofString(
                        "username=owner&password=synthetic-local-links-password"))
                .build(),
            java.net.http.HttpResponse.BodyHandlers.ofString());
    assertThat(login.statusCode()).isEqualTo(204);
    return client;
  }

  private tools.jackson.databind.JsonNode httpJson(java.net.http.HttpClient client, String path)
      throws Exception {
    var response =
        client.send(
            java.net.http.HttpRequest.newBuilder(
                    java.net.URI.create("http://127.0.0.1:" + port + path))
                .GET()
                .build(),
            java.net.http.HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    return tools.jackson.databind.json.JsonMapper.builder().build().readTree(response.body());
  }

  private static String encodedQuery(Object value) {
    return java.net.URLEncoder.encode(value.toString(), java.nio.charset.StandardCharsets.UTF_8);
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class ReadFixture {
    @Bean
    @Primary
    ReadSimulator readSimulator(CopyLifecycleIT.MutableClock clock) {
      return new ReadSimulator(clock);
    }
  }

  static class ReadSimulator extends CopyLifecycleIT.Simulator {
    List<AccountRead.Order> displayOrders = List.of();

    ReadSimulator(Clock clock) {
      super(clock);
    }

    public AccountRead.Snapshot<AccountRead.Order> readOrders(Collection<String> ids) {
      return new AccountRead.Snapshot<>(
          displayOrders, clock.instant(), clock.instant(), true, List.of());
    }

    public AccountRead.Snapshot<AccountRead.Order> readOrderHistory(
        Instant from, Instant through, Collection<String> ids) {
      return readOrders(ids);
    }

    public AccountRead.Snapshot<AccountRead.Execution> readExecutions(
        Instant from, Instant through) {
      return new AccountRead.Snapshot<>(List.of(), from, through, true, List.of());
    }
  }
}
