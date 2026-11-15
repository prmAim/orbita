package ru.orbita.broker;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.BooleanSupplier;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.OrbitaApplication;
import ru.orbita.control.*;
import ru.orbita.copy.*;
import ru.orbita.notifications.*;
import ru.orbita.web.OwnerCredentials;
import ru.tinkoff.piapi.contract.v1.PostOrderRequest;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** The retained UI preview and the normal integration test run this exact application scenario. */
class InterfaceDemoIT {
  @TempDir Path temporary;
  private static final String SYNTHETIC_PASSWORD = "synthetic-whole-chain-owner-password";

  @Test
  void ownershipFinalizationRefusesAConnectionSurvivingTheApplicationClose() throws Exception {
    try (var postgres = new PostgreSQLContainer("postgres:17.11-alpine")) {
      postgres.start();
      Flyway.configure()
          .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
          .load()
          .migrate();
      var dataSource =
          new DriverManagerDataSource(
              postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
      var db = new JdbcTemplate(dataSource);
      var ledger = InterfaceDemoLedger.begin(db, false);
      try (var survivingConnection = dataSource.getConnection()) {
        survivingConnection.setClientInfo("ApplicationName", "orbita-demo-harness");
        assertThat(
                db.queryForObject(
                    "select count(*) from pg_stat_activity where application_name='orbita-demo-harness'",
                    Integer.class))
            .isEqualTo(1);
        Throwable failure =
            catchThrowable(() -> captureAfterDrain(db, ledger, Duration.ofMillis(150)));
        assertThat(
                db.queryForObject("select captured from orbita_demo_preview.batch", Boolean.class))
            .isFalse();
        assertThat(
                db.queryForObject(
                    "select restored_at is null from orbita_demo_preview.batch", Boolean.class))
            .isTrue();
        assertThat(failure).isInstanceOf(AssertionError.class);
      }
      captureAfterDrain(db, ledger, Duration.ofSeconds(2));
      assertThat(
              db.queryForObject(
                  "select captured and restored_at is not null from orbita_demo_preview.batch",
                  Boolean.class))
          .isTrue();
      ledger.cleanup();
    }
  }

  @Test
  void wholeApplicationChainCreatesAndRemovesOnlyOwnedPreviewData() throws Exception {
    String url = System.getenv("ORBITA_DEMO_JDBC_URL");
    boolean external = url != null && !url.isBlank();
    boolean retain = external && "true".equals(System.getenv("ORBITA_DEMO_RETAIN"));
    if (external) {
      URI target = URI.create(url.substring("jdbc:".length()));
      if (!retain
          || !"postgresql".equals(target.getScheme())
          || !"127.0.0.1".equals(target.getHost())
          || target.getPort() <= 0
          || !"/orbita".equals(target.getPath())
          || target.getUserInfo() != null
          || target.getQuery() != null)
        throw new IllegalStateException(
            "Retained preview requires explicit opt-in and the wrapper's loopback orbita database");
    }
    PostgreSQLContainer postgres = null;
    ConfigurableApplicationContext app = null;
    InterfaceDemoLedger ledger = null;
    if (!external) {
      postgres = new PostgreSQLContainer("postgres:17.11-alpine");
      postgres.start();
      url = postgres.getJdbcUrl();
    }
    String username = external ? required("ORBITA_DEMO_JDBC_USERNAME") : postgres.getUsername();
    String dbPassword = external ? required("ORBITA_DEMO_JDBC_PASSWORD") : postgres.getPassword();
    var database = new DriverManagerDataSource(url, username, dbPassword);
    var db = new JdbcTemplate(database);
    String ownerPassword =
        external
            ? Files.readString(Path.of(required("ORBITA_DEMO_OWNER_PASSWORD_FILE")))
                .replaceFirst("\\r?\\n$", "")
            : SYNTHETIC_PASSWORD;
    try (var bank = WholeChainBrokerSimulator.open(temporary.resolve("tls"));
        var telegram = new WholeChainTelegramSimulator()) {
      Throwable scenarioFailure = null;
      try {
        if (!external) {
          try (var setup = start(url, username, dbPassword, bank, telegram, false)) {
            setup.getBean(OwnerCredentials.class).replace(ownerPassword.toCharArray());
            setup
                .getBean(TransactionTemplate.class)
                .executeWithoutResult(
                    _ ->
                        setup
                            .getBean(NotificationPublisher.class)
                            .publish(
                                "BASELINE",
                                "[ПРОВЕРКА] прежние данные",
                                Instant.now(),
                                "Прежнее уведомление, сохранить при удалении демонстрации"));
            setup.getBean(NotificationService.class).deliverDue();
          }
          await(
              () ->
                  db.queryForObject(
                          "select count(*) from pg_stat_activity where application_name='orbita-demo-harness'",
                          Integer.class)
                      == 0);
        }
        var encoder = new BCryptPasswordEncoder(12);
        var actualOwner = new OwnerCredentials(db, encoder).loadUserByUsername("owner");
        if (!encoder.matches(ownerPassword, actualOwner.getPassword()))
          throw new IllegalStateException(
              "Preview owner authentication failed before scenario startup");
        String credentialBefore = digest(db, "owner_credential"),
            controlBefore = digest(db, "control_state");
        Map<Long, String> oldJournal = journalSnapshot(db);
        Map<String, String> oldNotices = rowSnapshot(db, "notification"),
            oldAttempts = rowSnapshot(db, "notification_attempt");
        int initialTelegramRequests = telegram.payloads().size();
        ledger = InterfaceDemoLedger.begin(db, retain);
        app = start(url, username, dbPassword, bank, telegram);
        var browser = login(app, ownerPassword);
        var control = app.getBean(ControlService.class);
        var worker = app.getBean(TradingWorker.class);
        var notifications = app.getBean(NotificationService.class);
        UUID banner =
            app.getBean(TransactionTemplate.class)
                .execute(
                    _ ->
                        notifications.publish(
                            "PREVIEW_BATCH",
                            WholeChainBrokerSimulator.INSTRUMENT,
                            Instant.now(),
                            "[ТЕСТ] Временная демонстрация: локальные TLS-брокер и HTTP-Telegram. Реальные счета и Telegram не используются."));
        worker.run();
        assertThat(control.status().reconciled()).isTrue();
        assertThat(browser.post("/api/control/start").statusCode()).isEqualTo(200);
        app.getBean(SourceFeedLifecycle.class).ensureObservation();
        bank.awaitSubscription(Duration.ofSeconds(10));
        await(
            () ->
                Boolean.TRUE.equals(
                    db.queryForObject(
                        "select feed_ready from control_state where id=1", Boolean.class)));
        emit(bank, db, "shadow-buy", "shadow-buy-order", BrokerGateway.Side.BUY, 2);
        worker.run();
        bank.replaySource("[ТЕСТ] shadow-buy");
        bank.disconnectSource();
        bank.awaitSubscription(Duration.ofSeconds(10));
        await(
            () ->
                Boolean.TRUE.equals(
                    db.queryForObject(
                        "select feed_ready from control_state where id=1", Boolean.class)));
        worker.run();
        assertThat(
                db.queryForObject(
                    "select count(*) from source_execution where trade_id='[ТЕСТ] shadow-buy'",
                    Integer.class))
            .isEqualTo(1);
        var acceptedSource =
            db.queryForMap(
                "select id,account_id,side,lots,price,coefficient,new_lots from source_execution where trade_id='[ТЕСТ] shadow-buy'");
        assertThat(acceptedSource)
            .containsEntry("account_id", WholeChainBrokerSimulator.SOURCE)
            .containsEntry("side", "BUY")
            .containsEntry("new_lots", 2L);
        assertThat((BigDecimal) acceptedSource.get("lots")).isEqualByComparingTo("2");
        assertThat((BigDecimal) acceptedSource.get("price")).isEqualByComparingTo("100");
        assertThat((BigDecimal) acceptedSource.get("coefficient")).isEqualByComparingTo("1");
        assertThat(
                db.queryForObject(
                    "select count(*) from journal_entry where category='SOURCE_RECEIVED' and detail=?",
                    Integer.class,
                    acceptedSource.get("id")))
            .isEqualTo(1);
        assertThat(bank.calls("GetOperationsByCursor").size()).isGreaterThanOrEqualTo(2);
        emit(bank, db, "shadow-sell", "shadow-sell-order", BrokerGateway.Side.SELL, 2);
        worker.run();
        await(() -> control.shadowQualified());
        assertThat(bank.calls("PostOrder")).isEmpty();
        assertThat(browser.post("/api/control/permit", "{\"rubLimit\":\"1000\"}").statusCode())
            .isEqualTo(200);
        bank.setNextPostFill(0);
        emit(bank, db, "acceptance-buy", "acceptance-order", BrokerGateway.Side.BUY, 1);
        worker.run();
        UUID first = intent(db, "[ТЕСТ] acceptance-buy");
        assertPost(bank, first, 1);
        bank.fillOrder(first, 1, now());
        worker.run();
        worker.run();
        assertThat(
                db.queryForObject(
                    "select executed_lots from copy_intent where id=?", Long.class, first))
            .isEqualTo(1);
        assertThat(
                db.queryForObject(
                    "select count(*) from broker_fill where intent_id=?", Integer.class, first))
            .isEqualTo(1);
        assertThat(
                db.queryForObject(
                    "select live_accepted from control_state where id=1", Boolean.class))
            .isTrue();
        assertThat(browser.post("/api/control/automatic").statusCode()).isEqualTo(200);
        for (int deliveries = 0;
            deliveries < 10
                && notifications.list(200).stream().anyMatch(n -> n.status().equals("PENDING"));
            deliveries++) notifications.deliverDue();
        assertThat(notifications.list(200)).noneMatch(n -> n.status().equals("PENDING"));
        bank.setNextPostFill(2);
        emit(bank, db, "partial-buy", "partial-buy-order", BrokerGateway.Side.BUY, 4);
        worker.run();
        UUID partial = intent(db, "[ТЕСТ] partial-buy");
        assertPost(bank, partial, 4);
        bank.fillOrder(partial, 1, now());
        worker.run();
        assertThat(browser.post("/api/control/stop").statusCode()).isEqualTo(200);
        worker.run();
        assertThat(
                db.queryForMap(
                    "select executed_lots,skipped_lots,status from copy_intent where id=?",
                    partial))
            .containsEntry("executed_lots", 3L)
            .containsEntry("skipped_lots", 1L)
            .containsEntry("status", "FINAL");
        assertThat(bank.calls("CancelOrder")).hasSize(1);
        UUID naturalNotice =
            db.queryForObject(
                "select id from notification where category='ORDER_FINAL_SKIP' and instrument=? order by occurred_at desc limit 1",
                UUID.class,
                WholeChainBrokerSimulator.INSTRUMENT);
        assertThat(notifications.get(naturalNotice).text())
            .contains("исполнено 3/4", "пропущено 1");
        telegram.failNext();
        notifications.deliverDue();
        assertThat(notifications.get(naturalNotice).status()).isEqualTo("PENDING");
        assertThat(browser.post("/api/notifications/" + naturalNotice + "/retry").statusCode())
            .isEqualTo(200);
        var attempts = notifications.attempts(naturalNotice);
        assertThat(attempts).hasSize(2);
        assertThat(attempts)
            .extracting(NotificationService.Attempt::result)
            .containsExactly("FAILURE", "SUCCESS");
        assertThat(attempts)
            .extracting(NotificationService.Attempt::kind)
            .containsExactly("AUTOMATIC", "MANUAL");
        assertThat(attempts)
            .allSatisfy(
                attempt ->
                    assertThat(attempt.payload())
                        .isEqualTo(notifications.get(naturalNotice).text()));
        assertThat(
                telegram.payloads().stream()
                    .filter(text -> text.equals(notifications.get(naturalNotice).text()))
                    .count())
            .isEqualTo(2);
        assertThat(
                db.queryForObject(
                    "select j.detail from journal_entry j join notification n on n.journal_id=j.id where n.id=?",
                    String.class,
                    naturalNotice))
            .isEqualTo(notifications.get(naturalNotice).text());
        assertThat(browser.get("/api/notifications/" + naturalNotice + "/attempts").body())
            .contains("FAILURE", "SUCCESS");
        assertThat(browser.post("/api/control/start").statusCode()).isEqualTo(200);
        assertThat(browser.post("/api/control/automatic").statusCode()).isEqualTo(200);
        bank.setNextPostFill(0);
        emit(bank, db, "copied-sell", "copied-sell-order", BrokerGateway.Side.SELL, 2);
        worker.run();
        UUID sale = intent(db, "[ТЕСТ] copied-sell");
        assertPost(bank, sale, 2);
        bank.fillOrder(sale, 2, now());
        worker.run();
        worker.run();
        assertThat(
                db.queryForObject(
                    "select copied_lots from mirror_position where instrument_id=?",
                    Long.class,
                    WholeChainBrokerSimulator.INSTRUMENT))
            .isEqualTo(2);
        bank.setResources(BigDecimal.ZERO, 0, 100);
        emit(bank, db, "resource-skip", "resource-skip-order", BrokerGateway.Side.BUY, 2);
        worker.run();
        UUID skipped = intent(db, "[ТЕСТ] resource-skip");
        assertThat(
                db.queryForMap(
                    "select status,executed_lots,skipped_lots,reason,reserved_money,reserved_lots from copy_intent where id=?",
                    skipped))
            .containsEntry("status", "FINAL")
            .containsEntry("executed_lots", 0L)
            .containsEntry("skipped_lots", 2L)
            .containsEntry("reserved_lots", 0L);
        assertThat(
                db.queryForObject(
                    "select reserved_money from copy_intent where id=?", BigDecimal.class, skipped))
            .isZero();
        assertThat(
                db.queryForObject(
                    "select reason from copy_intent where id=?", String.class, skipped))
            .contains("Недостаточно собственных");
        assertThat(bank.calls("PostOrder")).hasSize(3);
        var tx = app.getBean(TransactionTemplate.class);
        UUID summary =
            tx.execute(
                _ ->
                    notifications.publish(
                        "PREVIEW_TRADE",
                        WholeChainBrokerSimulator.INSTRUMENT,
                        Instant.now(),
                        "[ТЕСТ] BUY 1 + BUY 4 (исполнено 3, пропущен 1) − SELL 2. Цена источника 100 RUB; K=1. Фактический остаток повторения: 2 лота. Все записи созданы цепочкой SDK→приложение."));
        JsonNode positions = browser.json.readTree(browser.get("/api/positions").body());
        assertThat(positions.toString())
            .contains(WholeChainBrokerSimulator.INSTRUMENT, "\"copiedLots\":2");
        JsonNode journal = browser.json.readTree(browser.get("/api/journal").body());
        assertThat(journal.toString())
            .contains("SOURCE_ACCEPTED", "ORDER_RECONCILED", "PREVIEW_TRADE", "[ТЕСТ]");
        JsonNode notices = browser.json.readTree(browser.get("/api/notifications").body());
        assertThat(notices.toString()).contains("DELIVERED", "PENDING");
        assertThat(browser.post("/api/control/stop").statusCode()).isEqualTo(200);
        worker.run();
        assertThat(
                db.queryForObject(
                    "select count(*) from copy_intent where status in ('WAITING','HALTED','ACTIVE','SEND_STARTED','UNKNOWN','CANCEL_STARTED')",
                    Integer.class))
            .isZero();
        app.getBean(SourceFeedLifecycle.class).close();
        app.close();
        app = null;
        await(
            () ->
                db.queryForObject(
                        "select count(*) from pg_stat_activity where application_name='orbita-demo-harness'",
                        Integer.class)
                    == 0);
        captureAfterDrain(db, ledger, Duration.ofSeconds(15));
        assertThat(digest(db, "control_state")).isEqualTo(controlBefore);
        assertThat(digest(db, "owner_credential")).isEqualTo(credentialBefore);
        assertThat(journalSnapshot(db)).containsAllEntriesOf(oldJournal);
        int sourceCount = db.queryForObject("select count(*) from source_execution", Integer.class);
        assertThatThrownBy(() -> InterfaceDemoLedger.begin(db, retain))
            .hasMessageContaining("Remove before regeneration");
        assertThat(db.queryForObject("select count(*) from source_execution", Integer.class))
            .isEqualTo(sourceCount);
        Map<Long, String> laterJournal;
        try (var viewer = start(url, username, dbPassword, null, null)) {
          var view = login(viewer, ownerPassword);
          assertThat(view.get("/api/positions").body()).contains("\"copiedLots\":2");
          assertThat(view.get("/api/journal").body()).contains("PREVIEW_TRADE");
          assertThat(
                  view.post(
                          "/api/instruments/"
                              + URLEncoder.encode(
                                      WholeChainBrokerSimulator.INSTRUMENT,
                                      java.nio.charset.StandardCharsets.UTF_8)
                                  .replace("+", "%20")
                              + "/pause",
                          "{\"text\":\"[ТЕСТ] проба кнопки после перезапуска\"}")
                      .statusCode())
              .isEqualTo(200);
          assertThat(db.queryForList("select instrument_id from instrument_state", String.class))
              .containsExactly(WholeChainBrokerSimulator.INSTRUMENT);
          int previous =
              db.queryForObject(
                  "select count(*) from notification_attempt where notification_id=?",
                  Integer.class,
                  summary);
          assertThat(view.post("/api/notifications/" + summary + "/retry").statusCode())
              .isEqualTo(200);
          assertThat(
                  db.queryForObject(
                      "select count(*) from notification_attempt where notification_id=?",
                      Integer.class,
                      summary))
              .isGreaterThan(previous);
          assertThat(view.get("/api/notifications/" + summary + "/attempts").body())
              .contains("Доставка Telegram отключена");
          laterJournal = new LinkedHashMap<>();
          db.query(
              "select id,md5(to_jsonb(j)::text) from journal_entry j where category='PROCESS_STARTED' and not exists(select 1 from orbita_demo_preview.owned o where o.relation='journal_entry' and o.row_id=j.id::text)",
              rs -> {
                laterJournal.put(rs.getLong(1), rs.getString(2));
              });
        }
        await(
            () ->
                db.queryForObject(
                        "select count(*) from pg_stat_activity where application_name='orbita-demo-harness'",
                        Integer.class)
                    == 0);
        String controlBeforeCleanup = digest(db, "control_state");
        assertThat(bank.audit())
            .allSatisfy(rpc -> assertThat(rpc.remoteAddress()).contains("127.0.0.1"));
        System.out.println(
            "[ТЕСТ] whole-chain preview: sourceExecutions="
                + db.queryForObject("select count(*) from source_execution", Integer.class)
                + ", PostOrder="
                + bank.calls("PostOrder").size()
                + ", CancelOrder="
                + bank.calls("CancelOrder").size()
                + ", brokerFills="
                + db.queryForObject("select count(*) from broker_fill", Integer.class)
                + ", TelegramHTTP="
                + (telegram.payloads().size() - initialTelegramRequests)
                + ", retained="
                + retain);
        if (!retain) {
          var completed = ledger;
          ledger = null;
          completed.cleanup();
          completed.cleanup();
          assertThat(journalSnapshot(db))
              .containsAllEntriesOf(oldJournal)
              .containsAllEntriesOf(laterJournal);
          assertThat(rowSnapshot(db, "notification"))
              .containsExactlyInAnyOrderEntriesOf(oldNotices);
          assertThat(rowSnapshot(db, "notification_attempt"))
              .containsExactlyInAnyOrderEntriesOf(oldAttempts);
          assertThat(db.queryForObject("select count(*) from source_execution", Integer.class))
              .isZero();
          assertThat(db.queryForObject("select count(*) from copy_intent", Integer.class)).isZero();
          assertThat(db.queryForObject("select count(*) from broker_fill", Integer.class)).isZero();
          assertThat(db.queryForObject("select count(*) from instrument_state", Integer.class))
              .isZero();
          assertThat(db.queryForObject("select count(*) from mirror_position", Integer.class))
              .isZero();
          assertThat(digest(db, "owner_credential")).isEqualTo(credentialBefore);
          assertThat(digest(db, "control_state")).isEqualTo(controlBeforeCleanup);
        }
        ledger = null;
      } catch (Throwable failure) {
        scenarioFailure = failure;
        throw failure;
      } finally {
        try {
          if (app != null) {
            try {
              app.getBean(ControlService.class).stop();
              app.getBean(TradingWorker.class).run();
              app.getBean(SourceFeedLifecycle.class).close();
            } finally {
              app.close();
              app = null;
            }
          }
          if (ledger != null) {
            captureAfterDrain(db, ledger, Duration.ofSeconds(15));
            ledger = null;
          }
        } catch (Throwable teardownFailure) {
          if (scenarioFailure != null) scenarioFailure.addSuppressed(teardownFailure);
          else throw teardownFailure;
        }
      }
    } finally {
      if (postgres != null) postgres.close();
    }
  }

  private static String required(String key) {
    String value = System.getenv(key);
    if (value == null || value.isBlank())
      throw new IllegalStateException("Missing preview setting " + key);
    return value;
  }

  private static void captureAfterDrain(
      JdbcTemplate db, InterfaceDemoLedger ledger, Duration timeout) throws Exception {
    await(
        () ->
            db.queryForObject(
                    "select count(*) from pg_stat_activity where application_name='orbita-demo-harness'",
                    Integer.class)
                == 0,
        timeout);
    ledger.captureOwnedFacts();
    ledger.restoreControl();
  }

  private static Instant now() {
    return Instant.now().truncatedTo(ChronoUnit.MICROS);
  }

  private static void emit(
      WholeChainBrokerSimulator bank,
      JdbcTemplate db,
      String trade,
      String order,
      BrokerGateway.Side side,
      long lots)
      throws Exception {
    Thread.sleep(25);
    bank.emitSource(
        "[ТЕСТ] " + trade, "[ТЕСТ] " + order, side, lots, WholeChainBrokerSimulator.PRICE, now());
    await(
        () ->
            db.queryForObject(
                    "select count(*) from source_execution where trade_id=?",
                    Integer.class,
                    "[ТЕСТ] " + trade)
                == 1);
  }

  private static UUID intent(JdbcTemplate db, String trade) {
    return db.queryForObject(
        "select i.id from copy_intent i join source_execution e on e.id=i.execution_id where e.trade_id=?",
        UUID.class,
        trade);
  }

  private static void assertPost(WholeChainBrokerSimulator bank, UUID id, long lots) {
    assertThat(
            bank.audit().stream()
                .filter(
                    rpc ->
                        rpc.request() instanceof PostOrderRequest request
                            && request.getOrderId().equals(id.toString()))
                .map(rpc -> (PostOrderRequest) rpc.request())
                .toList())
        .singleElement()
        .satisfies(
            request -> {
              assertThat(request.getQuantity()).isEqualTo(lots);
              assertThat(request.getAccountId()).isEqualTo(WholeChainBrokerSimulator.MIRROR);
              assertThat(request.getInstrumentId()).isEqualTo(WholeChainBrokerSimulator.INSTRUMENT);
            });
  }

  private static void await(BooleanSupplier condition) throws Exception {
    await(condition, Duration.ofSeconds(15));
  }

  private static void await(BooleanSupplier condition, Duration timeout) throws Exception {
    long end = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > end) throw new AssertionError("Whole-chain state did not converge");
      Thread.sleep(25);
    }
  }

  private static String digest(JdbcTemplate db, String relation) {
    return db.queryForObject("select md5(to_jsonb(t)::text) from " + relation + " t", String.class);
  }

  private static Map<Long, String> journalSnapshot(JdbcTemplate db) {
    Map<Long, String> result = new LinkedHashMap<>();
    db.query(
        "select id,md5(to_jsonb(j)::text) from journal_entry j order by id",
        rs -> {
          result.put(rs.getLong(1), rs.getString(2));
        });
    return result;
  }

  private static Map<String, String> rowSnapshot(JdbcTemplate db, String relation) {
    Map<String, String> result = new LinkedHashMap<>();
    db.query(
        "select id::text,md5(to_jsonb(t)::text) from " + relation + " t order by id",
        rs -> {
          result.put(rs.getString(1), rs.getString(2));
        });
    return result;
  }

  private static ConfigurableApplicationContext start(
      String url,
      String username,
      String password,
      WholeChainBrokerSimulator bank,
      WholeChainTelegramSimulator telegram) {
    return start(url, username, password, bank, telegram, bank != null);
  }

  private static ConfigurableApplicationContext start(
      String url,
      String username,
      String password,
      WholeChainBrokerSimulator bank,
      WholeChainTelegramSimulator telegram,
      boolean scenario) {
    var builder =
        new SpringApplicationBuilder(
            bank == null
                ? new Class<?>[] {OrbitaApplication.class}
                : new Class<?>[] {OrbitaApplication.class, Fixture.class});
    if (bank != null)
      builder.initializers(
          context -> {
            context.getBeanFactory().registerSingleton("wholeChainBank", bank);
            context.getBeanFactory().registerSingleton("wholeChainTelegram", telegram);
          });
    return builder.run(
        "--server.port=0",
        "--server.address=127.0.0.1",
        "--spring.datasource.url=" + url,
        "--spring.datasource.username=" + username,
        "--spring.datasource.password=" + password,
        "--spring.datasource.hikari.data-source-properties.ApplicationName=orbita-demo-harness",
        "--orbita.admin.reset-password=false",
        "--orbita.restored=false",
        "--orbita.broker.enabled=false",
        "--orbita.source-account=" + (scenario ? WholeChainBrokerSimulator.SOURCE : ""),
        "--orbita.mirror-account=" + (scenario ? WholeChainBrokerSimulator.MIRROR : ""),
        "--orbita.margin-disabled-confirmed=true",
        "--orbita.worker.enabled=" + scenario,
        "--orbita.worker.interval-millis=86400000",
        "--orbita.telegram.enabled=" + (bank != null),
        "--orbita.telegram.poll-interval=PT24H",
        "--orbita.telegram.token=synthetic-local-token",
        "--orbita.telegram.chat-id=123456",
        "--orbita.backup.enabled=false");
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class Fixture {
    @Bean(destroyMethod = "close")
    BrokerGateway wholeChainGateway(
        WholeChainBrokerSimulator bank, Clock clock, BrokerConnectivityEvents events)
        throws Exception {
      return bank.gateway(clock, events);
    }

    @Bean
    @Primary
    TelegramTransport wholeChainTransport(
        WholeChainTelegramSimulator telegram, TelegramSettings settings, JsonMapper json) {
      return telegram.transport(settings, json);
    }

    @Bean
    ClockEvidence syntheticContinuousUtc(Clock clock) {
      long began = BrokerReceiptClock.nanoTime();
      Instant initial = clock.instant();
      return new ClockEvidence() {
        public Optional<Bound> current() {
          Instant now = clock.instant();
          return Optional.of(
              new Bound(
                  now.minusMillis(2),
                  now.plusMillis(2),
                  "[ТЕСТ] синтетическая временная шкала локального стенда"));
        }

        public Optional<Bound> atReceipt(long receipt) {
          if (receipt < began) return Optional.empty();
          Instant at = initial.plusNanos(receipt - began);
          return Optional.of(
              new Bound(
                  at.minusMillis(2),
                  at.plusMillis(2),
                  "[ТЕСТ] синтетическая непрерывность стенда"));
        }
      };
    }
  }

  private static Browser login(ConfigurableApplicationContext app, String password)
      throws Exception {
    var browser =
        new Browser(
            URI.create(
                "http://127.0.0.1:"
                    + app.getEnvironment().getRequiredProperty("local.server.port")),
            app.getBean(JsonMapper.class));
    browser.csrf = browser.json.readTree(browser.get("/api/csrf").body());
    var response =
        browser.client.send(
            HttpRequest.newBuilder(browser.base.resolve("/api/login"))
                .header(browser.csrf.get("headerName").asText(), browser.csrf.get("token").asText())
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(
                    HttpRequest.BodyPublishers.ofString(
                        "username=owner&password="
                            + URLEncoder.encode(password, java.nio.charset.StandardCharsets.UTF_8)))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(204);
    browser.csrf = browser.json.readTree(browser.get("/api/csrf").body());
    return browser;
  }

  static class Browser {
    final URI base;
    final JsonMapper json;
    JsonNode csrf;
    final HttpClient client =
        HttpClient.newBuilder()
            .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ALL))
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    Browser(URI base, JsonMapper json) {
      this.base = base;
      this.json = json;
    }

    HttpResponse<String> get(String path) throws Exception {
      return client.send(
          HttpRequest.newBuilder(base.resolve(path)).GET().build(),
          HttpResponse.BodyHandlers.ofString());
    }

    HttpResponse<String> post(String path) throws Exception {
      return post(path, "");
    }

    HttpResponse<String> post(String path, String body) throws Exception {
      return client.send(
          HttpRequest.newBuilder(base.resolve(path))
              .header(csrf.get("headerName").asText(), csrf.get("token").asText())
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }
}
