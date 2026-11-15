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

  private record ExpectedSecurity(
      String ticker,
      String price,
      long sourceLots,
      long copiedLots,
      int executions,
      int posts,
      int fills,
      long skipped,
      int buys,
      int sells) {
    String id() {
      return "[ТЕСТ] " + ticker;
    }
  }

  // Independent literal outcomes from the worked preview matrix, never recomputed from orders.
  private static final List<ExpectedSecurity> EXPECTED_SECURITIES =
      List.of(
          new ExpectedSecurity("SBER", "100", 5, 2, 6, 3, 4, 3, 4, 2),
          new ExpectedSecurity("GAZP", "150", 4, 4, 3, 3, 3, 0, 2, 1),
          new ExpectedSecurity("LKOH", "200", 4, 4, 3, 3, 3, 0, 2, 1),
          new ExpectedSecurity("ROSN", "250", 7, 5, 3, 3, 3, 2, 2, 1),
          new ExpectedSecurity("NVTK", "300", 4, 1, 3, 2, 2, 3, 2, 1),
          new ExpectedSecurity("GMKN", "350", 2, 2, 1, 1, 1, 0, 1, 0),
          new ExpectedSecurity("PLZL", "400", 3, 3, 1, 1, 1, 0, 1, 0),
          new ExpectedSecurity("TATN", "450", 4, 4, 1, 1, 1, 0, 1, 0),
          new ExpectedSecurity("SNGS", "500", 5, 5, 1, 1, 1, 0, 1, 0),
          new ExpectedSecurity("VTBR", "550", 6, 6, 1, 1, 1, 0, 1, 0),
          new ExpectedSecurity("MOEX", "600", 7, 7, 1, 1, 1, 0, 1, 0),
          new ExpectedSecurity("AFLT", "650", 8, 8, 1, 1, 1, 0, 1, 0),
          new ExpectedSecurity("CHMF", "700", 9, 9, 1, 1, 1, 0, 1, 0),
          new ExpectedSecurity("MAGN", "750", 10, 10, 1, 1, 1, 0, 1, 0),
          new ExpectedSecurity("ALRS", "800", 11, 11, 1, 1, 1, 0, 1, 0));

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
      bank.setCapitals(new BigDecimal("1000000"), new BigDecimal("1000000"));
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
        executeAdditionalSecurities(bank, db, worker, browser);
        assertThat(bank.calls("PostOrder")).hasSize(24);
        assertThat(bank.calls("CancelOrder")).hasSize(2);
        assertThat(db.queryForObject("select count(*) from source_execution", Integer.class))
            .isEqualTo(28);
        assertThat(db.queryForObject("select count(*) from broker_fill", Integer.class))
            .isEqualTo(25);
        assertBankMatrix(bank);
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
        assertPortfolioMatrix(browser, db);
        assertCompleteJournal(browser, db);
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
          assertPortfolioMatrix(view, db);
          assertCompleteJournal(view, db);
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
              .containsExactlyInAnyOrderElementsOf(
                  EXPECTED_SECURITIES.stream().map(ExpectedSecurity::id).toList());
          assertThat(
                  db.queryForObject(
                      "select paused from instrument_state where instrument_id=?",
                      Boolean.class,
                      WholeChainBrokerSimulator.INSTRUMENT))
              .isTrue();
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

  private static void assertPortfolioMatrix(Browser browser, JdbcTemplate db) throws Exception {
    JsonNode positions = browser.json.readTree(browser.get("/api/positions").body());
    JsonNode instruments = browser.json.readTree(browser.get("/api/instruments").body());
    assertThat(positions.size()).isEqualTo(15);
    assertThat(instruments.size()).isEqualTo(15);
    var visibleInstruments = new ArrayList<String>();
    for (var instrument : instruments)
      visibleInstruments.add(instrument.get("instrumentId").asText());
    assertThat(visibleInstruments)
        .containsExactlyInAnyOrderElementsOf(
            EXPECTED_SECURITIES.stream().map(ExpectedSecurity::id).toList());
    Map<String, JsonNode> byInstrument = new LinkedHashMap<>();
    for (var position : positions)
      byInstrument.put(position.get("instrumentId").asText(), position);
    assertThat(byInstrument.keySet())
        .containsExactlyInAnyOrderElementsOf(
            EXPECTED_SECURITIES.stream().map(ExpectedSecurity::id).toList());
    for (var expected : EXPECTED_SECURITIES) {
      var position = byInstrument.get(expected.id());
      assertThat(position.get("copiedLots").asLong())
          .as(expected.id())
          .isEqualTo(expected.copiedLots());
      assertThat(position.get("manualLots").asLong()).isZero();
      assertThat(new BigDecimal(position.get("brokerLots").asText()))
          .isEqualByComparingTo(BigDecimal.valueOf(expected.copiedLots()));
      assertThat(
              db.queryForObject(
                  "select copied_lots from mirror_position where instrument_id=?",
                  Long.class,
                  expected.id()))
          .isEqualTo(expected.copiedLots());
      assertThat(
              db.queryForObject(
                  "select sum(case when side='BUY' then lots else -lots end) from source_execution where instrument_id=?",
                  BigDecimal.class,
                  expected.id()))
          .isEqualByComparingTo(BigDecimal.valueOf(expected.sourceLots()));
      assertThat(
              db.queryForObject(
                  "select count(*) from source_execution where instrument_id=?",
                  Integer.class,
                  expected.id()))
          .isEqualTo(expected.executions());
      assertThat(
              db.queryForObject(
                  "select count(*) from source_execution where instrument_id=? and side='BUY'",
                  Integer.class,
                  expected.id()))
          .isEqualTo(expected.buys());
      assertThat(
              db.queryForObject(
                  "select count(*) from source_execution where instrument_id=? and side='SELL'",
                  Integer.class,
                  expected.id()))
          .isEqualTo(expected.sells());
      assertThat(
              db.queryForObject(
                  "select count(*) from broker_fill f join copy_intent i on i.id=f.intent_id where i.instrument_id=?",
                  Integer.class,
                  expected.id()))
          .isEqualTo(expected.fills());
      assertThat(
              db.queryForObject(
                  "select coalesce(sum(i.skipped_lots),0) from copy_intent i join source_execution e on e.id=i.execution_id where i.instrument_id=? and e.trade_id not in ('[ТЕСТ] shadow-buy','[ТЕСТ] shadow-sell')",
                  Long.class,
                  expected.id()))
          .isEqualTo(expected.skipped());
      assertThat(
              db.queryForList(
                  "select distinct price from source_execution where instrument_id=?",
                  BigDecimal.class,
                  expected.id()))
          .singleElement()
          .satisfies(price -> assertThat(price).isEqualByComparingTo(expected.price()));
      assertThat(
              db.queryForList(
                  "select distinct coefficient from source_execution where instrument_id=?",
                  BigDecimal.class,
                  expected.id()))
          .singleElement()
          .satisfies(coefficient -> assertThat(coefficient).isEqualByComparingTo("1"));
    }
    assertThat(db.queryForObject("select sum(copied_lots) from mirror_position", Long.class))
        .isEqualTo(81);
    assertThat(
            db.queryForObject(
                "select count(*) from (select instrument_id from source_execution group by instrument_id having count(*) filter(where side='BUY')>=2 and count(*) filter(where side='SELL')>=1) repeated",
                Integer.class))
        .isEqualTo(5);
  }

  private record PreviewOperation(
      String ticker,
      String label,
      BrokerGateway.Side side,
      long lots,
      long filled,
      boolean resourceSkip) {}

  private static void executeAdditionalSecurities(
      WholeChainBrokerSimulator bank, JdbcTemplate db, TradingWorker worker, Browser browser)
      throws Exception {
    var buy = BrokerGateway.Side.BUY;
    var sell = BrokerGateway.Side.SELL;
    var operations =
        List.of(
            new PreviewOperation("GAZP", "buy-1", buy, 3, 3, false),
            new PreviewOperation("GAZP", "buy-2", buy, 2, 2, false),
            new PreviewOperation("GAZP", "sell-1", sell, 1, 1, false),
            new PreviewOperation("LKOH", "buy-1", buy, 5, 5, false),
            new PreviewOperation("LKOH", "sell-1", sell, 2, 2, false),
            new PreviewOperation("LKOH", "buy-2", buy, 1, 1, false),
            new PreviewOperation("ROSN", "buy-1", buy, 4, 4, false),
            new PreviewOperation("ROSN", "sell-1", sell, 1, 1, false),
            new PreviewOperation("ROSN", "partial-buy-2", buy, 4, 2, false),
            new PreviewOperation("NVTK", "buy-1", buy, 2, 2, false),
            new PreviewOperation("NVTK", "sell-1", sell, 1, 1, false),
            new PreviewOperation("NVTK", "skipped-buy-2", buy, 3, 0, true),
            new PreviewOperation("GMKN", "buy-1", buy, 2, 2, false),
            new PreviewOperation("PLZL", "buy-1", buy, 3, 3, false),
            new PreviewOperation("TATN", "buy-1", buy, 4, 4, false),
            new PreviewOperation("SNGS", "buy-1", buy, 5, 5, false),
            new PreviewOperation("VTBR", "buy-1", buy, 6, 6, false),
            new PreviewOperation("MOEX", "buy-1", buy, 7, 7, false),
            new PreviewOperation("AFLT", "buy-1", buy, 8, 8, false),
            new PreviewOperation("CHMF", "buy-1", buy, 9, 9, false),
            new PreviewOperation("MAGN", "buy-1", buy, 10, 10, false),
            new PreviewOperation("ALRS", "buy-1", buy, 11, 11, false));
    for (var operation : operations) {
      String instrument = "[ТЕСТ] " + operation.ticker();
      String trade = operation.ticker() + "-" + operation.label();
      if (operation.resourceSkip()) bank.setResources(instrument, BigDecimal.ZERO, 0, 100);
      else
        bank.setNextPostFill(
            instrument, operation.filled() < operation.lots() ? operation.filled() : 0);
      emit(
          bank,
          db,
          instrument,
          trade,
          operation.ticker() + "-order-" + operation.label(),
          operation.side(),
          operation.lots());
      worker.run();
      UUID id = intent(db, "[ТЕСТ] " + trade);
      if (operation.resourceSkip()) {
        assertThat(
                db.queryForMap(
                    "select executed_lots,skipped_lots,status from copy_intent where id=?", id))
            .containsEntry("executed_lots", 0L)
            .containsEntry("skipped_lots", operation.lots())
            .containsEntry("status", "FINAL");
        assertThat(bank.calls("PostOrder"))
            .noneMatch(
                rpc -> ((PostOrderRequest) rpc.request()).getOrderId().equals(id.toString()));
      } else {
        assertPost(bank, id, operation.lots(), instrument);
        if (operation.filled() == operation.lots()) bank.fillOrder(id, operation.filled(), now());
        worker.run();
        if (operation.filled() < operation.lots()) {
          assertThat(browser.post("/api/control/stop").statusCode()).isEqualTo(200);
          worker.run();
          assertThat(browser.post("/api/control/start").statusCode()).isEqualTo(200);
          assertThat(browser.post("/api/control/automatic").statusCode()).isEqualTo(200);
        }
        worker.run();
        assertThat(
                db.queryForMap(
                    "select executed_lots,skipped_lots,status from copy_intent where id=?", id))
            .containsEntry("executed_lots", operation.filled())
            .containsEntry("skipped_lots", operation.lots() - operation.filled())
            .containsEntry("status", "FINAL");
      }
    }
  }

  private static void assertBankMatrix(WholeChainBrokerSimulator bank) {
    for (var expected : EXPECTED_SECURITIES) {
      assertThat(bank.position(WholeChainBrokerSimulator.SOURCE, expected.id()))
          .as(expected.id() + " source")
          .isEqualTo(expected.sourceLots());
      assertThat(bank.position(WholeChainBrokerSimulator.MIRROR, expected.id()))
          .as(expected.id() + " mirror")
          .isEqualTo(expected.copiedLots());
      assertThat(
              bank.calls("PostOrder").stream()
                  .map(rpc -> (PostOrderRequest) rpc.request())
                  .filter(request -> request.getInstrumentId().equals(expected.id()))
                  .count())
          .as(expected.id() + " orders")
          .isEqualTo(expected.posts());
    }
    assertThat(
            bank.calls("PostOrder").stream()
                .map(rpc -> ((PostOrderRequest) rpc.request()).getOrderId())
                .toList())
        .doesNotHaveDuplicates();
  }

  private static void assertCompleteJournal(Browser browser, JdbcTemplate db) throws Exception {
    var rows = new ArrayList<JsonNode>();
    var ids = new HashSet<Long>();
    long before = 0;
    int pages = 0;
    while (true) {
      var response =
          browser.get("/api/journal?limit=37" + (before == 0 ? "" : "&before=" + before));
      assertThat(response.statusCode()).isEqualTo(200);
      var page = browser.json.readTree(response.body());
      if (page.isEmpty()) break;
      pages++;
      assertThat(pages).isLessThan(100);
      for (var row : page) {
        long id = row.get("id").asLong();
        assertThat(ids.add(id)).isTrue();
        if (before != 0) assertThat(id).isLessThan(before);
        rows.add(row);
      }
      before = page.get(page.size() - 1).get("id").asLong();
    }
    assertThat(pages).isGreaterThan(1);
    assertThat(rows.size())
        .isEqualTo(db.queryForObject("select count(*) from journal_entry", Integer.class));
    var sourceRows =
        rows.stream()
            .filter(row -> row.get("category").asText().equals("SOURCE_RECEIVED"))
            .toList();
    assertThat(sourceRows).hasSize(28);
    assertThat(
            sourceRows.stream()
                .map(row -> row.get("instrument").asText())
                .collect(java.util.stream.Collectors.toSet()))
        .containsExactlyInAnyOrderElementsOf(
            EXPECTED_SECURITIES.stream().map(ExpectedSecurity::id).toList());
    assertThat(rows.toString())
        .contains("SOURCE_ACCEPTED", "ORDER_RECONCILED", "PREVIEW_TRADE", "[ТЕСТ]");
    assertNaturalNotice(
        browser, db, rows, "[ТЕСТ] ROSN", "ORDER_FINAL_SKIP", "исполнено 2/4", "пропущено 2");
    assertNaturalNotice(
        browser,
        db,
        rows,
        "[ТЕСТ] NVTK",
        "COPY_SKIPPED",
        "Недостаточно собственных",
        "пропущено 3 лотов");
  }

  private static void assertNaturalNotice(
      Browser browser,
      JdbcTemplate db,
      List<JsonNode> journal,
      String instrument,
      String category,
      String... expectedText)
      throws Exception {
    var response = browser.get("/api/notifications?limit=200");
    assertThat(response.statusCode()).isEqualTo(200);
    var matches = new ArrayList<JsonNode>();
    for (var notice : browser.json.readTree(response.body()))
      if (notice.get("instrument").asText().equals(instrument)
          && notice.get("category").asText().equals(category)) matches.add(notice);
    assertThat(matches).hasSize(1);
    var notice = matches.getFirst();
    String text = notice.get("text").asText();
    assertThat(text).contains(expectedText);
    assertThat(
            db.queryForObject(
                "select j.detail from notification n join journal_entry j on j.id=n.journal_id where n.id=? and n.instrument=? and j.instrument=?",
                String.class,
                UUID.fromString(notice.get("id").asText()),
                instrument,
                instrument))
        .isEqualTo(text);
    assertThat(
            journal.stream()
                .filter(
                    row ->
                        row.get("category").asText().equals("NOTIFICATION_" + category)
                            && row.get("instrument").asText().equals(instrument))
                .map(row -> row.get("detail").asText())
                .toList())
        .contains(text);
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
    emit(bank, db, WholeChainBrokerSimulator.INSTRUMENT, trade, order, side, lots);
  }

  private static void emit(
      WholeChainBrokerSimulator bank,
      JdbcTemplate db,
      String instrument,
      String trade,
      String order,
      BrokerGateway.Side side,
      long lots)
      throws Exception {
    Thread.sleep(25);
    BigDecimal price =
        bank.securities().stream()
            .filter(s -> s.id().equals(instrument))
            .findFirst()
            .orElseThrow()
            .price();
    bank.emitSource(instrument, "[ТЕСТ] " + trade, "[ТЕСТ] " + order, side, lots, price, now());
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
    assertPost(bank, id, lots, WholeChainBrokerSimulator.INSTRUMENT);
  }

  private static void assertPost(
      WholeChainBrokerSimulator bank, UUID id, long lots, String instrument) {
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
              assertThat(request.getInstrumentId()).isEqualTo(instrument);
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
