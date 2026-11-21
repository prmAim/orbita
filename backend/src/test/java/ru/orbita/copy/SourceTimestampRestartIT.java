package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static ru.orbita.broker.WholeChainBrokerSimulator.*;

import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.SourceFeedLifecycle;
import ru.orbita.broker.WholeChainBrokerSimulator;
import ru.orbita.control.ControlService;
import ru.tinkoff.piapi.contract.v1.*;

@Testcontainers
class SourceTimestampRestartIT {
  private static final String TRADE = "a70-trade";
  private static final String ORDER = "a70-order";
  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  @Container final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17.11");
  @TempDir Path directory;

  @Test
  void duplicatesAndRestartDoNotExtendFirstReceiptOrAdmitLateHistory() throws Exception {
    var clock = new CopyLifecycleIT.MutableClock();
    var first = clock.instant().plusSeconds(1);
    var missing =
        OrderTrades.newBuilder()
            .setAccountId(SOURCE)
            .setOrderId(ORDER)
            .setInstrumentUid(INSTRUMENT)
            .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
            .addTrades(
                OrderTrade.newBuilder()
                    .setTradeId(TRADE)
                    .setQuantity(1)
                    .setPrice(Quotation.newBuilder().setUnits(100)))
            .build();
    try (var bank = WholeChainBrokerSimulator.open(directory)) {
      try (var app = WholeChainTestApplication.start(postgres, bank, clock)) {
        var control = app.getBean(ControlService.class);
        control.markReconciled();
        control.start();
        subscribe(app, bank);
        var db = app.getBean(JdbcTemplate.class);
        await().atMost(TIMEOUT).until(() -> feedReady(db));
        clock.now = first;
        bank.emitSourceFrame(missing);
        drain(bank);
        assertPending(db, first);
        clock.now = first.plusSeconds(40);
        bank.emitSourceFrame(missing);
        drain(bank);
        assertPending(db, first);
        clock.now = first.plusSeconds(49);
        bank.emitSourceFrame(missing);
        drain(bank);
        assertPending(db, first);
      }

      clock.now = first.plusSeconds(50);
      try (var restarted = WholeChainTestApplication.start(postgres, bank, clock)) {
        var db = restarted.getBean(JdbcTemplate.class);
        subscribe(restarted, bank);
        drain(bank);
        assertPending(db, first);
        assertThat(feedReady(db)).isFalse();
        // No copy of the target frame is sent after restarting the application.
        clock.now = first.plusSeconds(60);
        drain(bank);
        long readsAtBoundary = coveringSourceHistoryReads(bank, first);
        await()
            .atMost(TIMEOUT)
            .until(() -> coveringSourceHistoryReads(bank, first) > readsAtBoundary);
        drain(bank);
        assertPending(db, first);

        clock.now = first.plusSeconds(61);
        await()
            .atMost(TIMEOUT)
            .until(
                () ->
                    "SKIPPED"
                        .equals(
                            db.queryForObject(
                                "select disposition from source_execution", String.class)));
        assertExpired(db, first);

        // Publish only to Operations. No stream frame supplies the recovered timestamp.
        var lifecycle = restarted.getBean(SourceFeedLifecycle.class);
        bank.publishSourceHistory(
            INSTRUMENT, TRADE, ORDER, BrokerGateway.Side.BUY, 1, PRICE, first);
        var history = restarted.getBean(BrokerGateway.class).history(first, clock.instant());
        assertThat(history.executions())
            .singleElement()
            .satisfies(
                e -> {
                  assertThat(e.tradeId()).isEqualTo(TRADE);
                  assertThat(e.executedAt()).isEqualTo(first);
                });
        long historyReads = coveringSourceHistoryReads(bank, first);
        lifecycle.close();
        subscribe(restarted, bank);
        drain(bank);
        assertThat(coveringSourceHistoryReads(bank, first)).isGreaterThan(historyReads);
        assertThat(feedReady(db)).isTrue();
        restarted.getBean(ControlService.class).markReconciled();
        restarted.getBean(CopyService.class).retryPending();
        restarted.getBean(TradingService.class).dispatchReady();
        assertExpired(db, first);
        assertThat(restarted.getBean(CopyService.class).intents()).isEmpty();
        assertThat(bank.calls("PostOrder")).isEmpty();
      }
    }
  }

  private static void subscribe(ConfigurableApplicationContext app, WholeChainBrokerSimulator bank)
      throws Exception {
    app.getBean(SourceFeedLifecycle.class).ensureObservation();
    assertThat(bank.awaitSubscription(TIMEOUT)).isTrue();
  }

  // The metadata read for the empty frame follows processing of previous frames on the same lane.
  private static void drain(WholeChainBrokerSimulator bank) {
    String markerInstrument = bank.securities().get(1).id();
    java.util.function.LongSupplier markers =
        () ->
            bank.calls("GetInstrumentBy").stream()
                .filter(r -> ((InstrumentRequest) r.request()).getId().equals(markerInstrument))
                .count();
    long before = markers.getAsLong();
    bank.emitSourceFrame(
        OrderTrades.newBuilder()
            .setAccountId(SOURCE)
            .setOrderId("marker")
            .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
            .setInstrumentUid(markerInstrument)
            .build());
    await().atMost(TIMEOUT).until(() -> markers.getAsLong() > before);
  }

  private static long coveringSourceHistoryReads(WholeChainBrokerSimulator bank, Instant at) {
    return bank.calls("GetOperationsByCursor").stream()
        .map(r -> (GetOperationsByCursorRequest) r.request())
        .filter(r -> r.getAccountId().equals(SOURCE) && r.getCursor().isEmpty())
        .filter(
            r ->
                r.hasFrom()
                    && r.hasTo()
                    && !Instant.ofEpochSecond(r.getFrom().getSeconds(), r.getFrom().getNanos())
                        .isAfter(at)
                    && !Instant.ofEpochSecond(r.getTo().getSeconds(), r.getTo().getNanos())
                        .isBefore(at))
        .count();
  }

  private static boolean feedReady(JdbcTemplate db) {
    return Boolean.TRUE.equals(
        db.queryForObject("select feed_ready from control_state where id=1", Boolean.class));
  }

  private static void assertFirstReceipt(JdbcTemplate db, Instant first) {
    var saved =
        db.queryForMap(
            "select id, first_received, first_earliest, first_latest from incoming_receipt");
    assertThat(saved)
        .containsEntry("id", CopyService.key(SOURCE, TRADE))
        .containsEntry("first_received", Timestamp.from(first))
        .containsEntry("first_earliest", Timestamp.from(first))
        .containsEntry("first_latest", Timestamp.from(first));
  }

  private static void assertPending(JdbcTemplate db, Instant first) {
    assertFirstReceipt(db, first);
    assertThat(db.queryForMap("select trade_id, disposition, executed_at from source_execution"))
        .containsEntry("trade_id", TRADE)
        .containsEntry("disposition", "RECEIVED")
        .containsEntry("executed_at", null);
  }

  private static void assertExpired(JdbcTemplate db, Instant first) {
    assertFirstReceipt(db, first);
    assertThat(
            db.queryForMap(
                "select trade_id, source_order_id, disposition, accepted_sequence from source_execution"))
        .containsEntry("trade_id", TRADE)
        .containsEntry("source_order_id", ORDER)
        .containsEntry("disposition", "SKIPPED")
        .containsEntry("accepted_sequence", 0L);
    assertThat(
            db.queryForMap(
                "select occurred_at, detail from journal_entry where category='SOURCE_SKIPPED'"))
        .containsEntry("occurred_at", Timestamp.from(first.plusSeconds(61)))
        .containsEntry(
            "detail", CopyService.key(SOURCE, TRADE) + " Source timestamp recovery expired");
  }
}
