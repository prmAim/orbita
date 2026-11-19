package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestBuilders.formLogin;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.*;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.control.ControlService;
import ru.orbita.notifications.TelegramTransport;
import ru.orbita.web.OwnerCredentials;

@SpringBootTest(
    properties = {
      "orbita.source-account=source",
      "orbita.mirror-account=mirror",
      "orbita.clock-uncertainty-millis=0",
      "orbita.margin-disabled-confirmed=true",
      "orbita.worker.enabled=false"
    })
@AutoConfigureMockMvc
@Import(CopyLifecycleIT.Fixture.class)
class CopyLifecycleIT {
  @Autowired CopyService copies;
  @Autowired TradingService trading;

  private TradingService tradingTarget() {
    return org.springframework.test.util.AopTestUtils.getUltimateTargetObject(trading);
  }

  @Autowired ControlService control;
  @Autowired Simulator broker;
  @Autowired MutableClock clock;
  @Autowired org.springframework.jdbc.core.JdbcTemplate db;
  @Autowired ru.orbita.control.AlgorithmVersion algorithms;
  @Autowired AcceptanceService acceptance;
  @Autowired DurableSourceFeedJournal feed;
  @Autowired PositionRecoveryService recovery;
  @Autowired ManualReconciliationService manual;
  @Autowired ReconciliationService reconciliation;
  @Autowired ru.orbita.notifications.NotificationService notifications;
  @Autowired ru.orbita.runtime.ApplicationReadiness readiness;
  @Autowired MockMvc mvc;
  @Autowired OwnerCredentials ownerCredentials;
  @Autowired TelegramSimulator telegram;

  @org.junit.jupiter.api.BeforeEach
  void clean() {
    reset(db, broker, clock);
    telegram.messages.clear();
  }

  static void reset(
      org.springframework.jdbc.core.JdbcTemplate db, Simulator broker, MutableClock clock) {
    db.execute(
        "truncate broker_fill,copy_intent,source_execution,incoming_receipt,mirror_position,mirror_trade_receipt,instrument_state,journal_entry,notification_attempt,notification restart identity cascade");
    db.update(
        "update control_state set mode='NOT_STARTED',coefficient=null,ratio_numerator=null,ratio_denominator=null,active_capital_candidate_id=null,observation_boundary=null,resume_boundary=null,observation_boundary_trusted=false,resume_boundary_trusted=false,observation_activated=false,automatic_boundary=null,permit_limit=null,permit_issued_at=null,permit_id=null,permit_epoch=0,first_acceptance=true,live_accepted=false,reconciled=false,feed_ready=false,shadow_version=null,shadow_buy_checked=false,shadow_sell_checked=false,recalculation_paused=false,attempt_id=null,attempt_kind=null,generation=0,completed_due=null,retry_at=null,last_error=null,pause_started_at=null,failed_attempts=0 where id=1");
    clock.now = Instant.parse("2026-11-15T10:00:00Z");
    clock.evidenceAvailable = true;
    clock.nextEvidence = null;
    broker.posts = 0;
    broker.resourcesCalls = 0;
    broker.capitalCalls = 0;
    broker.capitalHook = null;
    broker.capitalResponse = null;
    broker.afterPostHook = null;
    broker.postHook = null;
    broker.afterReconcileHook = null;
    clock.receiptInstants.clear();
    broker.resourcesHook = null;
    broker.resourcesRequestHook = null;
    broker.blockPurchases = false;
    broker.settlementCurrency = "RUB";
    broker.cancels = 0;
    broker.mirrorCapital = new BigDecimal("100000");
    broker.sourceCapital = new BigDecimal("100000");
    broker.failCapital = false;
    broker.historyExecutions.clear();
    broker.failHistory = false;
    broker.quotes.clear();
    broker.mirrorExecutions.clear();
    broker.fillEveryPost = false;
    broker.rejectNext = null;
    broker.oppositePrice = new BigDecimal("100");
    broker.bankLots = 0;
    broker.extraBankLots = 0;
    broker.availableMoney = new BigDecimal("10000");
    broker.buyMaxLots = 100;
    broker.preliminaryCommission = BigDecimal.ONE;
    broker.additionalCommissionReserve = BigDecimal.ONE;
    broker.partialNext = -1;
    broker.orders.clear();
    broker.currencyCash = false;
    broker.zeroFees = false;
    broker.buyCommissionPerLot = null;
    broker.minimumBuyCommissionFloor = null;
    broker.buyTotalCosts.clear();
    broker.monotonicBuyPricing = true;
    broker.attestBuyTotalCosts = false;
    broker.finalCommissionAvailable = true;
    broker.instrumentLotSize = 1;
    broker.instrumentHook = null;
    broker.instrumentIdHook = null;
    broker.sentRequests.clear();
    broker.individualLotFills = false;
    broker.sourceCash = new BigDecimal("100");
    broker.mirrorCash = new BigDecimal("100");
  }

  @Test
  void acceptancePermitWaitsForPreviousAttemptAndIndependentFinalReconciliation() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    broker.partialNext = 0;
    copies.accept(source("first-acceptance", Side.BUY, 1, clock.instant()));
    trading.dispatchReady();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("ACTIVE"));
    assertThatThrownBy(() -> control.issuePermit(new BigDecimal("1000")))
        .isInstanceOf(IllegalStateException.class);
    var next =
        new SourceExecution(
            "source",
            "second-acceptance",
            "second-order",
            "other",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    copies.accept(next);
    trading.dispatchReady();
    assertThat(broker.posts).isEqualTo(1);
    var request = broker.sent;
    trading.apply(broker.evidence(request, OrderStatus.UNKNOWN, 0), false);
    assertThatThrownBy(() -> control.issuePermit(new BigDecimal("1000")))
        .isInstanceOf(IllegalStateException.class);
    broker.orders.put(request.requestId(), broker.evidence(request, OrderStatus.CANCELLED, 0));
    trading.apply(broker.evidence(request, OrderStatus.CANCELLED, 0), true);
    assertThatThrownBy(() -> control.issuePermit(new BigDecimal("1000")))
        .isInstanceOf(IllegalStateException.class);
    trading.reconcileOnly();
    assertThatThrownBy(() -> control.issuePermit(new BigDecimal("1000")))
        .isInstanceOf(IllegalStateException.class);
    broker.historyExecutions.add(next);
    assertThat(reconciliation.reconcile()).isTrue();
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(
        new SourceExecution(
            "source",
            "third-acceptance",
            "third-order",
            "other",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    trading.dispatchReady();
    assertThat(broker.posts).isEqualTo(2);
  }

  @Test
  void correctedArtifactReplaysSavedNaturalInputsWithoutChangingOldResultsOrSending() {
    var old = savedNaturalInputsWithFormerSellFailure();
    broker.historyExecutions.clear();
    broker.failHistory = true;
    control.recoverStartup();
    new ru.orbita.control.TradingWorker(
            control, copies, trading, reconciliation, recovery, acceptance, manual, readiness)
        .run();
    assertThat(control.shadowQualified()).isTrue();
    assertThat(acceptance.checks()).hasSize(4).containsAll(old);
    assertThat(
            acceptance.checks().stream()
                .filter(c -> c.algorithmVersion().equals(algorithms.current())))
        .hasSize(2)
        .allSatisfy(
            c -> {
              assertThat(c.executionMatches()).isTrue();
              assertThat(c.calculationMatches()).isTrue();
              assertThat(c.replayNewLots()).isEqualTo(1);
              assertThat(c.replayContribution()).isEqualByComparingTo("1");
            });
    assertThat(
            db.queryForObject(
                "select new_lots from source_execution where side='SELL'", Long.class))
        .isEqualTo(999);
    assertThat(broker.posts).isZero();
    assertThat(copies.intents()).hasSize(2);
  }

  private List<ShadowCheck.View> savedNaturalInputsWithFormerSellFailure() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    clock.now = clock.now.plusSeconds(1);
    var buy = source("saved-buy", Side.BUY, 1, clock.instant());
    copies.accept(buy);
    clock.now = clock.now.plusSeconds(1);
    var sell = source("saved-sell", Side.SELL, 1, clock.instant());
    copies.accept(sell);
    acceptance.verifySavedShadow();
    assertThat(acceptance.checks())
        .hasSize(2)
        .allSatisfy(c -> assertThat(c.executionMatches()).isTrue());
    // Model persisted evidence from the former artifact, whose SELL arithmetic was wrong.
    db.update("update source_execution set contribution=999,new_lots=999 where side='SELL'");
    db.update(
        "update shadow_check set algorithm_version='old-buggy-artifact',calculation_matches=false,detail='old incorrect SELL result' where side='SELL'");
    db.update("update shadow_check set algorithm_version='old-buggy-artifact' where side='BUY'");
    control.shadowChecked("old-buggy-artifact", "BUY", true);
    control.shadowChecked("old-buggy-artifact", "SELL", false);
    return acceptance.checks();
  }

  @Test
  void legacyShadowProtocolsAcquireStructuredEvidenceWithoutOverwritingHistoricalFailure() {
    savedNaturalInputsWithFormerSellFailure();
    db.update("update shadow_check set algorithm_version='legacy',independent_history=null");
    var old = acceptance.checks();
    broker.failHistory = false;
    acceptance.verifySavedShadow();
    assertThat(acceptance.checks()).hasSize(4).containsAll(old);
    assertThat(control.shadowQualified()).isTrue();
    assertThat(
            db.queryForObject(
                "select count(*) from shadow_check where independent_history is not null",
                Long.class))
        .isEqualTo(2);
    assertThat(broker.posts).isZero();
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "FILLED,false",
    "REJECTED,false",
    "FILLED,true",
    "REJECTED,true"
  })
  void terminalAcceptanceResponseStillRequiresIndependentOrderFeeAndPositionReconciliation(
      OrderStatus status, boolean zeroFees) {
    broker.zeroFees = zeroFees;
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    var execution = source("terminal-acceptance", Side.BUY, 1, clock.instant());
    broker.fillEveryPost = status == OrderStatus.FILLED;
    if (status == OrderStatus.REJECTED) broker.rejectNext = RejectionReason.OTHER;
    copies.accept(execution);
    trading.dispatchReady();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("FINAL"));
    assertThatThrownBy(() -> control.issuePermit(new BigDecimal("2000")))
        .isInstanceOf(IllegalStateException.class);
    reconciliation.reconcile();
    assertThatThrownBy(() -> control.issuePermit(new BigDecimal("2000")))
        .isInstanceOf(IllegalStateException.class);
    trading.reconcileOnly();
    assertThatThrownBy(() -> control.issuePermit(new BigDecimal("2000")))
        .isInstanceOf(IllegalStateException.class);
    assertThat(reconciliation.reconcile()).isTrue();
    control.issuePermit(new BigDecimal("2000"));
    assertThat(control.status().permitLimit()).isEqualByComparingTo("2000");
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void automaticModeStillDispatchesAnotherInstrumentWhileFirstOrderIsActive() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    broker.partialNext = 0;
    copies.accept(source("automatic-one", Side.BUY, 1, clock.instant()));
    trading.dispatchReady();
    broker.partialNext = 0;
    copies.accept(
        new SourceExecution(
            "source",
            "automatic-two",
            "automatic-two",
            "other",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    trading.dispatchReady();
    assertThat(broker.posts).isEqualTo(2);
    assertThat(copies.intents())
        .hasSize(2)
        .allSatisfy(i -> assertThat(i.status()).isEqualTo("ACTIVE"));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(Side.class)
  void haltRetryRechecksFinalQuoteAndKeepsFullQuantityWhenEqual(Side side) {
    runningAtFive();
    if (side == Side.SELL) {
      clock.now = clock.now.plusSeconds(1);
      copies.accept(source("holding", Side.BUY, 20, clock.instant()));
      trading.tick();
    }
    clock.now = clock.now.plusSeconds(1);
    broker.rejectNext = RejectionReason.HALTED;
    copies.accept(source("halt-price", side, 10, clock.instant()));
    trading.tick();
    int posts = broker.posts;
    broker.quotes.addAll(List.of(new BigDecimal("100"), new BigDecimal("100.1")));
    trading.dispatchReady();
    assertThat(broker.posts).isEqualTo(posts);
    broker.oppositePrice = new BigDecimal("100");
    trading.dispatchReady();
    assertThat(broker.posts).isEqualTo(posts + 1);
    assertThat(broker.sent.lots()).isEqualTo(50);
  }

  @Test
  void lostPostReplyAndDuplicateSourceHaveOneBrokerOrderAcrossRecovery() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    var source =
        new SourceExecution(
            "source",
            "t1",
            "o1",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    assertThat(copies.accept(source).newLots()).isEqualTo(1);
    trading.tick();
    assertThat(copies.intents())
        .singleElement()
        .extracting(CopyIntent.View::status)
        .isEqualTo("UNKNOWN");
    assertThat(copies.accept(source).newLots()).isEqualTo(1);
    trading.tick();
    trading.tick();
    assertThat(broker.posts).isEqualTo(1);
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.status()).isEqualTo("FINAL");
              assertThat(i.executedLots()).isEqualTo(1);
            });
    assertThat(control.status().permitLimit()).isNull();
  }

  @Test
  void stopDuringCapitalReadInvalidatesTheLateResultAndNextStartGetsFreshPair() {
    control.markReconciled();
    control.start();
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook = () -> control.stop();
    control.recalculate();
    assertThat(control.status().coefficient()).isEqualByComparingTo("1");
    assertThat(control.status().mode()).isEqualTo("STOPPING");
    broker.capitalHook = null;
    control.stoppedAfterReconciliation();
    control.markReconciled();
    control.start();
    assertThat(control.status().coefficient()).isEqualByComparingTo("2");
    assertThat(control.status().mode()).isEqualTo("SHADOW");
    assertThat(broker.capitalCalls).isEqualTo(6);
  }

  @Test
  void severalMissedMoscowDeadlinesHaveOneAtomicCurrentRecalculationAndStopDisablesCatchup() {
    control.markReconciled();
    control.start();
    clock.now = clock.now.plus(Duration.ofDays(3));
    assertThat(control.mandatoryDue()).isTrue();
    broker.mirrorCapital = new BigDecimal("300000");
    control.mandatoryRecalculation();
    assertThat(control.status().coefficient()).isEqualByComparingTo("3");
    assertThat(control.mandatoryDue()).isFalse();
    assertThat(broker.capitalCalls).isEqualTo(4);
    control.stop();
    clock.now = clock.now.plus(Duration.ofDays(3));
    assertThat(control.mandatoryDue()).isFalse();
  }

  @Test
  void failedFirstStartRemainsNotStartedAndZeroMirrorCapitalIsASuccessfulNextExplicitStart() {
    control.markReconciled();
    broker.failCapital = true;
    control.start();
    assertThat(control.status().mode()).isEqualTo("NOT_STARTED");
    assertThat(control.status().coefficient()).isNull();
    broker.failCapital = false;
    broker.mirrorCapital = BigDecimal.ZERO;
    assertThat(control.status().coefficient()).isNull();
    control.start();
    assertThat(control.status().mode()).isEqualTo("SHADOW");
    assertThat(control.status().coefficient()).isEqualByComparingTo("0");
  }

  @Test
  void statusApiExposesMandatoryRecalculationFailuresSeparatelyFromTelegramFailures()
      throws Exception {
    runningAtFive();
    broker.failCapital = true;

    control.mandatoryRecalculation();
    clock.now = clock.now.plusSeconds(61);
    control.mandatoryRecalculation();

    assertThat(control.status().recalculationPaused()).isTrue();
    assertThat(
            db.queryForObject(
                "select failed_attempts from control_state where id=1", Integer.class))
        .isEqualTo(2);
    assertThat(notifications.list(100))
        .filteredOn(event -> event.category().equals("RECALCULATION_ERROR"))
        .singleElement()
        .satisfies(
            event -> {
              assertThat(event.status()).isEqualTo("PENDING");
              assertThat(event.automaticFailures()).isZero();
            });
    var homeResponse =
        mvc.perform(get("/index.html")).andExpect(status().isOk()).andReturn().getResponse();
    var home = new String(homeResponse.getContentAsByteArray(), StandardCharsets.UTF_8);
    assertThat(home).contains("Ошибок обязательного пересчёта");
    var appResponse =
        mvc.perform(get("/app.js")).andExpect(status().isOk()).andReturn().getResponse();
    var app = new String(appResponse.getContentAsByteArray(), StandardCharsets.UTF_8);
    assertThat(app).contains("failed-attempts", "status.failedAttempts");

    ownerCredentials.replace("synthetic-only-passphrase".toCharArray());
    try {
      var login =
          mvc.perform(formLogin("/api/login").user("owner").password("synthetic-only-passphrase"))
              .andExpect(status().isNoContent())
              .andReturn();
      var session = (MockHttpSession) login.getRequest().getSession(false);
      mvc.perform(get("/api/status").session(session))
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.failedAttempts").value(2));
    } finally {
      ownerCredentials.lock();
    }
  }

  @Test
  void savedNaturalBuyAndSellAreIndependentlyRecheckedForCurrentCalculationArtifact() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    clock.now = clock.now.plusSeconds(1);
    var buy =
        new SourceExecution(
            "source",
            "natural-buy",
            "order-buy",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    copies.accept(buy);
    clock.now = clock.now.plusSeconds(1);
    var sell =
        new SourceExecution(
            "source",
            "natural-sell",
            "order-sell",
            "instrument",
            Side.SELL,
            BigDecimal.ONE,
            new BigDecimal("101"),
            "RUB",
            clock.instant(),
            clock.instant());
    copies.accept(sell);
    broker.historyExecutions.addAll(List.of(buy, sell));
    acceptance.verifySavedShadow();
    assertThat(acceptance.checks())
        .hasSize(2)
        .allSatisfy(
            c -> {
              assertThat(c.executionMatches()).isTrue();
              assertThat(c.calculationMatches()).isTrue();
              assertThat(c.algorithmVersion()).isEqualTo(algorithms.current());
            });
    assertThat(control.shadowQualified()).isTrue();
    assertThat(broker.posts).isZero();
    control.shadowChecked("older-algorithm", "BUY", true);
    assertThat(control.shadowQualified()).isFalse();
    acceptance.verifySavedShadow();
    assertThat(control.shadowQualified()).isTrue();
    assertThat(acceptance.checks()).hasSize(2);
  }

  @Test
  void missingTimestampSearchUsesDurableFirstReceiptAndNeverResurrectsExpiredExecution() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    clock.now = clock.now.plusSeconds(1);
    Instant received = clock.instant();
    var missing =
        new SourceExecution(
            "source",
            "missing-time",
            "order-missing",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            null,
            received);
    feed.firstReceived("source", "missing-time", received);
    assertThat(feed.receiptTimeWindow("source", "missing-time")).isEmpty();
    var captured = new ru.orbita.broker.SourceFeedJournal.TimeWindow(received, received);
    feed.firstReceived("source", "captured-arrival", received, captured);
    assertThat(feed.receiptTimeWindow("source", "captured-arrival")).contains(captured);
    assertThat(feed.firstReceived("source", "captured-arrival", received.plusSeconds(10)))
        .isEqualTo(received);
    assertThat(feed.receiptTimeWindow("source", "captured-arrival")).contains(captured);
    feed.remember(missing);
    clock.now = clock.now.plusSeconds(61);
    feed.skip(missing, "Не восстановлена метка за 60 секунд");
    var late =
        new SourceExecution(
            "source",
            "missing-time",
            "order-missing",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    feed.accept(late);
    assertThat(copies.intents()).isEmpty();
    assertThat(feed.firstReceived("source", "missing-time", clock.instant())).isEqualTo(received);
    assertThat(copies.accept(late).disposition()).isEqualTo("SKIPPED");
  }

  @Test
  void completeSourceCloseUsesAllFiftyCopiedLotsAfterCoefficientChangesToFour() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("buy", Side.BUY, 10, clock.instant()));
    trading.tick();
    broker.mirrorCapital = new BigDecimal("400000");
    control.recalculate();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("close", Side.SELL, 10, clock.instant()));
    reconciliation.reconcile();
    recovery.recover();
    trading.tick();
    assertThat(copies.intents().stream().filter(i -> i.side().equals("SELL")))
        .singleElement()
        .satisfies(i -> assertThat(i.executedLots()).isEqualTo(50));
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isZero());
  }

  @Test
  void lateOldCloseSellsOnlyFiftyAndPreservesTwentyFiveFromReopening() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    Instant first = clock.instant();
    copies.accept(source("old-buy", Side.BUY, 10, first));
    trading.tick();
    clock.now = clock.now.plusSeconds(2);
    copies.accept(source("new-buy", Side.BUY, 5, clock.instant()));
    trading.tick();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("late-close", Side.SELL, 10, first.plusSeconds(1)));
    reconciliation.reconcile();
    recovery.recover();
    trading.tick();
    assertThat(copies.intents().stream().filter(i -> i.side().equals("SELL")))
        .singleElement()
        .satisfies(i -> assertThat(i.executedLots()).isEqualTo(50));
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isEqualTo(25));
    assertThat(broker.posts).isEqualTo(3);
  }

  @Test
  void saleDeliveredBeforeItsBuyDoesNotCreateRoundTripAndOnlyRemainingThirtyAreBought() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(2);
    Instant saleAt = clock.instant();
    copies.accept(source("early-sale", Side.SELL, 4, saleAt));
    trading.tick();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("late-buy", Side.BUY, 10, saleAt.minusSeconds(1)));
    reconciliation.reconcile();
    recovery.recover();
    trading.tick();
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isEqualTo(30));
    assertThat(broker.posts).isEqualTo(1);
    assertThat(copies.intents().stream().filter(i -> i.side().equals("BUY")))
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.executedLots()).isEqualTo(30);
              assertThat(i.skippedLots()).isEqualTo(20);
            });
  }

  @Test
  void recoveryPublishesNoticeForTheBuyVolumeActuallySkippedAfterAConfirmedClose()
      throws Exception {
    runningAtFive();
    clock.now = clock.now.plusSeconds(2);
    Instant saleAt = clock.instant();
    copies.accept(source("recovery-close", Side.SELL, 10, saleAt));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("recovery-buy", Side.BUY, 10, saleAt.minusSeconds(1)));

    reconciliation.reconcile();
    recovery.recover();

    assertThat(copies.intents())
        .filteredOn(intent -> intent.sourceOrderId().equals("order-recovery-buy"))
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("FINAL");
              assertThat(intent.executedLots()).isZero();
              assertThat(intent.lots()).isEqualTo(50);
              assertThat(intent.skippedLots()).isEqualTo(50);
            });
    var notices =
        notifications.list(100).stream()
            .filter(event -> event.category().equals("COPY_SKIPPED"))
            .toList();
    assertThat(notices).hasSize(1);
    var notice = notices.getFirst();
    assertThat(notice.instrument()).isEqualTo("instrument");
    assertThat(notice.text())
        .contains(
            "instrument",
            "Покупка",
            "исполнено 0/50",
            "пропущено 50 лотов",
            "Подтверждённое закрытие");
    assertThat(
            db.queryForObject(
                "select j.detail from notification n join journal_entry j on j.id=n.journal_id where n.id=?",
                String.class,
                notice.id()))
        .isEqualTo(notice.text());

    ownerCredentials.replace("synthetic-only-passphrase".toCharArray());
    try {
      var login =
          mvc.perform(formLogin("/api/login").user("owner").password("synthetic-only-passphrase"))
              .andExpect(status().isNoContent())
              .andReturn();
      var session = (MockHttpSession) login.getRequest().getSession(false);
      mvc.perform(get("/api/notifications?limit=100").session(session))
          .andExpect(status().isOk())
          .andExpect(
              jsonPath(
                  "$[?(@.id=='" + notice.id() + "')].text",
                  org.hamcrest.Matchers.hasItem(notice.text())));

      notifications.deliverDue();
      assertThat(telegram.messages).contains(notice.text());
      assertThat(notifications.get(notice.id()).status()).isEqualTo("DELIVERED");
    } finally {
      ownerCredentials.lock();
    }
  }

  @Test
  void fullCloseRecoveryPreservesPriorResourceSkipInItsNotice() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("recovery-full-close-buy", Side.BUY, 2, clock.instant()));
    assertThat(
            db.update(
                "update copy_intent set skipped_lots=2,reason=? where source_order_id=?",
                TradeNotificationText.RESOURCE_LIMIT_SKIP_REASON,
                "order-recovery-full-close-buy"))
        .isEqualTo(1);
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("recovery-full-close-sale", Side.SELL, 2, clock.instant()));

    reconciliation.reconcile();
    recovery.recover();

    assertThat(copies.intents())
        .filteredOn(intent -> intent.sourceOrderId().equals("order-recovery-full-close-buy"))
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("FINAL");
              assertThat(intent.executedLots()).isZero();
              assertThat(intent.lots()).isEqualTo(10);
              assertThat(intent.skippedLots()).isEqualTo(10);
              assertThat(intent.reason())
                  .contains(
                      TradeNotificationText.RESOURCE_LIMIT_SKIP_REASON,
                      "Подтверждённое закрытие или проданная доля исходной покупки");
            });
    assertThat(notifications.list(100))
        .filteredOn(event -> event.category().equals("COPY_SKIPPED"))
        .singleElement()
        .satisfies(
            notice ->
                assertThat(notice.text())
                    .contains(
                        "исполнено 0/10",
                        "пропущено 10 лотов",
                        TradeNotificationText.RESOURCE_LIMIT_SKIP_REASON,
                        "Подтверждённое закрытие или проданная доля исходной покупки"));
  }

  @Test
  void expiredFinalSkipKeepsDistinctLastWaitingReasonWithoutDuplicateOrDanglingLabel() {
    String partial = TradeNotificationText.PARTIAL_RECOVERY_SKIP_REASON;
    String resource = TradeNotificationText.RESOURCE_LIMIT_SKIP_REASON;
    String reason = TradeNotificationText.expiredReason(partial, partial + "; " + resource);
    String notification = TradeNotificationText.finalSkip("SBER", "BUY", 0, 10, 10, reason);

    assertThat(notification)
        .contains("причина: " + partial + "; последнее ожидание: " + resource)
        .doesNotContain("последнее ожидание: " + partial);

    String withoutDistinctContext = TradeNotificationText.expiredReason(partial, partial);
    assertThat(withoutDistinctContext).isEqualTo(partial).doesNotContain("последнее ожидание:");
  }

  @Test
  void preserveSkipReasonsDeduplicatesMixedKnownReasonsInEitherOrder() {
    String partial = TradeNotificationText.PARTIAL_RECOVERY_SKIP_REASON;
    String resource = TradeNotificationText.RESOURCE_LIMIT_SKIP_REASON;

    assertThat(TradeNotificationText.preserveSkipReasons(partial, resource + "; " + partial))
        .isEqualTo(partial + "; " + resource);
    assertThat(TradeNotificationText.preserveSkipReasons(resource, partial + "; " + resource))
        .isEqualTo(resource + "; " + partial);
  }

  @Test
  void recoveryDoesNotNotifyForBuyVolumeFoldedIntoOneCorrectiveSale() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("recovery-first-buy", Side.BUY, 10, clock.instant()));
    trading.tick();
    broker.mirrorCapital = new BigDecimal("300000");
    control.recalculate();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("recovery-second-buy", Side.BUY, 10, clock.instant()));
    broker.instrumentHook =
        () -> {
          throw new IllegalStateException("synthetic metadata wait");
        };
    try {
      trading.tick();
    } finally {
      broker.instrumentHook = null;
    }
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("recovery-net-sale", Side.SELL, 10, clock.instant()));

    control.instrumentReconciled(
        "instrument", true, false, "Восстановление подтверждённой последовательности");
    trading.reconcileOnly();
    reconciliation.reconcile();
    recovery.recover();

    assertThat(copies.intents())
        .filteredOn(intent -> intent.sourceOrderId().equals("order-recovery-second-buy"))
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("FINAL");
              assertThat(intent.skippedLots()).isEqualTo(30);
              assertThat(intent.reason()).isEqualTo("Объём учтён единой корректирующей продажей");
            });
    assertThat(copies.intents())
        .filteredOn(intent -> intent.sourceOrderId().equals("order-recovery-net-sale"))
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("WAITING");
              assertThat(intent.lots()).isEqualTo(10);
            });
    assertThat(notifications.list(100))
        .noneMatch(
            event ->
                event.category().equals("COPY_SKIPPED") && event.instrument().equals("instrument"));
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void partialRecoverySkipKeepsItsReasonAndDoesNotReportItAsAResourceLimit() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("recovery-partial-buy", Side.BUY, 10, clock.instant()));
    broker.instrumentHook =
        () -> {
          throw new IllegalStateException("synthetic metadata wait");
        };
    try {
      trading.tick();
    } finally {
      broker.instrumentHook = null;
    }

    // Seed the persisted pre-send resource skip that recovery must retain while changing its size.
    assertThat(
            db.update(
                "update copy_intent set skipped_lots=5,reason=? where source_order_id=?",
                TradeNotificationText.RESOURCE_LIMIT_SKIP_REASON,
                "order-recovery-partial-buy"))
        .isEqualTo(1);

    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("recovery-partial-sale", Side.SELL, 5, clock.instant()));
    broker.instrumentHook =
        () -> {
          throw new IllegalStateException("synthetic metadata wait");
        };
    try {
      trading.tick();
    } finally {
      broker.instrumentHook = null;
    }
    control.instrumentReconciled(
        "instrument", true, false, "Восстановление подтверждённой последовательности");
    trading.reconcileOnly();
    reconciliation.reconcile();
    recovery.recover();

    assertThat(copies.intents())
        .filteredOn(intent -> intent.sourceOrderId().equals("order-recovery-partial-buy"))
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("WAITING");
              assertThat(intent.lots()).isEqualTo(50);
              assertThat(intent.skippedLots()).isEqualTo(25);
              assertThat(intent.reason())
                  .contains(
                      TradeNotificationText.RESOURCE_LIMIT_SKIP_REASON,
                      TradeNotificationText.PARTIAL_RECOVERY_SKIP_REASON);
            });

    broker.fillEveryPost = false;
    trading.dispatchReady();

    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.sent.lots()).isEqualTo(25);
    assertThat(copies.intents())
        .filteredOn(intent -> intent.sourceOrderId().equals("order-recovery-partial-buy"))
        .singleElement()
        .satisfies(intent -> assertThat(intent.status()).isEqualTo("UNKNOWN"));
    broker.orders.put(
        broker.sent.requestId(), broker.evidence(broker.sent, OrderStatus.CANCELLED, 10));
    trading.reconcileOnly();
    assertThat(copies.intents())
        .filteredOn(intent -> intent.sourceOrderId().equals("order-recovery-partial-buy"))
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("FINAL");
              assertThat(intent.executedLots()).isEqualTo(10);
            });
    var notices =
        notifications.list(100).stream()
            .filter(
                event ->
                    event.instrument().equals("instrument")
                        && Set.of("RESOURCE_SKIP", "ORDER_FINAL_SKIP", "COPY_SKIPPED")
                            .contains(event.category()))
            .toList();
    assertThat(notices)
        .singleElement()
        .satisfies(
            notice -> {
              assertThat(notice.category()).isEqualTo("ORDER_FINAL_SKIP");
              assertThat(notice.text())
                  .contains(
                      "instrument",
                      "Покупка",
                      "исполнено 10/50",
                      "пропущено 40 лотов",
                      TradeNotificationText.RESOURCE_LIMIT_SKIP_REASON,
                      TradeNotificationText.PARTIAL_RECOVERY_SKIP_REASON,
                      TradeNotificationText.CONFIRMED_PARTIAL_CANCEL_UNKNOWN_REASON);
              assertThat(notice.text())
                  .doesNotContain(
                      "PostOrder response", "GetOrderState by original UUID", "stages in lots");
            });
  }

  @Test
  void permitSurvivesTooExpensiveShadowAndIsConsumedAtExactlySixThousandWithoutShrinking() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("6000"));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(
        new SourceExecution(
            "source",
            "over",
            "over-order",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("5969.16"),
            "RUB",
            clock.instant(),
            clock.instant()));
    trading.tick();
    assertThat(control.status().permitLimit()).isEqualByComparingTo("6000");
    assertThat(broker.posts).isZero();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(
        new SourceExecution(
            "source",
            "exact",
            "exact-order",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("5968.16"),
            "RUB",
            clock.instant(),
            clock.instant()));
    trading.tick();
    assertThat(control.status().permitLimit()).isNull();
    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.sent.lots()).isEqualTo(1);
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("after-permit", Side.BUY, 1, clock.instant()));
    trading.tick();
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void confirmedManualSaleUsesManualLotsFirstAndOnlyThenReducesCopiedHoldingWhilePauseRemains() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("copied-buy", Side.BUY, 10, clock.instant()));
    trading.tick();
    control.pauseInstrument("instrument", "Согласованное исправление через брокера");
    clock.now = clock.now.plusSeconds(1);
    broker.mirrorExecutions.add(
        new SourceExecution(
            "mirror",
            "manual-buy",
            "",
            "instrument",
            Side.BUY,
            BigDecimal.TEN,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    clock.now = clock.now.plusSeconds(1);
    broker.mirrorExecutions.add(
        new SourceExecution(
            "mirror",
            "manual-sale",
            "",
            "instrument",
            Side.SELL,
            new BigDecimal("15"),
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    manual.reconcilePaused();
    manual.reconcilePaused();
    assertThat(copies.positions())
        .singleElement()
        .satisfies(
            p -> {
              assertThat(p.manualLots()).isZero();
              assertThat(p.copiedLots()).isEqualTo(45);
            });
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isTrue());
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void exactSavedCapitalRatiosOneThirdAndTwoThirdsReachWholeLotBoundaries() {
    broker.sourceCapital = new BigDecimal("3");
    broker.mirrorCapital = BigDecimal.ONE;
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    var first = new ArrayList<Long>();
    for (int index = 0; index < 3; index++) {
      clock.now = clock.now.plusSeconds(1);
      first.add(
          copies
              .accept(
                  new SourceExecution(
                      "source",
                      "third-" + index,
                      "third-order",
                      "instrument",
                      Side.BUY,
                      BigDecimal.ONE,
                      new BigDecimal("100"),
                      "RUB",
                      clock.instant(),
                      clock.instant()))
              .newLots());
    }
    assertThat(first).containsExactly(0L, 0L, 1L);
    broker.mirrorCapital = new BigDecimal("2");
    control.recalculate();
    var second = new ArrayList<Long>();
    for (int index = 0; index < 3; index++) {
      clock.now = clock.now.plusSeconds(1);
      second.add(
          copies
              .accept(
                  new SourceExecution(
                      "source",
                      "two-thirds-" + index,
                      "two-thirds-order",
                      "instrument",
                      Side.BUY,
                      BigDecimal.ONE,
                      new BigDecimal("100"),
                      "RUB",
                      clock.instant(),
                      clock.instant()))
              .newLots());
    }
    assertThat(second).containsExactly(0L, 1L, 1L);
    assertThat(control.status().ratioNumerator()).isEqualTo(java.math.BigInteger.TWO);
    assertThat(control.status().ratioDenominator()).isEqualTo(new java.math.BigInteger("3"));
    assertThat(
            copies
                .accept(
                    new SourceExecution(
                        "source",
                        "third-0",
                        "third-order",
                        "instrument",
                        Side.BUY,
                        BigDecimal.ONE,
                        new BigDecimal("100"),
                        "RUB",
                        Instant.parse("2026-11-15T10:00:01Z"),
                        clock.instant()))
                .acceptedCoefficient())
        .isEqualByComparingTo(
            new BigDecimal("0.33333333333333333333333333333333333333333333333333"));
  }

  @Test
  void confirmedHaltCanRetryAllFiftyLotsOnlyAtTheSameBestOppositePrice() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    broker.rejectNext = RejectionReason.HALTED;
    copies.accept(source("halted", Side.BUY, 10, clock.instant()));
    trading.tick();
    assertThat(broker.posts).isEqualTo(1);
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("HALTED"));
    trading.tick();
    assertThat(broker.posts).isEqualTo(2);
    assertThat(broker.sent.lots()).isEqualTo(50);
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isEqualTo(50));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({"105.99, 0", "106, 1"})
  void reducedBuyRespectsMinimumCommissionAndAdditionalReserve(
      String availableMoney, int expectedPosts) {
    runningAtFive();
    broker.availableMoney = new BigDecimal(availableMoney);
    broker.buyMaxLots = 1;
    broker.additionalCommissionReserve = BigDecimal.ONE;
    broker.instrumentLotSize = 1;
    broker.buyCommissionPerLot = new BigDecimal("0.10");
    broker.minimumBuyCommissionFloor = new BigDecimal("5");
    broker.fillEveryPost = false;
    clock.now = clock.now.plusSeconds(1);
    var execution =
        new SourceExecution(
            "source",
            "reduced-buy-minimum-fee",
            "reduced-buy-minimum-fee-order",
            "instrument",
            Side.BUY,
            new BigDecimal("200"),
            new BigDecimal("99.503"),
            "RUB",
            clock.instant(),
            clock.instant());
    broker.historyExecutions.add(execution);

    assertThat(copies.accept(execution).newLots()).isEqualTo(1000);
    trading.dispatchReady();

    assertThat(broker.posts).isEqualTo(expectedPosts);
    if (expectedPosts == 1) {
      assertThat(broker.sent.lots()).isEqualTo(1);
      assertThat(broker.sent.limitPrice()).isEqualByComparingTo("100");
      assertThat(copies.intents())
          .singleElement()
          .satisfies(intent -> assertThat(intent.reservedMoney()).isEqualByComparingTo("106"));
    } else {
      assertThat(copies.intents())
          .singleElement()
          .satisfies(
              intent -> {
                assertThat(intent.status()).isEqualTo("FINAL");
                assertThat(intent.skippedLots()).isEqualTo(1000);
                assertThat(intent.reservedMoney()).isEqualByComparingTo("0");
              });
      assertThat(copies.journal(0, 200))
          .filteredOn(entry -> entry.category().equals("SEND_STARTED"))
          .isEmpty();
    }
  }

  @Test
  void buyCommissionIsScaledToAffordableLotsBeforeTheOrderIsSent() {
    runningAtFive();
    broker.availableMoney = new BigDecimal("150");
    broker.buyMaxLots = 1;
    broker.preliminaryCommission = new BigDecimal("100");
    broker.buyCommissionPerLot = new BigDecimal("0.10");
    broker.additionalCommissionReserve = BigDecimal.ZERO;
    broker.fillEveryPost = false;
    clock.now = clock.now.plusSeconds(1);
    var execution =
        new SourceExecution(
            "source",
            "buy-commission-volume",
            "buy-commission-volume-order",
            "instrument",
            Side.BUY,
            new BigDecimal("200"),
            new BigDecimal("99.503"),
            "RUB",
            clock.instant(),
            clock.instant());
    broker.historyExecutions.add(execution);

    assertThat(copies.accept(execution).newLots()).isEqualTo(1000);
    trading.dispatchReady();

    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.sent.lots()).isEqualTo(1);
    assertThat(broker.sent.limitPrice()).isEqualByComparingTo("100");
    assertThat(copies.intents())
        .singleElement()
        .satisfies(intent -> assertThat(intent.reservedMoney()).isEqualByComparingTo("100.10"));
  }

  @Test
  void buyCostUsesTenUnderlyingUnitsPerLotAndRetainsCommissionAndAdditionalReserve() {
    runningAtFive();
    broker.availableMoney = new BigDecimal("1010");
    broker.buyMaxLots = 10;
    broker.preliminaryCommission = new BigDecimal("100");
    broker.buyCommissionPerLot = new BigDecimal("0.10");
    broker.additionalCommissionReserve = new BigDecimal("5");
    broker.instrumentLotSize = 10;
    broker.fillEveryPost = false;
    clock.now = clock.now.plusSeconds(1);
    var execution =
        new SourceExecution(
            "source",
            "buy-ten-units-per-lot",
            "buy-ten-units-per-lot-order",
            "instrument",
            Side.BUY,
            new BigDecimal("200"),
            new BigDecimal("99.503"),
            "RUB",
            clock.instant(),
            clock.instant());
    broker.historyExecutions.add(execution);

    assertThat(copies.accept(execution).newLots()).isEqualTo(1000);
    trading.dispatchReady();

    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.sent.lots()).isEqualTo(1);
    assertThat(broker.sent.limitPrice()).isEqualByComparingTo("100");
    assertThat(copies.intents())
        .singleElement()
        .satisfies(intent -> assertThat(intent.reservedMoney()).isEqualByComparingTo("1005.10"));
  }

  @Test
  void largeMinimumFeeFindsTheInteriorMaximumWithBoundedResourceProbes() {
    runningAtFive();
    broker.availableMoney = new BigDecimal("1000");
    broker.buyMaxLots = 10000;
    broker.preliminaryCommission = BigDecimal.ZERO;
    broker.buyCommissionPerLot = BigDecimal.ZERO;
    broker.minimumBuyCommissionFloor = new BigDecimal("900");
    broker.additionalCommissionReserve = BigDecimal.ONE;
    broker.instrumentLotSize = 1;
    broker.fillEveryPost = false;
    clock.now = clock.now.plusSeconds(1);
    var execution =
        new SourceExecution(
            "source",
            "large-minimum-fee",
            "large-minimum-fee-order",
            "instrument",
            Side.BUY,
            new BigDecimal("2000"),
            new BigDecimal("0.996"),
            "RUB",
            clock.instant(),
            clock.instant());
    broker.historyExecutions.add(execution);

    assertThat(copies.accept(execution).newLots()).isEqualTo(10000);
    trading.dispatchReady();

    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.sent.lots()).isEqualTo(99);
    assertThat(broker.sent.limitPrice()).isEqualByComparingTo("1");
    assertThat(copies.intents())
        .singleElement()
        .satisfies(intent -> assertThat(intent.reservedMoney()).isEqualByComparingTo("1000"));
    assertThat(broker.resourcesCalls).isLessThanOrEqualTo(17);
  }

  @Test
  void unconfirmedNonmonotoneBuyCostDescendsToAffordableThirdLot() {
    runningAtFive();
    broker.availableMoney = new BigDecimal("4");
    broker.buyMaxLots = 4;
    broker.preliminaryCommission = BigDecimal.ZERO;
    broker.additionalCommissionReserve = BigDecimal.ZERO;
    broker.instrumentLotSize = 1;
    broker.buyTotalCosts.put(1L, new BigDecimal("5.1"));
    broker.buyTotalCosts.put(2L, new BigDecimal("5.2"));
    broker.buyTotalCosts.put(3L, new BigDecimal("3"));
    broker.buyTotalCosts.put(4L, new BigDecimal("9"));
    broker.monotonicBuyPricing = false;
    broker.fillEveryPost = false;
    clock.now = clock.now.plusSeconds(1);
    var execution =
        new SourceExecution(
            "source",
            "nonmonotone-buy-cost",
            "nonmonotone-buy-cost-order",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("0.996"),
            "RUB",
            clock.instant(),
            clock.instant());
    broker.historyExecutions.add(execution);

    assertThat(copies.accept(execution).newLots()).isEqualTo(5);
    trading.dispatchReady();

    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.sent.lots()).isEqualTo(3);
    assertThat(broker.sent.limitPrice()).isEqualByComparingTo("1");
    assertThat(copies.intents())
        .singleElement()
        .satisfies(intent -> assertThat(intent.reservedMoney()).isEqualByComparingTo("3"));
  }

  @Test
  void changedSelectedBuyFeeWaitsWithoutPostingOrReserving() {
    runningAtFive();
    broker.availableMoney = new BigDecimal("210");
    broker.buyMaxLots = 2;
    broker.additionalCommissionReserve = BigDecimal.ZERO;
    broker.buyTotalCosts.put(2L, new BigDecimal("220"));
    broker.buyTotalCosts.put(1L, new BigDecimal("120"));
    Map<Long, Integer> reads = new HashMap<>();
    broker.resourcesRequestHook =
        request -> {
          if (request.side() == Side.BUY
              && request.lots() == 1
              && reads.merge(1L, 1, Integer::sum) == 2)
            broker.buyTotalCosts.put(1L, new BigDecimal("105"));
        };
    clock.now = clock.now.plusSeconds(1);
    var execution =
        new SourceExecution(
            "source",
            "changed-selected-buy-fee",
            "changed-selected-buy-fee-order",
            "instrument",
            Side.BUY,
            new BigDecimal("0.4"),
            new BigDecimal("200"),
            "RUB",
            clock.instant(),
            clock.instant());
    broker.historyExecutions.add(execution);

    assertThat(copies.accept(execution).newLots()).isEqualTo(2);
    trading.dispatchReady();

    assertThat(broker.posts).isZero();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("WAITING");
              assertThat(intent.reservedMoney()).isEqualByComparingTo("0");
            });
  }

  @Test
  void attestedDecreasingBuyTotalCostWaitsWithoutPostingOrReserving() {
    runningAtFive();
    broker.availableMoney = new BigDecimal("4");
    broker.buyMaxLots = 4;
    broker.oppositePrice = BigDecimal.ONE;
    broker.preliminaryCommission = BigDecimal.ZERO;
    broker.additionalCommissionReserve = BigDecimal.ZERO;
    broker.buyTotalCosts.put(4L, new BigDecimal("9"));
    broker.buyTotalCosts.put(2L, new BigDecimal("10"));
    broker.buyTotalCosts.put(1L, new BigDecimal("5.1"));
    broker.attestBuyTotalCosts = true;
    clock.now = clock.now.plusSeconds(1);
    var execution =
        new SourceExecution(
            "source",
            "decreasing-attested-buy-cost",
            "decreasing-attested-buy-cost-order",
            "instrument",
            Side.BUY,
            new BigDecimal("0.8"),
            BigDecimal.ONE,
            "RUB",
            clock.instant(),
            clock.instant());
    broker.historyExecutions.add(execution);

    assertThat(copies.accept(execution).newLots()).isEqualTo(4);
    trading.dispatchReady();

    assertThat(broker.posts).isZero();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("WAITING");
              assertThat(intent.reservedMoney()).isEqualByComparingTo("0");
            });
  }

  @Test
  void waitingSnapshotReturnsDatabasePagesInCyclicOrderFromInteriorCursor() throws Exception {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    for (String instrument : List.of("instrument-A", "instrument-B", "instrument-C")) {
      copies.accept(source("queue-" + instrument, instrument, Side.BUY, 1, clock.instant()));
      clock.now = clock.now.plusSeconds(1);
    }

    var cursorField = TradingService.class.getDeclaredField("readinessCursor");
    cursorField.setAccessible(true);
    var cursorConstructor = cursorField.getType().getDeclaredConstructor(long.class, UUID.class);
    cursorConstructor.setAccessible(true);
    var waitingSnapshot = TradingService.class.getDeclaredMethod("waitingSnapshot");
    waitingSnapshot.setAccessible(true);

    try {
      cursorField.set(tradingTarget(), null);
      @SuppressWarnings("unchecked")
      List<CopyIntent> initial = (List<CopyIntent>) waitingSnapshot.invoke(tradingTarget());
      var cursorIntent =
          initial.stream()
              .filter(intent -> intent.instrumentId.equals("instrument-B"))
              .findFirst()
              .orElseThrow();
      cursorField.set(
          tradingTarget(),
          cursorConstructor.newInstance(cursorIntent.prioritySequence, cursorIntent.id));

      @SuppressWarnings("unchecked")
      List<CopyIntent> ordered = (List<CopyIntent>) waitingSnapshot.invoke(tradingTarget());
      assertThat(ordered)
          .extracting(intent -> intent.instrumentId)
          .containsExactly("instrument-C", "instrument-A", "instrument-B");
    } finally {
      cursorField.set(tradingTarget(), null);
    }
  }

  @org.junit.jupiter.api.Timeout(value = 20, unit = java.util.concurrent.TimeUnit.SECONDS)
  @Test
  void readyIntentIsNotStarvedByHundredDelayedMetadataMisses() {
    runningAtFive();
    broker.availableMoney = new BigDecimal("150");
    broker.buyMaxLots = 1;
    broker.preliminaryCommission = new BigDecimal("0.5");
    broker.additionalCommissionReserve = BigDecimal.ZERO;
    broker.fillEveryPost = false;
    clock.now = clock.now.plusSeconds(1);
    var instrumentReads = new java.util.concurrent.atomic.AtomicInteger();
    broker.instrumentIdHook =
        id -> {
          if (!id.startsWith("waiting-instrument-")) return;
          instrumentReads.incrementAndGet();
          try {
            Thread.sleep(700);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
          }
          throw new IllegalStateException("synthetic instrument metadata unavailable");
        };
    for (int index = 1; index <= 100; index++)
      copies.accept(
          source("waiting-" + index, "waiting-instrument-" + index, Side.BUY, 1, clock.instant()));
    copies.accept(source("ready-101", "ready-101", Side.BUY, 1, clock.instant()));
    copies.accept(source("ready-102", "ready-102", Side.BUY, 1, clock.instant()));

    long startedNanos = System.nanoTime();
    try {
      trading.dispatchReady();
    } finally {
      broker.instrumentIdHook = null;
    }
    assertThat(System.nanoTime() - startedNanos)
        .isLessThan(java.util.concurrent.TimeUnit.SECONDS.toNanos(15));
    assertThat(instrumentReads.get()).isEqualTo(100);
    assertThat(copies.intents())
        .filteredOn(intent -> intent.instrumentId().startsWith("waiting-instrument-"))
        .hasSize(100)
        .allSatisfy(intent -> assertThat(intent.status()).isEqualTo("WAITING"));
    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.sentRequests).hasSize(1);
    String winner = broker.sentRequests.get(0).instrumentId();
    assertThat(winner).isIn("ready-101", "ready-102");
    String loser = winner.equals("ready-101") ? "ready-102" : "ready-101";
    assertThat(copies.intents())
        .filteredOn(intent -> intent.instrumentId().equals(winner))
        .singleElement()
        .satisfies(intent -> assertThat(intent.reservedMoney().signum()).isPositive());
    assertThat(copies.intents())
        .filteredOn(intent -> intent.instrumentId().equals(loser))
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("FINAL");
              assertThat(intent.reservedMoney()).isEqualByComparingTo(BigDecimal.ZERO);
            });
  }

  @Test
  void readyIntentAfterTheFirstHundredWaitingForDataIsConsideredInOrderWithoutDoubleReservation() {
    runningAtFive();
    broker.availableMoney = new BigDecimal("150");
    broker.buyMaxLots = 1;
    broker.preliminaryCommission = new BigDecimal("0.5");
    broker.buyCommissionPerLot = new BigDecimal("0.10");
    broker.additionalCommissionReserve = BigDecimal.ZERO;
    broker.fillEveryPost = false;
    clock.now = clock.now.plusSeconds(1);
    var instrumentReads = new java.util.concurrent.atomic.AtomicInteger();
    broker.instrumentIdHook =
        id -> {
          if (!id.startsWith("waiting-instrument-")) return;
          instrumentReads.incrementAndGet();
          throw new IllegalStateException("synthetic instrument metadata unavailable");
        };
    for (int index = 1; index <= 100; index++)
      copies.accept(
          source("waiting-" + index, "waiting-instrument-" + index, Side.BUY, 1, clock.instant()));
    copies.accept(source("ready-101", "ready-101", Side.BUY, 1, clock.instant()));
    copies.accept(source("ready-102", "ready-102", Side.BUY, 1, clock.instant()));

    try {
      trading.dispatchReady();
    } finally {
      broker.instrumentIdHook = null;
    }

    assertThat(instrumentReads.get()).isEqualTo(100);
    assertThat(copies.intents())
        .filteredOn(intent -> intent.instrumentId().startsWith("waiting-instrument-"))
        .hasSize(100)
        .allSatisfy(intent -> assertThat(intent.status()).isEqualTo("WAITING"));
    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.sentRequests).hasSize(1);
    String winner = broker.sentRequests.get(0).instrumentId();
    assertThat(winner).isIn("ready-101", "ready-102");
    assertThat(broker.sent.instrumentId()).isEqualTo(winner);
    assertThat(broker.sent.lots()).isEqualTo(1);
    String loser = winner.equals("ready-101") ? "ready-102" : "ready-101";
    assertThat(copies.intents())
        .filteredOn(intent -> intent.sourceOrderId().equals("order-" + winner))
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.limitPrice()).isEqualByComparingTo("100.50");
              assertThat(intent.reservedMoney())
                  .isEqualByComparingTo(new BigDecimal("100.50").add(broker.buyCommissionPerLot));
            });
    assertThat(copies.intents())
        .filteredOn(intent -> intent.sourceOrderId().equals("order-" + loser))
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("FINAL");
              assertThat(intent.skippedLots()).isEqualTo(5);
              assertThat(intent.reservedMoney()).isEqualByComparingTo(BigDecimal.ZERO);
            });
  }

  @org.junit.jupiter.api.Timeout(value = 15, unit = java.util.concurrent.TimeUnit.SECONDS)
  @Test
  void serialPrepareRefreshesCashAfterParallelBuySnapshots() throws Exception {
    runningAtFive();
    broker.availableMoney = new BigDecimal("150");
    broker.buyMaxLots = 1;
    broker.preliminaryCommission = BigDecimal.ZERO;
    broker.additionalCommissionReserve = BigDecimal.ZERO;
    broker.fillEveryPost = true;
    clock.now = clock.now.plusSeconds(1);
    broker.mirrorCapital = new BigDecimal("100000");
    control.recalculate();
    assertThat(
            copies
                .accept(source("cash-first", "cash-first", Side.BUY, 1, clock.instant()))
                .newLots())
        .isEqualTo(1L);
    assertThat(
            copies
                .accept(source("cash-second", "cash-second", Side.BUY, 1, clock.instant()))
                .newLots())
        .isEqualTo(1L);

    var arrivals = new java.util.concurrent.CountDownLatch(2);
    var release = new java.util.concurrent.CountDownLatch(1);
    var firstSnapshotRequests = java.util.concurrent.ConcurrentHashMap.<UUID>newKeySet();
    broker.resourcesRequestHook =
        request -> {
          if (!firstSnapshotRequests.add(request.requestId())) return;
          arrivals.countDown();
          try {
            if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS))
              throw new IllegalStateException("resource snapshot barrier timed out");
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
          }
        };
    broker.afterPostHook =
        () -> {
          if (broker.posts == 1)
            broker.availableMoney = broker.availableMoney.subtract(broker.sent.limitPrice());
        };
    var completedTasks = new java.util.concurrent.CountDownLatch(2);
    tradingTarget().readinessTaskCompletedHook = completedTasks::countDown;
    tradingTarget().beforeReadinessCollectionHook =
        () -> {
          try {
            if (!completedTasks.await(5, java.util.concurrent.TimeUnit.SECONDS))
              throw new IllegalStateException("readiness completion barrier timed out");
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
          }
        };
    var dispatcher = java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      var dispatch = dispatcher.submit(trading::dispatchReady);
      assertThat(arrivals.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      release.countDown();
      dispatch.get(10, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      release.countDown();
      dispatcher.shutdownNow();
      assertThat(dispatcher.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      broker.resourcesRequestHook = null;
      broker.afterPostHook = null;
      tradingTarget().readinessTaskCompletedHook = () -> {};
      tradingTarget().beforeReadinessCollectionHook = () -> {};
    }

    assertThat(broker.sentRequests).hasSize(1);
    assertThat(broker.posts).isEqualTo(1);
    assertThat(copies.intents()).hasSize(2);
    assertThat(copies.intents())
        .filteredOn(intent -> !intent.instrumentId().equals(broker.sent.instrumentId()))
        .singleElement()
        .satisfies(
            intent -> {
              assertThat(intent.status()).isEqualTo("FINAL");
              assertThat(intent.skippedLots()).isEqualTo(1);
              assertThat(intent.reservedMoney()).isEqualByComparingTo(BigDecimal.ZERO);
            });
  }

  @org.junit.jupiter.api.Timeout(value = 15, unit = java.util.concurrent.TimeUnit.SECONDS)
  @Test
  void simultaneouslyReadyIntentsPostInSourceJournalOrder() throws Exception {
    runningAtFive();
    broker.availableMoney = new BigDecimal("1000");
    broker.buyMaxLots = 1;
    broker.fillEveryPost = true;
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("tie-first", "tie-first", Side.BUY, 1, clock.instant()));
    copies.accept(source("tie-second", "tie-second", Side.BUY, 1, clock.instant()));

    var arrivals = new java.util.concurrent.CountDownLatch(2);
    var release = new java.util.concurrent.CountDownLatch(1);
    var firstReads = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
    broker.instrumentIdHook =
        id -> {
          if (!(id.equals("tie-first") || id.equals("tie-second")) || !firstReads.add(id)) return;
          arrivals.countDown();
          try {
            if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS))
              throw new IllegalStateException("readiness barrier timed out");
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
          }
        };
    var completedTasks = new java.util.concurrent.CountDownLatch(2);
    tradingTarget().readinessTaskCompletedHook = completedTasks::countDown;
    tradingTarget().beforeReadinessCollectionHook =
        () -> {
          try {
            if (!completedTasks.await(5, java.util.concurrent.TimeUnit.SECONDS))
              throw new IllegalStateException("readiness completion barrier timed out");
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
          }
        };
    var dispatcher = java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      var dispatch = dispatcher.submit(trading::dispatchReady);
      assertThat(arrivals.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      release.countDown();
      dispatch.get(10, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      release.countDown();
      dispatcher.shutdownNow();
      assertThat(dispatcher.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      broker.instrumentIdHook = null;
      tradingTarget().readinessTaskCompletedHook = () -> {};
      tradingTarget().beforeReadinessCollectionHook = () -> {};
    }

    assertThat(broker.sentRequests)
        .extracting(OrderRequest::instrumentId)
        .containsExactly("tie-first", "tie-second");
    assertThat(copies.intents()).hasSize(2);
  }

  @org.junit.jupiter.api.Timeout(value = 15, unit = java.util.concurrent.TimeUnit.SECONDS)
  @Test
  void readyPostDoesNotWaitForBlockedPeer() throws Exception {
    runningAtFive();
    broker.availableMoney = new BigDecimal("1000");
    broker.buyMaxLots = 1;
    broker.fillEveryPost = true;
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("slow-peer", "slow-peer", Side.BUY, 1, clock.instant()));
    copies.accept(source("ready-peer", "ready-peer", Side.BUY, 1, clock.instant()));

    var slowEntered = new java.util.concurrent.CountDownLatch(1);
    var releaseSlow = new java.util.concurrent.CountDownLatch(1);
    var readyPosted = new java.util.concurrent.CountDownLatch(1);
    var slowStillBlocked = new java.util.concurrent.atomic.AtomicBoolean();
    var firstSlowRead = new java.util.concurrent.atomic.AtomicBoolean();
    broker.instrumentIdHook =
        id -> {
          if (!id.equals("slow-peer") || !firstSlowRead.compareAndSet(false, true)) return;
          slowStillBlocked.set(true);
          slowEntered.countDown();
          try {
            if (!releaseSlow.await(5, java.util.concurrent.TimeUnit.SECONDS))
              throw new IllegalStateException("slow readiness timed out");
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
          } finally {
            slowStillBlocked.set(false);
          }
        };
    broker.postHook =
        request -> {
          if (request.instrumentId().equals("ready-peer")) readyPosted.countDown();
        };

    var dispatcher = java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      var dispatch = dispatcher.submit(trading::dispatchReady);
      assertThat(slowEntered.await(3, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      assertThat(readyPosted.await(3, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      assertThat(slowStillBlocked.get()).isTrue();
      releaseSlow.countDown();
      dispatch.get(5, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      releaseSlow.countDown();
      dispatcher.shutdownNow();
      assertThat(dispatcher.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      broker.instrumentIdHook = null;
      broker.postHook = null;
    }
  }

  private void runningAtFive() {
    broker.mirrorCapital = new BigDecimal("500000");
    broker.fillEveryPost = true;
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    control.confirmLiveAcceptance();
    control.enableAutomatic();
  }

  @Test
  void equalSeventyTwoLotsDoNotProveOrderButProvenOrderClearsOnlyWaitAndSeventyFiveStillPauses() {
    runningAtFive();
    broker.mirrorCapital = new BigDecimal("720000");
    control.recalculate();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("seventy-two", Side.BUY, 10, clock.instant()));
    trading.tick();
    control.instrumentReconciled("instrument", true, false, "Не доказан порядок");
    reconciliation.reconcile();
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.waiting()).isTrue());
    recovery.recover();
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.waiting()).isFalse());
    broker.extraBankLots = 3;
    reconciliation.reconcile();
    recovery.recover();
    assertThat(control.instruments())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.paused()).isTrue();
              assertThat(i.reconciled()).isFalse();
              assertThat(i.reason()).contains("75", "72");
            });
  }

  @Test
  void sixFilledFromSecondThirtyAreCancelledBeforeOneSixteenLotCorrectionToForty() {
    recoveryFixture(true);
    trading.reconcileOnly();
    reconciliation.reconcile();
    recovery.recover();
    trading.dispatchReady();
    assertThat(copies.intents().stream().filter(i -> i.side().equals("SELL")))
        .singleElement()
        .satisfies(i -> assertThat(i.executedLots()).isEqualTo(16));
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isEqualTo(40));
    assertThat(copies.holdingAllocations())
        .extracting(HoldingAllocation.View::lots)
        .containsExactly(25L, 15L);
    assertThat(broker.posts).isEqualTo(3);
  }

  @Test
  void
      sixOfTenCorrectionThenConfirmedCancellationLeavesOneFortyFourLotHoldingAllocatedTwentyNineAndFifteen() {
    recoveryFixture(false);
    reconciliation.reconcile();
    recovery.recover();
    broker.partialNext = 6;
    trading.dispatchReady();
    clock.now = clock.now.plusSeconds(60);
    trading.reconcileOnly();
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isEqualTo(44));
    assertThat(copies.holdingAllocations())
        .extracting(HoldingAllocation.View::lots)
        .containsExactly(29L, 15L);
    assertThat(copies.intents().stream().filter(i -> i.side().equals("SELL")))
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.executedLots()).isEqualTo(6);
              assertThat(i.skippedLots()).isEqualTo(4);
            });
    trading.tick();
    assertThat(broker.posts).isEqualTo(2);
  }

  @Test
  void correctionThatExpiresBeforeFirstSendLeavesFiftyAllocatedThirtyFiveAndFifteen() {
    recoveryFixture(false);
    reconciliation.reconcile();
    recovery.recover();
    clock.now = clock.now.plusSeconds(61);
    trading.dispatchReady();
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isEqualTo(50));
    assertThat(copies.holdingAllocations())
        .extracting(HoldingAllocation.View::lots)
        .containsExactly(35L, 15L);
    assertThat(broker.posts).isEqualTo(1);
  }

  private void recoveryFixture(boolean partialSecond) {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("allocation-first", Side.BUY, 10, clock.instant()));
    trading.tick();
    broker.mirrorCapital = new BigDecimal("300000");
    control.recalculate();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("allocation-second", Side.BUY, 10, clock.instant()));
    if (partialSecond) {
      broker.partialNext = 6;
      trading.tick();
    }
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("allocation-sale", Side.SELL, 10, clock.instant()));
    control.instrumentReconciled(
        "instrument", true, false, "Восстановление подтверждённой последовательности");
  }

  @Test
  void initialCurrencyCashIsAllowedAndGrossCashOrTransfersDoNotProveCopiedFxInventory() {
    broker.currencyCash = true;
    assertThat(reconciliation.reconcile()).isTrue();
    control.start();
    control.markFeedReady(true);
    assertThat(control.status().mode()).isEqualTo("SHADOW");
    assertThat(control.instruments().stream().filter(i -> i.instrumentId().equals("fx"))).isEmpty();
    clock.now = clock.now.plusSeconds(1);
    var fx =
        new SourceExecution(
            "source",
            "fx-buy",
            "fx-order",
            "fx",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("10"),
            "RUB",
            clock.instant(),
            clock.instant());
    copies.accept(fx);
    broker.historyExecutions.add(fx);
    broker.sourceCash = new BigDecimal("101");
    reconciliation.reconcile();
    recovery.recover();
    broker.sourceCash = new BigDecimal("111");
    broker.mirrorCash = new BigDecimal("120");
    reconciliation.reconcile();
    recovery.recover();
    assertThat(control.instruments().stream().filter(i -> i.instrumentId().equals("fx")))
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.waiting()).isTrue();
              assertThat(i.paused()).isFalse();
            });
    assertThat(copies.positions().stream().filter(p -> p.instrumentId().equals("fx")))
        .singleElement()
        .satisfies(
            p -> {
              assertThat(p.copiedLots()).isZero();
              assertThat(p.brokerInventoryQualified()).isFalse();
            });
    assertThat(broker.posts).isZero();
  }

  @Test
  void stopCannotReleaseRestoredReadOnlyRestrictionForAnExistingActiveOrder() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    broker.partialNext = 0;
    copies.accept(source("restored-active", Side.BUY, 1, clock.instant()));
    trading.tick();
    assertThat(broker.posts).isEqualTo(1);
    control.markRestored();
    control.stop();
    clock.now = clock.now.plusSeconds(120);
    int reads = broker.orderReads;
    trading.tick();
    trading.tick();
    assertThat(control.status().mode()).isEqualTo("RESTORED");
    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.cancels).isZero();
    assertThat(broker.orderReads).isGreaterThan(reads);
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("ACTIVE"));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void preservedInstrumentPauseRequiresNewReconciliationAfterStartupOrRestore(boolean restored) {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    broker.fillEveryPost = true;
    copies.accept(source("preserved-holding", Side.BUY, 1, clock.instant()));
    trading.tick();
    control.pauseInstrument("instrument", "Ручная проверка");
    control.instrumentReconciled("instrument", true, true, null);
    if (restored) control.markRestored();
    else control.recoverStartup();
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> control.resumeInstrument("instrument"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("успешная сверка");
    assertThat(control.instruments())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.paused()).isTrue();
              assertThat(i.reconciled()).isFalse();
            });
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isEqualTo(5));
    control.instrumentReconciled("instrument", true, true, null);
    control.resumeInstrument("instrument");
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isFalse());
  }

  @Test
  void postOrderTimerStartsAtRawReceiptBeforeDelayedClockEvidenceAndNeverGetsAnotherMinute() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    broker.partialNext = 0;
    copies.accept(source("raw-post-receipt", Side.BUY, 1, clock.instant()));
    Instant receipt = clock.instant();
    broker.afterPostHook = () -> clock.nextEvidence = () -> clock.now = clock.now.plusSeconds(10);
    trading.dispatchReady();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.deadline()).isEqualTo(receipt.plusSeconds(60));
              assertThat(i.deadlineReliable()).isTrue();
            });
    clock.now = receipt.plusSeconds(60);
    trading.reconcileOnly();
    assertThat(broker.cancels).isEqualTo(1);
  }

  @Test
  void firstConfirmedOrderStateTimerStartsAtRawReceiptBeforeConnectivityObserverDelay() {
    runningAtFive();
    broker.fillEveryPost = false;
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("raw-state-receipt", Side.BUY, 1, clock.instant()));
    trading.dispatchReady();
    assertThat(copies.intents()).singleElement().satisfies(i -> assertThat(i.deadline()).isNull());
    clock.now = clock.now.plusSeconds(2);
    broker.orders.put(broker.sent.requestId(), broker.evidence(broker.sent, OrderStatus.NEW, 0));
    Instant receipt = clock.instant();
    broker.afterReconcileHook = () -> clock.now = clock.now.plusSeconds(10);
    trading.reconcileOnly();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.deadline()).isEqualTo(receipt.plusSeconds(60));
              assertThat(i.deadlineReliable()).isTrue();
            });
    clock.now = receipt.plusSeconds(60);
    trading.reconcileOnly();
    assertThat(broker.cancels).isEqualTo(1);
  }

  @Test
  void unreliableOriginalTimerIsRetainedWithoutAssigningNewMinuteAfterTimeReturns() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    broker.partialNext = 0;
    broker.afterPostHook = () -> clock.evidenceAvailable = false;
    copies.accept(source("uncertain-time", Side.BUY, 1, clock.instant()));
    trading.tick();
    var original = copies.intents().getFirst();
    assertThat(original.deadlineReliable()).isFalse();
    assertThat(original.deadline()).isNotNull();
    broker.afterPostHook = null;
    clock.evidenceAvailable = true;
    clock.now = clock.now.plusSeconds(120);
    trading.reconcileOnly();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.deadline()).isEqualTo(original.deadline());
              assertThat(i.deadlineReliable()).isFalse();
              assertThat(i.status()).isEqualTo("ACTIVE");
            });
    assertThat(broker.cancels).isZero();
    control.stop();
    trading.reconcileOnly();
    assertThat(broker.cancels).isEqualTo(1);
  }

  @Test
  void resourceInspectionUsesMonotonicTimeEvenWhenWallClockDoesNotAdvance() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("slow-resources", Side.BUY, 1, clock.instant()));
    broker.resourcesHook =
        () -> {
          try {
            Thread.sleep(5050);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
          }
        };
    trading.dispatchReady();
    assertThat(broker.posts).isZero();
    broker.resourcesHook = null;
    trading.dispatchReady();
    assertThat(broker.posts).isEqualTo(1);
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"0.1", "0"})
  void fullCloseStillSellsFiveCopiedLotsWhenCurrentProportionalSaleRoundsToZero(
      String coefficient) {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("zero-close-buy", Side.BUY, 1, clock.instant()));
    trading.tick();
    broker.mirrorCapital = new BigDecimal(coefficient).multiply(new BigDecimal("100000"));
    control.recalculate();
    clock.now = clock.now.plusSeconds(1);
    assertThat(copies.accept(source("zero-close-sale", Side.SELL, 1, clock.instant())).newLots())
        .isZero();
    reconciliation.reconcile();
    recovery.recover();
    trading.dispatchReady();
    assertThat(copies.intents().stream().filter(i -> i.side().equals("SELL")))
        .singleElement()
        .satisfies(i -> assertThat(i.executedLots()).isEqualTo(5));
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isZero());
    assertThat(broker.posts).isEqualTo(2);
  }

  @Test
  void recoveryRoundsRetainedHalfLotsTogetherWithinTheirOneOriginalSourceOrder() {
    broker.fillEveryPost = true;
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    control.confirmLiveAcceptance();
    control.enableAutomatic();
    for (int index = 0; index < 2; index++) {
      clock.now = clock.now.plusSeconds(1);
      var buy =
          new SourceExecution(
              "source",
              "same-order-" + index,
              "same-source-order",
              "instrument",
              Side.BUY,
              BigDecimal.ONE,
              new BigDecimal("100"),
              "RUB",
              clock.instant(),
              clock.instant());
      copies.accept(buy);
      broker.historyExecutions.add(buy);
    }
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("half-sale", Side.SELL, 1, clock.instant()));
    control.instrumentReconciled(
        "instrument", true, false, "Подтверждённая история до первой отправки");
    reconciliation.reconcile();
    recovery.recover();
    trading.dispatchReady();
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isEqualTo(1));
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void sequentialUnsentBuysTenAtFiveAndTenAtThreeAfterSalesFourAndEightLeaveThirty() {
    runningAtFive();
    broker.blockPurchases = true;
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("sequence-buy-first", Side.BUY, 10, clock.instant()));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("sequence-sale-first", Side.SELL, 4, clock.instant()));
    trading.dispatchReady();
    broker.mirrorCapital = new BigDecimal("300000");
    control.recalculate();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("sequence-buy-second", Side.BUY, 10, clock.instant()));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("sequence-sale-second", Side.SELL, 8, clock.instant()));
    trading.dispatchReady();
    control.instrumentReconciled(
        "instrument", true, false, "Восстановленная последовательность неотправленных покупок");
    broker.blockPurchases = false;
    reconciliation.reconcile();
    recovery.recover();
    trading.dispatchReady();
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isEqualTo(30));
    assertThat(copies.intents().stream().filter(i -> i.side().equals("BUY")))
        .extracting(CopyIntent.View::executedLots)
        .containsExactlyInAnyOrder(15L, 15L);
    assertThat(broker.posts).isEqualTo(2);
  }

  @Test
  void firstObservationBoundaryStartsAtSuccessfulFeedRecoveryAndOrdinaryRestartPreservesIt() {
    control.markReconciled();
    control.start();
    clock.now = clock.now.plusSeconds(1);
    var beforeReady = source("before-feed-ready", Side.BUY, 1, clock.instant());
    copies.accept(beforeReady);
    clock.now = clock.now.plusSeconds(1);
    control.markFeedReady(true);
    Instant boundary = control.status().observationBoundary();
    copies.retryPending();
    assertThat(copies.accept(beforeReady).disposition()).isEqualTo("SKIPPED");
    clock.now = clock.now.plusSeconds(1);
    var afterReady = source("after-feed-ready", Side.BUY, 1, clock.instant());
    assertThat(copies.accept(afterReady).newLots()).isEqualTo(1);
    control.recoverStartup();
    control.markReconciled();
    clock.now = clock.now.plusSeconds(1);
    control.markFeedReady(true);
    assertThat(control.status().observationBoundary()).isEqualTo(boundary);
    assertThat(copies.accept(afterReady).newLots()).isEqualTo(1);
  }

  @Test
  void manualResumeKeepsTheAlreadyLiveObservationWithoutWaitingForAnotherSubscriptionAck() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.stop();
    control.stoppedAfterReconciliation();
    control.markReconciled();
    control.start();
    assertThat(control.gate().reconciled()).isTrue();
    clock.now = clock.now.plusSeconds(1);
    assertThat(
            copies.accept(source("resumed-with-live-feed", Side.BUY, 1, clock.instant())).newLots())
        .isEqualTo(1);
  }

  private SourceExecution source(String id, Side side, long lots, Instant executedAt) {
    return source(id, "instrument", side, lots, executedAt);
  }

  private SourceExecution source(
      String id, String instrumentId, Side side, long lots, Instant executedAt) {
    var result =
        new SourceExecution(
            "source",
            id,
            "order-" + id,
            instrumentId,
            side,
            BigDecimal.valueOf(lots),
            new BigDecimal("100"),
            "RUB",
            executedAt,
            clock.instant());
    broker.historyExecutions.add(result);
    return result;
  }

  @Test
  void sourceFullCloseInitiallyDeliveredAfterSixtySecondsPausesTheFiveRemainingCopiedLots() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    Instant buyAt = clock.instant();
    copies.accept(source("stale-close-buy", Side.BUY, 1, buyAt));
    trading.tick();
    clock.now = clock.now.plusSeconds(62);
    assertThat(
            copies
                .accept(source("stale-close-sale", Side.SELL, 1, buyAt.plusSeconds(1)))
                .disposition())
        .isEqualTo("SKIPPED");
    reconciliation.reconcile();
    recovery.recover();
    recovery.recover();
    trading.dispatchReady();
    assertThat(control.instruments())
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.paused()).isTrue();
              assertThat(i.reason()).contains("устарела", "5");
            });
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isEqualTo(5));
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void executionAdmissionWaitsForMissedMandatoryRecalculationBeforeTheWorkerRuns() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    clock.now = clock.now.plus(Duration.ofDays(2));
    control.recoverStartup();
    control.markReconciled();
    control.markFeedReady(true);
    var fresh = source("before-mandatory-worker", Side.BUY, 1, clock.instant());
    assertThat(copies.accept(fresh).disposition()).isEqualTo("RECEIVED");
    assertThat(control.status().recalculationPaused()).isTrue();
    broker.mirrorCapital = new BigDecimal("200000");
    control.recalculate();
    copies.retryPending();
    assertThat(copies.accept(fresh).acceptedCoefficient()).isEqualByComparingTo("2");
    assertThat(broker.capitalCalls).isEqualTo(4);
  }

  @Test
  void
      reconciledManualSaleReducesTheSourceCycleSoFullCloseSellsFortyFiveWithoutFalseResidualPause() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("manual-cycle-buy", Side.BUY, 10, clock.instant()));
    trading.tick();
    control.pauseInstrument("instrument", "Исправление через брокера");
    clock.now = clock.now.plusSeconds(1);
    broker.mirrorExecutions.add(
        new SourceExecution(
            "mirror",
            "manual-cycle-extra",
            "",
            "instrument",
            Side.BUY,
            BigDecimal.TEN,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    clock.now = clock.now.plusSeconds(1);
    broker.mirrorExecutions.add(
        new SourceExecution(
            "mirror",
            "manual-cycle-sale",
            "",
            "instrument",
            Side.SELL,
            new BigDecimal("15"),
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    manual.reconcilePaused();
    broker.bankLots = 45;
    reconciliation.reconcile();
    control.resumeInstrument("instrument");
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("manual-cycle-close", Side.SELL, 10, clock.instant()));
    reconciliation.reconcile();
    recovery.recover();
    trading.dispatchReady();
    assertThat(copies.intents().stream().filter(i -> i.side().equals("SELL")))
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.executedLots()).isEqualTo(45);
              assertThat(i.skippedLots()).isZero();
            });
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isZero());
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isFalse());
  }

  @Test
  void manuallyRemovingAllCopiedLotsDoesNotInventResidualOnALaterStaleSourceClose() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("all-removed-buy", Side.BUY, 10, clock.instant()));
    trading.tick();
    control.pauseInstrument("instrument", "Исправление через брокера");
    clock.now = clock.now.plusSeconds(1);
    broker.mirrorExecutions.add(
        new SourceExecution(
            "mirror",
            "all-removed-sale",
            "",
            "instrument",
            Side.SELL,
            new BigDecimal("50"),
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    manual.reconcilePaused();
    broker.bankLots = 0;
    reconciliation.reconcile();
    control.resumeInstrument("instrument");
    Instant closeAt = clock.instant().plusSeconds(1);
    clock.now = clock.now.plusSeconds(62);
    copies.accept(source("all-removed-close", Side.SELL, 10, closeAt));
    reconciliation.reconcile();
    recovery.recover();
    trading.dispatchReady();
    assertThat(copies.positions())
        .singleElement()
        .satisfies(p -> assertThat(p.copiedLots()).isZero());
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isFalse());
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void manualStopAtomicallyFinalizesUnsentCorrectionAndItsAllocationBeforeAnyWorkerTick() {
    recoveryFixture(false);
    reconciliation.reconcile();
    recovery.recover();
    control.stop();
    assertThat(copies.intents().stream().filter(i -> i.side().equals("SELL")))
        .singleElement()
        .satisfies(
            i -> {
              assertThat(i.status()).isEqualTo("FINAL");
              assertThat(i.skippedLots()).isEqualTo(10);
              assertThat(i.reason()).contains("ручная остановка");
            });
    assertThat(copies.holdingAllocations())
        .extracting(HoldingAllocation.View::lots)
        .containsExactly(35L, 15L);
    control.stoppedAfterReconciliation();
    control.markReconciled();
    control.start();
    trading.tick();
    assertThat(copies.intents().stream().filter(i -> i.side().equals("SELL")))
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("FINAL"));
    assertThat(broker.posts).isEqualTo(1);
  }

  @Test
  void failedDurableBrokerObservationStopsTheWholeDispatchTickBeforeAnotherReadyIntent() {
    runningAtFive();
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("observer-first", Side.BUY, 1, clock.instant()));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(source("observer-second", Side.BUY, 1, clock.instant()));
    var first = new java.util.concurrent.atomic.AtomicBoolean(true);
    broker.resourcesHook =
        () -> {
          if (first.getAndSet(false))
            throw new ru.orbita.broker.BrokerPersistenceFailure(
                "fixture observation",
                new org.springframework.dao.DataAccessResourceFailureException(
                    "fixture unavailable database"));
        };
    org.assertj.core.api.Assertions.assertThatThrownBy(trading::dispatchReady)
        .isInstanceOf(ru.orbita.broker.BrokerPersistenceFailure.class);
    assertThat(broker.posts).isZero();
    assertThat(copies.intents())
        .hasSize(2)
        .allSatisfy(i -> assertThat(i.status()).isEqualTo("WAITING"));
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class Fixture {
    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
      return new PostgreSQLContainer("postgres:17.11");
    }

    @Bean
    @Primary
    MutableClock mutableClock() {
      return new MutableClock();
    }

    @Bean
    Simulator simulator(MutableClock clock) {
      return new Simulator(clock);
    }

    @Bean
    TelegramSimulator telegramSimulator() {
      return new TelegramSimulator();
    }

    @Bean
    @Primary
    TelegramTransport telegramTransport(TelegramSimulator telegram) {
      return telegram;
    }

    @Bean
    ru.orbita.control.ClockEvidence clockEvidence(MutableClock clock) {
      return new ru.orbita.control.ClockEvidence() {
        public Optional<Bound> current() {
          Runnable observation = clock.nextEvidence;
          clock.nextEvidence = null;
          if (observation != null) observation.run();
          return clock.evidenceAvailable
              ? Optional.of(
                  new ru.orbita.control.ClockEvidence.Bound(
                      clock.instant(), clock.instant(), "test UTC clock"))
              : Optional.empty();
        }

        // This synthetic seam explicitly qualifies the captured receipt; current-only providers do
        // not.
        public Optional<Bound> atReceipt(long received) {
          if (current().isEmpty()) return Optional.empty();
          return Optional.ofNullable(clock.receiptInstants.get(received))
              .map(at -> new Bound(at, at, "synthetic independently retained receipt"));
        }
      };
    }
  }

  static class TelegramSimulator implements TelegramTransport {
    final List<String> messages = new ArrayList<>();

    @Override
    public Result send(String text) {
      messages.add(text);
      return Result.success();
    }
  }

  static class MutableClock extends Clock {
    Instant now = Instant.parse("2026-11-15T10:00:00Z");
    boolean evidenceAvailable = true;
    volatile Runnable nextEvidence;
    final Map<Long, Instant> receiptInstants = new java.util.concurrent.ConcurrentHashMap<>();

    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    public Clock withZone(ZoneId zone) {
      return this;
    }

    public Instant instant() {
      return now;
    }
  }

  static class Simulator implements BrokerGateway {
    boolean finalCommissionAvailable = true;
    int instrumentLotSize = 1;
    boolean individualLotFills;
    final Clock clock;
    int posts;
    int resourcesCalls;
    int capitalCalls;
    OrderRequest sent;
    final List<OrderRequest> sentRequests = new ArrayList<>();
    Runnable capitalHook;
    java.util.function.Function<String, CapitalEvidence> capitalResponse;
    boolean failCapital;
    boolean failHistory;
    final Deque<BigDecimal> quotes = new ArrayDeque<>();
    BigDecimal mirrorCapital = new BigDecimal("100000");
    BigDecimal sourceCapital = new BigDecimal("100000");
    final List<SourceExecution> historyExecutions = new ArrayList<>();
    final List<SourceExecution> mirrorExecutions = new ArrayList<>();
    boolean fillEveryPost;
    RejectionReason rejectNext;
    BigDecimal oppositePrice = new BigDecimal("100");
    long bankLots;
    long extraBankLots;
    BigDecimal availableMoney = new BigDecimal("10000");
    long buyMaxLots = 100;
    BigDecimal preliminaryCommission = BigDecimal.ONE;
    BigDecimal additionalCommissionReserve = BigDecimal.ONE;
    long partialNext = -1;
    final Map<UUID, OrderEvidence> orders = new HashMap<>();
    boolean currencyCash;
    boolean zeroFees;
    BigDecimal buyCommissionPerLot;
    BigDecimal minimumBuyCommissionFloor;
    final Map<Long, BigDecimal> buyTotalCosts = new HashMap<>();
    boolean monotonicBuyPricing = true;
    boolean attestBuyTotalCosts;
    java.util.function.Consumer<OrderRequest> resourcesRequestHook;
    BigDecimal sourceCash = new BigDecimal("100"), mirrorCash = new BigDecimal("100");
    Runnable afterPostHook;
    java.util.function.Consumer<OrderRequest> postHook;
    Runnable afterReconcileHook;
    Runnable resourcesHook;
    Runnable instrumentHook;
    java.util.function.Consumer<String> instrumentIdHook;
    boolean blockPurchases;
    String settlementCurrency = "RUB";
    int cancels;
    int orderReads;

    Simulator(Clock c) {
      clock = c;
    }

    public Instrument instrument(String id) {
      if (instrumentHook != null) instrumentHook.run();
      if (instrumentIdHook != null) instrumentIdHook.accept(id);
      return new Instrument(
          id, instrumentLotSize, new BigDecimal("0.01"), settlementCurrency, true, true, true);
    }

    public boolean buyTotalCostNondecreasing(
        String accountId, String instrumentId, String currency) {
      return monotonicBuyPricing && (buyTotalCosts.isEmpty() || attestBuyTotalCosts);
    }

    public CapitalEvidence capital(String account) {
      capitalCalls++;
      if (capitalResponse != null) return capitalResponse.apply(account);
      if (capitalHook != null) capitalHook.run();
      if (failCapital) throw new IllegalStateException("simulator unavailable");
      return new CapitalEvidence(
          account,
          "mirror".equals(account) ? mirrorCapital : sourceCapital,
          clock.instant(),
          clock.instant(),
          true,
          "simulator");
    }

    public AutoCloseable observe(ru.orbita.broker.SourceFeedJournal journal) {
      return () -> {};
    }

    public ResourceEvidence resources(OrderRequest r) {
      resourcesCalls++;
      if (resourcesHook != null) resourcesHook.run();
      if (resourcesRequestHook != null) resourcesRequestHook.accept(r);
      return new ResourceEvidence(
          "mirror",
          instrument(r.instrumentId()),
          availableMoney,
          buyMaxLots,
          100,
          quotedCommission(r),
          zeroFees ? BigDecimal.ZERO : additionalCommissionReserve,
          clock.instant(),
          clock.instant(),
          !(blockPurchases && r.side() == Side.BUY),
          "simulator");
    }

    private BigDecimal quotedCommission(OrderRequest request) {
      if (zeroFees) return BigDecimal.ZERO;
      BigDecimal total = buyTotalCosts.get(request.lots());
      if (request.side() == Side.BUY && total != null)
        return total
            .subtract(
                request
                    .limitPrice()
                    .multiply(BigDecimal.valueOf(instrumentLotSize))
                    .multiply(BigDecimal.valueOf(request.lots())))
            .subtract(additionalCommissionReserve);
      if (request.side() == Side.BUY && buyCommissionPerLot != null)
        return buyCommissionPerLot
            .multiply(BigDecimal.valueOf(request.lots()))
            .max(minimumBuyCommissionFloor == null ? BigDecimal.ZERO : minimumBuyCommissionFloor);
      return preliminaryCommission;
    }

    public History history(Instant a, Instant b) {
      if (failHistory) throw new IllegalStateException("saved evidence only");
      return new History(
          historyExecutions.stream()
              .filter(e -> !e.executedAt().isBefore(a) && !e.executedAt().isAfter(b))
              .toList(),
          List.of(),
          a,
          b);
    }

    public History mirrorHistory(Instant a, Instant b) {
      return new History(mirrorExecutions, List.of(), a, b);
    }

    public PositionEvidence positions(String account) {
      BigDecimal quantity =
          "source".equals(account)
              ? historyExecutions.stream()
                  .filter(e -> e.instrumentId().equals("instrument"))
                  .map(e -> e.lots().multiply(BigDecimal.valueOf(e.side() == Side.BUY ? 1 : -1)))
                  .reduce(BigDecimal.ZERO, BigDecimal::add)
              : BigDecimal.valueOf(bankLots + extraBankLots);
      var positions = new ArrayList<Position>();
      positions.add(new Position("instrument", quantity, BigDecimal.ZERO));
      if ("source".equals(account)) {
        historyExecutions.stream()
            .map(SourceExecution::instrumentId)
            .distinct()
            .filter(id -> !id.equals("instrument"))
            .forEach(
                id ->
                    positions.add(
                        new Position(
                            id,
                            historyExecutions.stream()
                                .filter(e -> e.instrumentId().equals(id))
                                .map(
                                    e ->
                                        e.lots()
                                            .multiply(
                                                BigDecimal.valueOf(e.side() == Side.BUY ? 1 : -1)))
                                .reduce(BigDecimal.ZERO, BigDecimal::add),
                            BigDecimal.ZERO)));
      }
      if (currencyCash)
        positions.add(
            new Position(
                "fx", null, null, "source".equals(account) ? sourceCash : mirrorCash, false));
      return new PositionEvidence(
          account, positions, clock.instant(), clock.instant(), true, "simulator");
    }

    public boolean positionAbsentMeansZero(String id, PositionEvidence evidence) {
      return evidence.complete();
    }

    public Optional<BigDecimal> bestOppositePrice(String id, Side side) {
      return Optional.ofNullable(quotes.isEmpty() ? oppositePrice : quotes.removeFirst());
    }

    public OrderEvidence post(OrderRequest r) {
      posts++;
      sent = r;
      sentRequests.add(r);
      if (postHook != null) postHook.accept(r);
      if (rejectNext != null) {
        var reason = rejectNext;
        rejectNext = null;
        var result =
            new OrderEvidence(
                r.requestId(),
                r.accountId(),
                "rejected",
                r.instrumentId(),
                r.side(),
                r.lots(),
                0,
                OrderStatus.REJECTED,
                List.of(),
                clock.instant(),
                "simulator confirmed halt",
                reason);
        orders.put(r.requestId(), result);
        return result;
      }
      if (partialNext >= 0) {
        long filled = partialNext;
        partialNext = -1;
        bankLots += filled * (r.side() == Side.BUY ? 1 : -1);
        var result =
            evidence(r, filled == 0 ? OrderStatus.NEW : OrderStatus.PARTIALLY_FILLED, filled);
        orders.put(r.requestId(), result);
        if (afterPostHook != null) afterPostHook.run();
        return result;
      }
      bankLots += (fillEveryPost ? r.lots() : 1) * (r.side() == Side.BUY ? 1 : -1);
      var result =
          evidence(
              r,
              fillEveryPost ? OrderStatus.FILLED : OrderStatus.UNKNOWN,
              fillEveryPost ? r.lots() : 0);
      orders.put(r.requestId(), fillEveryPost ? result : evidence(r, OrderStatus.FILLED, 1));
      if (fillEveryPost && afterPostHook != null) afterPostHook.run();
      return result;
    }

    public OrderEvidence reconcile(OrderRequest r) {
      orderReads++;
      var prior = orders.get(r.requestId());
      if (prior == null) prior = evidence(r, OrderStatus.UNKNOWN, 0);
      long received = receipt();
      var result =
          new OrderEvidence(
              prior.requestId(),
              prior.accountId(),
              prior.brokerOrderId(),
              prior.instrumentId(),
              prior.side(),
              prior.lotsRequested(),
              prior.lotsExecuted(),
              prior.status(),
              prior.fills(),
              clock.instant(),
              prior.detail(),
              prior.rejectionReason(),
              received,
              ru.orbita.broker.BrokerReceiptClock.domain());
      Runnable hook = afterReconcileHook;
      afterReconcileHook = null;
      if (hook != null) hook.run();
      return result;
    }

    public OrderEvidence cancel(OrderRequest r, String id) {
      cancels++;
      var prior = orders.get(r.requestId());
      var result =
          new OrderEvidence(
              r.requestId(),
              r.accountId(),
              "bank-order",
              r.instrumentId(),
              r.side(),
              r.lots(),
              prior == null ? 0 : prior.lotsExecuted(),
              OrderStatus.CANCELLED,
              prior == null ? List.of() : prior.fills(),
              clock.instant(),
              "simulator cancellation");
      orders.put(r.requestId(), result);
      return result;
    }

    OrderEvidence evidence(OrderRequest r, OrderStatus state, long filled) {
      long received = receipt();
      return new OrderEvidence(
          r.requestId(),
          r.accountId(),
          "bank-order",
          r.instrumentId(),
          r.side(),
          r.lots(),
          filled,
          state,
          individualLotFills
              ? java.util.stream.LongStream.range(0, filled)
                  .mapToObj(
                      index ->
                          new Fill(
                              "fill-" + r.requestId() + "-" + index,
                              BigDecimal.ONE,
                              r.limitPrice(),
                              settlementCurrency,
                              clock.instant()))
                  .toList()
              : filled == 0
                  ? List.of()
                  : List.of(
                      new Fill(
                          "fill-" + r.requestId(),
                          BigDecimal.valueOf(filled),
                          r.limitPrice(),
                          settlementCurrency,
                          clock.instant())),
          clock.instant(),
          "PostOrder response: GetOrderState by original UUID; stages in lots",
          state == OrderStatus.REJECTED ? RejectionReason.OTHER : RejectionReason.NONE,
          received,
          ru.orbita.broker.BrokerReceiptClock.domain());
    }

    private long receipt() {
      long received = ru.orbita.broker.BrokerReceiptClock.nanoTime();
      if (clock instanceof MutableClock mutable)
        mutable.receiptInstants.put(received, clock.instant());
      return received;
    }

    public CommissionEvidence commission(OrderRequest r) {
      var evidence = orders.get(r.requestId());
      boolean finalState =
          evidence != null
              && Set.of(OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED)
                  .contains(evidence.status());
      return new CommissionEvidence(
          r.accountId(),
          r.requestId(),
          zeroFees ? BigDecimal.ZERO : BigDecimal.ONE,
          settlementCurrency,
          finalState && finalCommissionAvailable,
          clock.instant(),
          "independent qualified simulator charge");
    }

    public void close() {}
  }
}
