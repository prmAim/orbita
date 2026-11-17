package ru.orbita.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.stub.StreamObserver;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.control.ControlService;
import ru.orbita.control.OrbitaProperties;
import ru.orbita.copy.DurableSourceFeedJournal;
import ru.orbita.notifications.*;
import ru.orbita.runtime.ApplicationReadiness;
import ru.tinkoff.piapi.contract.v1.*;

@SpringBootTest(properties = {"orbita.telegram.enabled=false", "orbita.worker.enabled=false"})
@Testcontainers
@Import(BrokerConnectivityIT.Fixture.class)
class BrokerConnectivityIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine");

  @DynamicPropertySource
  static void database(DynamicPropertyRegistry p) {
    p.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    p.add("spring.datasource.username", POSTGRES::getUsername);
    p.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @Autowired BrokerConnectivityService connectivity;
  @Autowired NotificationService notifications;
  @Autowired NotificationPublisher publisher;
  @Autowired TransactionTemplate transactions;
  @Autowired JdbcTemplate db;
  @Autowired ControlService control;
  @Autowired MutableClock clock;
  @Autowired TelegramSimulator telegram;
  @Autowired OrbitaProperties properties;
  @Autowired DurableSourceFeedJournal feedJournal;
  @Autowired ApplicationReadiness readiness;
  @TempDir java.nio.file.Path directory;

  @BeforeEach
  void reset() {
    db.execute("truncate notification_attempt,notification,journal_entry restart identity cascade");
    db.update("insert into broker_connectivity(singleton) values(true) on conflict do nothing");
    db.update(
        "update broker_connectivity set loss_notification_id=null,lost_at=null,last_recovered_at=null,restore_session=null where singleton");
    db.update(
        "update control_state set mode='STOPPED',reconciled=false,feed_ready=false where id=1");
    readiness.brokerPersistenceRecovered();
    readiness.mirrorPersistenceRecovered();
    clock.now = Instant.parse("2026-11-15T10:00:00Z");
    telegram.results.clear();
    telegram.messages.clear();
    telegram.beforeSend = null;
  }

  @Test
  void repeatedLossAndUndeliveredRecoveryShareOneStableNotificationId() {
    connectivity.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
    UUID id = connectivity.status().lossNotificationId();
    connectivity.connectionLost(BrokerConnectivityEvents.Cause.RPC_DEADLINE_EXCEEDED);
    assertThat(notifications.list(200)).hasSize(1);
    clock.now = clock.now.plusSeconds(5);
    connectivity.readSucceeded();
    connectivity.readSucceeded();
    assertThat(connectivity.status().disconnected()).isFalse();
    assertThat(notifications.list(200)).hasSize(1);
    assertThat(notifications.get(id).text())
        .contains(
            "Потеря связи",
            "Связь с брокером восстановлена",
            "Остановлено",
            id.toString(),
            "15.11.2026 13:00:00 МСК",
            "15.11.2026 13:00:05 МСК");
    assertThat(control.status().mode()).isEqualTo("STOPPED");
    assertThat(control.status().reconciled()).isFalse();
    notifications.deliverDue();
    assertThat(notifications.get(id).status()).isEqualTo("DELIVERED");
    assertThat(telegram.messages).containsExactly(notifications.get(id).text());
  }

  @Test
  void deliveredLossSurvivesANewServiceInstanceAndProducesOneRecoveryInTheCurrentMode() {
    connectivity.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
    UUID lost = connectivity.status().lossNotificationId();
    notifications.deliverDue();
    var restarted = restarted(properties);
    clock.now = clock.now.plusSeconds(8);
    restarted.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
    assertThat(restarted.status().lossNotificationId()).isEqualTo(lost);
    restarted.readSucceeded();
    restarted.readSucceeded();
    notifications.deliverDue();
    assertThat(notifications.list(200)).hasSize(2);
    assertThat(
            notifications.list(200).stream()
                .filter(n -> n.category().equals("BROKER_CONNECTION_RESTORED")))
        .singleElement()
        .satisfies(n -> assertThat(n.text()).contains("Остановлено", "STOPPED"));
    assertThat(notifications.get(lost).text()).doesNotContain("восстановлена");
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(TelegramTransport.Outcome.class)
  void recoveryDuringInFlightLossDeliveryUsesTheActualDeliveryOutcome(
      TelegramTransport.Outcome outcome) {
    connectivity.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
    UUID id = connectivity.status().lossNotificationId();
    String original = notifications.get(id).text();
    telegram.results.add(
        outcome == TelegramTransport.Outcome.SUCCESS
            ? TelegramTransport.Result.success()
            : outcome == TelegramTransport.Outcome.NO_RESPONSE
                ? TelegramTransport.Result.noResponse()
                : TelegramTransport.Result.failure("synthetic failure", Duration.ZERO));
    telegram.beforeSend =
        () -> {
          clock.now = clock.now.plusSeconds(1);
          connectivity.readSucceeded();
        };
    notifications.deliverDue();
    connectivity.readSucceeded();
    assertThat(notifications.attempts(id))
        .singleElement()
        .satisfies(a -> assertThat(a.payload()).isEqualTo(original));
    if (outcome == TelegramTransport.Outcome.SUCCESS) {
      assertThat(notifications.get(id).text()).isEqualTo(original);
      assertThat(
              notifications.list(200).stream()
                  .filter(n -> n.category().equals("BROKER_CONNECTION_RESTORED")))
          .hasSize(1);
    } else {
      assertThat(notifications.list(200)).hasSize(1);
      assertThat(notifications.get(id).text())
          .contains("Потеря связи", "Связь с брокером восстановлена", id.toString());
      clock.now = clock.now.plusSeconds(60);
      notifications.deliverDue();
      assertThat(notifications.get(id).status()).isEqualTo("DELIVERED");
      assertThat(telegram.messages.getLast()).isEqualTo(notifications.get(id).text());
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void restoredCopyDoesNotReviveOldLossButANewOutageSurvivesOrdinaryRestart(boolean delivered) {
    connectivity.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
    UUID old = connectivity.status().lossNotificationId();
    if (delivered) notifications.deliverDue();
    db.update("update control_state set mode='RESTORED' where id=1");
    var restored = restarted(new OrbitaProperties(null, null, null, true, false, false));
    restored.readSucceeded();
    restored.readSucceeded();
    assertThat(notifications.list(200)).hasSize(1);
    assertThat(notifications.get(old).status()).isEqualTo(delivered ? "DELIVERED" : "IGNORED");
    assertThat(notifications.get(old).text()).doesNotContain("восстановлена");
    restored.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
    UUID current = restored.status().lossNotificationId();
    notifications.deliverDue();
    assertThat(current).isNotEqualTo(old);
    restarted(properties).readSucceeded();
    assertThat(
            notifications.list(200).stream()
                .filter(n -> n.category().equals("BROKER_CONNECTION_RESTORED")))
        .singleElement()
        .satisfies(n -> assertThat(n.text()).contains("Восстановленная копия", "RESTORED"));
  }

  @Test
  void outboxAndConnectionStateRollbackTogetherOnPersistenceFailure() {
    connectivity.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
    UUID id = connectivity.status().lossNotificationId();
    String before = notifications.get(id).text();
    db.execute(
        "alter table broker_connectivity add constraint synthetic_no_recovery check(last_recovered_at is null)");
    try {
      org.assertj.core.api.Assertions.assertThatThrownBy(connectivity::readSucceeded)
          .isInstanceOf(BrokerPersistenceFailure.class);
      assertThat(connectivity.status().disconnected()).isTrue();
      assertThat(notifications.get(id).resolvedAt()).isNull();
      assertThat(notifications.get(id).text()).isEqualTo(before);
    } finally {
      db.execute("alter table broker_connectivity drop constraint synthetic_no_recovery");
    }
    connectivity.readSucceeded();
    assertThat(connectivity.status().disconnected()).isFalse();
  }

  private BrokerConnectivityService restarted(OrbitaProperties settings) {
    return new BrokerConnectivityService(
        db, transactions, publisher, notifications, clock, settings);
  }

  @Test
  void actualTlsBrokerReadRecoversTheNoticeEvenWhenItsCapitalIsUnqualified() throws Exception {
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          volatile boolean available;

          @Override
          public void getPortfolio(
              PortfolioRequest request, StreamObserver<PortfolioResponse> out) {
            if (!available) {
              out.onError(io.grpc.Status.UNAVAILABLE.asRuntimeException());
              return;
            }
            out.onNext(
                PortfolioResponse.newBuilder()
                    .setAccountId(request.getAccountId())
                    .setTotalAmountPortfolio(
                        MoneyValue.newBuilder().setCurrency("rub").setUnits(100))
                    .build());
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, operations);
        var gateway = BrokerTlsTestSupport.gateway(server.port(), clock, connectivity)) {
      assertThat(notifications.list(200)).isEmpty();
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> gateway.capital("source"))
          .isInstanceOf(ru.ttech.piapi.core.connector.exception.ServiceRuntimeException.class);
      UUID id = connectivity.status().lossNotificationId();
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> gateway.capital("mirror"))
          .isInstanceOf(ru.ttech.piapi.core.connector.exception.ServiceRuntimeException.class);
      assertThat(notifications.list(200)).hasSize(1);
      operations.available = true;
      clock.now = clock.now.plusSeconds(2);
      assertThat(gateway.capital("source").compositionConfirmed()).isFalse();
      gateway.capital("mirror");
      assertThat(connectivity.status().disconnected()).isFalse();
      assertThat(notifications.list(200)).hasSize(1);
      assertThat(notifications.get(id).text())
          .contains("Связь с брокером восстановлена", "Остановлено");
      assertThat(control.status().reconciled()).isFalse();
    }
  }

  @Test
  void asyncPersistenceFailureActivelyReconnectsAndRecoversWithoutAnotherSourceTrade()
      throws Exception {
    var stream =
        new MirrorAcknowledgements() {
          final java.util.concurrent.atomic.AtomicInteger subscriptions =
              new java.util.concurrent.atomic.AtomicInteger();

          @Override
          public void tradesStream(
              TradesStreamRequest request, StreamObserver<TradesStreamResponse> out) {
            if (request.getAccountsList().equals(List.of("mirror"))) {
              super.tradesStream(request, out);
              return;
            }
            subscriptions.incrementAndGet();
            out.onNext(
                TradesStreamResponse.newBuilder()
                    .setSubscription(
                        SubscriptionResponse.newBuilder()
                            .setStatus(ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK)
                            .addAccounts("source"))
                    .build());
            // Each live subscription emits its one ACK and no future frame or trade.
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          @Override
          public void getOperationsByCursor(
              GetOperationsByCursorRequest request,
              StreamObserver<GetOperationsByCursorResponse> out) {
            out.onNext(GetOperationsByCursorResponse.getDefaultInstance());
            out.onCompleted();
          }
        };
    db.update(
        "update control_state set observation_boundary=?,observation_activated=true where id=1",
        java.sql.Timestamp.from(clock.instant().minusSeconds(1)));
    assertThat(readiness.ready()).isTrue();
    db.execute(
        "alter table control_state add constraint synthetic_feed_commit_failure check(not feed_ready)");
    try (var server = BrokerTlsTestSupport.open(directory, stream, operations);
        var gateway = BrokerTlsTestSupport.gateway(server.port(), clock, connectivity);
        var feed = gateway.observe(feedJournal)) {
      try {
        eventually(() -> !readiness.ready());
        assertThat(readiness.initialized()).isTrue();
        assertThat(notifications.list(200)).isEmpty();
      } finally {
        db.execute("alter table control_state drop constraint synthetic_feed_commit_failure");
      }
      eventually(() -> readiness.ready() && stream.subscriptions.get() >= 2);
      assertThat(control.status().reconciled()).isFalse();
      assertThat(connectivity.status().disconnected()).isFalse();
      assertThat(notifications.list(200)).isEmpty();
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"awaiting-ack", "unqualified-linkage"})
  void subscriptionReadinessAndQualificationGapsAndIntentionalCloseAreNotConnectionLoss(
      String condition) throws Exception {
    var opened = new java.util.concurrent.CountDownLatch(1);
    var stream =
        new MirrorAcknowledgements() {
          @Override
          public void tradesStream(
              TradesStreamRequest request, StreamObserver<TradesStreamResponse> out) {
            if (request.getAccountsList().equals(List.of("mirror"))) {
              super.tradesStream(request, out);
              return;
            }
            if (condition.equals("unqualified-linkage"))
              out.onNext(
                  TradesStreamResponse.newBuilder()
                      .setSubscription(
                          SubscriptionResponse.newBuilder()
                              .setStatus(ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK)
                              .addAccounts("source"))
                      .build());
            opened.countDown();
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          @Override
          public void getOperationsByCursor(
              GetOperationsByCursorRequest request,
              StreamObserver<GetOperationsByCursorResponse> out) {
            out.onNext(
                GetOperationsByCursorResponse.newBuilder()
                    .addItems(
                        OperationItem.newBuilder()
                            .setBrokerAccountId("source")
                            .setInstrumentUid("instrument")
                            .setType(OperationType.OPERATION_TYPE_BUY)
                            .setTradesInfo(
                                OperationItemTrades.newBuilder()
                                    .addTrades(
                                        OperationItemTrade.newBuilder()
                                            .setNum("unknown-order-link")
                                            .setDate(
                                                TInvestBrokerGateway.timestamp(clock.instant()))
                                            .setQuantity(1)
                                            .setPrice(
                                                MoneyValue.newBuilder()
                                                    .setCurrency("rub")
                                                    .setUnits(100)))))
                    .build());
            out.onCompleted();
          }
        };
    var instruments =
        new InstrumentsServiceGrpc.InstrumentsServiceImplBase() {
          @Override
          public void getInstrumentBy(
              InstrumentRequest request, StreamObserver<InstrumentResponse> out) {
            out.onNext(
                InstrumentResponse.newBuilder()
                    .setInstrument(
                        ru.tinkoff.piapi.contract.v1.Instrument.newBuilder()
                            .setUid(request.getId())
                            .setLot(1)
                            .setCurrency("rub")
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }
        };
    db.update(
        "update control_state set observation_boundary=?,observation_activated=true where id=1",
        java.sql.Timestamp.from(clock.instant().minusSeconds(1)));
    try (var server = BrokerTlsTestSupport.open(directory, stream, operations, instruments);
        var gateway = BrokerTlsTestSupport.gateway(server.port(), clock, connectivity)) {
      var feed = gateway.observe(feedJournal);
      try {
        assertThat(opened.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        if (condition.equals("unqualified-linkage"))
          eventually(
              () ->
                  control.status().lastError() != null
                      && control.status().lastError().contains("Source order identity unresolved"));
        assertThat(notifications.list(200)).isEmpty();
        assertThat(connectivity.status().disconnected()).isFalse();
      } finally {
        feed.close();
      }
      assertThat(notifications.list(200)).isEmpty();
    }
  }

  private static void eventually(java.util.function.BooleanSupplier assertion) throws Exception {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(15);
    while (!assertion.getAsBoolean()) {
      if (System.nanoTime() >= deadline)
        throw new AssertionError("Expected asynchronous broker boundary result not observed");
      Thread.sleep(10);
    }
  }

  private static class MirrorAcknowledgements
      extends OrdersStreamServiceGrpc.OrdersStreamServiceImplBase {
    public void orderStateStream(
        OrderStateStreamRequest request, StreamObserver<OrderStateStreamResponse> out) {
      assertThat(request.getAccountsList()).containsExactly("mirror");
      out.onNext(
          OrderStateStreamResponse.newBuilder()
              .setSubscription(MirrorOrderFeedTlsTest.ack("mirror"))
              .build());
    }

    public void tradesStream(
        TradesStreamRequest request, StreamObserver<TradesStreamResponse> out) {
      assertThat(request.getAccountsList()).containsExactly("mirror");
      out.onNext(
          TradesStreamResponse.newBuilder()
              .setSubscription(MirrorOrderFeedTlsTest.ack("mirror"))
              .build());
    }
  }

  static final class MutableClock extends Clock {
    Instant now = Instant.parse("2026-11-15T10:00:00Z");

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

  static final class TelegramSimulator implements TelegramTransport {
    final ArrayDeque<Result> results = new ArrayDeque<>();
    final List<String> messages = new ArrayList<>();
    Runnable beforeSend;

    public Result send(String text) {
      messages.add(text);
      if (beforeSend != null) {
        var run = beforeSend;
        beforeSend = null;
        run.run();
      }
      return results.isEmpty() ? Result.success() : results.remove();
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class Fixture {
    @Bean
    @Primary
    MutableClock testClock() {
      return new MutableClock();
    }

    @Bean
    @Primary
    TelegramSimulator telegram() {
      return new TelegramSimulator();
    }
  }
}
