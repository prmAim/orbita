package ru.orbita.broker;

import static org.assertj.core.api.Assertions.*;

import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.tinkoff.piapi.contract.v1.*;

class MirrorOrderFeedTlsTest {
  @TempDir Path directory;

  @Test
  void wholeChainFixtureAcknowledgesBothMirrorStreamsAndSourceSeparately() throws Exception {
    var ready = new CountDownLatch(1);
    var sink = journal(new LinkedBlockingQueue<>(), () -> {}, () -> {}, ready::countDown);
    try (var bank = WholeChainBrokerSimulator.open(directory);
        var gateway = bank.gateway(Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS);
        var feed = gateway.observe(sink)) {
      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(bank.calls("OrderStateStream"))
          .singleElement()
          .satisfies(
              call ->
                  assertThat(((OrderStateStreamRequest) call.request()).getAccountsList())
                      .containsExactly(WholeChainBrokerSimulator.MIRROR));
      assertThat(bank.calls("TradesStream"))
          .extracting(call -> ((TradesStreamRequest) call.request()).getAccounts(0))
          .containsExactlyInAnyOrder(
              WholeChainBrokerSimulator.SOURCE, WholeChainBrokerSimulator.MIRROR);
    }
  }

  @Test
  void intentionalCloseDuringMirrorJournalWorkDoesNotReportPersistenceFailure() throws Exception {
    var entered = new CountDownLatch(1);
    var interrupted = new CountDownLatch(1);
    var failure = new CountDownLatch(1);
    var observations = new LinkedBlockingQueue<BrokerGateway.OrderEvidence>();
    var sink =
        journal(
            observations,
            () -> {
              entered.countDown();
              try {
                new CountDownLatch(1).await();
              } catch (InterruptedException shutdown) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                    "synthetic journal interrupted by intentional close", shutdown);
              }
            },
            failure::countDown);
    try (var server = BrokerTlsTestSupport.open(directory);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var feed = gateway.observe(sink);
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
      } finally {
        feed.close();
      }
      assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(failure.await(300, TimeUnit.MILLISECONDS)).isFalse();
    }
  }

  @Test
  void earlyFailureWithoutOrderIdAndNotFoundDoesNotInventOrderOrCommission() throws Exception {
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          public void postOrder(PostOrderRequest request, StreamObserver<PostOrderResponse> out) {
            out.onError(
                io.grpc.Status.INVALID_ARGUMENT.withDescription("30098").asRuntimeException());
          }

          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            assertThat(request.getOrderIdType()).isEqualTo(OrderIdType.ORDER_ID_TYPE_REQUEST);
            out.onError(io.grpc.Status.NOT_FOUND.asRuntimeException());
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var request =
          new BrokerGateway.OrderRequest(
              "mirror",
              UUID.randomUUID(),
              "instrument-1",
              BrokerGateway.Side.BUY,
              3,
              new java.math.BigDecimal("100"));
      var failure = gateway.post(request);
      assertThat(failure.status()).isEqualTo(BrokerGateway.OrderStatus.UNKNOWN);
      assertThat(failure.brokerOrderId()).isNull();
      assertThat(failure.sdkEvidence().responseOrderId()).isNull();
      assertThat(gateway.reconcile(request).status()).isEqualTo(BrokerGateway.OrderStatus.UNKNOWN);
      var commission = gateway.commission(request);
      assertThat(commission.qualified()).isFalse();
      assertThat(commission.amount()).isNull();
      assertThat(failure.rejectionReason()).isEqualTo(BrokerGateway.RejectionReason.NONE);
    }
  }

  @Test
  void acceptedPostWithLostReplyUsesOriginalRequestUuidForEveryLookup() throws Exception {
    var posts = new java.util.concurrent.atomic.AtomicInteger();
    var lookups = new java.util.concurrent.atomic.AtomicInteger();
    UUID id = UUID.randomUUID();
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          public void postOrder(PostOrderRequest request, StreamObserver<PostOrderResponse> out) {
            posts.incrementAndGet();
            assertThat(request.getOrderId()).isEqualTo(id.toString());
            out.onError(io.grpc.Status.DEADLINE_EXCEEDED.asRuntimeException());
          }

          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            assertThat(request.getAccountId()).isEqualTo("mirror");
            assertThat(request.getOrderId()).isEqualTo(id.toString());
            assertThat(request.getOrderIdType()).isEqualTo(OrderIdType.ORDER_ID_TYPE_REQUEST);
            if (lookups.incrementAndGet() < 3) {
              out.onError(io.grpc.Status.NOT_FOUND.asRuntimeException());
              return;
            }
            out.onNext(
                OrderState.newBuilder()
                    .setOrderRequestId(id.toString())
                    .setOrderId("real-bank-id")
                    .setInstrumentUid("instrument-1")
                    .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                    .setLotsRequested(3)
                    .setLotsExecuted(0)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED)
                    .build());
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var request =
          new BrokerGateway.OrderRequest(
              "mirror",
              id,
              "instrument-1",
              BrokerGateway.Side.BUY,
              3,
              new java.math.BigDecimal("100"));
      var lost = gateway.post(request);
      assertThat(lost.status()).isEqualTo(BrokerGateway.OrderStatus.UNKNOWN);
      assertThat(lost.brokerOrderId()).isNull();
      assertThat(gateway.reconcile(request).status()).isEqualTo(BrokerGateway.OrderStatus.UNKNOWN);
      assertThat(gateway.reconcile(request).status()).isEqualTo(BrokerGateway.OrderStatus.UNKNOWN);
      assertThat(gateway.reconcile(request).status())
          .isEqualTo(BrokerGateway.OrderStatus.CANCELLED);
      assertThat(posts.get()).isEqualTo(1);
      assertThat(lookups.get()).isEqualTo(3);
    }
  }

  @Test
  void malformedTradePriceDoesNotEraseOriginalStateObservation() throws Exception {
    var observations = new LinkedBlockingQueue<BrokerGateway.OrderEvidence>();
    var requestId = UUID.randomUUID();
    var broker =
        new OrdersStreamServiceGrpc.OrdersStreamServiceImplBase() {
          @Override
          public void orderStateStream(
              OrderStateStreamRequest request, StreamObserver<OrderStateStreamResponse> out) {
            assertThat(request.getAccountsList()).containsExactly("mirror");
            out.onNext(
                OrderStateStreamResponse.newBuilder().setSubscription(ack("mirror")).build());
            out.onNext(
                OrderStateStreamResponse.newBuilder()
                    .setOrderState(
                        OrderStateStreamResponse.OrderState.newBuilder()
                            .setAccountId("mirror")
                            .setOrderRequestId(requestId.toString())
                            .setOrderId("internal-id")
                            .setTradeOrderId("exchange-id")
                            .setInstrumentUid("instrument-1")
                            .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                            .setLotSize(10)
                            .setLotsRequested(3)
                            .setLotsExecuted(1)
                            .setLotsLeft(2)
                            .setExecutionReportStatus(
                                OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL)
                            .setStatusInfoValue(123456)
                            .addTrades(
                                OrderTrade.newBuilder()
                                    .setTradeId("trade-1")
                                    .setQuantity(10)
                                    .setPrice(
                                        Quotation.newBuilder().setUnits(100).setNano(1500000000))
                                    .setDateTime(
                                        TInvestBrokerGateway.timestamp(
                                            Instant.parse("2026-11-17T10:00:00Z")))))
                    .build());
          }

          @Override
          public void tradesStream(
              TradesStreamRequest request, StreamObserver<TradesStreamResponse> out) {
            out.onNext(
                TradesStreamResponse.newBuilder()
                    .setSubscription(ack(request.getAccounts(0)))
                    .build());
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS);
        var feed = gateway.observe(journal(observations))) {
      var observation = observations.poll(5, TimeUnit.SECONDS);
      assertThat(observation).isNotNull();
      assertThat(observation.requestId()).isEqualTo(requestId);
      assertThat(observation.sdkEvidence().responseOrderId()).isEqualTo("internal-id");
      assertThat(observation.sdkEvidence().tradeOrderId()).isEqualTo("exchange-id");
      assertThat(observation.sdkEvidence().statusInfoPresent()).isTrue();
      assertThat(observation.sdkEvidence().causeValue()).isEqualTo(123456);
      assertThat(observation.sdkEvidence().trades())
          .singleElement()
          .satisfies(
              t -> {
                assertThat(t.pieces()).isEqualTo(10);
                assertThat(t.price()).isNull();
              });
      assertThat(observation.rejectionReason()).isEqualTo(BrokerGateway.RejectionReason.NONE);
    }
  }

  static SubscriptionResponse ack(String account) {
    return SubscriptionResponse.newBuilder()
        .addAccounts(account)
        .setStatus(ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK)
        .build();
  }

  static SourceFeedJournal journal(BlockingQueue<BrokerGateway.OrderEvidence> observations) {
    return journal(observations, () -> {}, () -> {});
  }

  static SourceFeedJournal journal(
      BlockingQueue<BrokerGateway.OrderEvidence> observations,
      Runnable mirrorGap,
      Runnable unavailable) {
    return journal(observations, mirrorGap, unavailable, () -> {});
  }

  static SourceFeedJournal journal(
      BlockingQueue<BrokerGateway.OrderEvidence> observations,
      Runnable mirrorGap,
      Runnable unavailable,
      Runnable recovered) {
    return new SourceFeedJournal() {
      public void mirrorGap() {
        mirrorGap.run();
      }

      public void mirrorPersistenceUnavailable() {
        unavailable.run();
      }

      public void mirrorRecovered() {
        recovered.run();
      }

      public boolean supportsMirrorOrders() {
        return true;
      }

      public void mirrorOrder(BrokerGateway.OrderEvidence evidence) {
        observations.add(evidence);
      }

      public Instant observationBoundary() {
        return Instant.now().minusSeconds(1);
      }

      public Optional<String> sourceOrderId(String a, String t) {
        return Optional.empty();
      }

      public Optional<BrokerGateway.SourceExecution> remembered(String a, String t) {
        return Optional.empty();
      }

      public List<BrokerGateway.SourceExecution> pendingTimestampRecovery() {
        return List.of();
      }

      public Instant firstReceived(String a, String t, Instant at) {
        return at;
      }

      public void remember(BrokerGateway.SourceExecution e) {}

      public void accept(BrokerGateway.SourceExecution e) {}

      public void skip(BrokerGateway.SourceExecution e, String reason) {}

      public void gap(String reason) {}

      public void recovered() {}
    };
  }
}
