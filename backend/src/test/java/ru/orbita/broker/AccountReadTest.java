package ru.orbita.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.tinkoff.piapi.contract.v1.*;

class AccountReadTest {
  @TempDir Path directory;

  @Test
  void readsAllActiveOrdersAndUniqueActualStagesWithoutMutations() throws Exception {
    var mutations = new java.util.concurrent.atomic.AtomicInteger();
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          public void postOrder(PostOrderRequest request, StreamObserver<PostOrderResponse> out) {
            mutations.incrementAndGet();
            out.onError(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());
          }

          public void cancelOrder(
              CancelOrderRequest request, StreamObserver<CancelOrderResponse> out) {
            mutations.incrementAndGet();
            out.onError(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());
          }

          public void replaceOrder(
              ReplaceOrderRequest request, StreamObserver<PostOrderResponse> out) {
            mutations.incrementAndGet();
            out.onError(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());
          }

          public void getOrders(GetOrdersRequest request, StreamObserver<GetOrdersResponse> out) {
            assertThat(request.getAccountId()).isEqualTo("mirror");
            out.onNext(
                GetOrdersResponse.newBuilder()
                    .addOrders(order("one"))
                    .addOrders(order("two"))
                    .build());
            out.onCompleted();
          }

          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            assertThat(request.getPriceType()).isEqualTo(PriceType.PRICE_TYPE_CURRENCY);
            out.onNext(order(request.getOrderId()));
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var result = gateway.readOrders(List.of());
      assertThat(result.complete()).isTrue();
      assertThat(result.items()).hasSize(2);
      var one = result.items().getFirst();
      assertThat(one.brokerOrderId()).isEqualTo("one");
      assertThat(one.origin()).isEqualTo(AccountRead.Origin.UNKNOWN);
      assertThat(one.fillsComplete()).isTrue();
      assertThat(one.fills()).hasSize(2);
      assertThat(one.executionPrice().value()).isEqualByComparingTo("106");
      assertThat(one.executionPrice().currency()).isEqualTo("rub");
      assertThat(one.lotsExecuted()).isEqualTo(5);
      assertThat(gateway.readOrders(List.of()).items()).isEqualTo(result.items());
      assertThat(mutations.get()).isZero();
    }
  }

  @Test
  void unavailableDetailDoesNotEraseActiveOrderOrConfirmAverage() throws Exception {
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          public void getOrders(GetOrdersRequest request, StreamObserver<GetOrdersResponse> out) {
            out.onNext(GetOrdersResponse.newBuilder().addOrders(order("one")).build());
            out.onCompleted();
          }

          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            out.onError(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var result = gateway.readOrders(List.of());
      assertThat(result.complete()).isFalse();
      assertThat(result.items()).hasSize(1);
      assertThat(result.items().getFirst().fillsComplete()).isFalse();
      assertThat(result.items().getFirst().executionPrice()).isNull();
    }
  }

  @Test
  void holdingsRetainCurrenciesWithoutInventingFreeCashAndUsePortfolioTotalPieces()
      throws Exception {
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          public void getPositions(
              PositionsRequest request, StreamObserver<PositionsResponse> out) {
            out.onNext(
                PositionsResponse.newBuilder()
                    .setAccountId("mirror")
                    .addMoney(MoneyValue.newBuilder().setCurrency("rub").setUnits(1000))
                    .addMoney(MoneyValue.newBuilder().setCurrency("usd").setUnits(50))
                    .addBlocked(MoneyValue.newBuilder().setCurrency("rub").setUnits(200))
                    .build());
            out.onCompleted();
          }

          public void getPortfolio(
              PortfolioRequest request, StreamObserver<PortfolioResponse> out) {
            out.onNext(
                PortfolioResponse.newBuilder()
                    .setAccountId("mirror")
                    .addPositions(
                        PortfolioPosition.newBuilder()
                            .setInstrumentUid("instrument")
                            .setTicker("SYN")
                            .setInstrumentType("share")
                            .setQuantity(Quotation.newBuilder().setUnits(30))
                            .setQuantityLots(Quotation.newBuilder().setUnits(3)))
                    .build());
            out.onCompleted();
          }
        };
    var instruments =
        new InstrumentsServiceGrpc.InstrumentsServiceImplBase() {
          public void getInstrumentBy(
              InstrumentRequest request, StreamObserver<InstrumentResponse> out) {
            out.onNext(
                InstrumentResponse.newBuilder()
                    .setInstrument(
                        Instrument.newBuilder()
                            .setUid("instrument")
                            .setLot(10)
                            .setName("Synthetic")
                            .setTicker("SYN"))
                    .build());
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, operations, instruments);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var result = gateway.readHoldings();
      assertThat(result.securities().complete()).isTrue();
      assertThat(result.securities().items().getFirst().totalPieces()).isEqualByComparingTo("30");
      assertThat(result.securities().items().getFirst().totalLots()).isEqualByComparingTo("3");
      assertThat(result.cash().items())
          .extracting(AccountRead.Cash::currency)
          .containsExactly("rub", "usd");
      assertThat(result.cash().items().getFirst().available()).isNull();
      assertThat(result.cash().items().getFirst().total()).isNull();
      assertThat(result.cash().items().getFirst().blocked()).isEqualByComparingTo("200");
    }
  }

  @Test
  void historyUsesBoundedSdkFiltersAndReportsUnavailableOlderPeriod() throws Exception {
    var observed = new java.util.concurrent.atomic.AtomicInteger();
    var now = java.time.Instant.parse("2026-11-16T12:00:00Z");
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          public void getOrders(GetOrdersRequest request, StreamObserver<GetOrdersResponse> out) {
            if (!request.hasAdvancedFilters()) {
              out.onNext(GetOrdersResponse.getDefaultInstance());
              out.onCompleted();
              return;
            }
            assertThat(request.getAdvancedFilters().getExecutionStatusList())
                .contains(
                    OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED,
                    OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL);
            var filter = request.getAdvancedFilters();
            assertThat(filter.getTo().getSeconds() - filter.getFrom().getSeconds())
                .isLessThanOrEqualTo(3600);
            observed.incrementAndGet();
            out.onNext(GetOrdersResponse.getDefaultInstance());
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(),
                Clock.fixed(now, java.time.ZoneOffset.UTC),
                BrokerTlsTestSupport.NO_EVENTS)) {
      var result = gateway.readOrderHistory(now.minusSeconds(3600 * 48), now, List.of());
      assertThat(result.complete()).isFalse();
      assertThat(result.limitations()).anyMatch(s -> s.contains("today"));
      assertThat(observed.get()).isGreaterThan(1);
    }
  }

  @Test
  void cashStreamProvidesExplicitAvailableBlockedAndTotalPerCurrency() throws Exception {
    var events =
        new java.util.concurrent.LinkedBlockingQueue<AccountRead.Snapshot<AccountRead.Cash>>();
    var stream =
        new OperationsStreamServiceGrpc.OperationsStreamServiceImplBase() {
          public void positionsStream(
              PositionsStreamRequest request, StreamObserver<PositionsStreamResponse> out) {
            assertThat(request.getAccountsList()).containsExactly("mirror");
            var data =
                PositionData.newBuilder()
                    .setAccountId("mirror")
                    .setDate(com.google.protobuf.Timestamp.newBuilder().setSeconds(1800000000));
            data.addMoney(
                PositionsMoney.newBuilder()
                    .setAvailableValue(MoneyValue.newBuilder().setCurrency("rub").setUnits(800))
                    .setBlockedValue(MoneyValue.newBuilder().setCurrency("rub").setUnits(200)));
            data.addMoney(
                PositionsMoney.newBuilder()
                    .setAvailableValue(MoneyValue.newBuilder().setCurrency("usd").setUnits(50))
                    .setBlockedValue(MoneyValue.newBuilder().setCurrency("usd").setUnits(0)));
            out.onNext(PositionsStreamResponse.newBuilder().setPosition(data).build());
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, stream);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS);
        var subscription = gateway.observeCash(events::add)) {
      var result = events.poll(10, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(result).isNotNull();
      assertThat(result.items()).hasSize(2);
      assertThat(result.items().getFirst().total()).isEqualByComparingTo("1000");
      assertThat(result.items().getFirst().available()).isEqualByComparingTo("800");
      assertThat(result.items().getFirst().blocked()).isEqualByComparingTo("200");
      assertThat(result.complete()).isFalse();
    }
  }

  @Test
  void cappedHistorySplitsWindowsDeduplicatesOrdersAndRetainsIncompleteStatusWhenUnsplittable()
      throws Exception {
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    var now = java.time.Instant.parse("2026-11-16T12:00:00Z");
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          public void getOrders(GetOrdersRequest request, StreamObserver<GetOrdersResponse> out) {
            var reply = GetOrdersResponse.newBuilder();
            if (request.hasAdvancedFilters()) {
              calls.incrementAndGet();
              var filters = request.getAdvancedFilters();
              if (filters.getTo().getSeconds() - filters.getFrom().getSeconds() > 30)
                for (int i = 0; i < 100; i++) reply.addOrders(order("one"));
              else reply.addOrders(order("one"));
            }
            out.onNext(reply.build());
            out.onCompleted();
          }

          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            out.onNext(order(request.getOrderId()));
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(),
                Clock.fixed(now, java.time.ZoneOffset.UTC),
                BrokerTlsTestSupport.NO_EVENTS)) {
      var result = gateway.readOrderHistory(now.minusSeconds(60), now, List.of());
      assertThat(result.complete()).isTrue();
      assertThat(result.items()).hasSize(1);
      assertThat(calls.get()).isEqualTo(3);
    }
  }

  @Test
  void cashReconnectReportsGapAndRedeliversCurrencyWithoutDeletingMissingRows() throws Exception {
    var events =
        new java.util.concurrent.LinkedBlockingQueue<AccountRead.Snapshot<AccountRead.Cash>>();
    var connections = new java.util.concurrent.atomic.AtomicInteger();
    var stream =
        new OperationsStreamServiceGrpc.OperationsStreamServiceImplBase() {
          public void positionsStream(
              PositionsStreamRequest request, StreamObserver<PositionsStreamResponse> out) {
            int attempt = connections.incrementAndGet();
            if (attempt == 1) {
              out.onError(io.grpc.Status.UNAVAILABLE.asRuntimeException());
              return;
            }
            out.onNext(
                PositionsStreamResponse.newBuilder()
                    .setPosition(
                        PositionData.newBuilder()
                            .setAccountId("mirror")
                            .setDate(
                                com.google.protobuf.Timestamp.newBuilder().setSeconds(1800000000))
                            .addMoney(
                                PositionsMoney.newBuilder()
                                    .setAvailableValue(
                                        MoneyValue.newBuilder().setCurrency("rub").setUnits(800))
                                    .setBlockedValue(
                                        MoneyValue.newBuilder().setCurrency("rub").setUnits(200))))
                    .build());
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, stream);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS);
        var subscription = gateway.observeCash(events::add)) {
      var gap = events.poll(10, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(gap).isNotNull();
      assertThat(gap.items()).isEmpty();
      assertThat(gap.complete()).isFalse();
      var restored = events.poll(10, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(restored).isNotNull();
      assertThat(restored.items()).hasSize(1);
      assertThat(restored.items().getFirst().total()).isEqualByComparingTo("1000");
      assertThat(connections.get()).isEqualTo(2);
    }
  }

  @Test
  void incompleteOrConflictingStagesNeverUseLimitOrSubsetAsExecutionAverage() throws Exception {
    var state = new java.util.concurrent.atomic.AtomicReference<>(order("one"));
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          public void getOrders(GetOrdersRequest request, StreamObserver<GetOrdersResponse> out) {
            out.onNext(GetOrdersResponse.newBuilder().addOrders(state.get()).build());
            out.onCompleted();
          }

          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            out.onNext(state.get());
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      for (var incomplete :
          List.of(
              order("one").toBuilder().setLotsExecuted(6).build(),
              order("one").toBuilder().addStages(stage("a-one", 2, 999)).build())) {
        state.set(incomplete);
        var result = gateway.readOrders(List.of()).items().getFirst();
        assertThat(result.fillsComplete()).isFalse();
        assertThat(result.executionPrice()).isNull();
        assertThat(result.limitPrice().value()).isEqualByComparingTo("120");
      }
    }
  }

  @Test
  void malformedCashDecimalReportsGapAndSubsequentUpdateStillWorks() throws Exception {
    var events =
        new java.util.concurrent.LinkedBlockingQueue<AccountRead.Snapshot<AccountRead.Cash>>();
    var stream =
        new OperationsStreamServiceGrpc.OperationsStreamServiceImplBase() {
          public void positionsStream(
              PositionsStreamRequest request, StreamObserver<PositionsStreamResponse> out) {
            var money =
                PositionsMoney.newBuilder()
                    .setAvailableValue(
                        MoneyValue.newBuilder().setCurrency("rub").setUnits(1).setNano(-1))
                    .setBlockedValue(MoneyValue.newBuilder().setCurrency("rub"));
            var data =
                PositionData.newBuilder()
                    .setAccountId("mirror")
                    .setDate(com.google.protobuf.Timestamp.newBuilder().setSeconds(1800000000))
                    .addMoney(money);
            out.onNext(PositionsStreamResponse.newBuilder().setPosition(data).build());
            out.onNext(
                PositionsStreamResponse.newBuilder()
                    .setPosition(
                        data.clearMoney()
                            .addMoney(
                                money.setAvailableValue(
                                    MoneyValue.newBuilder().setCurrency("rub").setUnits(1))))
                    .build());
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, stream);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS);
        var subscription = gateway.observeCash(events::add)) {
      var gap = events.poll(3, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(gap).isNotNull();
      assertThat(gap.items()).isEmpty();
      var good = events.poll(3, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(good).isNotNull();
      assertThat(good.items().getFirst().total()).isEqualByComparingTo("1");
    }
  }

  private static OrderState order(String id) {
    var first = stage("a-" + id, 2, 100);
    var second = stage("b-" + id, 3, 110);
    return OrderState.newBuilder()
        .setOrderId(id)
        .setInstrumentUid("instrument")
        .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
        .setLotsRequested(10)
        .setLotsExecuted(5)
        .setOrderType(OrderType.ORDER_TYPE_LIMIT)
        .setInitialSecurityPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(120))
        .setExecutionReportStatus(OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL)
        .addStages(first)
        .addStages(second)
        .addStages(first)
        .build();
  }

  private static OrderStage stage(String id, long quantity, long price) {
    return OrderStage.newBuilder()
        .setTradeId(id)
        .setQuantity(quantity)
        .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(price))
        .setExecutionTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(1800000000))
        .build();
  }
}
