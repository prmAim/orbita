package ru.orbita.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.tinkoff.piapi.contract.v1.*;

class TInvestBrokerGatewayTest {
  @TempDir Path directory;

  @Test
  void lostPostResponseCausesOneExternalSendAndReconcilesByOriginalRequestId() throws Exception {
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          final List<PostOrderRequest> received = new ArrayList<>();
          LoopbackProxy proxy;
          GetOrderStateRequest lookup;

          @Override
          public void postOrder(
              PostOrderRequest request, StreamObserver<PostOrderResponse> response) {
            received.add(request);
            try {
              proxy.loseReply();
            } catch (java.io.IOException error) {
              throw new IllegalStateException("Simulator TCP reset failed", error);
            }
          }

          @Override
          public void getOrderState(
              GetOrderStateRequest request, StreamObserver<OrderState> response) {
            lookup = request;
            response.onNext(
                OrderState.newBuilder()
                    .setOrderId("broker-17")
                    .setOrderRequestId(request.getOrderId())
                    .setInstrumentUid("instrument-1")
                    .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                    .setLotsRequested(2)
                    .setLotsExecuted(1)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL)
                    .addStages(
                        OrderStage.newBuilder()
                            .setTradeId("trade-1")
                            .setQuantity(1)
                            .setExecutionTime(
                                com.google.protobuf.Timestamp.newBuilder().setSeconds(100))
                            .setPrice(
                                MoneyValue.newBuilder()
                                    .setUnits(101)
                                    .setNano(510000000)
                                    .setCurrency("rub")))
                    .build());
            response.onCompleted();
          }
        };
    try (var simulator = simulator(broker);
        var proxy = new LoopbackProxy(simulator.server.getPort());
        var gateway = gateway(proxy.port())) {
      broker.proxy = proxy;
      UUID id = UUID.randomUUID();
      var request =
          new BrokerGateway.OrderRequest(
              "mirror", id, "instrument-1", BrokerGateway.Side.BUY, 2, new BigDecimal("101.51"));
      assertThat(gateway.post(request).status()).isEqualTo(BrokerGateway.OrderStatus.UNKNOWN);
      assertThat(broker.received).hasSize(1);
      var evidence = gateway.reconcile(request);
      assertThat(evidence.status()).isEqualTo(BrokerGateway.OrderStatus.PARTIALLY_FILLED);
      assertThat(evidence.lotsExecuted()).isEqualTo(1);
      assertThat(evidence.fills().getFirst().lots()).isEqualByComparingTo("1");
      assertThat(broker.lookup.getOrderIdType()).isEqualTo(OrderIdType.ORDER_ID_TYPE_REQUEST);
      assertThat(broker.lookup.getOrderId()).isEqualTo(id.toString());
      assertThat(broker.lookup.getAccountId()).isEqualTo("mirror");
      assertThat(broker.received).hasSize(1);
      assertThat(broker.received.getFirst().getPrice())
          .isEqualTo(Quotation.newBuilder().setUnits(101).setNano(510000000).build());
    }
  }

  @Test
  void readsOwnLimitsAtExactLimitPriceButDoesNotInventCommissionQualification() throws Exception {
    var instruments =
        new InstrumentsServiceGrpc.InstrumentsServiceImplBase() {
          @Override
          public void getInstrumentBy(
              InstrumentRequest request, StreamObserver<InstrumentResponse> out) {
            out.onNext(
                InstrumentResponse.newBuilder()
                    .setInstrument(
                        ru.tinkoff.piapi.contract.v1.Instrument.newBuilder()
                            .setUid("instrument-1")
                            .setLot(10)
                            .setCurrency("cny")
                            .setBuyAvailableFlag(true)
                            .setSellAvailableFlag(true)
                            .setApiTradeAvailableFlag(true)
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }
        };
    var orders =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          GetMaxLotsRequest limits;
          GetOrderPriceRequest commission;

          @Override
          public void getMaxLots(
              GetMaxLotsRequest request, StreamObserver<GetMaxLotsResponse> out) {
            limits = request;
            out.onNext(
                GetMaxLotsResponse.newBuilder()
                    .setCurrency("cny")
                    .setBuyLimits(
                        GetMaxLotsResponse.BuyLimitsView.newBuilder()
                            .setBuyMaxLots(3)
                            .setBuyMoneyAmount(Quotation.newBuilder().setUnits(3333)))
                    .setBuyMarginLimits(
                        GetMaxLotsResponse.BuyLimitsView.newBuilder().setBuyMaxLots(999))
                    .setSellLimits(GetMaxLotsResponse.SellLimitsView.newBuilder().setSellMaxLots(2))
                    .build());
            out.onCompleted();
          }

          @Override
          public void getOrderPrice(
              GetOrderPriceRequest request, StreamObserver<GetOrderPriceResponse> out) {
            commission = request;
            out.onNext(
                GetOrderPriceResponse.newBuilder()
                    .setLotsRequested(2)
                    .setTotalOrderAmount(MoneyValue.newBuilder().setCurrency("cny").setUnits(2020))
                    .setInitialOrderAmount(
                        MoneyValue.newBuilder().setCurrency("cny").setUnits(2010))
                    .setServiceCommission(MoneyValue.newBuilder().setCurrency("cny").setUnits(5))
                    .setDealCommission(MoneyValue.newBuilder().setCurrency("cny").setUnits(5))
                    .build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(instruments, orders);
        var gateway = gateway(simulator.server.getPort())) {
      var request =
          new BrokerGateway.OrderRequest(
              "mirror",
              UUID.randomUUID(),
              "instrument-1",
              BrokerGateway.Side.BUY,
              2,
              new BigDecimal("100.50"));
      var resources = gateway.resources(request);
      assertThat(resources.ownMoney()).isEqualByComparingTo("3333");
      assertThat(resources.buyMaxLots()).isEqualTo(3);
      assertThat(resources.sellMaxLots()).isEqualTo(2);
      assertThat(resources.instrument().lotSize()).isEqualTo(10);
      assertThat(resources.qualified()).isFalse();
      assertThat(resources.preliminaryCommission()).isNull();
      assertThat(resources.additionalCommissionReserve()).isNull();
      assertThat(orders.limits.getPrice())
          .isEqualTo(Quotation.newBuilder().setUnits(100).setNano(500000000).build());
      assertThat(orders.commission.getQuantity()).isEqualTo(2);
    }
  }

  @Test
  void capitalAndAvailablePositionsPreserveBankTotalsAndDoNotSubtractBlockedTwice()
      throws Exception {
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          PortfolioRequest valuation;

          @Override
          public void getPortfolio(
              PortfolioRequest request, StreamObserver<PortfolioResponse> out) {
            valuation = request;
            out.onNext(
                PortfolioResponse.newBuilder()
                    .setAccountId(request.getAccountId())
                    .setTotalAmountPortfolio(
                        MoneyValue.newBuilder().setCurrency("rub").setUnits(1100).setNano(1))
                    .setTotalAmountCurrencies(
                        MoneyValue.newBuilder().setCurrency("rub").setUnits(100))
                    .build());
            out.onCompleted();
          }

          @Override
          public void getPositions(
              PositionsRequest request, StreamObserver<PositionsResponse> out) {
            out.onNext(
                PositionsResponse.newBuilder()
                    .setAccountId(request.getAccountId())
                    .addSecurities(
                        PositionsSecurities.newBuilder()
                            .setInstrumentUid("instrument-1")
                            .setBalance(20)
                            .setBlocked(10))
                    .build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(operations);
        var gateway = gateway(simulator.server.getPort())) {
      var capital = gateway.capital("source");
      assertThat(capital.totalRub()).isEqualByComparingTo("1100.000000001");
      assertThat(capital.compositionConfirmed()).isFalse();
      assertThat(operations.valuation.getCurrency())
          .isEqualTo(PortfolioRequest.CurrencyRequest.RUB);
      var positions = gateway.positions("mirror");
      assertThat(positions.complete()).isTrue();
      assertThat(positions.positions().getFirst().availablePieces()).isEqualByComparingTo("20");
      assertThat(positions.positions().getFirst().blockedPieces()).isEqualByComparingTo("10");
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "1800000000,source",
    "0,source",
    "1800000000,mirror"
  })
  void historyReadsEveryPageDeduplicatesOverlapAndReportsMissingExecutionTime(
      long seconds, String account) throws Exception {
    var at = java.time.Instant.ofEpochSecond(seconds);
    var instruments =
        new InstrumentsServiceGrpc.InstrumentsServiceImplBase() {
          @Override
          public void getInstrumentBy(
              InstrumentRequest request, StreamObserver<InstrumentResponse> out) {
            out.onNext(
                InstrumentResponse.newBuilder()
                    .setInstrument(
                        ru.tinkoff.piapi.contract.v1.Instrument.newBuilder()
                            .setUid("instrument-1")
                            .setLot(10)
                            .setCurrency("rub")
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          final List<String> cursors = new ArrayList<>();

          @Override
          public void getOperationsByCursor(
              GetOperationsByCursorRequest request,
              StreamObserver<GetOperationsByCursorResponse> out) {
            cursors.add(request.getCursor());
            var trade =
                OperationItemTrade.newBuilder()
                    .setNum("trade-1")
                    .setQuantity(20)
                    .setDate(
                        com.google.protobuf.Timestamp.newBuilder().setSeconds(at.getEpochSecond()))
                    .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(100));
            var page =
                GetOperationsByCursorResponse.newBuilder()
                    .addItems(
                        OperationItem.newBuilder()
                            .setBrokerAccountId(account)
                            .setInstrumentUid("instrument-1")
                            .setType(OperationType.OPERATION_TYPE_BUY)
                            .setTradesInfo(OperationItemTrades.newBuilder().addTrades(trade)));
            if (request.getCursor().isEmpty()) page.setHasNext(true).setNextCursor("page-2");
            else
              page.addItems(
                  OperationItem.newBuilder()
                      .setBrokerAccountId(account)
                      .setInstrumentUid("instrument-1")
                      .setType(OperationType.OPERATION_TYPE_SELL)
                      .setTradesInfo(
                          OperationItemTrades.newBuilder()
                              .addTrades(trade.setNum("missing-time").clearDate())));
            out.onNext(page.build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(instruments, operations);
        var gateway = gateway(simulator.server.getPort())) {
      var history =
          account.equals("source")
              ? gateway.history(at.minusSeconds(1), at.plusSeconds(1))
              : gateway.mirrorHistory(at.minusSeconds(1), at.plusSeconds(1));
      assertThat(history.executions()).hasSize(1);
      assertThat(history.executions().getFirst().tradeId()).isEqualTo("trade-1");
      assertThat(history.executions().getFirst().lots()).isEqualByComparingTo("2");
      assertThat(history.executions().getFirst().executedAt()).isEqualTo(at);
      assertThat(history.gaps()).anyMatch(gap -> gap.contains("missing-time"));
      assertThat(operations.cursors).containsExactly("", "page-2");
      assertThat(
              account.equals("source")
                  ? gateway.history(at.minusSeconds(1), at.plusSeconds(1)).executions()
                  : gateway.mirrorHistory(at.minusSeconds(1), at.plusSeconds(1)).executions())
          .hasSize(1);
    }
  }

  @Test
  void mirrorHistoryRetainsDuplicateFillAcrossPagesWithDifferentOperationIds() throws Exception {
    var at = java.time.Instant.now().minusSeconds(10);
    var instruments =
        new InstrumentsServiceGrpc.InstrumentsServiceImplBase() {
          @Override
          public void getInstrumentBy(
              InstrumentRequest request, StreamObserver<InstrumentResponse> out) {
            out.onNext(
                InstrumentResponse.newBuilder()
                    .setInstrument(
                        ru.tinkoff.piapi.contract.v1.Instrument.newBuilder()
                            .setUid("instrument-1")
                            .setLot(10)
                            .setCurrency("rub")
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          @Override
          public void getOperationsByCursor(
              GetOperationsByCursorRequest request,
              StreamObserver<GetOperationsByCursorResponse> out) {
            var trade =
                OperationItemTrade.newBuilder()
                    .setNum("mirror-trade-1")
                    .setQuantity(20)
                    .setDate(
                        com.google.protobuf.Timestamp.newBuilder()
                            .setSeconds(at.getEpochSecond())
                            .setNanos(at.getNano()))
                    .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(100));
            var operation =
                OperationItem.newBuilder()
                    .setId(
                        request.getCursor().isEmpty() ? "mirror-operation-1" : "mirror-operation-2")
                    .setBrokerAccountId("mirror")
                    .setInstrumentUid("instrument-1")
                    .setType(OperationType.OPERATION_TYPE_BUY)
                    .setTradesInfo(OperationItemTrades.newBuilder().addTrades(trade));
            var page = GetOperationsByCursorResponse.newBuilder().addItems(operation);
            if (request.getCursor().isEmpty()) page.setHasNext(true).setNextCursor("page-2");
            out.onNext(page.build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(instruments, operations);
        var gateway = gateway(simulator.server.getPort())) {
      var history = gateway.mirrorHistory(at.minusSeconds(1), at.plusSeconds(1));

      assertThat(history.executions())
          .singleElement()
          .satisfies(
              execution -> {
                assertThat(execution.tradeId()).isEqualTo("mirror-trade-1");
                assertThat(execution.lots()).isEqualByComparingTo("2");
                assertThat(execution.executedAt()).isEqualTo(at);
              });
      assertThat(history.gaps())
          .noneMatch(
              gap ->
                  gap.contains("Conflicting source operation identity")
                      || gap.contains("Conflicting history trade identity"));
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({"true,false", "false,false", "true,true"})
  void reconnectRequiresAcknowledgementAndQualifiedOriginalTimestampRecoveryWindow(
      boolean qualifiedTime, boolean delayedProvider) throws Exception {
    var at = java.time.Instant.now().minusSeconds(10);
    var arrivalUtc = java.time.Instant.now();
    var remembered =
        new java.util.concurrent.ConcurrentHashMap<String, BrokerGateway.SourceExecution>();
    var accepted =
        new java.util.concurrent.ConcurrentHashMap<String, BrokerGateway.SourceExecution>();
    var delivered = new java.util.concurrent.LinkedBlockingQueue<String>();
    var ready = new java.util.concurrent.LinkedBlockingQueue<Boolean>();
    var skipped = new java.util.concurrent.LinkedBlockingQueue<String>();
    var terminalTradeIds = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
    var unqualifiedWindow = new java.util.concurrent.CountDownLatch(1);
    var stream =
        new OrdersStreamServiceGrpc.OrdersStreamServiceImplBase() {
          volatile StreamObserver<TradesStreamResponse> live;
          final java.util.concurrent.LinkedBlockingQueue<Integer> subscriptions =
              new java.util.concurrent.LinkedBlockingQueue<>();
          int count;

          @Override
          public void tradesStream(
              TradesStreamRequest request, StreamObserver<TradesStreamResponse> out) {
            live = out;
            out.onNext(
                TradesStreamResponse.newBuilder()
                    .setSubscription(
                        SubscriptionResponse.newBuilder()
                            .setStatus(ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK)
                            .addAccounts("source"))
                    .build());
            subscriptions.add(++count);
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
                            .setUid("instrument-1")
                            .setLot(10)
                            .setCurrency("rub")
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          volatile boolean historyVisible;

          @Override
          public void getOperationsByCursor(
              GetOperationsByCursorRequest request,
              StreamObserver<GetOperationsByCursorResponse> out) {
            var page = GetOperationsByCursorResponse.newBuilder();
            if (historyVisible)
              page.addItems(
                  OperationItem.newBuilder()
                      .setId(
                          qualifiedTime && !delayedProvider
                              ? "unstable-operation-17"
                              : "source-order-1")
                      .setBrokerAccountId("source")
                      .setInstrumentUid("instrument-1")
                      .setType(OperationType.OPERATION_TYPE_BUY)
                      .setTradesInfo(
                          OperationItemTrades.newBuilder()
                              .addTrades(
                                  OperationItemTrade.newBuilder()
                                      .setNum("trade-1")
                                      .setQuantity(20)
                                      .setPrice(
                                          MoneyValue.newBuilder().setCurrency("rub").setUnits(100))
                                      .setDate(TInvestBrokerGateway.timestamp(at)))));
            out.onNext(page.build());
            out.onCompleted();
          }
        };
    var journal =
        new SourceFeedJournal() {
          final java.util.Map<String, java.time.Instant> receipts =
              new java.util.concurrent.ConcurrentHashMap<>();
          final java.util.Map<String, TimeWindow> receiptWindows =
              new java.util.concurrent.ConcurrentHashMap<>();

          public java.time.Instant observationBoundary() {
            return at.minusSeconds(1);
          }

          public java.util.Optional<String> sourceOrderId(String account, String trade) {
            return java.util.Optional.ofNullable(remembered.get(trade))
                .map(BrokerGateway.SourceExecution::sourceOrderId);
          }

          public java.util.Optional<BrokerGateway.SourceExecution> remembered(
              String account, String trade) {
            return java.util.Optional.ofNullable(remembered.get(trade));
          }

          public java.util.List<BrokerGateway.SourceExecution> pendingTimestampRecovery() {
            return remembered.values().stream()
                .filter(e -> e.executedAt() == null)
                .filter(e -> !terminalTradeIds.contains(e.tradeId()))
                .toList();
          }

          public java.time.Instant firstReceived(
              String account, String trade, java.time.Instant received) {
            return receipts.computeIfAbsent(trade, _ -> received);
          }

          public java.time.Instant firstReceived(
              String account, String trade, java.time.Instant received, TimeWindow captured) {
            if (captured != null) receiptWindows.putIfAbsent(trade, captured);
            return firstReceived(account, trade, received);
          }

          public java.util.Optional<TimeWindow> receiptTimeWindow(String account, String trade) {
            return java.util.Optional.ofNullable(receiptWindows.get(trade));
          }

          public java.util.Optional<TimeWindow> currentTimeWindow() {
            if (!qualifiedTime) return java.util.Optional.empty();
            var now =
                delayedProvider
                    ? arrivalUtc.plusSeconds(operations.historyVisible ? 65 : 10)
                    : java.time.Instant.now();
            return java.util.Optional.of(new TimeWindow(now.minusMillis(1), now.plusMillis(1)));
          }

          public java.util.Optional<TimeWindow> arrivalTimeWindow(
              long arrivedNanos, String domain) {
            assertThat(domain).isEqualTo(BrokerReceiptClock.domain());
            assertThat(arrivedNanos).isLessThanOrEqualTo(BrokerReceiptClock.nanoTime());
            if (!qualifiedTime) return java.util.Optional.empty();
            if (delayedProvider)
              return java.util.Optional.of(
                  new TimeWindow(arrivalUtc.minusMillis(1), arrivalUtc.plusMillis(1)));
            return currentTimeWindow();
          }

          public void remember(BrokerGateway.SourceExecution execution) {
            remembered.put(execution.tradeId(), execution);
            delivered.add("remembered");
          }

          public void accept(BrokerGateway.SourceExecution execution) {
            accepted.putIfAbsent(execution.tradeId(), execution);
            remembered.put(execution.tradeId(), execution);
            delivered.add("accepted");
          }

          public void skip(BrokerGateway.SourceExecution execution, String reason) {
            if (delayedProvider) {
              terminalTradeIds.add(execution.tradeId());
              skipped.add(execution.tradeId());
              remembered.put(execution.tradeId(), execution);
            } else throw new AssertionError(reason);
          }

          public void gap(String reason) {
            if (reason.contains("window cannot be established")) unqualifiedWindow.countDown();
          }

          public void recovered() {
            ready.add(true);
          }
        };
    try (var simulator = simulator(stream, instruments, operations);
        var gateway = gateway(simulator.server.getPort());
        var feed = gateway.observe(journal)) {
      assertThat(stream.subscriptions.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(1);
      assertThat(ready.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      stream.live.onNext(
          TradesStreamResponse.newBuilder()
              .setOrderTrades(
                  OrderTrades.newBuilder()
                      .setAccountId("source")
                      .setOrderId("source-order-1")
                      .setInstrumentUid("instrument-1")
                      .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                      .addTrades(
                          OrderTrade.newBuilder()
                              .setTradeId("trade-1")
                              .setQuantity(20)
                              .setPrice(Quotation.newBuilder().setUnits(100))))
              .build());
      assertThat(delivered.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("remembered");
      assertThat(accepted).isEmpty();
      operations.historyVisible = true;
      stream.live.onError(Status.UNAVAILABLE.asRuntimeException());
      assertThat(stream.subscriptions.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(2);
      if (delayedProvider) {
        assertThat(skipped.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("trade-1");
        assertThat(accepted).isEmpty();
      } else if (qualifiedTime) {
        assertThat(ready.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(accepted).hasSize(1);
        assertThat(accepted.get("trade-1").executedAt()).isEqualTo(at);
        assertThat(accepted.get("trade-1").lots()).isEqualByComparingTo("2");
        assertThat(accepted.get("trade-1").sourceOrderId()).isEqualTo("source-order-1");
      } else {
        assertThat(unqualifiedWindow.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(accepted).isEmpty();
      }
      if (delayedProvider) {
        assertThat(skipped).isEmpty();
        assertThat(remembered).containsKey("trade-1");
        assertThat(remembered.get("trade-1").sourceOrderId()).isEqualTo("source-order-1");
      }
    }
  }

  @Test
  void reconnectRecoversRecentExecutionCreatedDuringGapFromCanonicalOrderStages() throws Exception {
    observeGapWithCanonicalStages(HistoryScenario.SUCCESS);
  }

  @Test
  void twoOrdersClaimingOneCanonicalStageStayInGapWithoutAdmittingEitherTrade() throws Exception {
    observeGapWithCanonicalStages(HistoryScenario.CONFLICTING_ORDERS);
  }

  @Test
  void operationsPageFailureDuringOrderStateRetryRecoversOnTheSameSubscription() throws Exception {
    observeGapWithCanonicalStages(HistoryScenario.TRANSIENT_PAGE_FAILURE);
  }

  @Test
  void pendingTimestampExpiresWhileOrderStateRetriesRespectBackoff() throws Exception {
    observeGapWithCanonicalStages(HistoryScenario.PENDING_TIMESTAMP);
  }

  private enum HistoryScenario {
    SUCCESS,
    CONFLICTING_ORDERS,
    TRANSIENT_PAGE_FAILURE,
    PENDING_TIMESTAMP
  }

  private void observeGapWithCanonicalStages(HistoryScenario scenario) throws Exception {
    var nextFrameRead = new java.util.concurrent.CountDownLatch(1);
    var at = new java.util.concurrent.atomic.AtomicReference<java.time.Instant>();
    var boundary = java.time.Instant.now().minusSeconds(60);
    var operationsVisible = new java.util.concurrent.atomic.AtomicBoolean();
    var disconnectAt = new java.util.concurrent.atomic.AtomicReference<java.time.Instant>();
    var reconnectAt = new java.util.concurrent.atomic.AtomicReference<java.time.Instant>();
    var accepted = new java.util.concurrent.LinkedBlockingQueue<BrokerGateway.SourceExecution>();
    var gaps = new java.util.concurrent.LinkedBlockingQueue<String>();
    var recovered = new java.util.concurrent.LinkedBlockingQueue<Boolean>();
    var stateRequests = new java.util.concurrent.LinkedBlockingQueue<GetOrderStateRequest>();
    var stateAttempts = new java.util.concurrent.LinkedBlockingQueue<Long>();
    var pendingWindowChecked = new java.util.concurrent.CountDownLatch(1);
    var expirePending = new java.util.concurrent.atomic.AtomicBoolean();
    var skipped = new java.util.concurrent.LinkedBlockingQueue<String>();
    var stream =
        new OrdersStreamServiceGrpc.OrdersStreamServiceImplBase() {
          volatile StreamObserver<TradesStreamResponse> live;
          final java.util.concurrent.LinkedBlockingQueue<Integer> subscriptions =
              new java.util.concurrent.LinkedBlockingQueue<>();
          int count;

          @Override
          public void tradesStream(
              TradesStreamRequest request, StreamObserver<TradesStreamResponse> out) {
            live = out;
            out.onNext(
                TradesStreamResponse.newBuilder()
                    .setSubscription(
                        SubscriptionResponse.newBuilder()
                            .setStatus(ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK)
                            .addAccounts("source"))
                    .build());
            int subscription = ++count;
            if (subscription == 2) reconnectAt.set(java.time.Instant.now());
            subscriptions.add(subscription);
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
                            .setUid("instrument-1")
                            .setLot(10)
                            .setCurrency("rub")
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          int visibleReads;

          @Override
          public void getOperationsByCursor(
              GetOperationsByCursorRequest request,
              StreamObserver<GetOperationsByCursorResponse> out) {
            var page = GetOperationsByCursorResponse.newBuilder();
            if (operationsVisible.get()) {
              if (++visibleReads == 2 && scenario == HistoryScenario.TRANSIENT_PAGE_FAILURE) {
                out.onError(Status.UNAVAILABLE.asRuntimeException());
                return;
              }
              var item =
                  OperationItem.newBuilder()
                      .setId("source-order-17")
                      .setBrokerAccountId("source")
                      .setInstrumentUid("instrument-1")
                      .setType(OperationType.OPERATION_TYPE_BUY)
                      .setTradesInfo(
                          OperationItemTrades.newBuilder()
                              .addTrades(
                                  OperationItemTrade.newBuilder()
                                      .setNum("operation-trade-17")
                                      .setQuantity(30)
                                      .setDate(TInvestBrokerGateway.timestamp(at.get()))
                                      .setPrice(
                                          MoneyValue.newBuilder()
                                              .setCurrency("rub")
                                              .setUnits(101)
                                              .setNano(500000000))));
              page.addItems(item);
              if (scenario == HistoryScenario.CONFLICTING_ORDERS) {
                item.setId("source-order-18");
                item.getTradesInfoBuilder().getTradesBuilder(0).setNum("operation-trade-18");
                page.addItems(item);
              }
            }
            out.onNext(page.build());
            out.onCompleted();
          }
        };
    var orders =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          int attempts;

          @Override
          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            stateRequests.add(request);
            stateAttempts.add(System.nanoTime());
            attempts++;
            if ((scenario == HistoryScenario.TRANSIENT_PAGE_FAILURE && attempts == 1)
                || (scenario == HistoryScenario.PENDING_TIMESTAMP && attempts <= 2)) {
              out.onError(Status.UNAVAILABLE.asRuntimeException());
              return;
            }
            out.onNext(
                OrderState.newBuilder()
                    .setOrderId(request.getOrderId())
                    .setInstrumentUid("instrument-1")
                    .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                    .setLotsRequested(3)
                    .setLotsExecuted(3)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL)
                    .addStages(
                        OrderStage.newBuilder()
                            .setTradeId("canonical-trade-17")
                            .setQuantity(3)
                            .setExecutionTime(TInvestBrokerGateway.timestamp(at.get()))
                            .setPrice(
                                MoneyValue.newBuilder()
                                    .setCurrency("rub")
                                    .setUnits(101)
                                    .setNano(500000000)))
                    .build());
            out.onCompleted();
          }
        };
    var journal =
        new SourceFeedJournal() {
          public java.time.Instant observationBoundary() {
            return boundary;
          }

          public java.util.Optional<String> sourceOrderId(String account, String trade) {
            return accepted.stream()
                .filter(execution -> execution.accountId().equals(account))
                .filter(execution -> execution.tradeId().equals(trade))
                .map(BrokerGateway.SourceExecution::sourceOrderId)
                .findFirst();
          }

          public java.util.Optional<BrokerGateway.SourceExecution> remembered(
              String account, String trade) {
            return accepted.stream()
                .filter(execution -> execution.accountId().equals(account))
                .filter(execution -> execution.tradeId().equals(trade))
                .findFirst();
          }

          public java.util.List<BrokerGateway.SourceExecution> pendingTimestampRecovery() {
            return List.of();
          }

          public java.util.Optional<TimeWindow> receiptTimeWindow(String account, String trade) {
            return java.util.Optional.of(new TimeWindow(boundary, boundary));
          }

          public java.util.Optional<TimeWindow> currentTimeWindow() {
            var now = boundary.plusSeconds(expirePending.get() ? 61 : 1);
            pendingWindowChecked.countDown();
            return java.util.Optional.of(new TimeWindow(now, now));
          }

          public java.time.Instant firstReceived(
              String account, String trade, java.time.Instant received) {
            return received;
          }

          public void remember(BrokerGateway.SourceExecution execution) {}

          public void readSucceeded() {
            nextFrameRead.countDown();
          }

          public void accept(BrokerGateway.SourceExecution execution) {
            accepted.add(execution);
          }

          public void skip(BrokerGateway.SourceExecution execution, String reason) {
            skipped.add(execution.tradeId());
          }

          public void gap(String reason) {
            gaps.add(reason);
          }

          public void recovered() {
            recovered.add(true);
          }
        };

    try (var simulator = simulator(stream, instruments, operations, orders);
        var gateway = gateway(simulator.server.getPort());
        var feed = gateway.observe(journal)) {
      assertThat(stream.subscriptions.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(1);
      assertThat(recovered.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      gaps.clear();

      disconnectAt.set(java.time.Instant.now());
      stream.live.onError(Status.UNAVAILABLE.asRuntimeException());
      assertThat(gaps.poll(10, java.util.concurrent.TimeUnit.SECONDS))
          .isEqualTo("Source stream disconnected");
      at.set(disconnectAt.get().plusMillis(100));
      operationsVisible.set(true);

      assertThat(stream.subscriptions.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(2);
      assertThat(at.get()).isAfter(disconnectAt.get()).isBefore(reconnectAt.get());
      if (scenario == HistoryScenario.CONFLICTING_ORDERS) {
        assertThat(gaps.poll(10, java.util.concurrent.TimeUnit.SECONDS))
            .isEqualTo("Source subscription awaiting acknowledgement and history recovery");
        assertThat(gaps.poll(10, java.util.concurrent.TimeUnit.SECONDS))
            .isEqualTo("Ambiguous source history trade identity canonical-trade-17");
        assertThat(stateRequests)
            .extracting(GetOrderStateRequest::getOrderId)
            .containsExactlyInAnyOrder("source-order-17", "source-order-18");
        // The feed processes this empty frame after the complete history batch.
        stream.live.onNext(
            TradesStreamResponse.newBuilder()
                .setOrderTrades(
                    OrderTrades.newBuilder()
                        .setAccountId("source")
                        .setOrderId("marker-order")
                        .setInstrumentUid("instrument-1")
                        .setDirection(OrderDirection.ORDER_DIRECTION_BUY))
                .build());
        assertThat(nextFrameRead.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(accepted).isEmpty();
        assertThat(recovered).isEmpty();
        return;
      }
      if (scenario == HistoryScenario.PENDING_TIMESTAMP) {
        assertThat(stateAttempts.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isNotNull();
        var secondAttempt = stateAttempts.poll(10, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(secondAttempt).isNotNull();
        stream.live.onNext(
            TradesStreamResponse.newBuilder()
                .setOrderTrades(
                    OrderTrades.newBuilder()
                        .setAccountId("source")
                        .setOrderId("other-order")
                        .setInstrumentUid("instrument-1")
                        .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                        .addTrades(
                            OrderTrade.newBuilder()
                                .setTradeId("missing-timestamp")
                                .setQuantity(10)
                                .setPrice(Quotation.newBuilder().setUnits(1))))
                .build());
        assertThat(pendingWindowChecked.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        expirePending.set(true);
        assertThat(skipped.poll(5, java.util.concurrent.TimeUnit.SECONDS))
            .isEqualTo("missing-timestamp");
        var thirdAttempt = stateAttempts.poll(5, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(thirdAttempt).isNotNull();
        assertThat(thirdAttempt - secondAttempt)
            .isGreaterThanOrEqualTo(java.util.concurrent.TimeUnit.SECONDS.toNanos(2));
      }
      var execution = accepted.poll(10, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(execution).isNotNull();
      assertThat(execution.accountId()).isEqualTo("source");
      assertThat(execution.tradeId()).isEqualTo("canonical-trade-17");
      assertThat(execution.sourceOrderId()).isEqualTo("source-order-17");
      assertThat(execution.instrumentId()).isEqualTo("instrument-1");
      assertThat(execution.side()).isEqualTo(BrokerGateway.Side.BUY);
      assertThat(execution.lots()).isEqualByComparingTo("3");
      assertThat(execution.price()).isEqualByComparingTo("101.50");
      assertThat(execution.currency()).isEqualTo("rub");
      assertThat(execution.executedAt()).isEqualTo(at.get());
      var lookup = stateRequests.poll(10, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(lookup).isNotNull();
      assertThat(lookup.getAccountId()).isEqualTo("source");
      assertThat(lookup.getOrderId()).isEqualTo("source-order-17");
      assertThat(recovered.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      assertThat(accepted).isEmpty();
      assertThat(stream.subscriptions).isEmpty();
      if (scenario == HistoryScenario.TRANSIENT_PAGE_FAILURE) {
        assertThat(operations.visibleReads).isEqualTo(3);
        assertThat(orders.attempts).isEqualTo(2);
        assertThat(gaps).contains("History page unavailable");
      }
    }
  }

  @Test
  void reconnectRecoversOnlyMissingStageWhenSiblingStageIsAlreadyAccepted() throws Exception {
    var knownAt = java.time.Instant.now().minusSeconds(61);
    var boundary = knownAt.minusSeconds(10);
    var operationsVisible = new java.util.concurrent.atomic.AtomicBoolean();
    var missingAt = new java.util.concurrent.atomic.AtomicReference<java.time.Instant>();
    var disconnectAt = new java.util.concurrent.atomic.AtomicReference<java.time.Instant>();
    var reconnectAt = new java.util.concurrent.atomic.AtomicReference<java.time.Instant>();
    var accepted =
        new java.util.concurrent.ConcurrentHashMap<String, BrokerGateway.SourceExecution>();
    var remembered =
        new java.util.concurrent.ConcurrentHashMap<String, BrokerGateway.SourceExecution>();
    var admissions = new java.util.concurrent.LinkedBlockingQueue<BrokerGateway.SourceExecution>();
    var gaps = new java.util.concurrent.LinkedBlockingQueue<String>();
    var recovered = new java.util.concurrent.LinkedBlockingQueue<Boolean>();
    var stateRequests = new java.util.concurrent.LinkedBlockingQueue<GetOrderStateRequest>();
    var stream =
        new OrdersStreamServiceGrpc.OrdersStreamServiceImplBase() {
          volatile StreamObserver<TradesStreamResponse> live;
          final java.util.concurrent.LinkedBlockingQueue<Integer> subscriptions =
              new java.util.concurrent.LinkedBlockingQueue<>();
          int count;

          @Override
          public void tradesStream(
              TradesStreamRequest request, StreamObserver<TradesStreamResponse> out) {
            live = out;
            if (++count == 2) reconnectAt.set(java.time.Instant.now());
            out.onNext(
                TradesStreamResponse.newBuilder()
                    .setSubscription(
                        SubscriptionResponse.newBuilder()
                            .setStatus(ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK)
                            .addAccounts("source"))
                    .build());
            subscriptions.add(count);
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
                            .setUid("instrument-1")
                            .setLot(10)
                            .setCurrency("rub")
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          @Override
          public void getOperationsByCursor(
              GetOperationsByCursorRequest request,
              StreamObserver<GetOperationsByCursorResponse> out) {
            var page = GetOperationsByCursorResponse.newBuilder();
            if (operationsVisible.get())
              page.addItems(
                  OperationItem.newBuilder()
                      .setId("source-order-partial")
                      .setBrokerAccountId("source")
                      .setInstrumentUid("instrument-1")
                      .setType(OperationType.OPERATION_TYPE_BUY)
                      .setTradesInfo(
                          OperationItemTrades.newBuilder()
                              .addTrades(
                                  OperationItemTrade.newBuilder()
                                      .setNum("canonical-trade-known")
                                      .setQuantity(20)
                                      .setDate(TInvestBrokerGateway.timestamp(knownAt))
                                      .setPrice(
                                          MoneyValue.newBuilder().setCurrency("rub").setUnits(100)))
                              .addTrades(
                                  OperationItemTrade.newBuilder()
                                      .setNum("operation-trade-missing")
                                      .setQuantity(30)
                                      .setDate(TInvestBrokerGateway.timestamp(missingAt.get()))
                                      .setPrice(
                                          MoneyValue.newBuilder()
                                              .setCurrency("rub")
                                              .setUnits(101))))
                      .build());
            out.onNext(page.build());
            out.onCompleted();
          }
        };
    var orders =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          @Override
          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            stateRequests.add(request);
            out.onNext(
                OrderState.newBuilder()
                    .setOrderId("source-order-partial")
                    .setInstrumentUid("instrument-1")
                    .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                    .setLotsRequested(5)
                    .setLotsExecuted(5)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL)
                    .addStages(
                        OrderStage.newBuilder()
                            .setTradeId("canonical-trade-known")
                            .setQuantity(2)
                            .setExecutionTime(TInvestBrokerGateway.timestamp(knownAt))
                            .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(100)))
                    .addStages(
                        OrderStage.newBuilder()
                            .setTradeId("canonical-trade-missing")
                            .setQuantity(3)
                            .setExecutionTime(TInvestBrokerGateway.timestamp(missingAt.get()))
                            .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(101)))
                    .build());
            out.onCompleted();
          }
        };
    var journal =
        new SourceFeedJournal() {
          public java.time.Instant observationBoundary() {
            return boundary;
          }

          public java.util.Optional<String> sourceOrderId(String account, String trade) {
            return java.util.Optional.ofNullable(accepted.get(trade))
                .map(BrokerGateway.SourceExecution::sourceOrderId);
          }

          public java.util.Optional<BrokerGateway.SourceExecution> remembered(
              String account, String trade) {
            var saved = accepted.get(trade);
            return java.util.Optional.ofNullable(saved == null ? remembered.get(trade) : saved);
          }

          public java.util.List<BrokerGateway.SourceExecution> pendingTimestampRecovery() {
            return List.of();
          }

          public java.time.Instant firstReceived(
              String account, String trade, java.time.Instant received) {
            return received;
          }

          public void remember(BrokerGateway.SourceExecution execution) {
            remembered.putIfAbsent(execution.tradeId(), execution);
          }

          public void accept(BrokerGateway.SourceExecution execution) {
            remembered.put(execution.tradeId(), execution);
            if (accepted.putIfAbsent(execution.tradeId(), execution) == null)
              admissions.add(execution);
          }

          public void acceptRecovered(BrokerGateway.SourceExecution execution) {
            accept(execution);
          }

          public void skip(BrokerGateway.SourceExecution execution, String reason) {
            throw new AssertionError(reason);
          }

          public void gap(String reason) {
            gaps.add(reason);
          }

          public void recovered() {
            recovered.add(true);
          }
        };

    try (var simulator = simulator(stream, instruments, operations, orders);
        var gateway = gateway(simulator.server.getPort());
        var feed = gateway.observe(journal)) {
      assertThat(stream.subscriptions.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(1);
      assertThat(recovered.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      var knownStreamExecution =
          OrderTrades.newBuilder()
              .setAccountId("source")
              .setOrderId("source-order-partial")
              .setInstrumentUid("instrument-1")
              .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
              .addTrades(
                  OrderTrade.newBuilder()
                      .setTradeId("canonical-trade-known")
                      .setQuantity(20)
                      .setPrice(Quotation.newBuilder().setUnits(100))
                      .setDateTime(TInvestBrokerGateway.timestamp(knownAt)))
              .build();
      stream.live.onNext(
          TradesStreamResponse.newBuilder().setOrderTrades(knownStreamExecution).build());
      assertThat(admissions.poll(10, java.util.concurrent.TimeUnit.SECONDS))
          .extracting(BrokerGateway.SourceExecution::tradeId)
          .isEqualTo("canonical-trade-known");
      gaps.clear();

      disconnectAt.set(java.time.Instant.now());
      stream.live.onError(Status.UNAVAILABLE.asRuntimeException());
      assertThat(gaps.poll(10, java.util.concurrent.TimeUnit.SECONDS))
          .isEqualTo("Source stream disconnected");
      missingAt.set(disconnectAt.get().plusMillis(1));
      operationsVisible.set(true);

      assertThat(stream.subscriptions.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(2);
      assertThat(disconnectAt.get()).isBefore(missingAt.get());
      assertThat(missingAt.get()).isBefore(reconnectAt.get());
      var recoveredStage = admissions.poll(5, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(recoveredStage).isNotNull();
      assertThat(recoveredStage.tradeId()).isEqualTo("canonical-trade-missing");
      assertThat(recoveredStage.sourceOrderId()).isEqualTo("source-order-partial");
      assertThat(accepted.keySet())
          .containsExactlyInAnyOrder("canonical-trade-known", "canonical-trade-missing");
      assertThat(admissions).isEmpty();
      assertThat(gaps)
          .doesNotContain(
              "GetOrderState source history identity/count mismatch: source-order-partial",
              "Ambiguous or conflicting GetOrderState source history: source-order-partial");
      assertThat(recovered.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      assertThat(stateRequests)
          .singleElement()
          .satisfies(
              request -> {
                assertThat(request.getAccountId()).isEqualTo("source");
                assertThat(request.getOrderId()).isEqualTo("source-order-partial");
              });
    }
  }

  @Test
  void reconnectKeepsAmbiguousRecentHistoryInGapWithoutAdmittingExecutions() throws Exception {
    var at = java.time.Instant.now().minusSeconds(5);
    var operationsVisible = new java.util.concurrent.atomic.AtomicBoolean();
    var accepted = new java.util.concurrent.LinkedBlockingQueue<BrokerGateway.SourceExecution>();
    var remembered = new java.util.concurrent.LinkedBlockingQueue<BrokerGateway.SourceExecution>();
    var gaps = new java.util.concurrent.LinkedBlockingQueue<String>();
    var recovered = new java.util.concurrent.LinkedBlockingQueue<Boolean>();
    var stateRequests = new java.util.concurrent.LinkedBlockingQueue<GetOrderStateRequest>();
    var stream =
        new OrdersStreamServiceGrpc.OrdersStreamServiceImplBase() {
          volatile StreamObserver<TradesStreamResponse> live;
          final java.util.concurrent.LinkedBlockingQueue<Integer> subscriptions =
              new java.util.concurrent.LinkedBlockingQueue<>();
          int count;

          @Override
          public void tradesStream(
              TradesStreamRequest request, StreamObserver<TradesStreamResponse> out) {
            live = out;
            out.onNext(
                TradesStreamResponse.newBuilder()
                    .setSubscription(
                        SubscriptionResponse.newBuilder()
                            .setStatus(ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK)
                            .addAccounts("source"))
                    .build());
            subscriptions.add(++count);
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
                            .setUid("instrument-1")
                            .setLot(10)
                            .setCurrency("rub")
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          @Override
          public void getOperationsByCursor(
              GetOperationsByCursorRequest request,
              StreamObserver<GetOperationsByCursorResponse> out) {
            var page = GetOperationsByCursorResponse.newBuilder();
            if (operationsVisible.get())
              page.addItems(
                  OperationItem.newBuilder()
                      .setId("source-order-ambiguous")
                      .setBrokerAccountId("source")
                      .setInstrumentUid("instrument-1")
                      .setType(OperationType.OPERATION_TYPE_BUY)
                      .setTradesInfo(
                          OperationItemTrades.newBuilder()
                              .addTrades(
                                  OperationItemTrade.newBuilder()
                                      .setNum("operation-trade-a")
                                      .setQuantity(30)
                                      .setDate(TInvestBrokerGateway.timestamp(at))
                                      .setPrice(
                                          MoneyValue.newBuilder()
                                              .setCurrency("rub")
                                              .setUnits(101)
                                              .setNano(500000000)))
                              .addTrades(
                                  OperationItemTrade.newBuilder()
                                      .setNum("operation-trade-b")
                                      .setQuantity(30)
                                      .setDate(TInvestBrokerGateway.timestamp(at))
                                      .setPrice(
                                          MoneyValue.newBuilder()
                                              .setCurrency("rub")
                                              .setUnits(101)
                                              .setNano(500000000)))));
            out.onNext(page.build());
            out.onCompleted();
          }
        };
    var orders =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          @Override
          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            stateRequests.add(request);
            out.onNext(
                OrderState.newBuilder()
                    .setOrderId("source-order-ambiguous")
                    .setInstrumentUid("instrument-1")
                    .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                    .setLotsRequested(6)
                    .setLotsExecuted(6)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL)
                    .addStages(
                        OrderStage.newBuilder()
                            .setTradeId("canonical-trade-a")
                            .setQuantity(3)
                            .setExecutionTime(TInvestBrokerGateway.timestamp(at))
                            .setPrice(
                                MoneyValue.newBuilder()
                                    .setCurrency("rub")
                                    .setUnits(101)
                                    .setNano(500000000)))
                    .addStages(
                        OrderStage.newBuilder()
                            .setTradeId("canonical-trade-b")
                            .setQuantity(3)
                            .setExecutionTime(TInvestBrokerGateway.timestamp(at))
                            .setPrice(
                                MoneyValue.newBuilder()
                                    .setCurrency("rub")
                                    .setUnits(101)
                                    .setNano(500000000)))
                    .build());
            out.onCompleted();
          }
        };
    var journal =
        new SourceFeedJournal() {
          public java.time.Instant observationBoundary() {
            return at.minusSeconds(20);
          }

          public java.util.Optional<String> sourceOrderId(String account, String trade) {
            return java.util.Optional.empty();
          }

          public java.util.Optional<BrokerGateway.SourceExecution> remembered(
              String account, String trade) {
            return java.util.Optional.empty();
          }

          public java.util.List<BrokerGateway.SourceExecution> pendingTimestampRecovery() {
            return List.of();
          }

          public java.time.Instant firstReceived(
              String account, String trade, java.time.Instant received) {
            return received;
          }

          public void remember(BrokerGateway.SourceExecution execution) {
            remembered.add(execution);
          }

          public void accept(BrokerGateway.SourceExecution execution) {
            accepted.add(execution);
          }

          public void acceptRecovered(BrokerGateway.SourceExecution execution) {
            accepted.add(execution);
          }

          public void skip(BrokerGateway.SourceExecution execution, String reason) {
            throw new AssertionError(reason);
          }

          public void gap(String reason) {
            gaps.add(reason);
          }

          public void recovered() {
            recovered.add(true);
          }
        };

    try (var simulator = simulator(stream, instruments, operations, orders);
        var gateway = gateway(simulator.server.getPort());
        var feed = gateway.observe(journal)) {
      assertThat(stream.subscriptions.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(1);
      assertThat(recovered.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      gaps.clear();

      stream.live.onNext(
          TradesStreamResponse.newBuilder()
              .setOrderTrades(
                  OrderTrades.newBuilder()
                      .setAccountId("source")
                      .setOrderId("source-order-ambiguous")
                      .setInstrumentUid("instrument-1")
                      .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                      .addTrades(
                          OrderTrade.newBuilder()
                              .setTradeId("operation-trade-a")
                              .setQuantity(30)
                              .setPrice(Quotation.newBuilder().setUnits(101).setNano(500000000))))
              .build());
      var missingTimestamp = remembered.poll(10, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(missingTimestamp).isNotNull();
      assertThat(missingTimestamp.tradeId()).isEqualTo("operation-trade-a");
      assertThat(missingTimestamp.executedAt()).isNull();
      assertThat(accepted).isEmpty();

      stream.live.onError(Status.UNAVAILABLE.asRuntimeException());
      assertThat(gaps.poll(10, java.util.concurrent.TimeUnit.SECONDS))
          .isEqualTo("Source stream disconnected");
      operationsVisible.set(true);

      assertThat(stream.subscriptions.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(2);
      assertThat(gaps.poll(10, java.util.concurrent.TimeUnit.SECONDS))
          .isEqualTo("Source subscription awaiting acknowledgement and history recovery");
      assertThat(gaps.poll(10, java.util.concurrent.TimeUnit.SECONDS))
          .isEqualTo(
              "Ambiguous or conflicting GetOrderState source history: source-order-ambiguous");
      assertThat(stateRequests)
          .singleElement()
          .satisfies(
              request -> assertThat(request.getOrderId()).isEqualTo("source-order-ambiguous"));
      assertThat(accepted).isEmpty();
      assertThat(recovered).isEmpty();
    }
  }

  @Test
  void reusedOperationIdAcrossDifferentOperationItemsStaysUnresolved() throws Exception {
    var at = java.time.Instant.now().minusSeconds(5);
    var stateRequests = new java.util.concurrent.LinkedBlockingQueue<GetOrderStateRequest>();
    var instruments =
        new InstrumentsServiceGrpc.InstrumentsServiceImplBase() {
          @Override
          public void getInstrumentBy(
              InstrumentRequest request, StreamObserver<InstrumentResponse> out) {
            out.onNext(
                InstrumentResponse.newBuilder()
                    .setInstrument(
                        ru.tinkoff.piapi.contract.v1.Instrument.newBuilder()
                            .setUid("instrument-1")
                            .setLot(10)
                            .setCurrency("rub")
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          @Override
          public void getOperationsByCursor(
              GetOperationsByCursorRequest request,
              StreamObserver<GetOperationsByCursorResponse> out) {
            var page = GetOperationsByCursorResponse.newBuilder();
            page.addItems(operation("reused-operation-17", "operation-trade-first", 20, at, 100));
            page.addItems(
                operation(
                    "reused-operation-17", "operation-trade-second", 30, at.plusMillis(100), 101));
            out.onNext(page.build());
            out.onCompleted();
          }

          private OperationItem operation(
              String operationId,
              String tradeId,
              long quantity,
              java.time.Instant executedAt,
              long price) {
            return OperationItem.newBuilder()
                .setId(operationId)
                .setBrokerAccountId("source")
                .setInstrumentUid("instrument-1")
                .setType(OperationType.OPERATION_TYPE_BUY)
                .setTradesInfo(
                    OperationItemTrades.newBuilder()
                        .addTrades(
                            OperationItemTrade.newBuilder()
                                .setNum(tradeId)
                                .setQuantity(quantity)
                                .setDate(TInvestBrokerGateway.timestamp(executedAt))
                                .setPrice(
                                    MoneyValue.newBuilder().setCurrency("rub").setUnits(price))))
                .build();
          }
        };
    var orders =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          @Override
          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            stateRequests.add(request);
            out.onNext(
                OrderState.newBuilder()
                    .setOrderId("reused-operation-17")
                    .setInstrumentUid("instrument-1")
                    .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                    .setLotsRequested(5)
                    .setLotsExecuted(5)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL)
                    .addStages(
                        OrderStage.newBuilder()
                            .setTradeId("canonical-trade-first")
                            .setQuantity(2)
                            .setExecutionTime(TInvestBrokerGateway.timestamp(at))
                            .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(100)))
                    .addStages(
                        OrderStage.newBuilder()
                            .setTradeId("canonical-trade-second")
                            .setQuantity(3)
                            .setExecutionTime(TInvestBrokerGateway.timestamp(at.plusMillis(100)))
                            .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(101)))
                    .build());
            out.onCompleted();
          }
        };

    try (var simulator = simulator(instruments, operations, orders);
        var gateway = gateway(simulator.server.getPort())) {
      var history = gateway.history(at.minusSeconds(10), java.time.Instant.now());
      assertThat(history.complete()).isFalse();
      assertThat(history.gaps())
          .containsExactly("Conflicting source operation identity: reused-operation-17");
      assertThat(history.executions()).isEmpty();
      assertThat(history.sourceCandidates()).isEmpty();
      assertThat(stateRequests).isEmpty();
    }
  }

  @Test
  void orderTradesFrameWithoutTradesDoesNotAdmitAnExecution() throws Exception {
    var now = java.time.Instant.now();
    var subscriptions = new java.util.concurrent.LinkedBlockingQueue<Integer>();
    var accepted = new java.util.concurrent.LinkedBlockingQueue<BrokerGateway.SourceExecution>();
    var recovered = new java.util.concurrent.LinkedBlockingQueue<Boolean>();
    var gaps = new java.util.concurrent.LinkedBlockingQueue<String>();
    var frameProcessed = new java.util.concurrent.CountDownLatch(1);
    var stream =
        new OrdersStreamServiceGrpc.OrdersStreamServiceImplBase() {
          volatile StreamObserver<TradesStreamResponse> live;

          @Override
          public void tradesStream(
              TradesStreamRequest request, StreamObserver<TradesStreamResponse> out) {
            live = out;
            out.onNext(
                TradesStreamResponse.newBuilder()
                    .setSubscription(
                        SubscriptionResponse.newBuilder()
                            .setStatus(ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK)
                            .addAccounts("source"))
                    .build());
            subscriptions.add(1);
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
                            .setUid("instrument-1")
                            .setLot(10)
                            .setCurrency("rub")
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
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
    var journal =
        new SourceFeedJournal() {
          public java.time.Instant observationBoundary() {
            return now.minusSeconds(1);
          }

          public java.util.Optional<String> sourceOrderId(String account, String trade) {
            return java.util.Optional.empty();
          }

          public java.util.Optional<BrokerGateway.SourceExecution> remembered(
              String account, String trade) {
            return java.util.Optional.empty();
          }

          public java.util.List<BrokerGateway.SourceExecution> pendingTimestampRecovery() {
            return List.of();
          }

          public java.time.Instant firstReceived(
              String account, String trade, java.time.Instant received) {
            return received;
          }

          public void remember(BrokerGateway.SourceExecution execution) {
            throw new AssertionError("An order-only frame cannot remember a trade");
          }

          public void accept(BrokerGateway.SourceExecution execution) {
            accepted.add(execution);
          }

          public void acceptRecovered(BrokerGateway.SourceExecution execution) {
            accepted.add(execution);
          }

          public void skip(BrokerGateway.SourceExecution execution, String reason) {
            throw new AssertionError(reason);
          }

          public void gap(String reason) {
            gaps.add(reason);
          }

          public void recovered() {
            recovered.add(true);
          }

          public void readSucceeded() {
            frameProcessed.countDown();
          }
        };

    try (var simulator = simulator(stream, instruments, operations);
        var gateway = gateway(simulator.server.getPort());
        var feed = gateway.observe(journal)) {
      assertThat(subscriptions.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(1);
      assertThat(recovered.poll(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      gaps.clear();

      stream.live.onNext(
          TradesStreamResponse.newBuilder()
              .setOrderTrades(
                  OrderTrades.newBuilder()
                      .setAccountId("source")
                      .setOrderId("source-order-without-executions")
                      .setInstrumentUid("instrument-1")
                      .setDirection(OrderDirection.ORDER_DIRECTION_BUY))
              .build());

      assertThat(frameProcessed.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      assertThat(accepted).isEmpty();
      assertThat(recovered).isEmpty();
      assertThat(gaps).isEmpty();
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(BrokerGateway.Side.class)
  void currencyPairKeepsDirectionAndCurrencyPriceTypeAndForbidsSourceMutations(
      BrokerGateway.Side side) throws Exception {
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          PostOrderRequest received;

          @Override
          public void postOrder(PostOrderRequest request, StreamObserver<PostOrderResponse> out) {
            received = request;
            out.onNext(
                PostOrderResponse.newBuilder()
                    .setOrderId("broker-fx")
                    .setInstrumentUid(request.getInstrumentId())
                    .setDirection(request.getDirection())
                    .setLotsRequested(request.getQuantity())
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW)
                    .build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(broker);
        var gateway = gateway(simulator.server.getPort())) {
      var order =
          new BrokerGateway.OrderRequest(
              "mirror", UUID.randomUUID(), "CNYRUB-pair", side, 3, new BigDecimal("12.3456"));
      assertThat(gateway.post(order).status()).isEqualTo(BrokerGateway.OrderStatus.NEW);
      assertThat(broker.received.getDirection())
          .isEqualTo(
              side == BrokerGateway.Side.BUY
                  ? OrderDirection.ORDER_DIRECTION_BUY
                  : OrderDirection.ORDER_DIRECTION_SELL);
      assertThat(broker.received.getInstrumentId()).isEqualTo("CNYRUB-pair");
      assertThat(broker.received.getQuantity()).isEqualTo(3);
      assertThat(broker.received.getPriceType()).isEqualTo(PriceType.PRICE_TYPE_CURRENCY);
      assertThat(broker.received.getConfirmMarginTrade()).isFalse();
      org.assertj.core.api.Assertions.assertThatThrownBy(
              () ->
                  gateway.post(
                      new BrokerGateway.OrderRequest(
                          "source",
                          UUID.randomUUID(),
                          order.instrumentId(),
                          side,
                          3,
                          order.limitPrice())))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "INSTRUMENT_TYPE_BOND,0.1",
    "INSTRUMENT_TYPE_FUTURES,0.25"
  })
  void instrumentMetadataNormalizesBondAndFuturePriceIncrementToSettlementCurrency(
      String kind, String expected) throws Exception {
    var instruments =
        new InstrumentsServiceGrpc.InstrumentsServiceImplBase() {
          @Override
          public void getInstrumentBy(
              InstrumentRequest request, StreamObserver<InstrumentResponse> out) {
            out.onNext(
                InstrumentResponse.newBuilder()
                    .setInstrument(
                        ru.tinkoff.piapi.contract.v1.Instrument.newBuilder()
                            .setUid("instrument-1")
                            .setLot(1)
                            .setCurrency("rub")
                            .setInstrumentKind(InstrumentType.valueOf(kind))
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }

          @Override
          public void bondBy(InstrumentRequest request, StreamObserver<BondResponse> out) {
            out.onNext(
                BondResponse.newBuilder()
                    .setInstrument(
                        Bond.newBuilder()
                            .setUid("instrument-1")
                            .setCurrency("rub")
                            .setNominal(MoneyValue.newBuilder().setUnits(1000).setCurrency("rub"))
                            .setMinPriceIncrement(Quotation.newBuilder().setNano(10000000)))
                    .build());
            out.onCompleted();
          }

          @Override
          public void futureBy(InstrumentRequest request, StreamObserver<FutureResponse> out) {
            out.onNext(
                FutureResponse.newBuilder()
                    .setInstrument(
                        Future.newBuilder()
                            .setUid("instrument-1")
                            .setCurrency("rub")
                            .setMinPriceIncrement(Quotation.newBuilder().setUnits(1))
                            .setMinPriceIncrementAmount(Quotation.newBuilder().setNano(250000000)))
                    .build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(instruments);
        var gateway = gateway(simulator.server.getPort())) {
      assertThat(gateway.instrument("instrument-1").priceIncrement())
          .isEqualByComparingTo(expected);
    }
  }

  @Test
  void terminalOrderCommissionRemainsUnqualifiedUntilActualTotalInterpretationIsConfirmed()
      throws Exception {
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          @Override
          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            out.onNext(
                OrderState.newBuilder()
                    .setOrderId("broker-1")
                    .setOrderRequestId(request.getOrderId())
                    .setInstrumentUid("instrument-1")
                    .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                    .setLotsRequested(1)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED)
                    .setExecutedCommission(
                        MoneyValue.newBuilder().setCurrency("rub").setNano(100000000))
                    .build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(broker);
        var gateway = gateway(simulator.server.getPort())) {
      var request =
          new BrokerGateway.OrderRequest(
              "mirror",
              UUID.randomUUID(),
              "instrument-1",
              BrokerGateway.Side.BUY,
              1,
              BigDecimal.TEN);
      var commission = gateway.commission(request);
      assertThat(commission.amount()).isEqualByComparingTo("0.1");
      assertThat(commission.qualified()).isFalse();
      assertThat(commission.currency()).isEqualTo("rub");
    }
  }

  @Test
  void accountReadVerificationRefusesAnUnavailableMirrorAccount() throws Exception {
    var users =
        new UsersServiceGrpc.UsersServiceImplBase() {
          @Override
          public void getAccounts(
              GetAccountsRequest request, StreamObserver<GetAccountsResponse> out) {
            out.onNext(
                GetAccountsResponse.newBuilder()
                    .addAccounts(
                        Account.newBuilder()
                            .setId("source")
                            .setStatus(AccountStatus.ACCOUNT_STATUS_OPEN)
                            .setAccessLevel(AccessLevel.ACCOUNT_ACCESS_LEVEL_READ_ONLY))
                    .build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(users);
        var gateway = gateway(simulator.server.getPort())) {
      org.assertj.core.api.Assertions.assertThatThrownBy(gateway::verifyAccounts)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("mirror");
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(BrokerGateway.Side.class)
  void oppositeQuoteUsesAsksForBuyAndBidsForSell(BrokerGateway.Side side) throws Exception {
    var market =
        new MarketDataServiceGrpc.MarketDataServiceImplBase() {
          @Override
          public void getOrderBook(
              GetOrderBookRequest request, StreamObserver<GetOrderBookResponse> out) {
            out.onNext(
                GetOrderBookResponse.newBuilder()
                    .setInstrumentUid(request.getInstrumentId())
                    .addAsks(
                        Order.newBuilder()
                            .setQuantity(4)
                            .setPrice(Quotation.newBuilder().setUnits(100).setNano(200000000)))
                    .addBids(
                        Order.newBuilder()
                            .setQuantity(3)
                            .setPrice(Quotation.newBuilder().setUnits(99).setNano(800000000)))
                    .build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(market);
        var gateway = gateway(simulator.server.getPort())) {
      assertThat(gateway.bestOppositePrice("instrument-1", side).orElseThrow())
          .isEqualByComparingTo(side == BrokerGateway.Side.BUY ? "100.2" : "99.8");
    }
  }

  @Test
  void lostCancellationResponseKeepsUnknownUntilLateFillAndRemainingCancellationAreReconciled()
      throws Exception {
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          int cancels;

          @Override
          public void cancelOrder(
              CancelOrderRequest request, StreamObserver<CancelOrderResponse> out) {
            cancels++;
            out.onError(Status.UNAVAILABLE.asRuntimeException());
          }

          @Override
          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            var state =
                OrderState.newBuilder()
                    .setOrderId("broker-1")
                    .setOrderRequestId(request.getOrderId())
                    .setInstrumentUid("instrument-1")
                    .setDirection(OrderDirection.ORDER_DIRECTION_SELL)
                    .setLotsRequested(3)
                    .setLotsExecuted(2)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED);
            for (String id : List.of("before-cancel", "during-cancel"))
              state.addStages(
                  OrderStage.newBuilder()
                      .setTradeId(id)
                      .setQuantity(1)
                      .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(100))
                      .setExecutionTime(
                          com.google.protobuf.Timestamp.newBuilder().setSeconds(1800000000)));
            out.onNext(state.build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(broker);
        var gateway = gateway(simulator.server.getPort())) {
      var request =
          new BrokerGateway.OrderRequest(
              "mirror",
              UUID.randomUUID(),
              "instrument-1",
              BrokerGateway.Side.SELL,
              3,
              BigDecimal.valueOf(100));
      assertThat(gateway.cancel(request, "broker-1").status())
          .isEqualTo(BrokerGateway.OrderStatus.UNKNOWN);
      var finalResult = gateway.reconcile(request);
      assertThat(finalResult.status()).isEqualTo(BrokerGateway.OrderStatus.CANCELLED);
      assertThat(finalResult.lotsExecuted()).isEqualTo(2);
      assertThat(finalResult.fills())
          .extracting(BrokerGateway.Fill::tradeId)
          .containsExactly("before-cancel", "during-cancel");
      assertThat(broker.cancels).isEqualTo(1);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "INSTRUMENT_TYPE_CURRENCY,false",
    "INSTRUMENT_TYPE_SHARE,true"
  })
  void missingCurrencyPairPositionIsNotInventedAsZero(String kind, boolean covered)
      throws Exception {
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
                            .setInstrumentKind(InstrumentType.valueOf(kind)))
                    .build());
            out.onCompleted();
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          @Override
          public void getPositions(
              PositionsRequest request, StreamObserver<PositionsResponse> out) {
            out.onNext(PositionsResponse.newBuilder().setAccountId(request.getAccountId()).build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(instruments, operations);
        var gateway = gateway(simulator.server.getPort())) {
      assertThat(gateway.positionAbsentMeansZero("instrument-1", gateway.positions("mirror")))
          .isEqualTo(covered);
    }
  }

  @Test
  void currencyPositionsUseOnlyBankSuppliedPortfolioUidAndTotalWithoutInventingAvailableFunds()
      throws Exception {
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          @Override
          public void getPositions(
              PositionsRequest request, StreamObserver<PositionsResponse> out) {
            out.onNext(PositionsResponse.newBuilder().setAccountId(request.getAccountId()).build());
            out.onCompleted();
          }

          @Override
          public void getPortfolio(
              PortfolioRequest request, StreamObserver<PortfolioResponse> out) {
            out.onNext(
                PortfolioResponse.newBuilder()
                    .setAccountId(request.getAccountId())
                    .addPositions(
                        PortfolioPosition.newBuilder()
                            .setInstrumentUid("bank-currency-uid")
                            .setInstrumentType("currency")
                            .setQuantity(Quotation.newBuilder().setUnits(1250)))
                    .build());
            out.onCompleted();
          }
        };
    try (var simulator = simulator(operations);
        var gateway = gateway(simulator.server.getPort())) {
      var position = gateway.positions("source").positions().getFirst();
      assertThat(position.instrumentId()).isEqualTo("bank-currency-uid");
      assertThat(position.totalPieces()).isEqualByComparingTo("1250");
      assertThat(position.availabilityQualified()).isFalse();
      assertThat(position.availablePieces()).isNull();
      assertThat(position.blockedPieces()).isNull();
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void capitalReceiptPrecedesObserverPersistenceAndItsFailureStillPropagates(boolean failObserver)
      throws Exception {
    var wall =
        new java.util.concurrent.atomic.AtomicReference<>(
            java.time.Instant.parse("2026-11-15T12:00:00Z"));
    Clock clock =
        new Clock() {
          public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
          }

          public Clock withZone(java.time.ZoneId zone) {
            return this;
          }

          public java.time.Instant instant() {
            return wall.get();
          }
        };
    var observerStarted = new java.util.concurrent.atomic.AtomicLong();
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    var events =
        new BrokerConnectivityEvents() {
          public void connectionLost(Cause cause) {}

          public void restoredCopyOpened() {}

          public void readSucceeded() {
            observerStarted.set(BrokerReceiptClock.nanoTime());
            wall.updateAndGet(t -> t.plusSeconds(2));
            if (failObserver)
              throw new BrokerPersistenceFailure(
                  "synthetic observer",
                  new org.springframework.dao.DataAccessResourceFailureException(
                      "synthetic persistence failure"));
          }
        };
    var operations =
        new OperationsServiceGrpc.OperationsServiceImplBase() {
          @Override
          public void getPortfolio(
              PortfolioRequest request, StreamObserver<PortfolioResponse> out) {
            calls.incrementAndGet();
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
        var gateway = BrokerTlsTestSupport.gateway(server.port(), clock, events)) {
      var before = wall.get();
      long requested = BrokerReceiptClock.nanoTime();
      if (failObserver) {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> gateway.capital("source"))
            .isInstanceOf(BrokerPersistenceFailure.class);
      } else {
        var evidence = gateway.capital("source");
        assertThat(evidence.receivedAt()).isEqualTo(before);
        assertThat(evidence.receivedMonotonicNanos()).isBetween(requested, observerStarted.get());
        assertThat(evidence.receiptClockDomain()).isEqualTo(BrokerReceiptClock.domain());
      }
      assertThat(wall.get()).isEqualTo(before.plusSeconds(2));
      assertThat(calls.get()).isEqualTo(1);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void orderReceiptRetainsRawSdkTimeBeforeDurableReadObserver(boolean posting) throws Exception {
    var wall =
        new java.util.concurrent.atomic.AtomicReference<>(
            java.time.Instant.parse("2026-11-15T12:00:00Z"));
    Clock clock =
        new Clock() {
          public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
          }

          public Clock withZone(java.time.ZoneId zone) {
            return this;
          }

          public java.time.Instant instant() {
            return wall.get();
          }
        };
    var observerEntered = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
    var events =
        new BrokerConnectivityEvents() {
          public void connectionLost(Cause cause) {}

          public void restoredCopyOpened() {}

          public void readSucceeded() {
            observerEntered.set(BrokerReceiptClock.nanoTime());
            wall.updateAndGet(t -> t.plusSeconds(2));
          }
        };
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          public void postOrder(PostOrderRequest request, StreamObserver<PostOrderResponse> out) {
            out.onNext(
                PostOrderResponse.newBuilder()
                    .setOrderId("bank-order")
                    .setInstrumentUid("instrument-1")
                    .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                    .setLotsRequested(1)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW)
                    .build());
            out.onCompleted();
          }

          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            out.onNext(
                OrderState.newBuilder()
                    .setOrderId("bank-order")
                    .setOrderRequestId(request.getOrderId())
                    .setInstrumentUid("instrument-1")
                    .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
                    .setLotsRequested(1)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW)
                    .build());
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway = BrokerTlsTestSupport.gateway(server.port(), clock, events)) {
      var at = wall.get();
      long before = BrokerReceiptClock.nanoTime();
      var request =
          new BrokerGateway.OrderRequest(
              "mirror",
              UUID.randomUUID(),
              "instrument-1",
              BrokerGateway.Side.BUY,
              1,
              BigDecimal.ONE);
      var result = posting ? gateway.post(request) : gateway.reconcile(request);
      assertThat(result.status()).isEqualTo(BrokerGateway.OrderStatus.NEW);
      assertThat(result.observedAt()).isEqualTo(at);
      assertThat(result.receivedMonotonicNanos())
          .isNotNull()
          .isBetween(before, Math.min(observerEntered.get(), BrokerReceiptClock.nanoTime()));
      assertThat(result.receiptClockDomain()).isEqualTo(BrokerReceiptClock.domain());
    }
  }

  private BrokerTlsTestSupport.Simulator simulator(io.grpc.BindableService... services)
      throws Exception {
    return BrokerTlsTestSupport.open(directory, services);
  }

  private TInvestBrokerGateway gateway(int port) throws Exception {
    return BrokerTlsTestSupport.gateway(port, Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS);
  }
}
