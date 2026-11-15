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
            return remembered.values().stream().filter(e -> e.executedAt() == null).toList();
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
              skipped.add(execution.tradeId());
              remembered.remove(execution.tradeId());
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
