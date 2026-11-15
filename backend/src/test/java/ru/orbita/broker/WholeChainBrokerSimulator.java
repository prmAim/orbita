package ru.orbita.broker;

import io.grpc.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import ru.tinkoff.piapi.contract.v1.*;

/**
 * Stateful, loopback-only TLS bank for the integrated scenario and its optional retained preview.
 * State enters the application only through the real SDK. All qualification here is synthetic.
 */
public final class WholeChainBrokerSimulator implements AutoCloseable {
  public static final String SOURCE = "[ТЕСТ] SOURCE",
      MIRROR = "[ТЕСТ] MIRROR",
      INSTRUMENT = "[ТЕСТ] SBER";
  public static final BigDecimal PRICE = new BigDecimal("100"), COMMISSION = BigDecimal.ONE;

  public record Rpc(String method, Object request, String remoteAddress, Instant receivedAt) {}

  private record Execution(
      String account,
      String id,
      String order,
      BrokerGateway.Side side,
      long lots,
      BigDecimal price,
      Instant at) {}

  private final List<Rpc> audit = new CopyOnWriteArrayList<>();
  private final List<Execution> executions = new ArrayList<>();
  private final Map<UUID, SimulatedOrder> orders = new LinkedHashMap<>();
  private final Map<String, Long> positions = new HashMap<>();
  private final Map<String, BigDecimal> capitals = new HashMap<>();
  private final List<StreamObserver<TradesStreamResponse>> streams = new CopyOnWriteArrayList<>();
  private final BlockingQueue<Boolean> subscriptions = new LinkedBlockingQueue<>();
  private final ScheduledExecutorService heartbeat =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().daemon().name("whole-chain-bank-heartbeat").factory());
  private final BrokerTlsTestSupport.Simulator tls;
  private BigDecimal ownMoney = new BigDecimal("10000");
  private long maximumBuy = 100, maximumSell = 100, nextPostFill = Long.MAX_VALUE;
  private boolean closed;

  public static WholeChainBrokerSimulator open(Path certificateDirectory) throws Exception {
    Files.createDirectories(certificateDirectory);
    return new WholeChainBrokerSimulator(certificateDirectory);
  }

  private WholeChainBrokerSimulator(Path directory) throws Exception {
    positions.put(SOURCE, 0L);
    positions.put(MIRROR, 0L);
    capitals.put(SOURCE, new BigDecimal("10000"));
    capitals.put(MIRROR, new BigDecimal("10000"));
    var interceptor =
        new ServerInterceptor() {
          public <Q, S> ServerCall.Listener<Q> interceptCall(
              ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
            String remote =
                String.valueOf(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
            if (!remote.contains("127.0.0.1")) {
              call.close(
                  Status.PERMISSION_DENIED.withDescription("Loopback fixture only"),
                  new Metadata());
              return new ServerCall.Listener<>() {};
            }
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                next.startCall(call, headers)) {
              public void onMessage(Q request) {
                audit.add(
                    new Rpc(
                        call.getMethodDescriptor().getBareMethodName(),
                        request,
                        remote,
                        Instant.now()));
                super.onMessage(request);
              }
            };
          }
        };
    BindableService[] services = {
      users(), instruments(), operations(), market(), orders(), stream()
    };
    for (int i = 0; i < services.length; i++) {
      var service = services[i];
      services[i] = () -> ServerInterceptors.intercept(service, interceptor);
    }
    tls = BrokerTlsTestSupport.open(directory, services);
    heartbeat.scheduleWithFixedDelay(
        () -> {
          synchronized (WholeChainBrokerSimulator.this) {
            for (var out : streams)
              try {
                out.onNext(
                    TradesStreamResponse.newBuilder()
                        .setPing(
                            Ping.newBuilder()
                                .setTime(TInvestBrokerGateway.timestamp(Instant.now())))
                        .build());
              } catch (RuntimeException disconnected) {
                streams.remove(out);
              }
          }
        },
        2,
        2,
        TimeUnit.SECONDS);
  }

  public TInvestBrokerGateway gateway(Clock clock, BrokerConnectivityEvents events)
      throws Exception {
    return BrokerTlsTestSupport.gateway(
        tls.port(),
        clock,
        events,
        qualifications(clock.instant().plusSeconds(86400)),
        SOURCE,
        MIRROR);
  }

  public BrokerQualifications qualifications(Instant expiresAt) {
    return new BrokerQualifications(
        true,
        true,
        true,
        true,
        BigDecimal.ONE,
        "synthetic localhost whole-chain fixture",
        Set.of(SOURCE, MIRROR),
        MIRROR,
        Set.of(INSTRUMENT),
        "RUB",
        expiresAt,
        true,
        BrokerQualifications.OwnMoneyBasis.GROSS_BEFORE_PROGRAM_RESERVES);
  }

  public int port() {
    return tls.port();
  }

  public List<Rpc> audit() {
    return List.copyOf(audit);
  }

  public List<Rpc> calls(String method) {
    return audit.stream().filter(r -> r.method().equals(method)).toList();
  }

  public boolean awaitSubscription(Duration timeout) throws InterruptedException {
    return subscriptions.poll(timeout.toMillis(), TimeUnit.MILLISECONDS) != null;
  }

  public synchronized void setCapitals(BigDecimal source, BigDecimal mirror) {
    positive(source);
    positive(mirror);
    capitals.put(SOURCE, source);
    capitals.put(MIRROR, mirror);
  }

  public synchronized void setResources(BigDecimal money, long buyLots, long sellLots) {
    if (money == null || money.signum() < 0 || buyLots < 0 || sellLots < 0)
      throw new IllegalArgumentException("Nonnegative resources required");
    ownMoney = money;
    maximumBuy = buyLots;
    maximumSell = sellLots;
  }

  public synchronized void setNextPostFill(long lots) {
    if (lots < 0) throw new IllegalArgumentException("Nonnegative fill required");
    nextPostFill = lots;
  }

  public synchronized String brokerOrderId(UUID requestId) {
    return requireOrder(requestId).bankId;
  }

  public synchronized long position(String account) {
    return positions.getOrDefault(account, 0L);
  }

  public synchronized void disconnectSource() {
    for (var out : streams)
      out.onError(
          Status.UNAVAILABLE.withDescription("Synthetic stream interruption").asRuntimeException());
    streams.clear();
  }

  public synchronized void replaySource(String tradeId) {
    var execution =
        executions.stream()
            .filter(e -> e.account().equals(SOURCE) && e.id().equals(tradeId))
            .findFirst()
            .orElseThrow();
    sendSource(execution);
  }

  public synchronized void emitSource(
      String tradeId,
      String sourceOrderId,
      BrokerGateway.Side side,
      long lots,
      BigDecimal price,
      Instant at) {
    if (closed
        || tradeId == null
        || tradeId.isBlank()
        || sourceOrderId == null
        || sourceOrderId.isBlank()
        || side == null
        || lots <= 0
        || at == null) throw new IllegalArgumentException("Complete source execution required");
    positive(price);
    if (executions.stream().anyMatch(e -> e.account().equals(SOURCE) && e.id().equals(tradeId)))
      throw new IllegalArgumentException("Duplicate natural trade");
    var execution = new Execution(SOURCE, tradeId, sourceOrderId, side, lots, price, at);
    addExecution(execution);
    sendSource(execution);
  }

  private void sendSource(Execution execution) {
    var frame =
        TradesStreamResponse.newBuilder()
            .setOrderTrades(
                OrderTrades.newBuilder()
                    .setAccountId(SOURCE)
                    .setOrderId(execution.order())
                    .setInstrumentUid(INSTRUMENT)
                    .setDirection(direction(execution.side()))
                    .addTrades(
                        OrderTrade.newBuilder()
                            .setTradeId(execution.id())
                            .setQuantity(execution.lots())
                            .setPrice(TInvestBrokerGateway.quotation(execution.price()))
                            .setDateTime(TInvestBrokerGateway.timestamp(execution.at()))))
            .build();
    for (var out : streams) out.onNext(frame);
  }

  public synchronized void fillOrder(UUID requestId, long additionalLots, Instant at) {
    fill(requireOrder(requestId), additionalLots, at);
  }

  private void addExecution(Execution execution) {
    long delta = execution.side() == BrokerGateway.Side.BUY ? execution.lots() : -execution.lots();
    long position = Math.addExact(positions.getOrDefault(execution.account(), 0L), delta);
    if (position < 0)
      throw new IllegalArgumentException("Simulator does not permit short positions");
    positions.put(execution.account(), position);
    executions.add(execution);
  }

  private static void positive(BigDecimal value) {
    if (value == null || value.signum() <= 0)
      throw new IllegalArgumentException("Positive exact amount required");
  }

  private static OrderDirection direction(BrokerGateway.Side side) {
    return side == BrokerGateway.Side.BUY
        ? OrderDirection.ORDER_DIRECTION_BUY
        : OrderDirection.ORDER_DIRECTION_SELL;
  }

  private static MoneyValue money(BigDecimal amount) {
    var q = TInvestBrokerGateway.quotation(amount);
    return MoneyValue.newBuilder()
        .setCurrency("rub")
        .setUnits(q.getUnits())
        .setNano(q.getNano())
        .build();
  }

  private static <T> void reply(StreamObserver<T> out, T response) {
    out.onNext(response);
    out.onCompleted();
  }

  private void checkAccount(String account) {
    if (!account.equals(SOURCE) && !account.equals(MIRROR))
      throw Status.PERMISSION_DENIED.withDescription("Synthetic account only").asRuntimeException();
  }

  private void checkInstrument(String id) {
    if (!INSTRUMENT.equals(id))
      throw Status.NOT_FOUND.withDescription("Synthetic instrument only").asRuntimeException();
  }

  private SimulatedOrder requireOrder(UUID id) {
    var order = orders.get(id);
    if (order == null) throw Status.NOT_FOUND.asRuntimeException();
    return order;
  }

  private final class SimulatedOrder {
    final PostOrderRequest request;
    final String bankId;
    final List<Execution> fills = new ArrayList<>();
    OrderExecutionReportStatus status = OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW;

    SimulatedOrder(PostOrderRequest request) {
      this.request = request;
      bankId = "TEST-BANK-" + request.getOrderId();
    }

    long filled() {
      return fills.stream().mapToLong(Execution::lots).sum();
    }

    OrderState state() {
      var state =
          OrderState.newBuilder()
              .setOrderId(bankId)
              .setOrderRequestId(request.getOrderId())
              .setInstrumentUid(INSTRUMENT)
              .setDirection(request.getDirection())
              .setLotsRequested(request.getQuantity())
              .setLotsExecuted(filled())
              .setExecutionReportStatus(status)
              .setExecutedCommission(money(filled() > 0 ? COMMISSION : BigDecimal.ZERO));
      for (var fill : fills)
        state.addStages(
            OrderStage.newBuilder()
                .setTradeId(fill.id())
                .setQuantity(fill.lots())
                .setPrice(money(fill.price()))
                .setExecutionTime(TInvestBrokerGateway.timestamp(fill.at())));
      return state.build();
    }
  }

  private void fill(SimulatedOrder order, long lots, Instant at) {
    if (lots < 0 || lots > order.request.getQuantity() - order.filled() || at == null)
      throw new IllegalArgumentException("Fill exceeds unfilled order");
    if (lots == 0) return;
    if (order.status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED
        || order.status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL)
      throw new IllegalStateException("Terminal order cannot fill");
    var fill =
        new Execution(
            MIRROR,
            "TEST-FILL-" + order.request.getOrderId() + "-" + (order.fills.size() + 1),
            order.bankId,
            order.request.getDirection() == OrderDirection.ORDER_DIRECTION_BUY
                ? BrokerGateway.Side.BUY
                : BrokerGateway.Side.SELL,
            lots,
            TInvestBrokerGateway.decimal(
                order.request.getPrice().getUnits(), order.request.getPrice().getNano()),
            at);
    addExecution(fill);
    order.fills.add(fill);
    order.status =
        order.filled() == order.request.getQuantity()
            ? OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
            : OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL;
  }

  private BindableService users() {
    return new UsersServiceGrpc.UsersServiceImplBase() {
      public void getAccounts(GetAccountsRequest request, StreamObserver<GetAccountsResponse> out) {
        reply(
            out,
            GetAccountsResponse.newBuilder()
                .addAccounts(
                    Account.newBuilder()
                        .setId(SOURCE)
                        .setStatus(AccountStatus.ACCOUNT_STATUS_OPEN)
                        .setAccessLevel(AccessLevel.ACCOUNT_ACCESS_LEVEL_READ_ONLY))
                .addAccounts(
                    Account.newBuilder()
                        .setId(MIRROR)
                        .setStatus(AccountStatus.ACCOUNT_STATUS_OPEN)
                        .setAccessLevel(AccessLevel.ACCOUNT_ACCESS_LEVEL_FULL_ACCESS))
                .build());
      }
    };
  }

  private BindableService instruments() {
    return new InstrumentsServiceGrpc.InstrumentsServiceImplBase() {
      public void getInstrumentBy(
          InstrumentRequest request, StreamObserver<InstrumentResponse> out) {
        checkInstrument(request.getId());
        reply(
            out,
            InstrumentResponse.newBuilder()
                .setInstrument(
                    ru.tinkoff.piapi.contract.v1.Instrument.newBuilder()
                        .setUid(INSTRUMENT)
                        .setTicker(INSTRUMENT)
                        .setLot(1)
                        .setCurrency("rub")
                        .setInstrumentType("share")
                        .setInstrumentKind(InstrumentType.INSTRUMENT_TYPE_SHARE)
                        .setBuyAvailableFlag(true)
                        .setSellAvailableFlag(true)
                        .setApiTradeAvailableFlag(true)
                        .setMinPriceIncrement(
                            TInvestBrokerGateway.quotation(new BigDecimal("0.01"))))
                .build());
      }
    };
  }

  private BindableService operations() {
    return new OperationsServiceGrpc.OperationsServiceImplBase() {
      public void getPortfolio(PortfolioRequest request, StreamObserver<PortfolioResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          checkAccount(request.getAccountId());
          reply(
              out,
              PortfolioResponse.newBuilder()
                  .setAccountId(request.getAccountId())
                  .setTotalAmountPortfolio(money(capitals.get(request.getAccountId())))
                  .setTotalAmountCurrencies(money(capitals.get(request.getAccountId())))
                  .build());
        }
      }

      public void getPositions(PositionsRequest request, StreamObserver<PositionsResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          checkAccount(request.getAccountId());
          var result = PositionsResponse.newBuilder().setAccountId(request.getAccountId());
          long pieces = positions.get(request.getAccountId());
          if (pieces != 0)
            result.addSecurities(
                PositionsSecurities.newBuilder()
                    .setInstrumentUid(INSTRUMENT)
                    .setBalance(pieces)
                    .setBlocked(0));
          reply(out, result.build());
        }
      }

      public void getOperationsByCursor(
          GetOperationsByCursorRequest request, StreamObserver<GetOperationsByCursorResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          checkAccount(request.getAccountId());
          var result = GetOperationsByCursorResponse.newBuilder();
          if (!request.getCursor().isEmpty()) {
            reply(out, result.build());
            return;
          }
          Instant from = TInvestBrokerGateway.instant(request.getFrom()),
              through = TInvestBrokerGateway.instant(request.getTo());
          for (var execution : executions) {
            if (!execution.account().equals(request.getAccountId())
                || execution.at().isBefore(from)
                || execution.at().isAfter(through)) continue;
            result.addItems(
                OperationItem.newBuilder()
                    .setId("TEST-OP-" + execution.id())
                    .setBrokerAccountId(execution.account())
                    .setInstrumentUid(INSTRUMENT)
                    .setType(
                        execution.side() == BrokerGateway.Side.BUY
                            ? OperationType.OPERATION_TYPE_BUY
                            : OperationType.OPERATION_TYPE_SELL)
                    .setTradesInfo(
                        OperationItemTrades.newBuilder()
                            .addTrades(
                                OperationItemTrade.newBuilder()
                                    .setNum(execution.id())
                                    .setQuantity(execution.lots())
                                    .setPrice(money(execution.price()))
                                    .setDate(TInvestBrokerGateway.timestamp(execution.at())))));
          }
          reply(out, result.build());
        }
      }
    };
  }

  private BindableService market() {
    return new MarketDataServiceGrpc.MarketDataServiceImplBase() {
      public void getOrderBook(
          GetOrderBookRequest request, StreamObserver<GetOrderBookResponse> out) {
        checkInstrument(request.getInstrumentId());
        reply(
            out,
            GetOrderBookResponse.newBuilder()
                .setInstrumentUid(INSTRUMENT)
                .addAsks(
                    Order.newBuilder()
                        .setPrice(TInvestBrokerGateway.quotation(PRICE))
                        .setQuantity(100))
                .addBids(
                    Order.newBuilder()
                        .setPrice(TInvestBrokerGateway.quotation(PRICE))
                        .setQuantity(100))
                .build());
      }

      public void getTradingStatus(
          GetTradingStatusRequest request, StreamObserver<GetTradingStatusResponse> out) {
        checkInstrument(request.getInstrumentId());
        reply(
            out,
            GetTradingStatusResponse.newBuilder()
                .setInstrumentUid(INSTRUMENT)
                .setTradingStatus(SecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING)
                .setLimitOrderAvailableFlag(true)
                .setMarketOrderAvailableFlag(true)
                .setApiTradeAvailableFlag(true)
                .build());
      }
    };
  }

  private BindableService orders() {
    return new OrdersServiceGrpc.OrdersServiceImplBase() {
      public void getOrders(GetOrdersRequest request, StreamObserver<GetOrdersResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          checkAccount(request.getAccountId());
          var response = GetOrdersResponse.newBuilder();
          for (var order : orders.values())
            if (order.request.getAccountId().equals(request.getAccountId())
                && (order.status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW
                    || order.status
                        == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL))
              response.addOrders(order.state());
          reply(out, response.build());
        }
      }

      public void getMaxLots(GetMaxLotsRequest request, StreamObserver<GetMaxLotsResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          checkAccount(request.getAccountId());
          checkInstrument(request.getInstrumentId());
          reply(
              out,
              GetMaxLotsResponse.newBuilder()
                  .setCurrency("rub")
                  .setBuyLimits(
                      GetMaxLotsResponse.BuyLimitsView.newBuilder()
                          .setBuyMoneyAmount(TInvestBrokerGateway.quotation(ownMoney))
                          .setBuyMaxLots(maximumBuy))
                  .setSellLimits(
                      GetMaxLotsResponse.SellLimitsView.newBuilder().setSellMaxLots(maximumSell))
                  .build());
        }
      }

      public void getOrderPrice(
          GetOrderPriceRequest request, StreamObserver<GetOrderPriceResponse> out) {
        checkAccount(request.getAccountId());
        checkInstrument(request.getInstrumentId());
        var principal =
            TInvestBrokerGateway.decimal(
                    request.getPrice().getUnits(), request.getPrice().getNano())
                .multiply(BigDecimal.valueOf(request.getQuantity()));
        reply(
            out,
            GetOrderPriceResponse.newBuilder()
                .setLotsRequested(request.getQuantity())
                .setInitialOrderAmount(money(principal))
                .setTotalOrderAmount(money(principal.add(COMMISSION)))
                .setDealCommission(money(COMMISSION))
                .build());
      }

      public void postOrder(PostOrderRequest request, StreamObserver<PostOrderResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          if (!MIRROR.equals(request.getAccountId()))
            throw Status.PERMISSION_DENIED.asRuntimeException();
          checkInstrument(request.getInstrumentId());
          var id = UUID.fromString(request.getOrderId());
          if (orders.containsKey(id)) throw Status.ALREADY_EXISTS.asRuntimeException();
          var order = new SimulatedOrder(request);
          orders.put(id, order);
          fill(order, Math.min(request.getQuantity(), nextPostFill), Instant.now());
          nextPostFill = Long.MAX_VALUE;
          reply(
              out,
              PostOrderResponse.newBuilder()
                  .setOrderId(order.bankId)
                  .setInstrumentUid(INSTRUMENT)
                  .setDirection(request.getDirection())
                  .setLotsRequested(request.getQuantity())
                  .setLotsExecuted(order.filled())
                  .setExecutionReportStatus(order.status)
                  .build());
        }
      }

      public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          checkAccount(request.getAccountId());
          SimulatedOrder order =
              request.getOrderIdType() == OrderIdType.ORDER_ID_TYPE_REQUEST
                  ? requireOrder(UUID.fromString(request.getOrderId()))
                  : orders.values().stream()
                      .filter(o -> o.bankId.equals(request.getOrderId()))
                      .findFirst()
                      .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException());
          reply(out, order.state());
        }
      }

      public void cancelOrder(CancelOrderRequest request, StreamObserver<CancelOrderResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          if (!MIRROR.equals(request.getAccountId()))
            throw Status.PERMISSION_DENIED.asRuntimeException();
          var order =
              orders.values().stream()
                  .filter(o -> o.bankId.equals(request.getOrderId()))
                  .findFirst()
                  .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException());
          if (order.status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL)
            order.status = OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED;
          reply(
              out,
              CancelOrderResponse.newBuilder()
                  .setTime(TInvestBrokerGateway.timestamp(Instant.now()))
                  .build());
        }
      }
    };
  }

  private BindableService stream() {
    return new OrdersStreamServiceGrpc.OrdersStreamServiceImplBase() {
      public void tradesStream(
          TradesStreamRequest request, StreamObserver<TradesStreamResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          if (!request.getAccountsList().equals(List.of(SOURCE)))
            throw Status.PERMISSION_DENIED.asRuntimeException();
          streams.add(out);
          if (out instanceof ServerCallStreamObserver<TradesStreamResponse> server)
            server.setOnCancelHandler(() -> streams.remove(out));
          out.onNext(
              TradesStreamResponse.newBuilder()
                  .setSubscription(
                      SubscriptionResponse.newBuilder()
                          .setStatus(ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK)
                          .addAccounts(SOURCE))
                  .build());
          subscriptions.add(true);
        }
      }
    };
  }

  @Override
  public synchronized void close() throws Exception {
    closed = true;
    heartbeat.shutdownNow();
    for (var out : streams)
      try {
        out.onCompleted();
      } catch (RuntimeException ignored) {
      }
    streams.clear();
    tls.close();
  }
}
