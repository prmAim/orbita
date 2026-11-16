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

  public record Security(String id, BigDecimal price) {}

  private static final List<Security> SECURITIES =
      List.of(
          new Security("[ТЕСТ] SBER", new BigDecimal("100")),
          new Security("[ТЕСТ] GAZP", new BigDecimal("150")),
          new Security("[ТЕСТ] LKOH", new BigDecimal("200")),
          new Security("[ТЕСТ] ROSN", new BigDecimal("250")),
          new Security("[ТЕСТ] NVTK", new BigDecimal("300")),
          new Security("[ТЕСТ] GMKN", new BigDecimal("350")),
          new Security("[ТЕСТ] PLZL", new BigDecimal("400")),
          new Security("[ТЕСТ] TATN", new BigDecimal("450")),
          new Security("[ТЕСТ] SNGS", new BigDecimal("500")),
          new Security("[ТЕСТ] VTBR", new BigDecimal("550")),
          new Security("[ТЕСТ] MOEX", new BigDecimal("600")),
          new Security("[ТЕСТ] AFLT", new BigDecimal("650")),
          new Security("[ТЕСТ] CHMF", new BigDecimal("700")),
          new Security("[ТЕСТ] MAGN", new BigDecimal("750")),
          new Security("[ТЕСТ] ALRS", new BigDecimal("800")));

  public List<Security> securities() {
    return SECURITIES;
  }

  public record Rpc(String method, Object request, String remoteAddress, Instant receivedAt) {}

  private record Execution(
      String account,
      String instrument,
      String id,
      String order,
      BrokerGateway.Side side,
      long lots,
      BigDecimal price,
      Instant at) {}

  private final List<Rpc> audit = new CopyOnWriteArrayList<>();
  private final List<Execution> executions = new ArrayList<>();
  private final Map<UUID, SimulatedOrder> orders = new LinkedHashMap<>();
  private final Map<String, ExternalOrder> externalOrders = new LinkedHashMap<>();
  private final Set<UUID> unavailableRequestLookups = new HashSet<>(),
      lostCancelResponses = new HashSet<>();
  private boolean cashEnabled, loseNextPost;
  private final List<StreamObserver<PositionsStreamResponse>> cashStreams =
      new CopyOnWriteArrayList<>();

  private record PositionKey(String account, String instrument) {}

  private static final class Resources {
    BigDecimal money = new BigDecimal("10000");
    long buy = 100, sell = 100, nextFill = Long.MAX_VALUE;
  }

  private final Map<PositionKey, Long> positions = new HashMap<>();
  private final Map<String, Resources> resources = new LinkedHashMap<>();
  private final Map<String, BigDecimal> capitals = new HashMap<>();
  private final List<StreamObserver<TradesStreamResponse>> streams = new CopyOnWriteArrayList<>();
  private final BlockingQueue<Boolean> subscriptions = new LinkedBlockingQueue<>();
  private final ScheduledExecutorService heartbeat =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().daemon().name("whole-chain-bank-heartbeat").factory());
  private final BrokerTlsTestSupport.Simulator tls;
  private boolean closed;

  public static WholeChainBrokerSimulator open(Path certificateDirectory) throws Exception {
    Files.createDirectories(certificateDirectory);
    return new WholeChainBrokerSimulator(certificateDirectory);
  }

  private WholeChainBrokerSimulator(Path directory) throws Exception {
    for (var security : SECURITIES) resources.put(security.id(), new Resources());
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
      users(), instruments(), operations(), market(), orders(), stream(), cashStream()
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
        SECURITIES.stream().map(Security::id).collect(java.util.stream.Collectors.toSet()),
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
    setResources(INSTRUMENT, money, buyLots, sellLots);
  }

  public synchronized void setResources(
      String instrument, BigDecimal money, long buyLots, long sellLots) {
    checkInstrument(instrument);
    if (money == null || money.signum() < 0 || buyLots < 0 || sellLots < 0)
      throw new IllegalArgumentException("Nonnegative resources required");
    var value = resources.get(instrument);
    value.money = money;
    value.buy = buyLots;
    value.sell = sellLots;
  }

  public synchronized void setNextPostFill(long lots) {
    setNextPostFill(INSTRUMENT, lots);
  }

  public synchronized void setNextPostFill(String instrument, long lots) {
    checkInstrument(instrument);
    if (lots < 0) throw new IllegalArgumentException("Nonnegative fill required");
    resources.get(instrument).nextFill = lots;
  }

  public synchronized void enableCashCurrencies() {
    cashEnabled = true;
  }

  public synchronized void loseNextPostAndRequestLookup() {
    loseNextPost = true;
  }

  public synchronized void loseCancelResponseAndRequestLookup(UUID id) {
    requireOrder(id);
    lostCancelResponses.add(id);
  }

  public synchronized void createExternalOrder(
      String id,
      String instrument,
      BrokerGateway.Side side,
      long lots,
      BigDecimal limit,
      Instant created) {
    checkInstrument(instrument);
    positive(limit);
    if (id == null
        || id.isBlank()
        || side == null
        || lots <= 0
        || created == null
        || externalOrders.containsKey(id))
      throw new IllegalArgumentException("Unique complete external order required");
    externalOrders.put(id, new ExternalOrder(id, instrument, side, lots, limit, created));
  }

  public synchronized void fillExternalOrder(String id, long lots, BigDecimal price, Instant at) {
    var order = externalOrders.get(id);
    if (order == null) throw new IllegalArgumentException("Unknown external order");
    positive(price);
    if (lots <= 0 || lots > order.lots - order.filled() || at == null || order.cancelled)
      throw new IllegalArgumentException("Invalid external fill");
    var fill =
        new Execution(
            MIRROR,
            order.instrument,
            "[ТЕСТ] EXTERNAL-FILL " + id + " / " + (order.fills.size() + 1),
            id,
            order.side,
            lots,
            price,
            at);
    addExecution(fill);
    order.fills.add(fill);
  }

  public synchronized void cancelExternalOrderAtBroker(String id) {
    var order = externalOrders.get(id);
    if (order == null) throw new IllegalArgumentException("Unknown external order");
    order.cancelled = true;
  }

  private final class ExternalOrder {
    final String id, instrument;
    final BrokerGateway.Side side;
    final long lots;
    final BigDecimal limit;
    final Instant created;
    final List<Execution> fills = new ArrayList<>();
    boolean cancelled;

    ExternalOrder(
        String id,
        String instrument,
        BrokerGateway.Side side,
        long lots,
        BigDecimal limit,
        Instant created) {
      this.id = id;
      this.instrument = instrument;
      this.side = side;
      this.lots = lots;
      this.limit = limit;
      this.created = created;
    }

    long filled() {
      return fills.stream().mapToLong(Execution::lots).sum();
    }

    OrderState state() {
      var builder =
          OrderState.newBuilder()
              .setOrderId(id)
              .setInstrumentUid(instrument)
              .setTicker(instrument)
              .setDirection(direction(side))
              .setLotsRequested(lots)
              .setLotsExecuted(filled())
              .setOrderType(OrderType.ORDER_TYPE_LIMIT)
              .setInitialSecurityPrice(money(limit))
              .setOrderDate(TInvestBrokerGateway.timestamp(created))
              .setExecutedCommission(money(filled() > 0 ? COMMISSION : BigDecimal.ZERO))
              .setExecutionReportStatus(
                  cancelled
                      ? OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED
                      : filled() == lots
                          ? OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
                          : filled() > 0
                              ? OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL
                              : OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW);
      for (var fill : fills)
        builder.addStages(
            OrderStage.newBuilder()
                .setTradeId(fill.id())
                .setQuantity(fill.lots())
                .setPrice(money(fill.price()))
                .setExecutionTime(TInvestBrokerGateway.timestamp(fill.at())));
      return builder.build();
    }
  }

  private BigDecimal cashTotal(String account) {
    return capitals
        .get(account)
        .subtract(securitiesValue(account))
        .subtract(cashEnabled ? new BigDecimal("500") : BigDecimal.ZERO);
  }

  private static MoneyValue currencyMoney(String currency, BigDecimal value) {
    var number = TInvestBrokerGateway.quotation(value);
    return MoneyValue.newBuilder()
        .setCurrency(currency)
        .setUnits(number.getUnits())
        .setNano(number.getNano())
        .build();
  }

  private BindableService cashStream() {
    return new OperationsStreamServiceGrpc.OperationsStreamServiceImplBase() {
      public void positionsStream(
          PositionsStreamRequest request, StreamObserver<PositionsStreamResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          if (!request.getAccountsList().equals(List.of(MIRROR)) || !cashEnabled) {
            out.onError(Status.FAILED_PRECONDITION.asRuntimeException());
            return;
          }
          cashStreams.add(out);
          if (out instanceof ServerCallStreamObserver<PositionsStreamResponse> server)
            server.setOnCancelHandler(() -> cashStreams.remove(out));
          replyCash(out);
        }
      }
    };
  }

  private void replyCash(StreamObserver<PositionsStreamResponse> out) {
    out.onNext(
        PositionsStreamResponse.newBuilder()
            .setPosition(
                PositionData.newBuilder()
                    .setAccountId(MIRROR)
                    .setDate(TInvestBrokerGateway.timestamp(Instant.now()))
                    .addMoney(
                        PositionsMoney.newBuilder()
                            .setAvailableValue(
                                money(cashTotal(MIRROR).subtract(new BigDecimal("100"))))
                            .setBlockedValue(money(new BigDecimal("100"))))
                    .addMoney(
                        PositionsMoney.newBuilder()
                            .setAvailableValue(currencyMoney("usd", new BigDecimal("4")))
                            .setBlockedValue(currencyMoney("usd", BigDecimal.ONE))))
            .build());
  }

  public synchronized String brokerOrderId(UUID requestId) {
    return requireOrder(requestId).bankId;
  }

  public synchronized long position(String account) {
    return position(account, INSTRUMENT);
  }

  public synchronized long position(String account, String instrument) {
    checkAccount(account);
    checkInstrument(instrument);
    return positions.getOrDefault(new PositionKey(account, instrument), 0L);
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
    emitSource(INSTRUMENT, tradeId, sourceOrderId, side, lots, price, at);
  }

  public synchronized void emitSource(
      String instrument,
      String tradeId,
      String sourceOrderId,
      BrokerGateway.Side side,
      long lots,
      BigDecimal price,
      Instant at) {
    checkInstrument(instrument);
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
    var execution =
        new Execution(SOURCE, instrument, tradeId, sourceOrderId, side, lots, price, at);
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
                    .setInstrumentUid(execution.instrument())
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
    var key = new PositionKey(execution.account(), execution.instrument());
    long position = Math.addExact(positions.getOrDefault(key, 0L), delta);
    if (position < 0)
      throw new IllegalArgumentException("Simulator does not permit short positions");
    positions.put(key, position);
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
    security(id);
  }

  private Security security(String id) {
    return SECURITIES.stream()
        .filter(s -> s.id().equals(id))
        .findFirst()
        .orElseThrow(
            () ->
                Status.NOT_FOUND.withDescription("Synthetic instrument only").asRuntimeException());
  }

  private BigDecimal securitiesValue(String account) {
    return SECURITIES.stream()
        .map(s -> s.price().multiply(BigDecimal.valueOf(position(account, s.id()))))
        .reduce(BigDecimal.ZERO, BigDecimal::add);
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
    final Instant created = Instant.now();
    boolean dateConfirmed = true;
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
              .setInstrumentUid(request.getInstrumentId())
              .setTicker(request.getInstrumentId())
              .setOrderType(OrderType.ORDER_TYPE_LIMIT)
              .setInitialSecurityPrice(
                  money(
                      TInvestBrokerGateway.decimal(
                          request.getPrice().getUnits(), request.getPrice().getNano())))
              .setDirection(request.getDirection())
              .setLotsRequested(request.getQuantity())
              .setLotsExecuted(filled())
              .setExecutionReportStatus(status)
              .setExecutedCommission(money(filled() > 0 ? COMMISSION : BigDecimal.ZERO));
      if (dateConfirmed) state.setOrderDate(TInvestBrokerGateway.timestamp(created));
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
            order.request.getInstrumentId(),
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
                        .setUid(request.getId())
                        .setTicker(request.getId())
                        .setName("Синтетическая бумага " + request.getId())
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
          BigDecimal shares = securitiesValue(request.getAccountId());
          var portfolio =
              PortfolioResponse.newBuilder()
                  .setAccountId(request.getAccountId())
                  .setTotalAmountPortfolio(money(capitals.get(request.getAccountId())))
                  .setTotalAmountShares(money(shares))
                  .setTotalAmountCurrencies(
                      money(capitals.get(request.getAccountId()).subtract(shares)));
          for (var security : SECURITIES) {
            long lots = position(request.getAccountId(), security.id());
            if (lots != 0)
              portfolio.addPositions(
                  PortfolioPosition.newBuilder()
                      .setInstrumentUid(security.id())
                      .setInstrumentType("share")
                      .setQuantity(TInvestBrokerGateway.quotation(BigDecimal.valueOf(lots)))
                      .setCurrentPrice(money(security.price())));
          }
          reply(out, portfolio.build());
        }
      }

      public void getPositions(PositionsRequest request, StreamObserver<PositionsResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          checkAccount(request.getAccountId());
          var result = PositionsResponse.newBuilder().setAccountId(request.getAccountId());
          if (cashEnabled)
            result
                .addMoney(money(cashTotal(request.getAccountId())))
                .addBlocked(money(new BigDecimal("100")))
                .addMoney(currencyMoney("usd", new BigDecimal("5")))
                .addBlocked(currencyMoney("usd", BigDecimal.ONE));
          for (var security : SECURITIES) {
            long pieces = position(request.getAccountId(), security.id());
            if (pieces != 0)
              result.addSecurities(
                  PositionsSecurities.newBuilder()
                      .setInstrumentUid(security.id())
                      .setBalance(pieces)
                      .setBlocked(0));
          }
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
                    .setInstrumentUid(execution.instrument())
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
                .setInstrumentUid(request.getInstrumentId())
                .addAsks(
                    Order.newBuilder()
                        .setPrice(
                            TInvestBrokerGateway.quotation(
                                security(request.getInstrumentId()).price()))
                        .setQuantity(100))
                .addBids(
                    Order.newBuilder()
                        .setPrice(
                            TInvestBrokerGateway.quotation(
                                security(request.getInstrumentId()).price()))
                        .setQuantity(100))
                .build());
      }

      public void getTradingStatus(
          GetTradingStatusRequest request, StreamObserver<GetTradingStatusResponse> out) {
        checkInstrument(request.getInstrumentId());
        reply(
            out,
            GetTradingStatusResponse.newBuilder()
                .setInstrumentUid(request.getInstrumentId())
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
          var all = new ArrayList<OrderState>();
          for (var order : orders.values())
            if (order.request.getAccountId().equals(request.getAccountId())) all.add(order.state());
          if (request.getAccountId().equals(MIRROR))
            for (var external : externalOrders.values()) all.add(external.state());
          all.sort(
              Comparator.comparing(
                      (OrderState s) ->
                          s.hasOrderDate()
                              ? TInvestBrokerGateway.instant(s.getOrderDate())
                              : Instant.MAX)
                  .thenComparing(OrderState::getOrderId));
          for (var state : all) {
            if (request.hasAdvancedFilters()) {
              var filter = request.getAdvancedFilters();
              if (!state.hasOrderDate()) continue;
              var date = TInvestBrokerGateway.instant(state.getOrderDate());
              if (filter.hasFrom() && date.isBefore(TInvestBrokerGateway.instant(filter.getFrom()))
                  || filter.hasTo() && date.isAfter(TInvestBrokerGateway.instant(filter.getTo()))
                  || filter.getExecutionStatusCount() > 0
                      && !filter
                          .getExecutionStatusList()
                          .contains(state.getExecutionReportStatus())) continue;
            } else if (state.getExecutionReportStatus()
                    != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW
                && state.getExecutionReportStatus()
                    != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL) continue;
            response.addOrders(state);
            if (response.getOrdersCount() == 100) break;
          }
          reply(out, response.build());
        }
      }

      public void getMaxLots(GetMaxLotsRequest request, StreamObserver<GetMaxLotsResponse> out) {
        synchronized (WholeChainBrokerSimulator.this) {
          checkAccount(request.getAccountId());
          checkInstrument(request.getInstrumentId());
          var limit = resources.get(request.getInstrumentId());
          BigDecimal cash = cashTotal(request.getAccountId()).max(BigDecimal.ZERO).min(limit.money);
          BigDecimal price =
              TInvestBrokerGateway.decimal(
                  request.getPrice().getUnits(), request.getPrice().getNano());
          long maxBuy =
              price.signum() > 0
                  ? Math.min(limit.buy, cash.divideToIntegralValue(price).longValueExact())
                  : 0;
          reply(
              out,
              GetMaxLotsResponse.newBuilder()
                  .setCurrency("rub")
                  .setBuyLimits(
                      GetMaxLotsResponse.BuyLimitsView.newBuilder()
                          .setBuyMoneyAmount(TInvestBrokerGateway.quotation(cash))
                          .setBuyMaxLots(maxBuy))
                  .setSellLimits(
                      GetMaxLotsResponse.SellLimitsView.newBuilder()
                          .setSellMaxLots(
                              Math.min(
                                  limit.sell,
                                  position(request.getAccountId(), request.getInstrumentId()))))
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
          var limit = resources.get(request.getInstrumentId());
          fill(order, Math.min(request.getQuantity(), limit.nextFill), Instant.now());
          limit.nextFill = Long.MAX_VALUE;
          if (loseNextPost) {
            loseNextPost = false;
            order.dateConfirmed = false;
            unavailableRequestLookups.add(id);
            out.onError(
                Status.UNAVAILABLE
                    .withDescription("[ТЕСТ] Accepted order reply lost")
                    .asRuntimeException());
            return;
          }
          reply(
              out,
              PostOrderResponse.newBuilder()
                  .setOrderId(order.bankId)
                  .setInstrumentUid(request.getInstrumentId())
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
          if (request.getOrderIdType() == OrderIdType.ORDER_ID_TYPE_REQUEST) {
            var id = UUID.fromString(request.getOrderId());
            if (unavailableRequestLookups.contains(id)) {
              out.onError(
                  Status.UNAVAILABLE
                      .withDescription("[ТЕСТ] Request lookup unavailable")
                      .asRuntimeException());
              return;
            }
            reply(out, requireOrder(id).state());
            return;
          }
          var order =
              orders.values().stream()
                  .filter(o -> o.bankId.equals(request.getOrderId()))
                  .findFirst()
                  .orElse(null);
          if (order == null) {
            var external = externalOrders.get(request.getOrderId());
            if (external == null) {
              out.onError(Status.NOT_FOUND.asRuntimeException());
              return;
            }
            reply(out, external.state());
            return;
          }
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
          var id = UUID.fromString(order.request.getOrderId());
          if (lostCancelResponses.remove(id)) {
            unavailableRequestLookups.add(id);
            out.onError(
                Status.UNAVAILABLE
                    .withDescription("[ТЕСТ] Cancel reply lost")
                    .asRuntimeException());
            return;
          }
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
    for (var out : cashStreams)
      try {
        out.onCompleted();
      } catch (RuntimeException ignored) {
      }
    cashStreams.clear();
    tls.close();
  }
}
