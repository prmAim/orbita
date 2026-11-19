package ru.orbita.broker;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import ru.tinkoff.piapi.contract.v1.*;
import ru.ttech.piapi.core.connector.ServiceStubFactory;
import ru.ttech.piapi.core.connector.exception.ServiceRuntimeException;

/** Official SDK 1.51 adapter. Mutations deliberately use only the plain synchronous service. */
public final class TInvestBrokerGateway implements BrokerGateway {
  private final ServiceStubFactory factory;
  private final String sourceAccount;
  private final String mirrorAccount;
  private final Clock clock;
  private final BrokerQualifications qualifications;
  private final BrokerConnectivityEvents connectivity;
  private final String receiptClockDomain;

  public TInvestBrokerGateway(
      ServiceStubFactory factory,
      String sourceAccount,
      String mirrorAccount,
      Clock clock,
      BrokerQualifications qualifications,
      BrokerConnectivityEvents connectivity) {
    this.factory = Objects.requireNonNull(factory);
    if (sourceAccount == null
        || sourceAccount.isBlank()
        || mirrorAccount == null
        || mirrorAccount.isBlank()
        || sourceAccount.equals(mirrorAccount))
      throw new IllegalArgumentException("Distinct source and mirror accounts required");
    this.sourceAccount = sourceAccount;
    this.mirrorAccount = mirrorAccount;
    this.clock = Objects.requireNonNull(clock);
    this.qualifications = Objects.requireNonNull(qualifications);
    this.connectivity = Objects.requireNonNull(connectivity);
    this.receiptClockDomain = BrokerReceiptClock.domain();
  }

  @Override
  public AutoCloseable observeCash(
      java.util.function.Consumer<AccountRead.Snapshot<AccountRead.Cash>> sink) {
    return new AccountCashFeed(factory, mirrorAccount, clock, sink);
  }

  @Override
  public boolean buyTotalCostNondecreasing(
      String accountId, String instrumentId, String currency) {
    return qualifications.buyTotalCostNondecreasingQualified(
        accountId, instrumentId, currency, clock.instant());
  }

  @Override
  public AccountRead.Snapshot<AccountRead.Order> readOrderHistory(
      Instant from, Instant through, java.util.Collection<String> retainedBrokerOrderIds) {
    return new TInvestAccountReader(factory, mirrorAccount, clock)
        .history(from, through, retainedBrokerOrderIds);
  }

  @Override
  public AccountRead.Snapshot<AccountRead.Execution> readExecutions(Instant from, Instant through) {
    var started = clock.instant();
    var history = mirrorHistory(from, through);
    var executions =
        history.executions().stream()
            .map(
                e ->
                    new AccountRead.Execution(
                        e.tradeId(),
                        null,
                        e.instrumentId(),
                        e.side(),
                        e.lots(),
                        "lots",
                        new AccountRead.Price(
                            e.price(), e.currency(), "currency per instrument unit"),
                        e.executedAt(),
                        null,
                        "GetOperationsByCursor.tradesInfo; order link and individual commission unavailable"))
            .toList();
    return new AccountRead.Snapshot<>(
        executions, started, clock.instant(), history.complete(), history.gaps());
  }

  @Override
  public AccountRead.Snapshot<AccountRead.Order> readOrders(
      java.util.Collection<String> retainedBrokerOrderIds) {
    return new TInvestAccountReader(factory, mirrorAccount, clock).orders(retainedBrokerOrderIds);
  }

  @Override
  public AccountRead.Holdings readHoldings() {
    return new TInvestAccountReader(factory, mirrorAccount, clock).holdings();
  }

  @Override
  public OrderEvidence post(OrderRequest request) {
    validateMutation(request);
    var payload =
        PostOrderRequest.newBuilder()
            .setAccountId(request.accountId())
            .setOrderId(request.requestId().toString())
            .setInstrumentId(request.instrumentId())
            .setDirection(direction(request.side()))
            .setQuantity(request.lots())
            .setOrderType(OrderType.ORDER_TYPE_LIMIT)
            .setPrice(quotation(request.limitPrice()))
            .setPriceType(PriceType.PRICE_TYPE_CURRENCY)
            .setConfirmMarginTrade(false)
            .build();
    OrderSdkEvidence raw = null;
    try {
      var reply =
          factory
              .newSyncService(OrdersServiceGrpc::newBlockingStub)
              .callSyncMethod(
                  stub -> stub.withDeadlineAfter(5, TimeUnit.SECONDS).postOrder(payload));
      long receivedNanos = BrokerReceiptClock.nanoTime();
      Instant receivedAt = clock.instant();
      raw = OrderSdkEvidence.post(reply, receivedAt);
      if (reply.getOrderId().isBlank()
          || !reply.getOrderRequestId().isEmpty()
              && !reply.getOrderRequestId().equals(request.requestId().toString())
          || reply.getLotsRequested() != request.lots()
          || reply.getLotsExecuted() < 0
          || reply.getLotsExecuted() > request.lots()
          || reply.getDirection() != direction(request.side())
          || !reply.getInstrumentUid().equals(request.instrumentId()))
        return unknown(request, "PostOrder response identity mismatch", raw);
      return new OrderEvidence(
          request.requestId(),
          request.accountId(),
          reply.getOrderId(),
          request.instrumentId(),
          request.side(),
          reply.getLotsRequested(),
          reply.getLotsExecuted(),
          status(reply.getExecutionReportStatus()),
          List.of(),
          receivedAt,
          "PostOrder response",
          reason(status(reply.getExecutionReportStatus())),
          receivedNanos,
          receiptClockDomain,
          raw);
    } catch (BrokerPersistenceFailure failure) {
      throw failure;
    } catch (RuntimeException failure) {
      if (raw != null) return unknown(request, "PostOrder response requires reconciliation", raw);
      var failed =
          OrderSdkEvidence.failure(OrderSdkEvidence.Source.POST_ORDER, failure, clock.instant());
      recordTransportFailure(failure);
      return unknown(request, "PostOrder outcome requires reconciliation", failed);
    }
  }

  @Override
  public OrderEvidence reconcile(OrderRequest request) {
    validateMutation(request);
    OrderSdkEvidence raw = null;
    try {
      var payload =
          GetOrderStateRequest.newBuilder()
              .setAccountId(request.accountId())
              .setOrderId(request.requestId().toString())
              .setOrderIdType(OrderIdType.ORDER_ID_TYPE_REQUEST)
              .setPriceType(PriceType.PRICE_TYPE_CURRENCY)
              .build();
      var receipt =
          readWithReceipt(
              () ->
                  factory
                      .newSyncService(OrdersServiceGrpc::newBlockingStub)
                      .callSyncMethod(
                          stub ->
                              stub.withDeadlineAfter(5, TimeUnit.SECONDS).getOrderState(payload)));
      var reply = receipt.payload();
      raw = OrderSdkEvidence.state(reply, receipt.wallAt());
      if (!reply.getOrderRequestId().equals(request.requestId().toString())
          || reply.getOrderId().isBlank()
          || !reply.getInstrumentUid().equals(request.instrumentId())
          || reply.getDirection() != direction(request.side())
          || reply.getLotsRequested() != request.lots()
          || reply.getLotsExecuted() < 0
          || reply.getLotsExecuted() > request.lots())
        return unknown(request, "GetOrderState identity/quantity mismatch", raw);
      var fills = new ArrayList<Fill>();
      var tradeIds = new java.util.HashSet<String>();
      for (var stage : reply.getStagesList()) {
        if (stage.getTradeId().isBlank()
            || !tradeIds.add(stage.getTradeId())
            || stage.getQuantity() <= 0
            || !stage.hasPrice()
            || !stage.hasExecutionTime()) return unknown(request, "Incomplete stage evidence", raw);
        fills.add(
            new Fill(
                stage.getTradeId(),
                BigDecimal.valueOf(stage.getQuantity()),
                decimal(stage.getPrice().getUnits(), stage.getPrice().getNano()),
                stage.getPrice().getCurrency(),
                instant(stage.getExecutionTime())));
      }
      long stageLots =
          fills.stream().map(Fill::lots).reduce(BigDecimal.ZERO, BigDecimal::add).longValueExact();
      if (stageLots != reply.getLotsExecuted())
        return unknown(request, "Stage quantity differs from executed lots", raw);
      return new OrderEvidence(
          request.requestId(),
          request.accountId(),
          reply.getOrderId(),
          request.instrumentId(),
          request.side(),
          reply.getLotsRequested(),
          reply.getLotsExecuted(),
          status(reply.getExecutionReportStatus()),
          fills,
          receipt.wallAt(),
          "GetOrderState by original UUID; stages in lots",
          reason(status(reply.getExecutionReportStatus())),
          receipt.monotonicNanos(),
          receiptClockDomain,
          raw);
    } catch (BrokerPersistenceFailure failure) {
      throw failure;
    } catch (RuntimeException failure) {
      return unknown(
          request,
          "Order lookup unresolved",
          raw != null
              ? raw
              : OrderSdkEvidence.failure(
                  OrderSdkEvidence.Source.GET_ORDER_STATE, failure, clock.instant()));
    }
  }

  @Override
  public OrderEvidence cancel(OrderRequest request, String brokerOrderId) {
    validateMutation(request);
    if (brokerOrderId == null || brokerOrderId.isBlank())
      throw new IllegalArgumentException("Broker order ID required");
    try {
      var payload =
          CancelOrderRequest.newBuilder()
              .setAccountId(request.accountId())
              .setOrderId(brokerOrderId)
              .build();
      factory
          .newSyncService(OrdersServiceGrpc::newBlockingStub)
          .callSyncMethod(stub -> stub.withDeadlineAfter(5, TimeUnit.SECONDS).cancelOrder(payload));
    } catch (BrokerPersistenceFailure failure) {
      throw failure;
    } catch (RuntimeException failure) {
      var failed =
          OrderSdkEvidence.failure(OrderSdkEvidence.Source.CANCEL_ORDER, failure, clock.instant());
      recordTransportFailure(failure);
      return unknown(request, "Cancellation outcome requires reconciliation", failed);
    }
    return reconcile(request);
  }

  @Override
  public CommissionEvidence commission(OrderRequest request) {
    validateMutation(request);
    var evidence = reconcile(request);
    if (evidence.status() != OrderStatus.FILLED
        && evidence.status() != OrderStatus.CANCELLED
        && evidence.status() != OrderStatus.REJECTED)
      return new CommissionEvidence(
          request.accountId(),
          request.requestId(),
          null,
          null,
          false,
          clock.instant(),
          "Order final result unresolved");
    try {
      var payload =
          GetOrderStateRequest.newBuilder()
              .setAccountId(request.accountId())
              .setOrderId(request.requestId().toString())
              .setOrderIdType(OrderIdType.ORDER_ID_TYPE_REQUEST)
              .setPriceType(PriceType.PRICE_TYPE_CURRENCY)
              .build();
      var state =
          read(
              () ->
                  factory
                      .newSyncService(OrdersServiceGrpc::newBlockingStub)
                      .callSyncMethod(
                          stub ->
                              stub.withDeadlineAfter(5, TimeUnit.SECONDS).getOrderState(payload)));
      boolean identity =
          state.getOrderRequestId().equals(request.requestId().toString())
              && state.getOrderId().equals(evidence.brokerOrderId())
              && state.getInstrumentUid().equals(request.instrumentId())
              && state.getDirection() == direction(request.side())
              && state.getLotsRequested() == request.lots()
              && state.getLotsExecuted() == evidence.lotsExecuted()
              && status(state.getExecutionReportStatus()) == evidence.status();
      var fee = state.getExecutedCommission();
      BigDecimal amount =
          state.hasExecutedCommission() ? decimal(fee.getUnits(), fee.getNano()) : null;
      boolean qualified =
          identity
              && amount != null
              && amount.signum() >= 0
              && qualifications.actualCommissionTotalConfirmed()
              && qualifications.scopeMatches(
                  request.accountId(), request.instrumentId(), fee.getCurrency(), clock.instant());
      return new CommissionEvidence(
          request.accountId(),
          request.requestId(),
          amount,
          fee.getCurrency(),
          qualified,
          clock.instant(),
          qualified
              ? qualifications.evidenceReference()
              : "Actual commission composition/finality not independently qualified");
    } catch (BrokerPersistenceFailure failure) {
      throw failure;
    } catch (RuntimeException unresolved) {
      return new CommissionEvidence(
          request.accountId(),
          request.requestId(),
          null,
          null,
          false,
          clock.instant(),
          "Actual commission lookup unresolved");
    }
  }

  @Override
  public Instrument instrument(String instrumentId) {
    var payload =
        InstrumentRequest.newBuilder()
            .setIdType(InstrumentIdType.INSTRUMENT_ID_TYPE_UID)
            .setId(instrumentId)
            .build();
    var reply =
        read(
            () ->
                factory
                    .newSyncService(InstrumentsServiceGrpc::newBlockingStub)
                    .callSyncMethod(
                        stub ->
                            stub.withDeadlineAfter(5, TimeUnit.SECONDS).getInstrumentBy(payload)));
    var value = reply.getInstrument();
    var increment =
        decimal(value.getMinPriceIncrement().getUnits(), value.getMinPriceIncrement().getNano());
    if (value.getInstrumentKind() == InstrumentType.INSTRUMENT_TYPE_BOND) {
      var bond =
          read(() ->
                  factory
                      .newSyncService(InstrumentsServiceGrpc::newBlockingStub)
                      .callSyncMethod(
                          stub -> stub.withDeadlineAfter(5, TimeUnit.SECONDS).bondBy(payload)))
              .getInstrument();
      if (!bond.getUid().equals(instrumentId)
          || !bond.hasNominal()
          || !bond.getNominal().getCurrency().equalsIgnoreCase(value.getCurrency()))
        throw new IllegalStateException("Bond nominal/settlement currency unconfirmed");
      increment =
          decimal(bond.getMinPriceIncrement().getUnits(), bond.getMinPriceIncrement().getNano())
              .multiply(decimal(bond.getNominal().getUnits(), bond.getNominal().getNano()))
              .movePointLeft(2);
    } else if (value.getInstrumentKind() == InstrumentType.INSTRUMENT_TYPE_FUTURES) {
      var future =
          read(() ->
                  factory
                      .newSyncService(InstrumentsServiceGrpc::newBlockingStub)
                      .callSyncMethod(
                          stub -> stub.withDeadlineAfter(5, TimeUnit.SECONDS).futureBy(payload)))
              .getInstrument();
      if (!future.getUid().equals(instrumentId)
          || !future.getCurrency().equalsIgnoreCase(value.getCurrency())
          || !future.hasMinPriceIncrementAmount())
        throw new IllegalStateException("Future settlement price increment unconfirmed");
      increment =
          decimal(
              future.getMinPriceIncrementAmount().getUnits(),
              future.getMinPriceIncrementAmount().getNano());
    }
    if (!value.getUid().equals(instrumentId)
        || value.getLot() <= 0
        || increment.signum() <= 0
        || value.getCurrency().isBlank())
      throw new IllegalStateException("Incomplete instrument metadata");
    return new Instrument(
        value.getUid(),
        value.getLot(),
        increment,
        value.getCurrency(),
        value.getBuyAvailableFlag(),
        value.getSellAvailableFlag(),
        value.getApiTradeAvailableFlag());
  }

  @Override
  public CapitalEvidence capital(String accountId) {
    validateAccount(accountId);
    var started = clock.instant();
    var payload =
        PortfolioRequest.newBuilder()
            .setAccountId(accountId)
            .setCurrency(PortfolioRequest.CurrencyRequest.RUB)
            .build();
    var receipt =
        readWithReceipt(
            () ->
                factory
                    .newSyncService(OperationsServiceGrpc::newBlockingStub)
                    .callSyncMethod(
                        stub -> stub.withDeadlineAfter(5, TimeUnit.SECONDS).getPortfolio(payload)));
    var reply = receipt.payload();
    if (!reply.getAccountId().equals(accountId)
        || !reply.hasTotalAmountPortfolio()
        || !reply.getTotalAmountPortfolio().getCurrency().equalsIgnoreCase("rub"))
      return new CapitalEvidence(
          accountId,
          null,
          started,
          receipt.wallAt(),
          false,
          "Incomplete RUB portfolio response",
          receipt.monotonicNanos(),
          receiptClockDomain);
    var total = reply.getTotalAmountPortfolio();
    return new CapitalEvidence(
        accountId,
        decimal(total.getUnits(), total.getNano()),
        started,
        receipt.wallAt(),
        qualifications.capitalQualified(accountId, clock.instant()),
        qualifications.evidenceReference(),
        receipt.monotonicNanos(),
        receiptClockDomain);
  }

  @Override
  public ResourceEvidence resources(OrderRequest request) {
    validateMutation(request);
    Instant started = clock.instant();
    var instrument = instrument(request.instrumentId());
    if (request.limitPrice().remainder(instrument.priceIncrement()).signum() != 0)
      throw new IllegalArgumentException("Limit price is not a multiple of the price increment");
    var limitsRequest =
        GetMaxLotsRequest.newBuilder()
            .setAccountId(request.accountId())
            .setInstrumentId(request.instrumentId())
            .setPrice(quotation(request.limitPrice()))
            .build();
    var service = factory.newSyncService(OrdersServiceGrpc::newBlockingStub);
    var limits =
        read(
            () ->
                service.callSyncMethod(
                    stub -> stub.withDeadlineAfter(5, TimeUnit.SECONDS).getMaxLots(limitsRequest)));
    var priceRequest =
        GetOrderPriceRequest.newBuilder()
            .setAccountId(request.accountId())
            .setInstrumentId(request.instrumentId())
            .setQuantity(request.lots())
            .setDirection(direction(request.side()))
            .setPrice(quotation(request.limitPrice()))
            .build();
    var price =
        read(
            () ->
                service.callSyncMethod(
                    stub ->
                        stub.withDeadlineAfter(5, TimeUnit.SECONDS).getOrderPrice(priceRequest)));
    boolean complete =
        limits.hasBuyLimits()
            && limits.getBuyLimits().hasBuyMoneyAmount()
            && limits.hasSellLimits()
            && limits.getCurrency().equalsIgnoreCase(instrument.currency())
            && price.getLotsRequested() == request.lots();
    BigDecimal ownMoney =
        complete
            ? decimal(
                limits.getBuyLimits().getBuyMoneyAmount().getUnits(),
                limits.getBuyLimits().getBuyMoneyAmount().getNano())
            : null;
    // Component meanings/overlap differ by instrument and tariff. No arithmetic on unqualified
    // components.
    BigDecimal preliminary = null;
    boolean scoped =
        qualifications.resourcesQualified(
            request.accountId(), instrument.id(), instrument.currency(), clock.instant());
    if (scoped
        && price.hasTotalOrderAmount()
        && price.hasInitialOrderAmount()
        && price.getTotalOrderAmount().getCurrency().equalsIgnoreCase(instrument.currency())
        && price.getInitialOrderAmount().getCurrency().equalsIgnoreCase(instrument.currency())) {
      preliminary =
          decimal(price.getTotalOrderAmount().getUnits(), price.getTotalOrderAmount().getNano())
              .subtract(
                  decimal(
                      price.getInitialOrderAmount().getUnits(),
                      price.getInitialOrderAmount().getNano()));
      if (preliminary.signum() < 0) preliminary = null;
    }
    boolean qualified =
        complete
            && preliminary != null
            && scoped
            && ownMoney.signum() >= 0
            && instrument.apiAvailable()
            && (request.side() == Side.BUY
                ? instrument.buyAvailable()
                : instrument.sellAvailable());
    BigDecimal reserve = scoped ? qualifications.additionalCommissionReserve() : null;
    BigDecimal rubTotal =
        qualified && instrument.currency().equalsIgnoreCase("rub")
            ? request
                .limitPrice()
                .multiply(BigDecimal.valueOf(request.lots()))
                .multiply(BigDecimal.valueOf(instrument.lotSize()))
                .add(preliminary)
                .add(reserve)
            : null;
    return new ResourceEvidence(
        request.accountId(),
        instrument,
        ownMoney,
        limits.hasBuyLimits() ? limits.getBuyLimits().getBuyMaxLots() : 0,
        limits.hasSellLimits() ? limits.getSellLimits().getSellMaxLots() : 0,
        preliminary,
        reserve,
        started,
        clock.instant(),
        qualified,
        qualified
            ? qualifications.evidenceReference()
            : "Resource qualification: "
                + String.join(
                    "; ",
                    qualifications.resourceProblems(
                        request.accountId(),
                        instrument.id(),
                        instrument.currency(),
                        clock.instant()))
                + (complete ? "" : "; incomplete broker response"),
        rubTotal);
  }

  @Override
  public java.util.Optional<BigDecimal> bestOppositePrice(String instrumentId, Side side) {
    if (instrumentId == null || instrumentId.isBlank() || side == null)
      return java.util.Optional.empty();
    try {
      var request =
          GetOrderBookRequest.newBuilder().setInstrumentId(instrumentId).setDepth(1).build();
      var reply =
          read(
              () ->
                  factory
                      .newSyncService(MarketDataServiceGrpc::newBlockingStub)
                      .callSyncMethod(
                          stub ->
                              stub.withDeadlineAfter(5, TimeUnit.SECONDS).getOrderBook(request)));
      if (!reply.getInstrumentUid().equals(instrumentId)) return java.util.Optional.empty();
      var levels = side == Side.BUY ? reply.getAsksList() : reply.getBidsList();
      if (levels.isEmpty() || levels.getFirst().getQuantity() <= 0 || !levels.getFirst().hasPrice())
        return java.util.Optional.empty();
      var price =
          decimal(levels.getFirst().getPrice().getUnits(), levels.getFirst().getPrice().getNano());
      return price.signum() > 0 ? java.util.Optional.of(price) : java.util.Optional.empty();
    } catch (BrokerPersistenceFailure failure) {
      throw failure;
    } catch (RuntimeException unavailable) {
      return java.util.Optional.empty();
    }
  }

  @Override
  public History history(Instant from, Instant through) {
    return readHistory(sourceAccount, from, through);
  }

  @Override
  public History mirrorHistory(Instant from, Instant through) {
    return readHistory(mirrorAccount, from, through);
  }

  private History readHistory(String accountId, Instant from, Instant through) {
    if (from == null || through == null || from.isAfter(through))
      throw new IllegalArgumentException("Ordered history interval required");
    var result = new java.util.LinkedHashMap<String, SourceExecution>();
    var gaps = new ArrayList<String>();
    var metadata = new java.util.HashMap<String, Instrument>();
    Instant window = from;
    do {
      Instant end =
          window.plusSeconds(86400).isBefore(through) ? window.plusSeconds(86400) : through;
      String cursor = "";
      var visited = new java.util.HashSet<String>();
      do {
        if (!visited.add(cursor)) {
          gaps.add("History cursor cycle");
          break;
        }
        var payload =
            GetOperationsByCursorRequest.newBuilder()
                .setAccountId(accountId)
                .setFrom(timestamp(window))
                .setTo(timestamp(end))
                .setLimit(1000)
                .setCursor(cursor)
                .setState(OperationState.OPERATION_STATE_EXECUTED)
                .setWithoutTrades(false)
                .build();
        GetOperationsByCursorResponse page;
        try {
          page =
              read(
                  () ->
                      factory
                          .newSyncService(OperationsServiceGrpc::newBlockingStub)
                          .callSyncMethod(
                              stub ->
                                  stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                                      .getOperationsByCursor(payload)));
        } catch (BrokerPersistenceFailure failure) {
          throw failure;
        } catch (RuntimeException failure) {
          gaps.add("History page unavailable");
          break;
        }
        for (var item : page.getItemsList()) {
          Side side =
              switch (item.getType()) {
                case OPERATION_TYPE_BUY, OPERATION_TYPE_BUY_CARD -> Side.BUY;
                case OPERATION_TYPE_SELL -> Side.SELL;
                default -> null;
              };
          if (side == null) continue;
          if (!accountId.equals(item.getBrokerAccountId()) || item.getInstrumentUid().isBlank()) {
            gaps.add("History account/instrument mismatch");
            continue;
          }
          if (!item.hasTradesInfo() || item.getTradesInfo().getTradesCount() == 0) {
            gaps.add("History executed operation has no individual trades: " + item.getId());
            continue;
          }
          for (var trade : item.getTradesInfo().getTradesList()) {
            try {
              if (trade.getNum().isBlank()
                  || trade.getQuantity() <= 0
                  || !trade.hasPrice()
                  || !trade.hasDate()) throw new IllegalArgumentException("Incomplete trade");
              var info = metadata.computeIfAbsent(item.getInstrumentUid(), this::instrument);
              var time = instant(trade.getDate());
              if (time.isBefore(from) || time.isAfter(through)) continue;
              var price = decimal(trade.getPrice().getUnits(), trade.getPrice().getNano());
              if (price.signum() <= 0
                  || !trade.getPrice().getCurrency().equalsIgnoreCase(info.currency()))
                throw new IllegalArgumentException("Price/currency mismatch");
              var execution =
                  new SourceExecution(
                      accountId,
                      trade.getNum(),
                      "",
                      info.id(),
                      side,
                      BigDecimal.valueOf(trade.getQuantity())
                          .divide(BigDecimal.valueOf(info.lotSize())),
                      price,
                      info.currency(),
                      time,
                      clock.instant());
              var previous = result.putIfAbsent(trade.getNum(), execution);
              if (previous != null
                  && (!previous.instrumentId().equals(execution.instrumentId())
                      || previous.side() != execution.side()
                      || previous.lots().compareTo(execution.lots()) != 0
                      || previous.price().compareTo(execution.price()) != 0
                      || !previous.executedAt().equals(execution.executedAt())))
                gaps.add("Conflicting history trade identity: " + trade.getNum());
            } catch (BrokerPersistenceFailure failure) {
              throw failure;
            } catch (RuntimeException incomplete) {
              gaps.add("Incomplete history execution: " + trade.getNum());
            }
          }
        }
        if (!page.getHasNext()) break;
        if (page.getNextCursor().isBlank()) {
          gaps.add("History has_next without cursor");
          break;
        }
        cursor = page.getNextCursor();
      } while (true);
      window = end;
    } while (window.isBefore(through));
    return new History(new ArrayList<>(result.values()), gaps, from, through);
  }

  @Override
  public PositionEvidence positions(String accountId) {
    validateAccount(accountId);
    var started = clock.instant();
    var payload = PositionsRequest.newBuilder().setAccountId(accountId).build();
    var reply =
        read(
            () ->
                factory
                    .newSyncService(OperationsServiceGrpc::newBlockingStub)
                    .callSyncMethod(
                        stub -> stub.withDeadlineAfter(5, TimeUnit.SECONDS).getPositions(payload)));
    var positions = new ArrayList<Position>();
    reply
        .getSecuritiesList()
        .forEach(
            p ->
                positions.add(
                    new Position(
                        p.getInstrumentUid(),
                        BigDecimal.valueOf(p.getBalance()),
                        BigDecimal.valueOf(p.getBlocked()))));
    reply
        .getFuturesList()
        .forEach(
            p ->
                positions.add(
                    new Position(
                        p.getInstrumentUid(),
                        BigDecimal.valueOf(p.getBalance()),
                        BigDecimal.valueOf(p.getBlocked()))));
    reply
        .getOptionsList()
        .forEach(
            p ->
                positions.add(
                    new Position(
                        p.getInstrumentUid(),
                        BigDecimal.valueOf(p.getBalance()),
                        BigDecimal.valueOf(p.getBlocked()))));
    String currencyDetail = "currency UID/total coverage unavailable";
    try {
      var portfolioRequest =
          PortfolioRequest.newBuilder()
              .setAccountId(accountId)
              .setCurrency(PortfolioRequest.CurrencyRequest.RUB)
              .build();
      var portfolio =
          read(
              () ->
                  factory
                      .newSyncService(OperationsServiceGrpc::newBlockingStub)
                      .callSyncMethod(
                          stub ->
                              stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                                  .getPortfolio(portfolioRequest)));
      if (portfolio.getAccountId().equals(accountId)) {
        var currencyTotals = new java.util.LinkedHashMap<String, BigDecimal>();
        var conflicts = new java.util.HashSet<String>();
        for (var p : portfolio.getPositionsList()) {
          if (!p.getInstrumentType().equalsIgnoreCase("currency")
              || p.getInstrumentUid().isBlank()
              || !p.hasQuantity()) continue;
          var total = decimal(p.getQuantity().getUnits(), p.getQuantity().getNano());
          var previous = currencyTotals.putIfAbsent(p.getInstrumentUid(), total);
          if (previous != null && previous.compareTo(total) != 0)
            conflicts.add(p.getInstrumentUid());
        }
        for (var entry : currencyTotals.entrySet()) {
          if (conflicts.contains(entry.getKey())
              || positions.stream().anyMatch(p -> p.instrumentId().equals(entry.getKey())))
            continue;
          positions.add(new Position(entry.getKey(), null, null, entry.getValue(), false));
        }
        currencyDetail =
            "GetPortfolio currency totals by exact bank UID; available/blocked split unqualified";
      }
    } catch (BrokerPersistenceFailure failure) {
      throw failure;
    } catch (RuntimeException unavailableCurrencyMapping) {
      /* Missing currency UID is explicitly not evidence of zero. */
    }
    boolean complete =
        reply.getAccountId().equals(accountId)
            && !reply.getLimitsLoadingInProgress()
            && positions.stream().allMatch(p -> !p.instrumentId().isBlank());
    return new PositionEvidence(
        accountId,
        positions,
        started,
        clock.instant(),
        complete,
        complete
            ? "GetPositions available balances and separate blocks; " + currencyDetail
            : "Positions incomplete/loading or account mismatch");
  }

  @Override
  public boolean positionAbsentMeansZero(String instrumentId, PositionEvidence evidence) {
    if (evidence == null || !evidence.complete()) return false;
    try {
      validateAccount(evidence.accountId());
      var payload =
          InstrumentRequest.newBuilder()
              .setIdType(InstrumentIdType.INSTRUMENT_ID_TYPE_UID)
              .setId(instrumentId)
              .build();
      var value =
          read(() ->
                  factory
                      .newSyncService(InstrumentsServiceGrpc::newBlockingStub)
                      .callSyncMethod(
                          stub ->
                              stub.withDeadlineAfter(5, TimeUnit.SECONDS).getInstrumentBy(payload)))
              .getInstrument();
      if (!value.getUid().equals(instrumentId)) return false;
      return switch (value.getInstrumentKind().name()) {
        case "INSTRUMENT_TYPE_SHARE",
            "INSTRUMENT_TYPE_BOND",
            "INSTRUMENT_TYPE_ETF",
            "INSTRUMENT_TYPE_FUTURES",
            "INSTRUMENT_TYPE_OPTION",
            "INSTRUMENT_TYPE_SP" ->
            true;
        default -> false;
      };
    } catch (BrokerPersistenceFailure failure) {
      throw failure;
    } catch (RuntimeException unqualified) {
      return false;
    }
  }

  @Override
  public AutoCloseable observe(SourceFeedJournal journal) {
    verifyAccounts();
    var source = new SourceExecutionFeed(factory, this, sourceAccount, clock, journal);
    if (!journal.supportsMirrorOrders()) return source;
    var mirror = new MirrorOrderFeed(factory, mirrorAccount, clock, journal);
    return () -> {
      try {
        mirror.close();
      } finally {
        source.close();
      }
    };
  }

  public void verifyAccounts() {
    var response =
        read(
            () ->
                factory
                    .newSyncService(UsersServiceGrpc::newBlockingStub)
                    .callSyncMethod(
                        stub ->
                            stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                                .getAccounts(GetAccountsRequest.getDefaultInstance())));
    for (String id : List.of(sourceAccount, mirrorAccount)) {
      var matches = response.getAccountsList().stream().filter(a -> a.getId().equals(id)).toList();
      if (matches.size() != 1
          || matches.getFirst().getStatus() != AccountStatus.ACCOUNT_STATUS_OPEN)
        throw new IllegalStateException(
            "Configured "
                + (id.equals(sourceAccount) ? "source" : "mirror")
                + " account is not uniquely open");
      var access = matches.getFirst().getAccessLevel();
      if (access != AccessLevel.ACCOUNT_ACCESS_LEVEL_FULL_ACCESS
          && (!id.equals(sourceAccount) || access != AccessLevel.ACCOUNT_ACCESS_LEVEL_READ_ONLY))
        throw new IllegalStateException("Configured account access is insufficient");
    }
  }

  private void validateMutation(OrderRequest request) {
    if (!mirrorAccount.equals(request.accountId()))
      throw new IllegalArgumentException("Only mirror account accepts mutations");
    if (request.requestId() == null
        || request.instrumentId() == null
        || request.instrumentId().isBlank()
        || request.side() == null
        || request.lots() <= 0
        || request.limitPrice() == null
        || request.limitPrice().signum() <= 0)
      throw new IllegalArgumentException("Complete positive limit order required");
  }

  private void validateAccount(String accountId) {
    if (!sourceAccount.equals(accountId) && !mirrorAccount.equals(accountId))
      throw new IllegalArgumentException("Account outside configured pair");
  }

  private static OrderDirection direction(Side side) {
    return side == Side.BUY
        ? OrderDirection.ORDER_DIRECTION_BUY
        : OrderDirection.ORDER_DIRECTION_SELL;
  }

  static OrderStatus status(OrderExecutionReportStatus status) {
    return status(
        status == null || status == OrderExecutionReportStatus.UNRECOGNIZED
            ? null
            : status.getNumber());
  }

  /** Shared numeric SDK status conversion; optional/future values remain UNKNOWN. */
  static OrderStatus status(Integer value) {
    return switch (value == null ? 0 : value) {
      case OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW_VALUE -> OrderStatus.NEW;
      case OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL_VALUE ->
          OrderStatus.PARTIALLY_FILLED;
      case OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL_VALUE -> OrderStatus.FILLED;
      case OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED_VALUE ->
          OrderStatus.CANCELLED;
      case OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED_VALUE ->
          OrderStatus.REJECTED;
      default -> OrderStatus.UNKNOWN;
    };
  }

  private static RejectionReason reason(OrderStatus status) {
    return status == OrderStatus.REJECTED || status == OrderStatus.CANCELLED
        ? RejectionReason.OTHER
        : RejectionReason.NONE;
  }

  private OrderEvidence unknown(OrderRequest request, String detail, OrderSdkEvidence raw) {
    return new OrderEvidence(
        request.requestId(),
        request.accountId(),
        null,
        request.instrumentId(),
        request.side(),
        request.lots(),
        0,
        OrderStatus.UNKNOWN,
        List.of(),
        raw.receivedAt(),
        detail,
        RejectionReason.NONE,
        null,
        null,
        raw);
  }

  static BigDecimal decimal(long units, int nanos) {
    if (Math.abs((long) nanos) >= 1_000_000_000L
        || units > 0 && nanos < 0
        || units < 0 && nanos > 0) throw new IllegalArgumentException("Malformed broker decimal");
    return BigDecimal.valueOf(units).add(BigDecimal.valueOf(nanos, 9));
  }

  static Quotation quotation(BigDecimal number) {
    var scaled = number.setScale(9, java.math.RoundingMode.UNNECESSARY);
    var units = scaled.longValue();
    return Quotation.newBuilder()
        .setUnits(units)
        .setNano(scaled.subtract(BigDecimal.valueOf(units)).movePointRight(9).intValueExact())
        .build();
  }

  static Instant instant(com.google.protobuf.Timestamp timestamp) {
    if (timestamp.getNanos() < 0
        || timestamp.getNanos() >= 1_000_000_000
        || timestamp.getSeconds() < -62135596800L
        || timestamp.getSeconds() > 253402300799L)
      throw new IllegalArgumentException("Invalid broker execution timestamp");
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  static com.google.protobuf.Timestamp timestamp(Instant instant) {
    return com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  private record RpcReceipt<T>(T payload, Instant wallAt, long monotonicNanos) {}

  private <T> T read(java.util.function.Supplier<T> rpc) {
    return readWithReceipt(rpc).payload();
  }

  private <T> RpcReceipt<T> readWithReceipt(java.util.function.Supplier<T> rpc) {
    T reply;
    try {
      reply = rpc.get();
    } catch (ServiceRuntimeException failure) {
      recordTransportFailure(failure);
      throw failure;
    }
    // Raw receipt precedes every observer/database side effect. The monotonic point is first.
    long receivedNanos = BrokerReceiptClock.nanoTime();
    Instant receivedAt = clock.instant();
    // Persistence is deliberately outside the remote-call catch boundary.
    durableObservation("successful broker read", connectivity::readSucceeded);
    return new RpcReceipt<>(reply, receivedAt, receivedNanos);
  }

  private void recordTransportFailure(RuntimeException failure) {
    io.grpc.Status.Code code =
        failure instanceof ServiceRuntimeException sdk
            ? sdk.getErrorType()
            : failure instanceof io.grpc.StatusRuntimeException grpc
                ? grpc.getStatus().getCode()
                : null;
    BrokerConnectivityEvents.Cause cause =
        code == io.grpc.Status.Code.UNAVAILABLE
            ? BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE
            : code == io.grpc.Status.Code.DEADLINE_EXCEEDED
                ? BrokerConnectivityEvents.Cause.RPC_DEADLINE_EXCEEDED
                : null;
    if (cause != null)
      durableObservation("broker connection loss", () -> connectivity.connectionLost(cause));
  }

  private static void durableObservation(String operation, Runnable callback) {
    try {
      callback.run();
    } catch (BrokerPersistenceFailure failure) {
      throw failure;
    } catch (RuntimeException failure) {
      throw new BrokerPersistenceFailure(operation, failure);
    }
  }

  @Override
  public void close() {
    factory.getChannel().shutdownNow();
  }
}
