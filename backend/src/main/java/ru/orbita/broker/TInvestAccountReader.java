package ru.orbita.broker;

import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import ru.tinkoff.piapi.contract.v1.*;
import ru.ttech.piapi.core.connector.ServiceStubFactory;

/** Display-only SDK reader; deliberately has no post/cancel/replace path. */
final class TInvestAccountReader {
  private final ServiceStubFactory factory;
  private final String account;
  private final Clock clock;

  TInvestAccountReader(ServiceStubFactory factory, String account, Clock clock) {
    this.factory = factory;
    this.account = account;
    this.clock = clock;
  }

  AccountRead.Snapshot<AccountRead.Order> orders(Collection<String> retainedIds) {
    var started = clock.instant();
    var gaps = new ArrayList<String>();
    var states = new LinkedHashMap<String, OrderState>();
    try {
      var response =
          factory
              .newSyncService(OrdersServiceGrpc::newBlockingStub)
              .callSyncMethod(
                  s ->
                      s.withDeadlineAfter(5, TimeUnit.SECONDS)
                          .getOrders(GetOrdersRequest.newBuilder().setAccountId(account).build()));
      if (response.getOrdersCount() >= 100)
        gaps.add("GetOrders reached response cap; active membership unconfirmed");
      for (var state : response.getOrdersList()) {
        if (state.getOrderId().isBlank()) {
          gaps.add("Active order without broker ID");
          continue;
        }
        var previous = states.putIfAbsent(state.getOrderId(), state);
        if (previous != null && !previous.equals(state))
          gaps.add("Conflicting active order: " + state.getOrderId());
      }
    } catch (RuntimeException unavailable) {
      gaps.add("GetOrders unavailable: " + unavailable.getClass().getSimpleName());
    }
    for (String id : retainedIds) {
      if (id == null || id.isBlank())
        throw new IllegalArgumentException("Retained broker ID required");
      states.putIfAbsent(id, null);
    }
    var result = new ArrayList<AccountRead.Order>();
    for (var entry : states.entrySet()) {
      var state = entry.getValue();
      boolean confirmed = true;
      try {
        var request =
            GetOrderStateRequest.newBuilder()
                .setAccountId(account)
                .setOrderId(entry.getKey())
                .setOrderIdType(OrderIdType.ORDER_ID_TYPE_EXCHANGE)
                .setPriceType(PriceType.PRICE_TYPE_CURRENCY)
                .build();
        var detail =
            factory
                .newSyncService(OrdersServiceGrpc::newBlockingStub)
                .callSyncMethod(
                    s -> s.withDeadlineAfter(5, TimeUnit.SECONDS).getOrderState(request));
        if (!detail.getOrderId().equals(entry.getKey()))
          throw new IllegalArgumentException("Order identity mismatch");
        state = detail;
      } catch (RuntimeException unavailable) {
        confirmed = false;
        gaps.add("GetOrderState unavailable: " + entry.getKey());
      }
      if (state == null) continue;
      try {
        result.add(order(state, confirmed));
      } catch (RuntimeException invalid) {
        gaps.add("Invalid order: " + entry.getKey());
      }
    }
    return new AccountRead.Snapshot<>(result, started, clock.instant(), gaps.isEmpty(), gaps);
  }

  AccountRead.Snapshot<AccountRead.Order> history(
      Instant from, Instant through, Collection<String> retainedIds) {
    if (from == null || through == null || from.isAfter(through))
      throw new IllegalArgumentException("Ordered interval required");
    var started = clock.instant();
    var gaps = new ArrayList<String>();
    var ids = new LinkedHashSet<String>(retainedIds);
    // SDK only documents final orders for today. UTC midnight is the conservative intersection
    // of the UTC and Moscow trading day; outside it completeness cannot be attested.
    var midnight =
        started
            .atZone(java.time.ZoneOffset.UTC)
            .toLocalDate()
            .atStartOfDay(java.time.ZoneOffset.UTC)
            .toInstant();
    if (from.isBefore(midnight))
      gaps.add(
          "GetOrders final-order history is limited to today; older order inventory unavailable");
    if (through.isAfter(started)) gaps.add("Future history interval unavailable");
    Instant window = from.isBefore(midnight) ? midnight : from;
    Instant end = through.isAfter(started) ? started : through;
    while (window.isBefore(end)) {
      Instant until = window.plusSeconds(3600).isBefore(end) ? window.plusSeconds(3600) : end;
      historyWindow(window, until, ids, gaps, 0);
      window = until;
    }
    var orders = orders(ids);
    gaps.addAll(orders.limitations());
    var items =
        orders.items().stream()
            .filter(
                o ->
                    o.createdAt() == null
                        || !o.createdAt().isBefore(from) && !o.createdAt().isAfter(through))
            .toList();
    return new AccountRead.Snapshot<>(items, started, clock.instant(), gaps.isEmpty(), gaps);
  }

  private void historyWindow(
      Instant from, Instant to, Set<String> ids, List<String> gaps, int depth) {
    try {
      var filters =
          GetOrdersRequest.GetOrdersRequestFilters.newBuilder()
              .setFrom(TInvestBrokerGateway.timestamp(from))
              .setTo(TInvestBrokerGateway.timestamp(to))
              .addAllExecutionStatus(
                  List.of(
                      OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW,
                      OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL,
                      OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL,
                      OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED,
                      OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED));
      var request =
          GetOrdersRequest.newBuilder().setAccountId(account).setAdvancedFilters(filters).build();
      var response =
          factory
              .newSyncService(OrdersServiceGrpc::newBlockingStub)
              .callSyncMethod(s -> s.withDeadlineAfter(5, TimeUnit.SECONDS).getOrders(request));
      for (var order : response.getOrdersList())
        if (!order.getOrderId().isBlank()) ids.add(order.getOrderId());
        else gaps.add("History order missing broker ID");
      if (response.getOrdersCount() >= 100) {
        long nanos = java.time.Duration.between(from, to).toNanos();
        if (depth >= 12 || nanos <= 1_000_000_000L) {
          gaps.add("History response cap in bounded interval");
          return;
        }
        Instant mid = from.plusNanos(nanos / 2);
        historyWindow(from, mid, ids, gaps, depth + 1);
        historyWindow(mid, to, ids, gaps, depth + 1);
      }
    } catch (RuntimeException unavailable) {
      gaps.add("GetOrders history window unavailable");
    }
  }

  AccountRead.Holdings holdings() {
    var started = clock.instant();
    var securities = new ArrayList<AccountRead.Security>();
    var cash = new ArrayList<AccountRead.Cash>();
    var securityGaps = new ArrayList<String>();
    var cashGaps = new ArrayList<String>();
    try {
      var portfolio =
          factory
              .newSyncService(OperationsServiceGrpc::newBlockingStub)
              .callSyncMethod(
                  s ->
                      s.withDeadlineAfter(5, TimeUnit.SECONDS)
                          .getPortfolio(
                              PortfolioRequest.newBuilder().setAccountId(account).build()));
      if (!account.equals(portfolio.getAccountId()))
        throw new IllegalArgumentException("Portfolio account mismatch");
      var ids = new HashSet<String>();
      for (var position : portfolio.getPositionsList()) {
        if (position.getInstrumentType().equals("currency")) continue;
        if (position.getInstrumentUid().isBlank()
            || !position.hasQuantity()
            || !ids.add(position.getInstrumentUid())) {
          securityGaps.add("Incomplete/duplicate portfolio position");
          continue;
        }
        var pieces =
            TInvestBrokerGateway.decimal(
                position.getQuantity().getUnits(), position.getQuantity().getNano());
        if (pieces.signum() == 0) continue;
        Integer lot = null;
        String name = "";
        String ticker = position.getTicker();
        BigDecimal lots = null;
        String limitation = "";
        try {
          var request =
              InstrumentRequest.newBuilder()
                  .setIdType(InstrumentIdType.INSTRUMENT_ID_TYPE_UID)
                  .setId(position.getInstrumentUid())
                  .build();
          InstrumentResponse instrumentResponse =
              factory
                  .newSyncService(InstrumentsServiceGrpc::newBlockingStub)
                  .callSyncMethod(
                      s -> s.withDeadlineAfter(5, TimeUnit.SECONDS).getInstrumentBy(request));
          var info = instrumentResponse.getInstrument();
          if (!info.getUid().equals(position.getInstrumentUid()) || info.getLot() <= 0)
            throw new IllegalArgumentException("Invalid instrument lot");
          lot = info.getLot();
          name = info.getName();
          ticker = info.getTicker();
          lots = pieces.divide(BigDecimal.valueOf(lot));
          if (position.hasQuantityLots()
              && lots.compareTo(
                      TInvestBrokerGateway.decimal(
                          position.getQuantityLots().getUnits(),
                          position.getQuantityLots().getNano()))
                  != 0) {
            lots = null;
            limitation = "Portfolio quantityLots disagrees with quantity/lot";
          }
        } catch (RuntimeException unavailable) {
          limitation = "Instrument lot/name unavailable";
        }
        securities.add(
            new AccountRead.Security(
                position.getInstrumentUid(),
                ticker,
                name,
                lot,
                pieces,
                lots,
                "GetPortfolio.positions.quantity; GetInstrumentBy.lot",
                limitation));
      }
    } catch (RuntimeException unavailable) {
      securityGaps.add("GetPortfolio unavailable or incomplete");
    }
    Instant securitiesReceived = clock.instant();
    try {
      var positions =
          factory
              .newSyncService(OperationsServiceGrpc::newBlockingStub)
              .callSyncMethod(
                  s ->
                      s.withDeadlineAfter(5, TimeUnit.SECONDS)
                          .getPositions(
                              PositionsRequest.newBuilder().setAccountId(account).build()));
      if (!account.equals(positions.getAccountId()) || positions.getLimitsLoadingInProgress())
        throw new IllegalArgumentException("Positions incomplete");
      var money = new LinkedHashMap<String, BigDecimal>();
      var blocked = new LinkedHashMap<String, BigDecimal>();
      for (var value : positions.getMoneyList()) putMoney(money, value);
      for (var value : positions.getBlockedList()) putMoney(blocked, value);
      var currencies = new LinkedHashSet<>(money.keySet());
      currencies.addAll(blocked.keySet());
      for (String currency : currencies)
        cash.add(
            new AccountRead.Cash(
                currency,
                null,
                null,
                blocked.get(currency),
                money.get(currency),
                null,
                "GetPositions.money; GetPositions.blocked",
                "GetPositions.money does not confirm free/total composition; absent blocked is unknown; guarantee excluded"));
    } catch (RuntimeException unavailable) {
      cashGaps.add("GetPositions unavailable or incomplete");
    }
    return new AccountRead.Holdings(
        new AccountRead.Snapshot<>(
            securities, started, securitiesReceived, securityGaps.isEmpty(), securityGaps),
        new AccountRead.Snapshot<>(cash, started, clock.instant(), cashGaps.isEmpty(), cashGaps));
  }

  private static void putMoney(Map<String, BigDecimal> values, MoneyValue value) {
    if (value.getCurrency().isBlank()
        || values.putIfAbsent(
                value.getCurrency().toLowerCase(Locale.ROOT),
                TInvestBrokerGateway.decimal(value.getUnits(), value.getNano()))
            != null) throw new IllegalArgumentException("Invalid/duplicate currency");
  }

  private AccountRead.Order order(OrderState state, boolean confirmed) {
    var gaps = new ArrayList<String>();
    var fills = new LinkedHashMap<String, AccountRead.Execution>();
    BrokerGateway.Side side =
        switch (state.getDirection()) {
          case ORDER_DIRECTION_BUY -> BrokerGateway.Side.BUY;
          case ORDER_DIRECTION_SELL -> BrokerGateway.Side.SELL;
          default -> null;
        };
    if (side == null
        || state.getInstrumentUid().isBlank()
        || state.getLotsRequested() < 0
        || state.getLotsExecuted() < 0
        || state.getLotsExecuted() > state.getLotsRequested())
      throw new IllegalArgumentException("Invalid order identity/quantity");
    for (var stage : state.getStagesList()) {
      if (stage.getTradeId().isBlank()
          || stage.getQuantity() <= 0
          || !stage.hasPrice()
          || !stage.hasExecutionTime()) {
        gaps.add("Incomplete execution stage");
        continue;
      }
      var price = price(stage.getPrice());
      if (price.currency().isBlank() || price.value().signum() <= 0) {
        gaps.add("Invalid execution price/currency");
        continue;
      }
      var fill =
          new AccountRead.Execution(
              stage.getTradeId(),
              state.getOrderId(),
              state.getInstrumentUid(),
              side,
              BigDecimal.valueOf(stage.getQuantity()),
              "lots",
              price,
              TInvestBrokerGateway.instant(stage.getExecutionTime()),
              null,
              confirmed
                  ? "GetOrderState.stages; individual commission unavailable"
                  : "GetOrders.stages; unit/completeness unconfirmed");
      var previous = fills.putIfAbsent(stage.getTradeId(), fill);
      if (previous != null && !previous.equals(fill))
        gaps.add("Conflicting execution: " + stage.getTradeId());
    }
    var quantity =
        fills.values().stream()
            .map(AccountRead.Execution::quantity)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
    boolean complete =
        confirmed
            && gaps.isEmpty()
            && quantity.compareTo(BigDecimal.valueOf(state.getLotsExecuted())) == 0;
    if (!complete) gaps.add("Stages do not confirm the whole executed quantity");
    AccountRead.Price average = null;
    if (complete && quantity.signum() > 0) {
      var currencies = fills.values().stream().map(f -> f.price().currency()).distinct().toList();
      if (currencies.size() == 1) {
        var numerator =
            fills.values().stream()
                .map(f -> f.price().value().multiply(f.quantity()))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        average =
            new AccountRead.Price(
                numerator.divide(quantity, MathContext.DECIMAL128),
                currencies.getFirst(),
                "currency per instrument unit");
      } else {
        complete = false;
        gaps.add("Mixed execution currencies");
      }
    }
    var status =
        switch (state.getExecutionReportStatus()) {
          case EXECUTION_REPORT_STATUS_NEW -> BrokerGateway.OrderStatus.NEW;
          case EXECUTION_REPORT_STATUS_PARTIALLYFILL -> BrokerGateway.OrderStatus.PARTIALLY_FILLED;
          case EXECUTION_REPORT_STATUS_FILL -> BrokerGateway.OrderStatus.FILLED;
          case EXECUTION_REPORT_STATUS_CANCELLED -> BrokerGateway.OrderStatus.CANCELLED;
          case EXECUTION_REPORT_STATUS_REJECTED -> BrokerGateway.OrderStatus.REJECTED;
          default -> BrokerGateway.OrderStatus.UNKNOWN;
        };
    return new AccountRead.Order(
        state.getOrderId(),
        state.getOrderRequestId(),
        state.getInstrumentUid(),
        state.getTicker(),
        side,
        state.getLotsRequested(),
        state.getLotsExecuted(),
        status,
        state.getOrderType().name(),
        state.hasOrderDate() ? TInvestBrokerGateway.instant(state.getOrderDate()) : null,
        confirmed
                && state.getOrderType() == OrderType.ORDER_TYPE_LIMIT
                && state.hasInitialSecurityPrice()
            ? price(state.getInitialSecurityPrice())
            : null,
        new ArrayList<>(fills.values()),
        complete,
        average,
        AccountRead.Origin.UNKNOWN,
        gaps);
  }

  private static AccountRead.Price price(MoneyValue value) {
    return new AccountRead.Price(
        TInvestBrokerGateway.decimal(value.getUnits(), value.getNano()),
        value.getCurrency().toLowerCase(Locale.ROOT),
        "currency per instrument unit");
  }
}
