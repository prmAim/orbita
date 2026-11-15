package ru.orbita.broker;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import ru.tinkoff.piapi.contract.v1.*;
import ru.ttech.piapi.core.connector.ServiceStubFactory;

/**
 * Acknowledged subscriptions plus explicit history recovery. All journal callbacks run in one lane.
 */
final class SourceExecutionFeed implements AutoCloseable {
  private final ServiceStubFactory factory;
  private final BrokerGateway gateway;
  private final String source;
  private final Clock clock;
  private final SourceFeedJournal journal;
  private final ScheduledExecutorService lane =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().daemon().name("orbita-source-feed").factory());
  private final Map<String, BrokerGateway.SourceExecution> pending = new LinkedHashMap<>();
  private volatile ClientCallStreamObserver<TradesStreamRequest> call;
  private boolean acknowledged;
  private volatile boolean closed;
  private boolean reconnecting;
  private volatile long lastFrameNanos;
  private long connectedAtNanos;
  private long generation;

  SourceExecutionFeed(
      ServiceStubFactory factory,
      BrokerGateway gateway,
      String source,
      Clock clock,
      SourceFeedJournal journal) {
    this.factory = factory;
    this.gateway = gateway;
    this.source = source;
    this.clock = clock;
    this.journal = journal;
    lane.execute(this::connect);
    lane.scheduleWithFixedDelay(this::watchdog, 1, 1, TimeUnit.SECONDS);
  }

  private void connect() {
    if (closed) return;
    acknowledged = false;
    reconnecting = false;
    lastFrameNanos = System.nanoTime();
    connectedAtNanos = lastFrameNanos;
    final long attempt = ++generation;
    try {
      journal.gap("Source subscription awaiting acknowledgement and history recovery");
      var request =
          TradesStreamRequest.newBuilder().addAccounts(source).setPingDelayMs(5000).build();
      factory
          .newAsyncService(OrdersStreamServiceGrpc::newStub)
          .getStub()
          .tradesStream(
              request,
              new ClientResponseObserver<TradesStreamRequest, TradesStreamResponse>() {
                @Override
                public void beforeStart(ClientCallStreamObserver<TradesStreamRequest> stream) {
                  call = stream;
                }

                @Override
                public void onNext(TradesStreamResponse frame) {
                  long arrivedNanos = BrokerReceiptClock.nanoTime();
                  Instant arrivedAt = clock.instant();
                  SourceFeedJournal.TimeWindow captured;
                  try {
                    captured =
                        journal
                            .arrivalTimeWindow(arrivedNanos, BrokerReceiptClock.domain())
                            .orElse(null);
                  } catch (RuntimeException failure) {
                    dispatch(
                        () -> {
                          throw failure;
                        });
                    return;
                  }
                  lastFrameNanos = arrivedNanos;
                  dispatch(
                      () -> {
                        if (attempt == generation) receive(frame, arrivedAt, captured);
                      });
                }

                @Override
                public void onError(Throwable error) {
                  dispatch(
                      () -> {
                        if (attempt == generation && !reconnecting) {
                          reportTransportFailure(error);
                          disconnect("Source stream disconnected");
                        }
                      });
                }

                @Override
                public void onCompleted() {
                  dispatch(
                      () -> {
                        if (attempt == generation && !reconnecting) {
                          if (acknowledged)
                            journal.connectionLost(BrokerConnectivityEvents.Cause.STREAM_CLOSED);
                          disconnect("Source stream completed");
                        }
                      });
                }
              });
    } catch (BrokerPersistenceFailure failure) {
      journal.persistenceUnavailable();
      disconnect("Source connection/journal unavailable");
    } catch (RuntimeException failure) {
      if (isPersistenceFailure(failure)) journal.persistenceUnavailable();
      try {
        reportTransportFailure(failure);
      } finally {
        disconnect("Source connection/journal unavailable");
      }
    }
  }

  private void dispatch(Runnable work) {
    if (!lane.isShutdown())
      lane.execute(
          () -> {
            if (!closed)
              try {
                work.run();
              } catch (BrokerPersistenceFailure failure) {
                journal.persistenceUnavailable();
                disconnect("Source persistence unavailable");
              } catch (RuntimeException failure) {
                if (isPersistenceFailure(failure)) journal.persistenceUnavailable();
                try {
                  journal.gap(
                      "Source observation/journal failure: " + failure.getClass().getSimpleName());
                } finally {
                  disconnect("Source observation suspended");
                }
              }
          });
  }

  private void receive(
      TradesStreamResponse frame, Instant arrivedAt, SourceFeedJournal.TimeWindow capturedWindow) {
    if (frame.hasSubscription()) {
      var subscription = frame.getSubscription();
      if (subscription.getStatus() != ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK
          || !subscription.getAccountsList().equals(java.util.List.of(source))) {
        disconnect("Source subscription rejected or account mismatch");
        return;
      }
      acknowledged = true;
      recover();
    }
    if (!frame.hasOrderTrades()) return;
    if (!acknowledged) {
      journal.gap("Trade received before subscription acknowledgement");
      return;
    }
    journal.readSucceeded();
    var order = frame.getOrderTrades();
    if (!source.equals(order.getAccountId())
        || order.getOrderId().isBlank()
        || order.getInstrumentUid().isBlank()) {
      journal.gap("Source stream identity mismatch");
      return;
    }
    BrokerGateway.Side side;
    if (order.getDirection() == OrderDirection.ORDER_DIRECTION_BUY) side = BrokerGateway.Side.BUY;
    else if (order.getDirection() == OrderDirection.ORDER_DIRECTION_SELL)
      side = BrokerGateway.Side.SELL;
    else {
      journal.gap("Source trade direction missing");
      return;
    }
    var instrument = gateway.instrument(order.getInstrumentUid());
    for (var trade : order.getTradesList()) {
      if (trade.getTradeId().isBlank() || trade.getQuantity() <= 0 || !trade.hasPrice()) {
        journal.gap("Incomplete source trade identity/quantity/price");
        continue;
      }
      var previous = journal.remembered(source, trade.getTradeId());
      Instant received =
          journal.firstReceived(source, trade.getTradeId(), arrivedAt, capturedWindow);
      Instant executed = null;
      try {
        if (trade.hasDateTime()) executed = TInvestBrokerGateway.instant(trade.getDateTime());
      } catch (IllegalArgumentException invalid) {
        /* Exact history recovery only. */
      }
      var execution =
          new BrokerGateway.SourceExecution(
              source,
              trade.getTradeId(),
              order.getOrderId(),
              instrument.id(),
              side,
              BigDecimal.valueOf(trade.getQuantity())
                  .divide(BigDecimal.valueOf(instrument.lotSize())),
              TInvestBrokerGateway.decimal(trade.getPrice().getUnits(), trade.getPrice().getNano()),
              instrument.currency(),
              executed,
              received);
      if (executed == null) {
        if (previous.isPresent() && previous.get().executedAt() != null) continue;
        journal.remember(execution);
        pending.putIfAbsent(execution.tradeId(), execution);
        recover();
      } else if (previous.isPresent() && previous.get().executedAt() == null) {
        switch (searchWindow(execution)) {
          case WITHIN -> {
            journal.acceptRecovered(execution);
            pending.remove(execution.tradeId());
          }
          case EXPIRED -> {
            journal.skip(previous.get(), "Source timestamp recovery expired");
            pending.remove(execution.tradeId());
          }
          case UNKNOWN ->
              journal.gap(
                  "Original timestamp recovery window cannot be established "
                      + execution.tradeId());
        }
      } else {
        journal.remember(execution);
        journal.accept(execution);
      }
    }
  }

  private void recover() {
    Instant boundary = journal.observationBoundary();
    if (boundary == null) {
      journal.gap("Observation boundary has not been saved");
      return;
    }
    for (var execution : journal.pendingTimestampRecovery())
      pending.putIfAbsent(execution.tradeId(), execution);
    var expired =
        pending.values().stream().filter(e -> searchWindow(e) == SearchWindow.EXPIRED).toList();
    for (var execution : expired) {
      journal.skip(execution, "Source timestamp recovery expired");
      pending.remove(execution.tradeId());
    }
    var history = gateway.history(boundary, clock.instant());
    boolean complete = history.complete();
    for (String gap : history.gaps()) journal.gap(gap);
    for (var trade : history.executions()) {
      String orderId = journal.sourceOrderId(source, trade.tradeId()).orElse("");
      if (orderId.isBlank()) {
        journal.gap("Source order identity unresolved for history trade " + trade.tradeId());
        complete = false;
        continue;
      }
      Instant received = journal.firstReceived(source, trade.tradeId(), clock.instant());
      var candidate = journal.remembered(source, trade.tradeId()).orElse(null);
      if (candidate != null
          && (!candidate.instrumentId().equals(trade.instrumentId())
              || candidate.side() != trade.side()
              || candidate.lots().compareTo(trade.lots()) != 0
              || candidate.price().compareTo(trade.price()) != 0
              || !candidate.currency().equalsIgnoreCase(trade.currency()))) {
        journal.gap("Ambiguous source timestamp recovery " + trade.tradeId());
        complete = false;
        continue;
      }
      if (candidate != null && candidate.executedAt() == null) {
        var window = searchWindow(candidate);
        if (window == SearchWindow.EXPIRED) {
          pending.remove(trade.tradeId());
          journal.skip(candidate, "Source timestamp recovery expired");
          continue;
        }
        if (window == SearchWindow.UNKNOWN) {
          journal.gap(
              "Original timestamp recovery window cannot be established " + trade.tradeId());
          complete = false;
          continue;
        }
      }
      var resolved =
          new BrokerGateway.SourceExecution(
              source,
              trade.tradeId(),
              orderId,
              trade.instrumentId(),
              trade.side(),
              trade.lots(),
              trade.price(),
              trade.currency(),
              trade.executedAt(),
              received);
      journal.acceptRecovered(resolved);
      pending.remove(trade.tradeId());
    }
    if (complete && pending.isEmpty()) journal.recovered();
  }

  private enum SearchWindow {
    WITHIN,
    EXPIRED,
    UNKNOWN
  }

  private SearchWindow searchWindow(BrokerGateway.SourceExecution execution) {
    var first = journal.receiptTimeWindow(source, execution.tradeId());
    var current = journal.currentTimeWindow();
    if (first.isEmpty() || current.isEmpty()) return SearchWindow.UNKNOWN;
    if (current.get().earliest().isAfter(first.get().latest().plusSeconds(60)))
      return SearchWindow.EXPIRED;
    if (!current.get().latest().isAfter(first.get().earliest().plusSeconds(60))
        && !current.get().earliest().isBefore(first.get().latest())) return SearchWindow.WITHIN;
    return SearchWindow.UNKNOWN;
  }

  private void watchdog() {
    if (closed || reconnecting) return;
    try {
      long maximum = acknowledged ? TimeUnit.SECONDS.toNanos(15) : TimeUnit.SECONDS.toNanos(5);
      if (System.nanoTime() - (acknowledged ? lastFrameNanos : connectedAtNanos) > maximum) {
        if (acknowledged) journal.connectionLost(BrokerConnectivityEvents.Cause.HEARTBEAT_TIMEOUT);
        disconnect("Source acknowledgement/heartbeat deadline exceeded");
      } else if (!pending.isEmpty()) recover();
    } catch (BrokerPersistenceFailure failure) {
      journal.persistenceUnavailable();
      disconnect("Source persistence unavailable");
    } catch (RuntimeException failure) {
      if (isPersistenceFailure(failure)) journal.persistenceUnavailable();
      disconnect("Source recovery failed");
    }
  }

  private void disconnect(String reason) {
    if (closed || reconnecting) return;
    reconnecting = true;
    acknowledged = false;
    if (call != null) call.cancel(reason, null);
    try {
      journal.gap(reason);
    } catch (RuntimeException unavailableJournal) {
      if (isPersistenceFailure(unavailableJournal)) journal.persistenceUnavailable();
      /* No dependent trading proceeds without the application journal. */
    } finally {
      if (!closed) lane.schedule(this::connect, 1, TimeUnit.SECONDS);
    }
  }

  private void reportTransportFailure(Throwable failure) {
    var code = io.grpc.Status.fromThrowable(failure).getCode();
    try {
      if (code == io.grpc.Status.Code.UNAVAILABLE)
        journal.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
      else if (code == io.grpc.Status.Code.DEADLINE_EXCEEDED)
        journal.connectionLost(BrokerConnectivityEvents.Cause.RPC_DEADLINE_EXCEEDED);
    } catch (RuntimeException persistence) {
      if (isPersistenceFailure(persistence)) journal.persistenceUnavailable();
      throw persistence;
    }
  }

  private static boolean isPersistenceFailure(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof BrokerPersistenceFailure
          || cause instanceof org.springframework.dao.DataAccessException
          || cause instanceof org.springframework.transaction.TransactionException
          || cause instanceof jakarta.persistence.PersistenceException) return true;
    }
    return false;
  }

  @Override
  public void close() {
    closed = true;
    if (call != null) call.cancel("Orbita source observation closed", null);
    lane.shutdownNow();
  }
}
