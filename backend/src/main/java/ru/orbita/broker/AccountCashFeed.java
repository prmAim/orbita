package ru.orbita.broker;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import ru.tinkoff.piapi.contract.v1.*;
import ru.ttech.piapi.core.connector.ServiceStubFactory;

/** Dedicated read-only currency update feed. Missing currencies never mean zero. */
final class AccountCashFeed implements AutoCloseable {
  private final ServiceStubFactory factory;
  private final String account;
  private final Clock clock;
  private final Consumer<AccountRead.Snapshot<AccountRead.Cash>> sink;
  private volatile Thread worker;
  private final ScheduledExecutorService lane =
      Executors.newSingleThreadScheduledExecutor(
          task -> {
            worker = Thread.ofPlatform().daemon().name("orbita-account-cash").unstarted(task);
            return worker;
          });
  private volatile ClientCallStreamObserver<PositionsStreamRequest> call;
  private volatile boolean closed;
  private long generation;

  AccountCashFeed(
      ServiceStubFactory factory,
      String account,
      Clock clock,
      Consumer<AccountRead.Snapshot<AccountRead.Cash>> sink) {
    this.factory = factory;
    this.account = account;
    this.clock = clock;
    this.sink = Objects.requireNonNull(sink);
    lane.execute(this::connect);
  }

  private void connect() {
    if (closed) return;
    long attempt = ++generation;
    try {
      factory
          .newAsyncService(OperationsStreamServiceGrpc::newStub)
          .getStub()
          .positionsStream(
              PositionsStreamRequest.newBuilder()
                  .addAccounts(account)
                  .setWithInitialPositions(true)
                  .build(),
              new ClientResponseObserver<PositionsStreamRequest, PositionsStreamResponse>() {
                public void beforeStart(ClientCallStreamObserver<PositionsStreamRequest> stream) {
                  call = stream;
                  if (closed) stream.cancel("Cash feed closed", null);
                }

                public void onNext(PositionsStreamResponse frame) {
                  var arrived = clock.instant();
                  dispatch(
                      () -> {
                        if (attempt == generation && frame.hasPosition()) {
                          try {
                            receive(frame.getPosition(), arrived);
                          } catch (IllegalArgumentException invalidBankFrame) {
                            if (closed) throw invalidBankFrame;
                            emit(
                                new AccountRead.Snapshot<>(
                                    List.of(),
                                    null,
                                    arrived,
                                    false,
                                    List.of(
                                        "Cash stream malformed bank frame; retain last successful values")));
                          }
                        }
                      });
                }

                public void onError(Throwable error) {
                  dispatch(() -> disconnected(attempt));
                }

                public void onCompleted() {
                  dispatch(() -> disconnected(attempt));
                }
              });
    } catch (RuntimeException unavailable) {
      disconnected(attempt);
    }
  }

  private void disconnected(long attempt) {
    if (closed || attempt != generation) return;
    emit(
        new AccountRead.Snapshot<>(
            List.of(),
            null,
            clock.instant(),
            false,
            List.of("Cash stream disconnected; retain last successful values")));
    if (!closed) lane.schedule(this::connect, 1, TimeUnit.SECONDS);
  }

  private void receive(PositionData data, java.time.Instant arrived) {
    if (!account.equals(data.getAccountId()) || !data.hasDate()) {
      emit(
          new AccountRead.Snapshot<>(
              List.of(), null, arrived, false, List.of("Cash stream account/time unconfirmed")));
      return;
    }
    var rows = new ArrayList<AccountRead.Cash>();
    var seen = new HashSet<String>();
    var bankAt = TInvestBrokerGateway.instant(data.getDate());
    for (var money : data.getMoneyList()) {
      var available = money.hasAvailableValue() ? money.getAvailableValue() : null;
      var blocked = money.hasBlockedValue() ? money.getBlockedValue() : null;
      String currency =
          available != null
              ? available.getCurrency()
              : blocked != null ? blocked.getCurrency() : "";
      if (currency.isBlank()
          || !seen.add(currency.toLowerCase(Locale.ROOT))
          || available != null
              && blocked != null
              && !available.getCurrency().equalsIgnoreCase(blocked.getCurrency())) {
        emit(
            new AccountRead.Snapshot<>(
                List.of(), null, arrived, false, List.of("Cash stream currency unconfirmed")));
        return;
      }
      BigDecimal free =
          available == null
              ? null
              : TInvestBrokerGateway.decimal(available.getUnits(), available.getNano());
      BigDecimal reserved =
          blocked == null
              ? null
              : TInvestBrokerGateway.decimal(blocked.getUnits(), blocked.getNano());
      rows.add(
          new AccountRead.Cash(
              currency.toLowerCase(Locale.ROOT),
              free == null || reserved == null ? null : free.add(reserved),
              free,
              reserved,
              null,
              bankAt,
              "PositionsStream.position.money.available_value + blocked_value",
              "Currency update, not account inventory; excludes futures guarantee"));
    }
    emit(
        new AccountRead.Snapshot<>(
            rows,
            null,
            arrived,
            false,
            List.of("Partial currency updates; merge by currency, never delete omitted rows")));
  }

  private void emit(AccountRead.Snapshot<AccountRead.Cash> value) {
    try {
      sink.accept(value);
    } catch (RuntimeException persistenceFailure) {
      close();
      throw persistenceFailure;
    }
  }

  private void dispatch(Runnable task) {
    if (closed) return;
    try {
      lane.execute(
          () -> {
            if (!closed) task.run();
          });
    } catch (RejectedExecutionException shutdown) {
      if (!closed) throw shutdown;
    }
  }

  public void close() {
    closed = true;
    var current = call;
    if (current != null) current.cancel("Cash feed closed", null);
    lane.shutdownNow();
    if (Thread.currentThread() != worker) {
      try {
        if (!lane.awaitTermination(5, TimeUnit.SECONDS))
          throw new IllegalStateException("Cash feed callback did not finish during close");
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Cash feed close interrupted", interrupted);
      }
    }
  }
}
