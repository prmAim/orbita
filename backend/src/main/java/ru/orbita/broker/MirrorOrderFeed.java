package ru.orbita.broker;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import ru.tinkoff.piapi.contract.v1.*;
import ru.ttech.piapi.core.connector.ServiceStubFactory;

/** Separate mirror state/trade subscriptions. A gap never supplies a terminal result. */
final class MirrorOrderFeed implements AutoCloseable {
  private final ServiceStubFactory factory;
  private final String account;
  private final Clock clock;
  private final SourceFeedJournal journal;
  private final ScheduledExecutorService lane =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().daemon().name("orbita-mirror-feed").factory());
  private final List<ClientCallStreamObserver<?>> calls = new ArrayList<>();
  private volatile boolean closed;
  private long generation;
  private boolean stateAck, tradeAck, reconnecting;
  private long stateFrame, tradeFrame;

  MirrorOrderFeed(
      ServiceStubFactory factory, String account, Clock clock, SourceFeedJournal journal) {
    this.factory = factory;
    this.account = account;
    this.clock = clock;
    this.journal = journal;
    lane.execute(this::connect);
    lane.scheduleWithFixedDelay(
        () -> {
          if (!closed
              && !reconnecting
              && (System.nanoTime() - stateFrame > TimeUnit.SECONDS.toNanos(20)
                  || System.nanoTime() - tradeFrame > TimeUnit.SECONDS.toNanos(20))) disconnect();
        },
        1,
        1,
        TimeUnit.SECONDS);
  }

  private void connect() {
    if (closed) return;
    long attempt = ++generation;
    stateAck = false;
    tradeAck = false;
    reconnecting = false;
    stateFrame = tradeFrame = System.nanoTime();
    try {
      journal.mirrorGap();
      var stub = factory.newAsyncService(OrdersStreamServiceGrpc::newStub).getStub();
      stub.orderStateStream(
          OrderStateStreamRequest.newBuilder()
              .addAccounts(account)
              .setPingDelayMillis(5000)
              .build(),
          new ClientResponseObserver<OrderStateStreamRequest, OrderStateStreamResponse>() {
            public void beforeStart(ClientCallStreamObserver<OrderStateStreamRequest> call) {
              register(call);
            }

            public void onNext(OrderStateStreamResponse frame) {
              var at = clock.instant();
              dispatch(
                  attempt,
                  () -> {
                    stateFrame = System.nanoTime();
                    if (frame.hasSubscription()) {
                      stateAck = acknowledged(frame.getSubscription());
                      recovered();
                    }
                    if (frame.hasOrderState()) {
                      if (!stateAck) {
                        disconnect();
                        return;
                      }
                      journal.mirrorOrder(MirrorOrderParser.state(frame.getOrderState(), at));
                    }
                  });
            }

            public void onError(Throwable failure) {
              dispatch(attempt, MirrorOrderFeed.this::disconnect);
            }

            public void onCompleted() {
              dispatch(attempt, MirrorOrderFeed.this::disconnect);
            }
          });
      stub.tradesStream(
          TradesStreamRequest.newBuilder().addAccounts(account).setPingDelayMs(5000).build(),
          new ClientResponseObserver<TradesStreamRequest, TradesStreamResponse>() {
            public void beforeStart(ClientCallStreamObserver<TradesStreamRequest> call) {
              register(call);
            }

            public void onNext(TradesStreamResponse frame) {
              var at = clock.instant();
              dispatch(
                  attempt,
                  () -> {
                    tradeFrame = System.nanoTime();
                    if (frame.hasSubscription()) {
                      tradeAck = acknowledged(frame.getSubscription());
                      recovered();
                    }
                    if (frame.hasOrderTrades()) {
                      if (!tradeAck) {
                        disconnect();
                        return;
                      }
                      journal.mirrorOrder(MirrorOrderParser.trades(frame.getOrderTrades(), at));
                    }
                  });
            }

            public void onError(Throwable failure) {
              dispatch(attempt, MirrorOrderFeed.this::disconnect);
            }

            public void onCompleted() {
              dispatch(attempt, MirrorOrderFeed.this::disconnect);
            }
          });
    } catch (RuntimeException failure) {
      failed();
    }
  }

  private void register(ClientCallStreamObserver<?> call) {
    synchronized (calls) {
      if (closed) call.cancel("Mirror observation closed", null);
      else calls.add(call);
    }
  }

  private boolean acknowledged(SubscriptionResponse response) {
    boolean valid =
        response.getStatus() == ResultSubscriptionStatus.RESULT_SUBSCRIPTION_STATUS_OK
            && response.getAccountsList().equals(List.of(account));
    if (!valid) disconnect();
    return valid;
  }

  private void recovered() {
    if (stateAck && tradeAck && !reconnecting) journal.mirrorRecovered();
  }

  private void dispatch(long attempt, Runnable work) {
    if (closed) return;
    try {
      lane.execute(
          () -> {
            if (closed || reconnecting || attempt != generation) return;
            try {
              work.run();
            } catch (RuntimeException failure) {
              failed();
            }
          });
    } catch (RejectedExecutionException shutdown) {
      if (!closed) throw shutdown;
    }
  }

  private void failed() {
    journal.mirrorPersistenceUnavailable();
    disconnect();
  }

  private void disconnect() {
    if (closed || reconnecting) return;
    reconnecting = true;
    synchronized (calls) {
      for (var call : calls) call.cancel("Mirror subscription recovery", null);
      calls.clear();
    }
    try {
      journal.mirrorGap();
    } catch (RuntimeException failure) {
      journal.mirrorPersistenceUnavailable();
    }
    if (!closed) lane.schedule(this::connect, 1, TimeUnit.SECONDS);
  }

  public void close() {
    closed = true;
    synchronized (calls) {
      for (var call : calls) call.cancel("Mirror observation closed", null);
      calls.clear();
    }
    lane.shutdownNow();
  }
}
