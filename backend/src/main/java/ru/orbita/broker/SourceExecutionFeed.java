package ru.orbita.broker;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import ru.orbita.persistence.PostgresTimestampPrecision;
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

  private record CachedSourceHistory(
      String fingerprint,
      Instant latestExecution,
      BrokerGateway.SourceHistoryResolution resolution) {}

  private record ResolvedHistoryExecution(
      BrokerGateway.SourceExecution raw,
      BrokerGateway.SourceExecution canonical,
      String canonicalTradeId) {}

  private final Map<String, CachedSourceHistory> resolvedHistoryByOperationId =
      new LinkedHashMap<>();
  private volatile ClientCallStreamObserver<TradesStreamRequest> call;
  private boolean acknowledged;
  private volatile boolean closed;
  private boolean reconnecting;
  private volatile long lastFrameNanos;
  private long connectedAtNanos;
  private long generation;
  private boolean historyRetryPending;
  private long historyRetryAtNanos;
  private int historyRetryFailures;

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
      Instant executed = null;
      try {
        if (trade.hasDateTime()) executed = TInvestBrokerGateway.instant(trade.getDateTime());
      } catch (IllegalArgumentException invalid) {
        /* Exact history recovery only. */
      }
      var incoming =
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
              arrivedAt);
      var timestampUpdate =
          journal.withStreamAdmission(() -> recordStreamExecution(incoming, capturedWindow));
      if (timestampUpdate.isEmpty()) continue;
      var observation = timestampUpdate.get();
      if (observation.executedAt() == null) {
        pending.putIfAbsent(incoming.tradeId(), observation);
        recover();
      } else pending.remove(incoming.tradeId());
    }
  }

  /** Returns a pending or resolved timestamp observation after its durable admission. */
  private Optional<BrokerGateway.SourceExecution> recordStreamExecution(
      BrokerGateway.SourceExecution incoming, SourceFeedJournal.TimeWindow capturedWindow) {
    var alias = journal.sourceExecutionAlias(incoming);
    if (alias.status() != SourceFeedJournal.AliasStatus.NONE) {
      if (alias.status() == SourceFeedJournal.AliasStatus.CONFLICT
          || !sameStreamReplay(incoming, alias.execution())) {
        journal.gap("Conflicting durable source execution alias for trade " + incoming.tradeId());
        return Optional.empty();
      }
      return Optional.of(alias.execution());
    }
    var previous = journal.remembered(source, incoming.tradeId());
    if (incoming.executedAt() == null
        && previous.isPresent()
        && previous.get().executedAt() != null) {
      if (!sameStreamReplay(incoming, previous.get())) {
        journal.gap("Conflicting known source stream execution for trade " + incoming.tradeId());
        return Optional.empty();
      }
      return previous;
    }
    Instant received =
        journal.firstReceived(source, incoming.tradeId(), incoming.receivedAt(), capturedWindow);
    var execution =
        new BrokerGateway.SourceExecution(
            incoming.accountId(),
            incoming.tradeId(),
            incoming.sourceOrderId(),
            incoming.instrumentId(),
            incoming.side(),
            incoming.lots(),
            incoming.price(),
            incoming.currency(),
            incoming.executedAt(),
            received);
    if (execution.executedAt() == null) {
      journal.remember(execution);
      return Optional.of(execution);
    } else if (previous.isPresent() && previous.get().executedAt() == null) {
      switch (searchWindow(execution)) {
        case WITHIN -> {
          journal.acceptRecovered(execution);
          return Optional.of(execution);
        }
        case EXPIRED -> {
          journal.skip(previous.get(), "Source timestamp recovery expired");
          return Optional.of(execution);
        }
        case UNKNOWN ->
            journal.gap(
                "Original timestamp recovery window cannot be established " + execution.tradeId());
      }
    } else {
      journal.remember(execution);
      journal.accept(execution);
    }
    return Optional.empty();
  }

  private static boolean sameStreamReplay(
      BrokerGateway.SourceExecution incoming, BrokerGateway.SourceExecution stored) {
    return incoming.executedAt() == null
        ? stored.executedAt() != null
            && incoming.sourceOrderId().equals(stored.sourceOrderId())
            && sameHistoryFieldsExceptTime(incoming, stored)
        : samePersistedHistoryExecution(incoming, stored);
  }

  private void recover() {
    Instant boundary = journal.observationBoundary();
    if (boundary == null) {
      clearHistoryRetry();
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
    if (historyRetryPending && System.nanoTime() - historyRetryAtNanos < 0) return;
    var history = gateway.history(boundary, clock.instant());
    boolean complete = history.complete();
    for (String gap : history.gaps()) journal.gap(gap);
    var recoverable = new java.util.ArrayList<ResolvedHistoryExecution>();
    var candidatesByTrade = new LinkedHashMap<String, BrokerGateway.SourceHistoryCandidate>();
    for (var candidate : history.sourceCandidates())
      candidatesByTrade.putIfAbsent(candidate.execution().tradeId(), candidate);
    var knownCandidatesByOperation =
        new LinkedHashMap<String, java.util.List<BrokerGateway.SourceHistoryCandidate>>();
    var unresolvedByOperationId =
        new LinkedHashMap<String, java.util.List<BrokerGateway.SourceHistoryCandidate>>();
    boolean retryableHistoryFailure = history.retryableFailure();
    Instant lookupThrough = clock.instant();
    Instant lookupFrom = lookupThrough.minusSeconds(60);
    resolvedHistoryByOperationId
        .entrySet()
        .removeIf(entry -> entry.getValue().latestExecution().isBefore(lookupFrom));
    for (var trade : history.executions()) {
      var historyCandidate = candidatesByTrade.get(trade.tradeId());
      if (historyCandidate != null && !sameHistoryExecution(trade, historyCandidate.execution())) {
        journal.gap("Conflicting source history candidate for trade " + trade.tradeId());
        complete = false;
        continue;
      }
      var alias = journal.sourceExecutionAlias(trade);
      if (alias.status() == SourceFeedJournal.AliasStatus.CONFLICT) {
        journal.gap("Conflicting durable source execution alias for trade " + trade.tradeId());
        complete = false;
        continue;
      }
      if (alias.status() == SourceFeedJournal.AliasStatus.MATCH) {
        var canonical = alias.execution();
        if (!samePersistedHistoryFields(trade, canonical)) {
          journal.gap("Conflicting durable source execution alias for trade " + trade.tradeId());
          complete = false;
          continue;
        }
        recoverable.add(new ResolvedHistoryExecution(trade, canonical, alias.canonicalTradeId()));
        addKnownCandidate(
            knownCandidatesByOperation, historyCandidate, canonical, alias.canonicalTradeId());
        continue;
      }
      var knownOrderId = journal.sourceOrderId(source, trade.tradeId());
      if (knownOrderId.isPresent()) {
        var remembered = journal.remembered(source, trade.tradeId()).orElse(null);
        var canonical =
            remembered == null
                ? withSourceOrderId(trade, knownOrderId.get())
                : withSourceOrderId(remembered, knownOrderId.get());
        if (remembered != null
            && !java.util.Objects.equals(remembered.sourceOrderId(), knownOrderId.get())) {
          journal.gap("Conflicting known source history order for trade " + trade.tradeId());
          complete = false;
          continue;
        }
        if (remembered != null
            && remembered.executedAt() == null
            && trade.executedAt() != null
            && sameHistoryFieldsExceptTime(trade, canonical)) {
          if (!pending.containsKey(trade.tradeId())) {
            // An exact replay of an already terminal raw event is not a new timestamp recovery.
            continue;
          }
          var window = searchWindow(remembered);
          if (window == SearchWindow.UNKNOWN) {
            journal.gap(
                "Original timestamp recovery window cannot be established " + trade.tradeId());
            complete = false;
            continue;
          }
          if (window == SearchWindow.EXPIRED) {
            pending.remove(trade.tradeId());
            journal.skip(remembered, "Source timestamp recovery expired");
            continue;
          }
          if (trade.executedAt().isBefore(lookupFrom)
              || trade.executedAt().isAfter(lookupThrough)) {
            journal.gap("Source order identity unresolved for history trade " + trade.tradeId());
            complete = false;
            continue;
          }
          recoverable.add(
              new ResolvedHistoryExecution(
                  trade, withSourceOrderId(trade, knownOrderId.get()), trade.tradeId()));
          continue;
        }
        if (!samePersistedHistoryFields(trade, canonical)) {
          journal.gap("Conflicting known source history execution for trade " + trade.tradeId());
          complete = false;
          continue;
        }
        recoverable.add(new ResolvedHistoryExecution(trade, canonical, trade.tradeId()));
        addKnownCandidate(knownCandidatesByOperation, historyCandidate, canonical, trade.tradeId());
        continue;
      }
      if (historyCandidate == null
          || historyCandidate.operationId().isBlank()
          || !sameHistoryExecution(trade, historyCandidate.execution())) {
        journal.gap("Source order identity unresolved for history trade " + trade.tradeId());
        complete = false;
        continue;
      }
      if (trade.executedAt() == null
          || trade.executedAt().isBefore(lookupFrom)
          || trade.executedAt().isAfter(lookupThrough)) {
        journal.gap("Source order identity unresolved for history trade " + trade.tradeId());
        complete = false;
        continue;
      }
      unresolvedByOperationId
          .computeIfAbsent(historyCandidate.operationId(), _ -> new java.util.ArrayList<>())
          .add(historyCandidate);
    }
    for (var entry : unresolvedByOperationId.entrySet()) {
      var candidates =
          new java.util.ArrayList<>(
              knownCandidatesByOperation.getOrDefault(entry.getKey(), java.util.List.of()));
      candidates.addAll(entry.getValue());
      String fingerprint = historyFingerprint(candidates);
      var cached = resolvedHistoryByOperationId.get(entry.getKey());
      BrokerGateway.SourceHistoryResolution resolution;
      if (cached == null) {
        resolution = gateway.resolveRecentSourceHistory(candidates);
      } else if (!cached.fingerprint().equals(fingerprint)) {
        resolvedHistoryByOperationId.remove(entry.getKey());
        resolution =
            new BrokerGateway.SourceHistoryResolution(
                List.of(),
                List.of(
                    "Source history candidates changed after canonical lookup: " + entry.getKey()));
      } else {
        resolution = cached.resolution();
      }
      retryableHistoryFailure |= resolution.retryableFailure();
      for (String gap : resolution.gaps()) {
        journal.gap(gap);
        complete = false;
      }
      if (resolution.gaps().isEmpty()
          && resolution.executions().size() != entry.getValue().size()) {
        journal.gap("Canonical source history result count mismatch: " + entry.getKey());
        complete = false;
        resolvedHistoryByOperationId.remove(entry.getKey());
        continue;
      }
      if (resolution.executions().isEmpty()) {
        if (resolution.gaps().isEmpty())
          journal.gap("Canonical source history execution unavailable: " + entry.getKey());
        complete = false;
        resolvedHistoryByOperationId.remove(entry.getKey());
        continue;
      }
      if (!resolution.gaps().isEmpty()) {
        complete = false;
        resolvedHistoryByOperationId.remove(entry.getKey());
        continue;
      }
      var resolvedForOperation = new java.util.ArrayList<ResolvedHistoryExecution>();
      var matchedRawTradeIds = new java.util.HashSet<String>();
      boolean oneToOne = true;
      for (var canonical : resolution.executions()) {
        var matches =
            candidates.stream()
                .filter(candidate -> candidate.acceptedExecution() == null)
                .filter(candidate -> sameHistoryFields(candidate.execution(), canonical))
                .toList();
        if (matches.size() != 1) {
          journal.gap(
              "Canonical source history execution does not identify one raw trade: "
                  + canonical.tradeId());
          complete = false;
          oneToOne = false;
          break;
        }
        var match = matches.getFirst();
        if (!matchedRawTradeIds.add(match.execution().tradeId())) {
          journal.gap(
              "Canonical source history maps multiple stages to raw trade: "
                  + match.execution().tradeId());
          complete = false;
          oneToOne = false;
          break;
        }
        resolvedForOperation.add(
            new ResolvedHistoryExecution(match.execution(), canonical, canonical.tradeId()));
      }
      if (!oneToOne || resolvedForOperation.size() != entry.getValue().size()) {
        complete = false;
        resolvedHistoryByOperationId.remove(entry.getKey());
        continue;
      }
      recoverable.addAll(resolvedForOperation);
      if (cached == null) {
        Instant latestExecution =
            candidates.stream()
                .map(candidate -> candidate.execution().executedAt())
                .max(Instant::compareTo)
                .orElseThrow();
        resolvedHistoryByOperationId.put(
            entry.getKey(), new CachedSourceHistory(fingerprint, latestExecution, resolution));
      }
    }
    var claimsByTradeId = new LinkedHashMap<String, ResolvedHistoryExecution>();
    var ambiguousStages = new java.util.HashSet<String>();
    for (var recovery : recoverable) {
      // Raw, stage and stored execution IDs share the same durable account/trade namespace.
      for (String tradeId :
          new java.util.LinkedHashSet<>(
              List.of(
                  recovery.raw().tradeId(),
                  recovery.canonicalTradeId(),
                  recovery.canonical().tradeId()))) {
        var previous = claimsByTradeId.putIfAbsent(tradeId, recovery);
        if (previous != null && previous != recovery && !sameRecovery(previous, recovery)) {
          ambiguousStages.add(previous.canonicalTradeId());
          ambiguousStages.add(recovery.canonicalTradeId());
          journal.gap("Ambiguous source history trade identity " + tradeId);
          complete = false;
        }
      }
    }
    var processedRawIds = new java.util.HashSet<String>();
    for (var recovery : recoverable) {
      if (ambiguousStages.contains(recovery.canonicalTradeId())
          || !processedRawIds.add(recovery.raw().tradeId())) continue;
      var raw = recovery.raw();
      var trade = recovery.canonical();
      String orderId = trade.sourceOrderId();
      if (orderId.isBlank()) {
        journal.gap("Source order identity unresolved for history trade " + trade.tradeId());
        complete = false;
        continue;
      }
      var candidate =
          journal
              .remembered(source, trade.tradeId())
              .or(() -> journal.remembered(source, raw.tradeId()))
              .orElse(recovery.canonical());
      Instant received = journal.firstReceived(source, candidate.tradeId(), clock.instant());
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
      if (!journal.acceptRecovered(raw, resolved, recovery.canonicalTradeId())) {
        journal.gap("Conflicting durable source execution alias for trade " + raw.tradeId());
        complete = false;
        continue;
      }
      pending.remove(raw.tradeId());
      pending.remove(trade.tradeId());
    }
    if (retryableHistoryFailure) scheduleHistoryRetry();
    else clearHistoryRetry();
    if (complete && pending.isEmpty()) journal.recovered();
  }

  private BrokerGateway.SourceExecution withSourceOrderId(
      BrokerGateway.SourceExecution execution, String sourceOrderId) {
    return new BrokerGateway.SourceExecution(
        execution.accountId(),
        execution.tradeId(),
        sourceOrderId,
        execution.instrumentId(),
        execution.side(),
        execution.lots(),
        execution.price(),
        execution.currency(),
        execution.executedAt(),
        execution.receivedAt());
  }

  private static boolean sameHistoryExecution(
      BrokerGateway.SourceExecution first, BrokerGateway.SourceExecution second) {
    return first.accountId().equals(second.accountId())
        && first.sourceOrderId().equals(second.sourceOrderId())
        && sameHistoryFields(first, second);
  }

  private static boolean sameRecovery(
      ResolvedHistoryExecution first, ResolvedHistoryExecution second) {
    return first.canonicalTradeId().equals(second.canonicalTradeId())
        && first.canonical().tradeId().equals(second.canonical().tradeId())
        && sameHistoryFields(first.raw(), second.raw())
        && samePersistedHistoryExecution(first.canonical(), second.canonical());
  }

  private static boolean samePersistedHistoryExecution(
      BrokerGateway.SourceExecution first, BrokerGateway.SourceExecution second) {
    return first.accountId().equals(second.accountId())
        && first.sourceOrderId().equals(second.sourceOrderId())
        && samePersistedHistoryFields(first, second);
  }

  private static boolean sameHistoryFields(
      BrokerGateway.SourceExecution first, BrokerGateway.SourceExecution second) {
    return sameHistoryFieldsExceptTime(first, second)
        && java.util.Objects.equals(first.executedAt(), second.executedAt());
  }

  private static boolean samePersistedHistoryFields(
      BrokerGateway.SourceExecution first, BrokerGateway.SourceExecution second) {
    return sameHistoryFieldsExceptTime(first, second)
        && java.util.Objects.equals(
            PostgresTimestampPrecision.normalize(first.executedAt()),
            PostgresTimestampPrecision.normalize(second.executedAt()));
  }

  private static boolean sameHistoryFieldsExceptTime(
      BrokerGateway.SourceExecution first, BrokerGateway.SourceExecution second) {
    return first.accountId().equals(second.accountId())
        && first.instrumentId().equals(second.instrumentId())
        && first.side() == second.side()
        && first.lots().compareTo(second.lots()) == 0
        && first.price().compareTo(second.price()) == 0
        && first.currency().equalsIgnoreCase(second.currency());
  }

  private static void addKnownCandidate(
      Map<String, List<BrokerGateway.SourceHistoryCandidate>> knownCandidatesByOperation,
      BrokerGateway.SourceHistoryCandidate candidate,
      BrokerGateway.SourceExecution canonical,
      String canonicalTradeId) {
    if (candidate == null || candidate.operationId().isBlank()) return;
    knownCandidatesByOperation
        .computeIfAbsent(candidate.operationId(), _ -> new java.util.ArrayList<>())
        .add(
            new BrokerGateway.SourceHistoryCandidate(
                candidate.operationId(),
                candidate.execution(),
                withTradeId(canonical, canonicalTradeId),
                candidate.operationFingerprint()));
  }

  private static BrokerGateway.SourceExecution withTradeId(
      BrokerGateway.SourceExecution execution, String tradeId) {
    return new BrokerGateway.SourceExecution(
        execution.accountId(),
        tradeId,
        execution.sourceOrderId(),
        execution.instrumentId(),
        execution.side(),
        execution.lots(),
        execution.price(),
        execution.currency(),
        execution.executedAt(),
        execution.receivedAt());
  }

  private static String historyFingerprint(
      java.util.List<BrokerGateway.SourceHistoryCandidate> candidates) {
    return candidates.stream()
        .map(
            candidate -> {
              var execution = candidate.execution();
              return String.join(
                  "\u001f",
                  candidate.operationId(),
                  candidate.operationFingerprint(),
                  execution.accountId(),
                  execution.tradeId(),
                  execution.instrumentId(),
                  execution.side().name(),
                  execution.lots().stripTrailingZeros().toPlainString(),
                  execution.price().stripTrailingZeros().toPlainString(),
                  execution.currency().toUpperCase(java.util.Locale.ROOT),
                  execution.executedAt().toString(),
                  candidate.acceptedExecution() == null
                      ? ""
                      : candidate.acceptedExecution().tradeId()
                          + "\u001f"
                          + candidate.acceptedExecution().sourceOrderId());
            })
        .sorted()
        .collect(java.util.stream.Collectors.joining("\n"));
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
      long nowNanos = System.nanoTime();
      if (nowNanos - (acknowledged ? lastFrameNanos : connectedAtNanos) > maximum) {
        if (acknowledged) journal.connectionLost(BrokerConnectivityEvents.Cause.HEARTBEAT_TIMEOUT);
        disconnect("Source acknowledgement/heartbeat deadline exceeded");
      } else if (acknowledged
          && (!pending.isEmpty() || (historyRetryPending && nowNanos - historyRetryAtNanos >= 0)))
        recover();
    } catch (BrokerPersistenceFailure failure) {
      journal.persistenceUnavailable();
      disconnect("Source persistence unavailable");
    } catch (RuntimeException failure) {
      if (isPersistenceFailure(failure)) journal.persistenceUnavailable();
      disconnect("Source recovery failed");
    }
  }

  private void scheduleHistoryRetry() {
    historyRetryFailures = Math.min(historyRetryFailures + 1, 5);
    long delaySeconds = Math.min(1L << (historyRetryFailures - 1), 15L);
    historyRetryAtNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(delaySeconds);
    historyRetryPending = true;
  }

  private void clearHistoryRetry() {
    historyRetryPending = false;
    historyRetryAtNanos = 0;
    historyRetryFailures = 0;
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
