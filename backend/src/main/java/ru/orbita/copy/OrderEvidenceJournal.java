package ru.orbita.copy;

import jakarta.persistence.EntityManager;
import java.time.Clock;
import java.util.*;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import ru.orbita.broker.BrokerGateway.OrderEvidence;
import ru.orbita.broker.BrokerGateway.OrderRequest;
import ru.orbita.broker.BuyResourceRejectionClassifier;
import ru.orbita.broker.OrderSdkEvidence;
import ru.orbita.journal.JournalEntry;
import tools.jackson.databind.json.JsonMapper;

/** Append-only inbox in the existing trading journal. Receipt commits before any decision. */
@Component
public class OrderEvidenceJournal {
  private final EntityManager em;
  private final TransactionTemplate tx;
  private final TransactionTemplate receiptTx;
  private final JsonMapper json;
  private final Clock clock;

  public OrderEvidenceJournal(
      EntityManager em, TransactionTemplate tx, JsonMapper json, Clock clock) {
    this.em = em;
    this.tx = tx;
    this.json = json;
    this.clock = clock;
    this.receiptTx = new TransactionTemplate(Objects.requireNonNull(tx.getTransactionManager()));
    this.receiptTx.setPropagationBehavior(
        org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  record Saved(int format, OrderEvidence evidence, List<String> stages, boolean postResponse) {}

  record Pending(long sequence, OrderEvidence evidence, boolean postResponse) {}

  long receive(OrderEvidence evidence, boolean postResponse) {
    var raw = evidence.sdkEvidence();
    var stages =
        raw == null
            ? List.<String>of()
            : raw.stages().stream()
                .map(s -> Base64.getEncoder().encodeToString(s.toByteArray()))
                .toList();
    var saved =
        new Saved(
            1, copy(evidence, raw == null ? null : raw(raw, List.of())), stages, postResponse);
    String payload = json.writeValueAsString(saved);
    return receiptTx.execute(
        _ -> {
          var row =
              new JournalEntry(
                  clock.instant(), "ORDER_EVIDENCE_RECEIVED", evidence.instrumentId(), payload);
          em.persist(row);
          em.flush();
          return row.id();
        });
  }

  long highWater() {
    return tx.execute(
        _ ->
            em.createQuery(
                    "select coalesce(max(e.id),0) from JournalEntry e where e.category='ORDER_EVIDENCE_RECEIVED'",
                    Long.class)
                .getSingleResult());
  }

  List<Pending> pending(long after, long through) {
    return tx.execute(
        _ ->
            em
                .createQuery(
                    "from JournalEntry e where e.id>:after and e.id<=:through and e.category='ORDER_EVIDENCE_RECEIVED' and not exists (select a.id from JournalEntry a where a.category='ORDER_EVIDENCE_APPLIED' and a.detail=str(e.id)) order by e.id",
                    JournalEntry.class)
                .setParameter("after", after)
                .setParameter("through", through)
                .setMaxResults(200)
                .getResultList()
                .stream()
                .map(this::decode)
                .toList());
  }

  /** Complete attempt history, including unapplied receipts and trades linked by the stream IDs. */
  List<Pending> history(OrderRequest attempt) {
    var rows =
        em.createNativeQuery(
                """
        with receipts as materialized (
          select * from journal_entry where category='ORDER_EVIDENCE_RECEIVED'
        ), own as materialized (
          select * from receipts where detail::jsonb #>> '{evidence,accountId}'=:account
        ), direct as materialized (
          select * from own where detail::jsonb #>> '{evidence,requestId}'=:request
            or detail::jsonb #>> '{evidence,sdkEvidence,responseRequestId}'=:request
        )
        select * from own where id in (select id from direct)
          or (detail::jsonb #>> '{evidence,sdkEvidence,source}'='ORDER_STATE_STREAM'
            and exists (select 1 from direct d
              where d.detail::jsonb #>> '{evidence,sdkEvidence,source}'='ORDER_STATE_STREAM'
                and d.detail::jsonb #>> '{evidence,sdkEvidence,responseOrderId}'=own.detail::jsonb #>> '{evidence,sdkEvidence,responseOrderId}'
                and d.detail::jsonb #>> '{evidence,sdkEvidence,tradeOrderId}'=own.detail::jsonb #>> '{evidence,sdkEvidence,tradeOrderId}'))
          or (detail::jsonb #>> '{evidence,sdkEvidence,source}'='TRADES_STREAM'
            and detail::jsonb #>> '{evidence,sdkEvidence,responseOrderId}' in (
              select detail::jsonb #>> '{evidence,sdkEvidence,tradeOrderId}' from direct
              where detail::jsonb #>> '{evidence,sdkEvidence,source}'='ORDER_STATE_STREAM'))
        order by id
        """,
                JournalEntry.class)
            .setParameter("account", attempt.accountId())
            .setParameter("request", attempt.requestId().toString())
            .getResultList();
    return rows.stream().map(row -> decode((JournalEntry) row)).toList();
  }

  record ReasonEvidence(
      String classifierVersion,
      UUID requestId,
      ru.orbita.broker.BrokerGateway.OrderStatus status,
      ru.orbita.broker.BrokerGateway.RejectionReason reason,
      String detail,
      List<Long> receipts) {}

  /**
   * Successful cancel returns GET_ORDER_STATE facts, so its mutation barrier must be checked too.
   */
  boolean onlyOriginalMutation(UUID requestId) {
    return em.createQuery(
                """
        select count(c) from JournalEntry c where c.category='CANCEL_STARTED' and c.detail=:request
          and not exists (select n.id from JournalEntry n where n.category='CANCEL_KNOWN_NOT_SENT'
            and n.id>c.id and n.detail like :notSent
            and not exists (select later.id from JournalEntry later where later.category='CANCEL_STARTED'
              and later.detail=:request and later.id>c.id and later.id<n.id))
        """,
                Long.class)
            .setParameter("request", requestId.toString())
            .setParameter("notSent", requestId + ":%")
            .getSingleResult()
        == 0;
  }

  Optional<ReasonEvidence> latestReason(UUID requestId) {
    var rows =
        em.createNativeQuery(
                """
        with reasons as materialized (
          select * from journal_entry where category in ('ORDER_REASON_QUALIFIED','ORDER_REASON_INVALIDATED')
        )
        select * from reasons where detail::jsonb ->> 'requestId'=:request order by id desc limit 1
        """,
                JournalEntry.class)
            .setParameter("request", requestId.toString())
            .getResultList();
    return rows.isEmpty()
        ? Optional.empty()
        : Optional.of(
            json.readValue(((JournalEntry) rows.getFirst()).view().detail(), ReasonEvidence.class));
  }

  void qualifyReason(
      OrderRequest attempt,
      BuyResourceRejectionClassifier.Qualification result,
      List<Pending> history) {
    writeReason(
        "ORDER_REASON_QUALIFIED",
        attempt,
        result.status(),
        result.reason(),
        result.detail(),
        history);
  }

  void invalidateReason(
      OrderRequest attempt,
      ru.orbita.broker.BrokerGateway.OrderStatus status,
      String detail,
      List<Pending> history) {
    writeReason(
        "ORDER_REASON_INVALIDATED",
        attempt,
        status,
        ru.orbita.broker.BrokerGateway.RejectionReason.OTHER,
        detail,
        history);
  }

  private void writeReason(
      String category,
      OrderRequest attempt,
      ru.orbita.broker.BrokerGateway.OrderStatus status,
      ru.orbita.broker.BrokerGateway.RejectionReason reason,
      String detail,
      List<Pending> history) {
    em.persist(
        new JournalEntry(
            clock.instant(),
            category,
            attempt.instrumentId(),
            json.writeValueAsString(
                new ReasonEvidence(
                    BuyResourceRejectionClassifier.VERSION,
                    attempt.requestId(),
                    status,
                    reason,
                    detail,
                    history.stream().map(Pending::sequence).toList()))));
  }

  /** Only a previously correlated stream UUID can establish the exchange trade-order link. */
  record TradeLink(
      long receipt,
      UUID requestId,
      String account,
      String instrument,
      int direction,
      String internalOrderId,
      String exchangeOrderId) {}

  void rememberLink(long receipt, OrderEvidence evidence) {
    var raw = evidence.sdkEvidence();
    if (raw == null
        || raw.source() != OrderSdkEvidence.Source.ORDER_STATE_STREAM
        || raw.responseOrderId() == null
        || raw.responseOrderId().isBlank()
        || raw.tradeOrderId() == null
        || raw.tradeOrderId().isBlank()) return;
    var link =
        new TradeLink(
            receipt,
            evidence.requestId(),
            evidence.accountId(),
            evidence.instrumentId(),
            raw.directionValue(),
            raw.responseOrderId(),
            raw.tradeOrderId());
    em.persist(
        new JournalEntry(
            clock.instant(),
            "ORDER_TRADE_LINK",
            evidence.instrumentId(),
            json.writeValueAsString(link)));
  }

  Optional<UUID> linkedRequest(OrderSdkEvidence observation) {
    boolean state = observation.source() == OrderSdkEvidence.Source.ORDER_STATE_STREAM;
    var exchangeId = state ? observation.tradeOrderId() : observation.responseOrderId();
    if (exchangeId == null
        || exchangeId.isBlank()
        || state
            && (observation.responseOrderId() == null || observation.responseOrderId().isBlank()))
      return Optional.empty();
    var query =
        em.createNativeQuery(
                """
            with links as materialized (select * from journal_entry where category='ORDER_TRADE_LINK')
            select distinct on (detail::jsonb ->> 'requestId') * from links
            where detail::jsonb ->> 'account'=:account
            and detail::jsonb ->> 'exchangeOrderId'=:orderId
            """
                    + (state ? " and detail::jsonb ->> 'internalOrderId'=:internalId" : "")
                    + " order by detail::jsonb ->> 'requestId',id desc limit 201",
                JournalEntry.class)
            .setParameter("account", observation.responseAccountId())
            .setParameter("orderId", exchangeId);
    if (state) query.setParameter("internalId", observation.responseOrderId());
    var candidates = query.getResultList();
    if (candidates.size() > 200)
      return Optional.empty(); // Ambiguous excessive history needs unary reconciliation.
    Set<UUID> ids = new HashSet<>();
    for (var candidate : candidates) {
      var row = (JournalEntry) candidate;
      var link = json.readValue(row.view().detail(), TradeLink.class);
      {
        var intent = em.find(CopyIntent.class, link.requestId());
        if (intent != null
            && intent.instrumentId.equals(link.instrument())
            && intent.side.equals(link.direction() == 1 ? "BUY" : "SELL")
            && intent.requestedLots > 0) ids.add(link.requestId());
      }
    }
    return ids.size() == 1 ? Optional.of(ids.iterator().next()) : Optional.empty();
  }

  boolean applied(long sequence) {
    return em.createQuery(
                "select count(e) from JournalEntry e where e.category='ORDER_EVIDENCE_APPLIED' and e.detail=:sequence",
                Long.class)
            .setParameter("sequence", Long.toString(sequence))
            .getSingleResult()
        > 0;
  }

  void markApplied(long sequence, String instrument) {
    em.persist(
        new JournalEntry(
            clock.instant(), "ORDER_EVIDENCE_APPLIED", instrument, Long.toString(sequence)));
  }

  private Pending decode(JournalEntry row) {
    var saved = json.readValue(row.view().detail(), Saved.class);
    if (saved.format() != 1)
      throw new IllegalStateException("Unsupported order evidence journal format");
    var evidence = saved.evidence();
    if (evidence.sdkEvidence() != null) {
      var stages =
          saved.stages().stream()
              .map(
                  s -> {
                    try {
                      return ru.tinkoff.piapi.contract.v1.OrderStage.parseFrom(
                          Base64.getDecoder().decode(s));
                    } catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
                      throw new IllegalStateException("Invalid saved order stage", invalid);
                    }
                  })
              .toList();
      evidence = copy(evidence, raw(evidence.sdkEvidence(), stages));
    }
    return new Pending(row.id(), evidence, saved.postResponse());
  }

  private static OrderSdkEvidence raw(
      OrderSdkEvidence r, List<ru.tinkoff.piapi.contract.v1.OrderStage> stages) {
    return new OrderSdkEvidence(
        r.source(),
        r.sdkVersion(),
        r.classifierVersion(),
        r.responseRequestId(),
        r.responseAccountId(),
        r.responseOrderId(),
        r.tradeOrderId(),
        r.responseInstrumentId(),
        r.directionValue(),
        r.executionStatusValue(),
        r.statusInfoPresent(),
        r.causeValue(),
        r.requestedLots(),
        r.executedLots(),
        r.remainingLots(),
        r.cancelledLots(),
        stages,
        r.receivedAt(),
        r.eventAt(),
        r.transport(),
        r.lotSize(),
        r.trades());
  }

  private static OrderEvidence copy(OrderEvidence e, OrderSdkEvidence raw) {
    return new OrderEvidence(
        e.requestId(),
        e.accountId(),
        e.brokerOrderId(),
        e.instrumentId(),
        e.side(),
        e.lotsRequested(),
        e.lotsExecuted(),
        e.status(),
        e.fills(),
        e.observedAt(),
        e.detail(),
        e.rejectionReason(),
        e.receivedMonotonicNanos(),
        e.receiptClockDomain(),
        raw);
  }
}
