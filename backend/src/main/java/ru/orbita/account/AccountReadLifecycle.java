package ru.orbita.account;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.runtime.ApplicationReadiness;

/** Independent account display lifecycle; no source-feed or trading controls are touched. */
@Component
@ConditionalOnExpression(
    "${orbita.account-read.enabled:true} && ${orbita.broker.enabled:false} && !${orbita.admin.reset-password:false}")
public final class AccountReadLifecycle implements AutoCloseable {
  private final ObjectProvider<BrokerGateway> gateways;
  private final AccountReadService reads;
  private final ApplicationReadiness readiness;
  private final java.time.Clock clock;
  private AutoCloseable cash;
  private boolean closed;
  private final java.util.concurrent.atomic.AtomicReference<RuntimeException> cashFailure =
      new java.util.concurrent.atomic.AtomicReference<>();
  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(AccountReadLifecycle.class);

  public AccountReadLifecycle(
      ObjectProvider<BrokerGateway> gateways,
      AccountReadService reads,
      ApplicationReadiness readiness,
      java.time.Clock clock) {
    this.gateways = gateways;
    this.reads = reads;
    this.readiness = readiness;
    this.clock = clock;
  }

  @Scheduled(fixedDelayString = "${orbita.account-read.refresh-ms:30000}")
  public synchronized void refresh() {
    if (closed || !readiness.initialized()) return;
    var gateway = gateways.getIfAvailable();
    var previousFailure = cashFailure.get();
    if (previousFailure != null) {
      try {
        if (cash != null) cash.close();
      } catch (Exception closeFailure) {
        previousFailure.addSuppressed(closeFailure);
        throw new IllegalStateException("Failed cash subscription did not close", previousFailure);
      }
      cash = null;
      reads.acceptCash(
          new ru.orbita.broker.AccountRead.Snapshot<>(
              java.util.List.of(),
              null,
              clock.instant(),
              false,
              java.util.List.of(
                  "Previous cash persistence failed; retained values require a fresh callback")));
      cashFailure.compareAndSet(previousFailure, null);
    }
    if (cash == null && gateway != null)
      cash =
          gateway.observeCash(
              snapshot -> {
                try {
                  reads.acceptCash(snapshot);
                } catch (RuntimeException persistenceFailure) {
                  cashFailure.compareAndSet(null, persistenceFailure);
                  log.error(
                      "Account cash persistence failed; subscription closed and retry required",
                      persistenceFailure);
                  throw persistenceFailure;
                }
              });
    reads.refresh();
  }

  @PreDestroy
  public synchronized void close() throws Exception {
    closed = true;
    if (cash != null) {
      cash.close();
      cash = null;
    }
  }
}
