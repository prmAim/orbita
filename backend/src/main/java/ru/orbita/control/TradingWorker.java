package ru.orbita.control;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import ru.orbita.copy.*;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnExpression("${orbita.worker.enabled:true} and !${orbita.admin.reset-password:false}")
public class TradingWorker {
  private static final Logger LOG = LoggerFactory.getLogger(TradingWorker.class);
  private final ControlService control;
  private final CopyService copies;
  private final TradingService trading;
  private final ReconciliationService reconciliation;
  private final PositionRecoveryService recovery;
  private final AcceptanceService acceptance;
  private final ManualReconciliationService manual;
  private final ru.orbita.runtime.ApplicationReadiness readiness;
  private boolean restoredCapitalRequested;

  public TradingWorker(
      ControlService control,
      CopyService copies,
      TradingService trading,
      ReconciliationService reconciliation,
      PositionRecoveryService recovery,
      AcceptanceService acceptance,
      ManualReconciliationService manual,
      ru.orbita.runtime.ApplicationReadiness readiness) {
    this.control = control;
    this.copies = copies;
    this.trading = trading;
    this.reconciliation = reconciliation;
    this.recovery = recovery;
    this.acceptance = acceptance;
    this.manual = manual;
    this.readiness = readiness;
  }

  @Scheduled(fixedDelayString = "${orbita.worker.interval-millis:1000}")
  public void run() {
    if (!readiness.ready()) return;
    try {
      control.recoverAbandonedCapitalAttempts();
      if (!restoredCapitalRequested && "RESTORED".equals(control.status().mode())) {
        restoredCapitalRequested = true;
        control.recalculate();
      }
      if (control.mandatoryDue()) control.mandatoryRecalculation();
      trading.reconcileOnly();
      manual.reconcilePaused();
      reconciliation.reconcile();
      copies.retryPending();
      recovery.recover();
      acceptance.verifySavedShadow();
      acceptance.verifyLive();
      trading.dispatchReady();
    } catch (RuntimeException e) {
      // No additional broker action after a failed transaction or uncertain commit in this tick.
      LOG.error(
          "Worker suspended until next reconciliation; failure type={}",
          e.getClass().getSimpleName());
    }
  }
}
