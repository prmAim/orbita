package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.control.ControlService;

@SpringBootTest(
    properties = {
      "orbita.source-account=source",
      "orbita.mirror-account=mirror",
      "orbita.worker.enabled=false"
    })
@Import({CopyLifecycleIT.Fixture.class, CapitalTimingIT.JdbcTiming.class})
class CapitalTimingIT {
  @Autowired ControlService control;
  @Autowired CopyService copies;
  @Autowired CopyLifecycleIT.Simulator broker;
  @Autowired CopyLifecycleIT.MutableClock clock;
  @Autowired JdbcTemplate database;
  @Autowired CommitDelay delay;

  @BeforeEach
  void reset() {
    delay.reset();
    CopyLifecycleIT.reset(database, broker, clock);
  }

  @ParameterizedTest
  @EnumSource(PersistenceFault.class)
  void numericPersistenceFailureEndsTheCallerAndRequiresFreshReconciliation(
      PersistenceFault fault) {
    control.markReconciled();
    control.start();
    broker.mirrorCapital = new BigDecimal("200000");
    if (fault == PersistenceFault.INSERT)
      database.execute(
          "alter table capital_candidate add constraint numeric_write_fault check(source_capital<0) not valid");
    else delay.persistenceFailure.set(fault);
    var continued = new java.util.concurrent.atomic.AtomicBoolean();
    Throwable failure;
    try {
      failure =
          org.assertj.core.api.Assertions.catchThrowable(
              () -> {
                control.recalculate();
                continued.set(true);
              });
    } finally {
      database.execute(
          "alter table capital_candidate drop constraint if exists numeric_write_fault");
    }
    assertThat(failure).isInstanceOf(RuntimeException.class);
    assertThat(continued).isFalse();
    assertThat(control.status().coefficient()).isEqualByComparingTo("1");
    control.recoverAbandonedCapitalAttempts();
    assertThat(control.status().recalculationRunning()).isFalse();
    assertThat(control.status().reconciled()).isFalse();
    assertThat(control.recalculate().coefficient()).isEqualByComparingTo("2");
    assertThat(broker.posts).isZero();
  }

  enum PersistenceFault {
    INSERT,
    BEFORE_COMMIT,
    AFTER_COMMIT
  }

  @ParameterizedTest
  @EnumSource(CommitPhase.class)
  void lateNumericCommitOrItsAcknowledgementNeverPublishesANewAcceptedRatio(CommitPhase phase)
      throws Exception {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    clock.now = clock.now.plusSeconds(86400);
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) delay.arm(phase);
        };
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var result = executor.submit(control::recalculate);
      assertThat(delay.entered.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(control.status().coefficient()).isEqualByComparingTo("1");
      assertThat(result.get(15, TimeUnit.SECONDS).coefficient()).isEqualByComparingTo("1");
    }
    assertThat(control.status().lastError()).contains("5 секунд");
    assertThat(control.mandatoryDue()).isTrue();
    var execution =
        new BrokerGateway.SourceExecution(
            "source",
            "after-late-commit",
            "late-order",
            "instrument",
            BrokerGateway.Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant());
    assertThat(control.gate().exactCoefficient().decimal(50)).isEqualByComparingTo("1");
    assertThat(control.gate().recalculationPaused()).isTrue();
    assertThat(copies.accept(execution).acceptedCoefficient()).isNull();
    assertThat(broker.posts).isZero();
  }

  @ParameterizedTest
  @EnumSource(CommitPhase.class)
  void firstStartRemainsUnstartedWhenNumericCommitOrItsAcknowledgementIsLate(CommitPhase phase)
      throws Exception {
    control.markReconciled();
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 2) delay.arm(phase);
        };
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var result = executor.submit(control::start);
      assertThat(delay.entered.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(control.status().coefficient()).isNull();
      assertThat(result.get(15, TimeUnit.SECONDS).coefficient()).isNull();
    }
    assertThat(control.status().mode()).isEqualTo("NOT_STARTED");
    assertThat(control.status().lastError()).contains("5 секунд");
    assertThat(broker.posts).isZero();
  }

  @Test
  void firstObserverDelayCannotHideTheRawReplyGapWithFrozenWallTime() {
    control.markReconciled();
    control.start();
    broker.capitalResponse =
        account -> {
          if (account.equals("mirror")) pause(1700);
          var receipt =
              new BrokerGateway.CapitalEvidence(
                  account,
                  new BigDecimal(account.equals("source") ? "100000" : "200000"),
                  clock.instant(),
                  clock.instant(),
                  true,
                  "raw RPC simulator",
                  ru.orbita.broker.BrokerReceiptClock.nanoTime(),
                  ru.orbita.broker.BrokerReceiptClock.domain());
          // The first RPC reply is already complete; the following durable observer delays return.
          if (account.equals("source")) pause(1700);
          return receipt;
        };
    var result = control.recalculate();
    assertThat(result.coefficient()).isEqualByComparingTo("1");
    assertThat(result.lastError()).contains("3 секунды");
    assertThat(broker.posts).isZero();
  }

  private static void pause(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(failure);
    }
  }

  @Test
  void frozenWallClockDoesNotHideThreePointTwoSecondsBetweenFullCapitalReplies() {
    control.markReconciled();
    control.start();
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4)
            try {
              Thread.sleep(3200);
            } catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(interrupted);
            }
        };
    var result = control.recalculate();
    assertThat(result.coefficient()).isEqualByComparingTo("1");
    assertThat(result.lastError()).contains("3 секунды");
  }

  @Test
  void timelyDurableNumericSaveDoesNotRequireAnotherTimedReferenceConfirmation() throws Exception {
    control.markReconciled();
    control.start();
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) {
            delay.afterPromotionCommit = () -> clock.now = clock.now.plusSeconds(86400);
            delay.armPromotion();
          }
        };
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var result = executor.submit(control::recalculate);
      assertThat(delay.entered.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(result.get(15, TimeUnit.SECONDS).coefficient()).isEqualByComparingTo("2");
    }
    assertThat(control.status().lastError()).isNull();
    assertThat(control.mandatoryDue()).isTrue();
  }

  @Test
  void restartDoesNotPromoteAnUnacknowledgedDurableCandidate() {
    control.markReconciled();
    control.start();
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) delay.armCrash();
        };
    org.assertj.core.api.Assertions.assertThatThrownBy(control::recalculate)
        .isInstanceOf(SimulatedCrash.class);
    assertThat(control.status().coefficient()).isEqualByComparingTo("1");
    assertThat(control.status().recalculationRunning()).isTrue();
    control.recoverStartup();
    assertThat(control.status().coefficient()).isEqualByComparingTo("1");
    assertThat(control.status().recalculationRunning()).isFalse();
    control.recalculate();
    assertThat(control.status().coefficient()).isEqualByComparingTo("2");
    assertThat(broker.capitalCalls).isEqualTo(6);
  }

  @ParameterizedTest
  @EnumSource(CommitPhase.class)
  void aPersistenceIntervalCrossingMoscowTwoOClockIsRejectedInsteadOfCompletingTheDay(
      CommitPhase phase) {
    clock.now = java.time.Instant.parse("2026-11-15T22:59:58Z");
    control.markReconciled();
    control.start();
    clock.now = java.time.Instant.parse("2026-11-15T22:59:59.900Z");
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) {
            Runnable cross = () -> clock.now = java.time.Instant.parse("2026-11-15T23:00:00.100Z");
            if (phase == CommitPhase.BEFORE_DELEGATE) delay.beforeNumericCommit = cross;
            else delay.afterNumericCommit = cross;
            delay.armClockAdvance(phase);
          }
        };
    var result = control.recalculate();
    assertThat(result.coefficient()).isEqualByComparingTo("1");
    assertThat(result.lastError()).contains("02:00");
    assertThat(control.mandatoryDue()).isTrue();
  }

  @Test
  void aProvenSaveBeforeMoscowTwoLeavesTheUpcomingDeadlinePending() {
    clock.now = java.time.Instant.parse("2026-11-15T22:59:58Z");
    control.markReconciled();
    control.start();
    clock.now = java.time.Instant.parse("2026-11-15T22:59:59.500Z");
    broker.mirrorCapital = new BigDecimal("200000");
    assertThat(control.recalculate().coefficient()).isEqualByComparingTo("2");
    clock.now = java.time.Instant.parse("2026-11-15T23:00:00.100Z");
    assertThat(control.mandatoryDue()).isTrue();
  }

  @Test
  void aProvenSaveAfterMoscowTwoCompletesTheCurrentDeadline() {
    clock.now = java.time.Instant.parse("2026-11-15T22:59:58Z");
    control.markReconciled();
    control.start();
    clock.now = java.time.Instant.parse("2026-11-15T22:59:59.900Z");
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4)
            clock.now = java.time.Instant.parse("2026-11-15T23:00:00.100Z");
        };
    assertThat(control.recalculate().coefficient()).isEqualByComparingTo("2");
    assertThat(control.mandatoryDue()).isFalse();
  }

  @Test
  void stopAndSourceAdmissionCannotInterleaveBetweenDurableNumericSaveAndAcceptance()
      throws Exception {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    clock.now = clock.now.plusSeconds(1);
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) delay.armBarrier();
        };
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var recalculation = executor.submit(control::recalculate);
      assertThat(delay.entered.await(10, TimeUnit.SECONDS)).isTrue();
      var stop = executor.submit(control::stop);
      var admission =
          executor.submit(
              () ->
                  copies.accept(
                      new BrokerGateway.SourceExecution(
                          "source",
                          "stage-race",
                          "stage-order",
                          "instrument",
                          BrokerGateway.Side.BUY,
                          BigDecimal.ONE,
                          new BigDecimal("100"),
                          "RUB",
                          clock.instant(),
                          clock.instant())));
      try {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> stop.get(300, TimeUnit.MILLISECONDS))
            .isInstanceOf(TimeoutException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> admission.get(300, TimeUnit.MILLISECONDS))
            .isInstanceOf(TimeoutException.class);
      } finally {
        delay.release.countDown();
      }
      assertThat(stop.get(10, TimeUnit.SECONDS).coefficient()).isEqualByComparingTo("2");
      var event = admission.get(10, TimeUnit.SECONDS);
      if (event.acceptedCoefficient() != null)
        assertThat(event.acceptedCoefficient()).isEqualByComparingTo("2");
      else assertThat(event.disposition()).isEqualTo("SKIPPED");
      recalculation.get(10, TimeUnit.SECONDS);
    }
    assertThat(control.status().mode()).isEqualTo("STOPPING");
    assertThat(control.status().coefficient()).isEqualByComparingTo("2");
  }

  enum CommitPhase {
    BEFORE_DELEGATE,
    AFTER_DELEGATE
  }

  enum WriteTarget {
    NUMERIC,
    PROMOTION
  }

  record DelayPlan(
      CommitPhase phase, WriteTarget target, boolean crash, long millis, boolean barrier) {}

  static final class SimulatedCrash extends Error {}

  static final class CommitDelay {
    final AtomicReference<DelayPlan> phase = new AtomicReference<>();
    final AtomicReference<PersistenceFault> persistenceFailure = new AtomicReference<>();
    volatile CountDownLatch entered = new CountDownLatch(1);
    volatile CountDownLatch release = new CountDownLatch(1);
    volatile Runnable afterPromotionCommit;
    volatile Runnable beforeNumericCommit, afterNumericCommit;

    void reset() {
      phase.set(null);
      persistenceFailure.set(null);
      entered = new CountDownLatch(1);
      release = new CountDownLatch(1);
      afterPromotionCommit = null;
      beforeNumericCommit = null;
      afterNumericCommit = null;
    }

    void arm(CommitPhase value) {
      phase.set(new DelayPlan(value, WriteTarget.NUMERIC, false, 5100, false));
    }

    void armPromotion() {
      phase.set(
          new DelayPlan(CommitPhase.AFTER_DELEGATE, WriteTarget.PROMOTION, false, 5100, false));
    }

    void armCrash() {
      phase.set(new DelayPlan(CommitPhase.AFTER_DELEGATE, WriteTarget.NUMERIC, true, 0, false));
    }

    void armClockAdvance(CommitPhase value) {
      phase.set(new DelayPlan(value, WriteTarget.NUMERIC, false, 0, false));
    }

    void armBarrier() {
      phase.set(new DelayPlan(CommitPhase.AFTER_DELEGATE, WriteTarget.NUMERIC, false, 0, true));
    }

    void pause(DelayPlan plan) throws Exception {
      if (plan.barrier()) {
        if (!release.await(4, TimeUnit.SECONDS))
          throw new IllegalStateException("Fixture barrier expired");
      } else if (plan.millis() > 0) Thread.sleep(plan.millis());
    }

    Connection wrap(Connection connection) {
      boolean[] capitalWrite = {false};
      boolean[] numericWrite = {false};
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if (method.getName().equals("prepareStatement")) {
                  String sql = ((String) args[0]).toLowerCase(Locale.ROOT);
                  if (sql.startsWith("insert into capital_candidate")) numericWrite[0] = true;
                  DelayPlan selected = phase.get();
                  if (selected != null
                      && (selected.target() == WriteTarget.NUMERIC
                              && sql.startsWith("insert into capital_candidate")
                          || selected.target() == WriteTarget.PROMOTION
                              && sql.startsWith("update control_state "))) capitalWrite[0] = true;
                }
                if (method.getName().equals("commit") && numericWrite[0]) {
                  numericWrite[0] = false;
                  PersistenceFault failure = persistenceFailure.getAndSet(null);
                  if (failure != null) {
                    if (failure == PersistenceFault.AFTER_COMMIT) connection.commit();
                    throw new SQLTransientConnectionException(
                        "Fixture lost numeric COMMIT response", "08006");
                  }
                }
                if (method.getName().equals("commit") && capitalWrite[0]) {
                  DelayPlan selected = phase.getAndSet(null);
                  capitalWrite[0] = false;
                  if (selected != null) {
                    if (selected.phase() == CommitPhase.BEFORE_DELEGATE) {
                      entered.countDown();
                      pause(selected);
                    }
                    if (selected.target() == WriteTarget.NUMERIC && beforeNumericCommit != null)
                      beforeNumericCommit.run();
                    Object result = invoke(connection, method, args);
                    if (selected.target() == WriteTarget.NUMERIC && afterNumericCommit != null)
                      afterNumericCommit.run();
                    if (selected.phase() == CommitPhase.AFTER_DELEGATE) entered.countDown();
                    if (selected.crash()) throw new SimulatedCrash();
                    if (selected.target() == WriteTarget.PROMOTION && afterPromotionCommit != null)
                      afterPromotionCommit.run();
                    if (selected.phase() == CommitPhase.AFTER_DELEGATE) pause(selected);
                    return result;
                  }
                }
                return invoke(connection, method, args);
              });
    }

    static Object invoke(Object target, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException failure) {
        throw failure.getCause();
      }
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class JdbcTiming {
    @Bean
    @Role(org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE)
    static CommitDelay capitalCommitDelay() {
      return new CommitDelay();
    }

    @Bean
    static BeanPostProcessor delayedCapitalDataSource(CommitDelay delay) {
      return new BeanPostProcessor() {
        @Override
        public Object postProcessAfterInitialization(Object bean, String name) {
          if (!(bean instanceof DataSource source)) return bean;
          return new DelegatingDataSource(source) {
            @Override
            public Connection getConnection() throws SQLException {
              return delay.wrap(super.getConnection());
            }

            @Override
            public Connection getConnection(String user, String password) throws SQLException {
              return delay.wrap(super.getConnection(user, password));
            }
          };
        }
      };
    }
  }
}
