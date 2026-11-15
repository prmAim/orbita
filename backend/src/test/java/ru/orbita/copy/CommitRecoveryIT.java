package ru.orbita.copy;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.control.*;

@SpringBootTest(
    properties = {
      "orbita.source-account=source",
      "orbita.mirror-account=mirror",
      "orbita.margin-disabled-confirmed=true",
      "orbita.worker.enabled=false"
    })
@Import({CopyLifecycleIT.Fixture.class, CommitRecoveryIT.JdbcFaults.class})
class CommitRecoveryIT {
  @Autowired CopyService copies;
  @Autowired TradingService trading;
  @Autowired ControlService control;
  @Autowired AlgorithmVersion algorithms;
  @Autowired CopyLifecycleIT.MutableClock clock;
  @Autowired CopyLifecycleIT.Simulator broker;
  @Autowired JdbcTemplate db;
  @Autowired CommitFault fault;
  @Autowired DurableSourceFeedJournal feed;

  @BeforeEach
  void clean() {
    feed.recovered();
    fault.plan.set(null);
    fault.slowControlWriteMillis.set(0);
    fault.slowSendCommitMillis.set(0);
    fault.afterSendCommit = null;
    fault.beforeSendCommit = null;
    fault.afterCancelCommit = null;
    CopyLifecycleIT.reset(db, broker, clock);
  }

  @org.junit.jupiter.api.Test
  void slowFinalClockObservationIsIncludedInResourceAgeBeforeActualPost() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(
        new SourceExecution(
            "source",
            "slow-final-clock",
            "slow-final-clock-order",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    fault.slowSendCommitMillis.set(1);
    fault.afterSendCommit =
        () ->
            clock.nextEvidence =
                () -> {
                  try {
                    Thread.sleep(5050);
                  } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                  }
                };
    trading.dispatchReady();
    assertThat(broker.posts).isZero();
    assertThat(control.status().permitLimit()).isEqualByComparingTo("1000");
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("WAITING"));
    trading.dispatchReady();
    assertThat(broker.posts).isEqualTo(1);
  }

  @org.junit.jupiter.api.Test
  void asynchronousPersistenceFailureAfterSendCommitBlocksPostUntilDurableFeedRecovery() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(
        new SourceExecution(
            "source",
            "async-observer",
            "async-order",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    fault.slowSendCommitMillis.set(1);
    fault.afterSendCommit = feed::persistenceUnavailable;
    trading.dispatchReady();
    assertThat(broker.posts).isZero();
    assertThat(control.gate().reconciled()).isFalse();
    assertThat(control.status().permitLimit()).isEqualByComparingTo("1000");
    trading.dispatchReady();
    assertThat(broker.posts).isZero();
    broker.partialNext = 0;
    feed.recovered();
    trading.dispatchReady();
    assertThat(broker.posts).isEqualTo(1);
    feed.persistenceUnavailable();
    clock.now = clock.now.plusSeconds(120);
    trading.reconcileOnly();
    assertThat(broker.cancels).isZero();
    feed.recovered();
    trading.reconcileOnly();
    assertThat(broker.cancels).isEqualTo(1);
  }

  @org.junit.jupiter.api.Test
  void asynchronousPersistenceFailureAfterCancelCommitKeepsCancellationKnownUnsentAndRetryable() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    broker.partialNext = 0;
    copies.accept(
        new SourceExecution(
            "source",
            "async-cancel",
            "async-cancel-order",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    trading.dispatchReady();
    clock.now = clock.now.plusSeconds(120);
    fault.afterCancelCommit = feed::persistenceUnavailable;
    trading.reconcileOnly();
    assertThat(broker.cancels).isZero();
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("ACTIVE"));
    feed.recovered();
    trading.reconcileOnly();
    assertThat(broker.cancels).isEqualTo(1);
  }

  @org.junit.jupiter.api.Test
  void slowDatabaseFlushDoesNotAcceptAnOverdueCapitalPair() {
    control.markReconciled();
    control.start();
    broker.mirrorCapital = new BigDecimal("200000");
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 4) fault.slowControlWriteMillis.set(5050);
        };
    control.recalculate();
    assertThat(control.status().coefficient()).isEqualByComparingTo("1");
    assertThat(control.status().lastError()).contains("5 секунд");
    assertThat(control.status().mode()).isEqualTo("SHADOW");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void databaseFailureWhileRecordingCancellationStopsTheTickAndReconcilesWithoutRepeatingCancel(
      boolean committed) {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    broker.partialNext = 0;
    copies.accept(
        new SourceExecution(
            "source",
            "cancel-commit",
            "cancel-order",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    trading.dispatchReady();
    clock.now = clock.now.plusSeconds(60);
    fault.plan.set(new Plan("FINAL", committed));
    assertThatThrownBy(trading::reconcileOnly).isInstanceOf(RuntimeException.class);
    assertThat(broker.cancels).isEqualTo(1);
    assertThat(fault.plan.get()).isNull();
    control.recoverStartup();
    control.markReconciled();
    control.markFeedReady(true);
    trading.tick();
    trading.tick();
    assertThat(broker.cancels).isEqualTo(1);
    assertThat(copies.intents())
        .singleElement()
        .satisfies(i -> assertThat(i.status()).isEqualTo("FINAL"));
  }

  @ParameterizedTest
  @CsvSource({"NONE", "REVOKE", "STOP", "REPLACE"})
  void slowSendBarrierNeverPostsStaleResourcesAndDoesNotResurrectChangedPermit(String ownerAction) {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(
        new SourceExecution(
            "source",
            "late-send-commit",
            "late-send-order",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    fault.slowSendCommitMillis.set(5050);
    fault.afterSendCommit =
        switch (ownerAction) {
          case "REVOKE" -> () -> control.revokePermit();
          case "STOP" -> () -> control.stop();
          case "REPLACE" -> () -> control.issuePermit(new BigDecimal("2000"));
          default -> null;
        };
    trading.dispatchReady();
    assertThat(broker.posts).isZero();
    if (ownerAction.equals("NONE")) {
      assertThat(control.status().permitLimit()).isEqualByComparingTo("1000");
      trading.dispatchReady();
      assertThat(broker.posts).isEqualTo(1);
    } else if (ownerAction.equals("REPLACE")) {
      assertThat(control.status().permitLimit()).isEqualByComparingTo("2000");
      trading.dispatchReady();
      assertThat(broker.posts).isZero();
    } else {
      assertThat(control.status().permitLimit()).isNull();
      trading.tick();
      assertThat(broker.posts).isZero();
    }
  }

  @org.junit.jupiter.api.Test
  void instrumentPauseCannotCommitInTheMiddleOfTheSendingBarrier() throws Exception {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(
        new SourceExecution(
            "source",
            "pause-race",
            "pause-order",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    var reached = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.CountDownLatch(1);
    fault.beforeSendCommit =
        () -> {
          reached.countDown();
          try {
            if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS))
              throw new IllegalStateException("Fixture barrier timed out");
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
        };
    try (var pool = java.util.concurrent.Executors.newFixedThreadPool(2)) {
      var send = pool.submit(trading::dispatchReady);
      assertThat(reached.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      var pause = pool.submit(() -> control.pauseInstrument("instrument", "Согласованная пауза"));
      try {
        assertThatThrownBy(() -> pause.get(300, java.util.concurrent.TimeUnit.MILLISECONDS))
            .isInstanceOf(java.util.concurrent.TimeoutException.class);
      } finally {
        release.countDown();
      }
      send.get(5, java.util.concurrent.TimeUnit.SECONDS);
      pause.get(5, java.util.concurrent.TimeUnit.SECONDS);
    }
    var journal = copies.journal(0, 200);
    long sendId =
        journal.stream()
            .filter(e -> e.category().equals("SEND_STARTED"))
            .findFirst()
            .orElseThrow()
            .id();
    long pauseId =
        journal.stream()
            .filter(e -> e.category().equals("INSTRUMENT_PAUSED"))
            .findFirst()
            .orElseThrow()
            .id();
    assertThat(sendId).isLessThan(pauseId);
    assertThat(control.instruments())
        .singleElement()
        .satisfies(i -> assertThat(i.paused()).isTrue());
  }

  @ParameterizedTest
  @CsvSource({"SEND_STARTED,false", "SEND_STARTED,true", "UNKNOWN,false", "UNKNOWN,true"})
  void uncertainCommitIsReconciledFromDurableStateWithoutDuplicatingBrokerAction(
      String stage, boolean committed) {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    clock.now = clock.now.plusSeconds(1);
    control.issuePermit(new BigDecimal("1000"));
    clock.now = clock.now.plusSeconds(1);
    copies.accept(
        new SourceExecution(
            "source",
            "commit-trade",
            "commit-order",
            "instrument",
            Side.BUY,
            BigDecimal.ONE,
            new BigDecimal("100"),
            "RUB",
            clock.instant(),
            clock.instant()));
    fault.plan.set(new Plan(stage, committed));
    assertThatThrownBy(trading::dispatchReady).isInstanceOf(RuntimeException.class);
    assertThat(fault.plan.get())
        .isNull(); // the real JDBC transaction reached the intended durable barrier
    assertThat(broker.posts).isEqualTo(stage.equals("UNKNOWN") ? 1 : 0);
    control.recoverStartup();
    control.markReconciled();
    control.markFeedReady(true);
    trading.tick();
    trading.tick();
    trading.tick();
    if (stage.equals("SEND_STARTED") && committed) {
      assertThat(broker.posts).isZero();
      assertThat(copies.intents())
          .singleElement()
          .satisfies(
              i -> {
                assertThat(i.status()).isEqualTo("UNKNOWN");
                assertThat(i.reservedMoney()).isPositive();
              });
    } else {
      assertThat(broker.posts).isEqualTo(1);
      assertThat(copies.intents())
          .singleElement()
          .satisfies(
              i -> {
                assertThat(i.status()).isEqualTo("FINAL");
                assertThat(i.executedLots()).isEqualTo(1);
              });
    }
  }

  record Plan(String status, boolean commitBeforeDisconnect) {}

  static final class CommitFault {
    final AtomicReference<Plan> plan = new AtomicReference<>();
    final java.util.concurrent.atomic.AtomicLong slowControlWriteMillis =
        new java.util.concurrent.atomic.AtomicLong();
    final java.util.concurrent.atomic.AtomicLong slowSendCommitMillis =
        new java.util.concurrent.atomic.AtomicLong();
    volatile Runnable afterSendCommit;
    volatile Runnable beforeSendCommit;
    volatile Runnable afterCancelCommit;

    Connection wrap(Connection connection) {
      boolean[] selected = {false};
      boolean[] sends = {false};
      boolean[] cancels = {false};
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if (method.getName().equals("prepareStatement")) {
                  String sql = ((String) args[0]).toLowerCase(java.util.Locale.ROOT);
                  boolean writesIntent =
                      sql.startsWith("update copy_intent ")
                          || sql.startsWith("insert into copy_intent ");
                  boolean writesCapitalCandidate = sql.startsWith("insert into capital_candidate");
                  var statement = (PreparedStatement) invoke(connection, method, args);
                  return Proxy.newProxyInstance(
                      PreparedStatement.class.getClassLoader(),
                      new Class<?>[] {PreparedStatement.class},
                      (p, m, a) -> {
                        Plan current = plan.get();
                        if (writesIntent
                            && m.getName().equals("setString")
                            && a != null
                            && a.length == 2
                            && "SEND_STARTED".equals(a[1])) sends[0] = true;
                        if (writesIntent
                            && m.getName().equals("setString")
                            && a != null
                            && a.length == 2
                            && "CANCEL_STARTED".equals(a[1])) cancels[0] = true;
                        if (writesIntent
                            && current != null
                            && m.getName().equals("setString")
                            && a != null
                            && a.length == 2
                            && current.status().equals(a[1])) selected[0] = true;
                        if (writesCapitalCandidate
                            && (m.getName().equals("executeBatch")
                                || m.getName().equals("executeUpdate"))) {
                          long delay = slowControlWriteMillis.getAndSet(0);
                          if (delay > 0) Thread.sleep(delay);
                        }
                        return invoke(statement, m, a);
                      });
                }
                if (method.getName().equals("commit") && sends[0]) {
                  sends[0] = false;
                  Runnable before = beforeSendCommit;
                  beforeSendCommit = null;
                  if (before != null) before.run();
                  long delay = slowSendCommitMillis.getAndSet(0);
                  if (delay > 0) {
                    Thread.sleep(delay);
                    connection.commit();
                    Runnable action = afterSendCommit;
                    afterSendCommit = null;
                    if (action != null) {
                      var thread = Thread.ofVirtual().start(action);
                      thread.join();
                    }
                    return null;
                  }
                }
                if (method.getName().equals("commit") && cancels[0]) {
                  cancels[0] = false;
                  Runnable action = afterCancelCommit;
                  afterCancelCommit = null;
                  if (action != null) {
                    connection.commit();
                    action.run();
                    return null;
                  }
                }
                if (method.getName().equals("commit") && selected[0]) {
                  Plan current = plan.getAndSet(null);
                  selected[0] = false;
                  if (current != null) {
                    if (current.commitBeforeDisconnect()) connection.commit();
                    throw new SQLTransientConnectionException(
                        "Fixture lost COMMIT response", "08006");
                  }
                }
                return invoke(connection, method, args);
              });
    }

    static Object invoke(Object target, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class JdbcFaults {
    @Bean
    @Role(org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE)
    static CommitFault commitFault() {
      return new CommitFault();
    }

    @Bean
    static BeanPostProcessor faultDataSource(CommitFault fault) {
      return new BeanPostProcessor() {
        @Override
        public Object postProcessAfterInitialization(Object bean, String name) {
          if (!(bean instanceof DataSource source)) return bean;
          return new DelegatingDataSource(source) {
            @Override
            public Connection getConnection() throws SQLException {
              return fault.wrap(super.getConnection());
            }

            @Override
            public Connection getConnection(String user, String password) throws SQLException {
              return fault.wrap(super.getConnection(user, password));
            }
          };
        }
      };
    }
  }
}
