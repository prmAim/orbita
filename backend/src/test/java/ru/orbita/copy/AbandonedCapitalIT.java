package ru.orbita.copy;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import ru.orbita.broker.BrokerConnectivityEvents;
import ru.orbita.broker.BrokerConnectivityService;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.BrokerPersistenceFailure;
import ru.orbita.control.AlgorithmVersion;
import ru.orbita.control.ControlService;

@SpringBootTest(
    properties = {
      "orbita.source-account=source",
      "orbita.mirror-account=mirror",
      "orbita.margin-disabled-confirmed=true",
      "orbita.worker.enabled=false"
    })
@Import({CopyLifecycleIT.Fixture.class, AbandonedCapitalIT.JdbcFaults.class})
class AbandonedCapitalIT {
  @Autowired ControlService control;
  @Autowired CopyService copies;
  @Autowired TradingService trading;
  @Autowired CopyLifecycleIT.Simulator broker;
  @Autowired CopyLifecycleIT.MutableClock clock;
  @Autowired AlgorithmVersion algorithms;
  @Autowired JdbcTemplate database;
  @Autowired BrokerConnectivityService connectivity;
  @Autowired CommitFault fault;

  @BeforeEach
  void reset() {
    fault.commitBeforeDisconnect.set(null);
    database.execute(
        "alter table broker_connectivity drop constraint if exists capital_observer_fault");
    CopyLifecycleIT.reset(database, broker, clock);
    connectivity.restoredCopyOpened();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void uncertainCleanupCommitIsRetriedWithoutContinuingTheFailedTick(boolean committed) {
    prepareReadyTradingWork();
    connectivity.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
    database.execute(
        "alter table broker_connectivity add constraint capital_observer_fault check(last_recovered_at is null)");
    broker.capitalHook =
        () -> {
          try {
            connectivity.readSucceeded();
          } catch (BrokerPersistenceFailure failure) {
            fault.commitBeforeDisconnect.set(committed);
            throw failure;
          }
        };
    Throwable original =
        catchThrowable(
            () -> {
              control.recalculate();
              trading.tick();
            });
    assertThat(original).hasCauseInstanceOf(BrokerPersistenceFailure.class);
    assertThat(original.getSuppressed()).hasSize(1);
    assertThat(fault.commitBeforeDisconnect.get()).isNull();
    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.cancels).isZero();
    assertThat(control.status().coefficient()).isEqualByComparingTo("1");
    assertThat(control.status().recalculationRunning()).isEqualTo(!committed);
    database.execute("alter table broker_connectivity drop constraint capital_observer_fault");
    control.recoverAbandonedCapitalAttempts();
    control.recoverAbandonedCapitalAttempts();
    assertThat(control.status().recalculationRunning()).isFalse();
    assertThat(control.status().reconciled()).isFalse();
    broker.mirrorCapital = new BigDecimal("200000");
    assertThat(control.recalculate().coefficient()).isEqualByComparingTo("2");
    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.cancels).isZero();
  }

  @Test
  void abandonmentOfAnOlderGenerationNeverClearsANewerLiveAttempt() throws Exception {
    control.markReconciled();
    control.start();
    connectivity.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
    database.execute(
        "alter table broker_connectivity add constraint capital_observer_fault check(last_recovered_at is null)");
    CountDownLatch oldEntered = new CountDownLatch(1),
        newEntered = new CountDownLatch(1),
        oldRelease = new CountDownLatch(1),
        newRelease = new CountDownLatch(1);
    broker.capitalHook =
        () -> {
          if (broker.capitalCalls == 3) {
            oldEntered.countDown();
            await(oldRelease);
            connectivity.readSucceeded();
          } else if (broker.capitalCalls == 4) {
            newEntered.countDown();
            await(newRelease);
          }
        };
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var old = executor.submit(() -> catchThrowable(control::recalculate));
      try {
        assertThat(oldEntered.await(5, TimeUnit.SECONDS)).isTrue();
        control.stop();
        control.stoppedAfterReconciliation();
        control.markReconciled();
        broker.mirrorCapital = new BigDecimal("200000");
        var current = executor.submit(control::start);
        assertThat(newEntered.await(5, TimeUnit.SECONDS)).isTrue();
        oldRelease.countDown();
        assertThat(old.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(BrokerPersistenceFailure.class);
        control.recoverAbandonedCapitalAttempts();
        assertThat(control.status().recalculationRunning()).isTrue();
        assertThat(control.status().reconciled()).isTrue();
        database.execute("alter table broker_connectivity drop constraint capital_observer_fault");
        newRelease.countDown();
        assertThat(current.get(5, TimeUnit.SECONDS).coefficient()).isEqualByComparingTo("2");
        assertThat(control.status().mode()).isEqualTo("SHADOW");
        assertThat(control.status().recalculationRunning()).isFalse();
      } finally {
        oldRelease.countDown();
        newRelease.countDown();
      }
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS))
        throw new IllegalStateException("Fixture barrier timed out");
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(failure);
    }
  }

  static final class CommitFault {
    final AtomicReference<Boolean> commitBeforeDisconnect = new AtomicReference<>();

    Connection wrap(Connection connection) {
      boolean[] cleanup = {false};
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if (method.getName().equals("prepareStatement")
                    && ((String) args[0])
                        .toLowerCase(java.util.Locale.ROOT)
                        .startsWith("update control_state ")
                    && commitBeforeDisconnect.get() != null) cleanup[0] = true;
                if (method.getName().equals("commit") && cleanup[0]) {
                  cleanup[0] = false;
                  Boolean committed = commitBeforeDisconnect.getAndSet(null);
                  if (committed != null) {
                    if (committed) connection.commit();
                    throw new SQLTransientConnectionException(
                        "Fixture lost cleanup COMMIT response", "08006");
                  }
                }
                try {
                  return method.invoke(connection, args);
                } catch (InvocationTargetException failure) {
                  throw failure.getCause();
                }
              });
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class JdbcFaults {
    @Bean
    @org.springframework.context.annotation.Role(
        org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE)
    static CommitFault abandonedCommitFault() {
      return new CommitFault();
    }

    @Bean
    static BeanPostProcessor abandonedFaultDataSource(CommitFault fault) {
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

  @AfterEach
  void removeFault() {
    database.execute(
        "alter table broker_connectivity drop constraint if exists capital_observer_fault");
  }

  @Test
  void
      failedConnectivityPersistenceStopsTheTickAndNextExplicitRecalculationCanRecoverInTheSameJvm() {
    prepareReadyTradingWork();
    connectivity.connectionLost(BrokerConnectivityEvents.Cause.TRANSPORT_UNAVAILABLE);
    database.execute(
        "alter table broker_connectivity add constraint capital_observer_fault check(last_recovered_at is null)");
    broker.capitalHook = connectivity::readSucceeded;
    Throwable failure =
        catchThrowable(
            () -> {
              control.recalculate();
              trading.tick();
            });
    assertThat(failure)
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(BrokerPersistenceFailure.class);
    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.cancels).isZero();
    assertThat(control.status().coefficient()).isEqualByComparingTo("1");
    assertThat(connectivity.status().disconnected()).isTrue();

    database.execute("alter table broker_connectivity drop constraint capital_observer_fault");
    broker.mirrorCapital = new BigDecimal("200000");
    var result = control.recalculate();
    assertThat(result.recalculationRunning()).isFalse();
    assertThat(result.coefficient()).isEqualByComparingTo("2");
    assertThat(result.reconciled()).isFalse();
    assertThat(broker.capitalCalls).isEqualTo(5);
    assertThat(broker.posts).isEqualTo(1);
    assertThat(broker.cancels).isZero();
  }

  private void prepareReadyTradingWork() {
    control.markReconciled();
    control.start();
    control.markFeedReady(true);
    control.shadowChecked(algorithms.current(), "BUY", true);
    control.shadowChecked(algorithms.current(), "SELL", true);
    control.confirmLiveAcceptance();
    control.enableAutomatic();
    clock.now = clock.now.plusSeconds(1);
    broker.partialNext = 0;
    copies.accept(execution("active-order"));
    trading.dispatchReady();
    assertThat(broker.posts).isEqualTo(1);
    clock.now = clock.now.plusSeconds(61);
    copies.accept(execution("waiting-order"));
  }

  private BrokerGateway.SourceExecution execution(String id) {
    return new BrokerGateway.SourceExecution(
        "source",
        id,
        "source-" + id,
        "instrument",
        BrokerGateway.Side.BUY,
        BigDecimal.ONE,
        new BigDecimal("100"),
        "RUB",
        clock.instant(),
        clock.instant());
  }
}
