package ru.orbita.copy;

import java.time.Clock;
import java.util.Optional;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.OrbitaApplication;
import ru.orbita.broker.BrokerGateway;
import ru.orbita.broker.BrokerTlsTestSupport;
import ru.orbita.broker.WholeChainBrokerSimulator;
import ru.orbita.control.ClockEvidence;

/** Boots another isolated application instance against the same synthetic bank and database. */
final class WholeChainTestApplication {
  private WholeChainTestApplication() {}

  static ConfigurableApplicationContext start(
      PostgreSQLContainer postgres, WholeChainBrokerSimulator bank, Clock clock) {
    return new SpringApplicationBuilder(OrbitaApplication.class)
        .web(WebApplicationType.NONE)
        .registerShutdownHook(false)
        .initializers(
            context -> {
              var registry = (GenericApplicationContext) context;
              registry.registerBean(
                  "wholeChainGateway",
                  BrokerGateway.class,
                  () -> {
                    try {
                      return bank.gateway(clock, BrokerTlsTestSupport.NO_EVENTS);
                    } catch (Exception failure) {
                      throw new IllegalStateException("Local TLS gateway setup failed", failure);
                    }
                  },
                  definition -> {
                    definition.setPrimary(true);
                    definition.setDestroyMethodName("close");
                  });
              registry.registerBean(
                  "wholeChainClock",
                  Clock.class,
                  () -> clock,
                  definition -> definition.setPrimary(true));
              registry.registerBean(
                  "wholeChainClockEvidence",
                  ClockEvidence.class,
                  () ->
                      new ClockEvidence() {
                        public Optional<Bound> current() {
                          return Optional.of(
                              new Bound(
                                  clock.instant(), clock.instant(), "synthetic whole-chain clock"));
                        }

                        public Optional<Bound> atReceipt(long received) {
                          return current();
                        }
                      },
                  definition -> definition.setPrimary(true));
            })
        .run(
            "--spring.datasource.url=" + postgres.getJdbcUrl(),
            "--spring.datasource.username=" + postgres.getUsername(),
            "--spring.datasource.password=" + postgres.getPassword(),
            "--orbita.source-account=" + WholeChainBrokerSimulator.SOURCE,
            "--orbita.mirror-account=" + WholeChainBrokerSimulator.MIRROR,
            "--orbita.margin-disabled-confirmed=true",
            "--orbita.worker.enabled=false",
            "--orbita.broker.enabled=false",
            "--orbita.telegram.enabled=false",
            "--orbita.backup.enabled=false");
  }
}
