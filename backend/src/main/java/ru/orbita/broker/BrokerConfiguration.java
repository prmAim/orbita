package ru.orbita.broker;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Clock;
import java.util.Properties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.orbita.control.OrbitaProperties;
import ru.ttech.piapi.core.connector.ConnectorConfiguration;
import ru.ttech.piapi.core.connector.ServiceStubFactory;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(BrokerQualificationSettings.class)
@ConditionalOnProperty(
    name = "orbita.admin.reset-password",
    havingValue = "false",
    matchIfMissing = true)
public class BrokerConfiguration {
  @Bean
  @ConditionalOnMissingBean
  BrokerQualifications brokerQualifications(
      BrokerQualificationSettings settings, OrbitaProperties accounts) {
    return settings.qualifiedProfile(accounts);
  }

  @Bean(destroyMethod = "close")
  @ConditionalOnProperty(prefix = "orbita.broker", name = "enabled", havingValue = "true")
  BrokerGateway brokerGateway(
      BrokerSettings settings,
      OrbitaProperties accounts,
      Clock clock,
      BrokerQualifications qualifications,
      BrokerConnectivityEvents connectivity)
      throws IOException {
    if (!accounts.accountsConfigured() || settings.tokenFile() == null)
      throw new IllegalStateException(
          "Broker requires distinct account identifiers and a token file");
    String token = Files.readString(settings.tokenFile()).strip();
    if (token.isBlank()) throw new IllegalStateException("Broker token file is empty");
    var properties = new Properties();
    properties.setProperty("token", token);
    properties.setProperty("app.name", "orbita");
    var configuration = ConnectorConfiguration.loadFromProperties(properties);
    return new TInvestBrokerGateway(
        ServiceStubFactory.create(configuration),
        accounts.sourceAccount(),
        accounts.mirrorAccount(),
        clock,
        qualifications,
        connectivity);
  }
}
