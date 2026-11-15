package ru.orbita.control;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
public class RuntimeConfiguration {
  @Bean
  public Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  public TransactionTemplate transactionTemplate(PlatformTransactionManager manager) {
    return new TransactionTemplate(manager);
  }
}
