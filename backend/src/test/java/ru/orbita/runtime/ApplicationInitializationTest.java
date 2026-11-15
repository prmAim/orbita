package ru.orbita.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class ApplicationInitializationTest {
  @Test
  void passwordResetDoesNotCreateRecoveryInitializerOrOpenBackgroundWorkers() {
    new ApplicationContextRunner()
        .withUserConfiguration(ApplicationInitialization.class, ApplicationReadiness.class)
        .withPropertyValues("orbita.admin.reset-password=true")
        .run(
            context -> {
              assertThat(context).hasNotFailed().doesNotHaveBean(ApplicationInitialization.class);
              assertThat(context.getBean(ApplicationReadiness.class).initialized()).isFalse();
              assertThat(context.getBean(ApplicationReadiness.class).ready()).isFalse();
            });
  }
}
