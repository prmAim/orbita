package ru.orbita.web;

import java.util.Arrays;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Local interactive maintenance entry point. No password is accepted in a command line or logged.
 */
@Component
@ConditionalOnProperty(name = "orbita.admin.reset-password", havingValue = "true")
final class CredentialAdmin implements ApplicationRunner {
  private final OwnerCredentials credentials;
  private final ConfigurableApplicationContext context;

  CredentialAdmin(OwnerCredentials credentials, ConfigurableApplicationContext context) {
    this.credentials = credentials;
    this.context = context;
  }

  @Override
  public void run(ApplicationArguments arguments) {
    var console = System.console();
    if (console == null) throw new IllegalStateException("Нужен локальный интерактивный терминал");
    credentials.lock();
    console.printf("Прежний доступ отозван. Журнал закрыт до входа с новым паролем.%n");
    char[] password = console.readPassword("Новый пароль (от 12 символов): ");
    char[] confirmation = console.readPassword("Повторите пароль: ");
    try {
      if (password == null || confirmation == null || !Arrays.equals(password, confirmation))
        throw new IllegalArgumentException("Пароли не совпадают. Доступ остаётся закрытым.");
      credentials.replace(password);
      console.printf("Пароль сохранён.%n");
    } finally {
      if (password != null) Arrays.fill(password, '\0');
      if (confirmation != null) Arrays.fill(confirmation, '\0');
      SpringApplication.exit(context);
    }
  }
}
