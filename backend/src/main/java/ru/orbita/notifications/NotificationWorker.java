package ru.orbita.notifications;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
final class NotificationWorker {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationWorker.class);
  private final NotificationService service;
  private final TelegramSettings settings;
  private final ru.orbita.runtime.ApplicationReadiness readiness;
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private final Semaphore capacity = new Semaphore(4);

  NotificationWorker(
      NotificationService service,
      TelegramSettings settings,
      ru.orbita.runtime.ApplicationReadiness readiness) {
    this.service = service;
    this.settings = settings;
    this.readiness = readiness;
  }

  @Scheduled(fixedDelayString = "${orbita.telegram.poll-interval:PT1S}")
  void tick() {
    if (!readiness.initialized() || !settings.enabled() || !capacity.tryAcquire()) return;
    executor.submit(
        () -> {
          try {
            service.deliverDue();
          } catch (RuntimeException exception) {
            LOG.warn(
                "Не удалось обработать очередь Telegram: {}", exception.getClass().getSimpleName());
          } finally {
            capacity.release();
          }
        });
  }

  @PreDestroy
  void close() {
    executor.close();
  }
}
