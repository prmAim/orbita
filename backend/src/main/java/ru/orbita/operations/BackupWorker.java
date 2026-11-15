package ru.orbita.operations;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
final class BackupWorker {
  private final BackupService service;
  private final ru.orbita.runtime.ApplicationReadiness readiness;
  private final AtomicBoolean running = new AtomicBoolean();
  private final ExecutorService executor =
      Executors.newSingleThreadExecutor(Thread.ofVirtual().name("orbita-backup").factory());

  BackupWorker(BackupService service, ru.orbita.runtime.ApplicationReadiness readiness) {
    this.service = service;
    this.readiness = readiness;
  }

  @Scheduled(fixedDelayString = "${orbita.backup.poll-interval:PT1M}")
  void tick() {
    if (!readiness.initialized() || !running.compareAndSet(false, true)) return;
    executor.submit(
        () -> {
          try {
            service.tick();
          } catch (RuntimeException failure) {
            LoggerFactory.getLogger(BackupWorker.class)
                .warn("Ошибка резервного копирования: {}", failure.getClass().getSimpleName());
          } finally {
            running.set(false);
          }
        });
  }

  @PreDestroy
  void close() {
    executor.close();
  }
}
