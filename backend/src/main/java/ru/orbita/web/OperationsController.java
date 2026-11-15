package ru.orbita.web;

import java.util.List;
import java.util.UUID;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.orbita.notifications.NotificationService;
import ru.orbita.operations.BackupService;

@RestController
final class OperationsController {
  private final NotificationService notifications;
  private final BackupService backups;

  OperationsController(NotificationService notifications, BackupService backups) {
    this.notifications = notifications;
    this.backups = backups;
  }

  @GetMapping("/api/backups")
  List<BackupService.View> backups() {
    return backups.list();
  }

  @GetMapping("/api/csrf")
  CsrfToken csrf(CsrfToken token) {
    return token;
  }

  @GetMapping("/api/notifications")
  List<NotificationService.View> notifications(@RequestParam(defaultValue = "100") int limit) {
    return notifications.list(limit);
  }

  @GetMapping("/api/notifications/{id}/attempts")
  List<NotificationService.Attempt> attempts(@PathVariable UUID id) {
    return notifications.attempts(id);
  }

  @PostMapping("/api/notifications/{id}/retry")
  NotificationService.View retry(@PathVariable UUID id) {
    return notifications.retryNow(id);
  }
}
