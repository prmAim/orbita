package ru.orbita.web;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.List;
import org.springframework.web.bind.annotation.*;
import ru.orbita.operations.MigrationService;

@RestController
@RequestMapping("/api/migration")
final class MigrationController {
  private final MigrationService migration;

  MigrationController(MigrationService migration) {
    this.migration = migration;
  }

  @PostMapping("/manifest")
  MigrationService.Manifest export(@Valid @RequestBody Execution request) {
    return migration.exportManifest(request.executionId());
  }

  @PostMapping("/verify")
  MigrationService.Verification compare(@RequestBody MigrationService.Manifest manifest) {
    return migration.compare(manifest);
  }

  @GetMapping("/verifications")
  List<MigrationService.Verification> list() {
    return migration.verifications();
  }

  record Execution(@NotBlank @Size(max = 500) String executionId) {}
}
