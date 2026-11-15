package ru.orbita.web;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.web.bind.annotation.*;
import ru.orbita.control.ControlService;
import ru.orbita.copy.AcceptanceService;
import ru.orbita.copy.CopyService;
import ru.orbita.copy.ShadowCheck;
import ru.orbita.journal.JournalEntry;

@RestController
@RequestMapping("/api")
final class ControlController {
  private final ControlService control;
  private final AcceptanceService acceptance;
  private final CopyService copies;

  ControlController(ControlService control, AcceptanceService acceptance, CopyService copies) {
    this.control = control;
    this.acceptance = acceptance;
    this.copies = copies;
  }

  @GetMapping("/positions")
  List<PositionResponse> positions() {
    return copies.positions().stream()
        .map(
            row ->
                new PositionResponse(
                    row.instrumentId(),
                    row.copiedLots(),
                    row.manualLots(),
                    StatusResponse.decimal(row.brokerLots()),
                    row.brokerReadAt(),
                    row.brokerInventoryQualified()))
        .toList();
  }

  @GetMapping("/acceptance/checks")
  List<ShadowCheck.View> acceptance() {
    return acceptance.checks();
  }

  @GetMapping("/status")
  StatusResponse status() {
    return StatusResponse.from(control.status());
  }

  @GetMapping("/instruments")
  List<ControlService.InstrumentStatus> instruments() {
    return control.instruments();
  }

  @GetMapping("/journal")
  List<JournalEntry.View> journal(
      @RequestParam(defaultValue = "0") long before,
      @RequestParam(defaultValue = "100") int limit) {
    return copies.journal(before, limit);
  }

  @PostMapping("/control/start")
  StatusResponse start() {
    return StatusResponse.from(control.start());
  }

  @PostMapping("/control/stop")
  StatusResponse stop() {
    return StatusResponse.from(control.stop());
  }

  @PostMapping("/control/recalculate")
  StatusResponse recalculate() {
    return StatusResponse.from(control.recalculate());
  }

  @PostMapping("/control/permit")
  StatusResponse permit(@Valid @RequestBody Permit request) {
    return StatusResponse.from(control.issuePermit(request.rubLimit()));
  }

  @PostMapping("/control/revoke-permit")
  StatusResponse revoke() {
    return StatusResponse.from(control.revokePermit());
  }

  @PostMapping("/control/automatic")
  StatusResponse automatic() {
    return StatusResponse.from(control.enableAutomatic());
  }

  @PostMapping("/instruments/{id}/pause")
  void pause(@PathVariable @Size(max = 200) String id, @Valid @RequestBody Reason reason) {
    control.pauseInstrument(id, reason.text());
  }

  @PostMapping("/instruments/{id}/resume")
  void resume(@PathVariable @Size(max = 200) String id) {
    control.resumeInstrument(id);
  }

  record Permit(@NotNull @DecimalMin(value = "0", inclusive = false) BigDecimal rubLimit) {}

  record Reason(@NotBlank @Size(max = 1000) String text) {}

  record PositionResponse(
      String instrumentId,
      long copiedLots,
      long manualLots,
      String brokerLots,
      java.time.Instant brokerReadAt,
      boolean brokerInventoryQualified) {}
}
