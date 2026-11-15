package ru.orbita.valuation;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;
import ru.orbita.valuation.QualifiedRateProvider.Purpose;

/**
 * Authenticated local manifest context; neither its values nor this endpoint establish an FX rate.
 */
@RestController
@ConditionalOnProperty(prefix = "orbita.rub-valuation", name = "evidence-directory")
@ConditionalOnProperty(
    name = "orbita.admin.reset-password",
    havingValue = "false",
    matchIfMissing = true)
public class RubValuationContextController {
  private final QualifiedRateFileProvider provider;

  public RubValuationContextController(QualifiedRateFileProvider provider) {
    this.provider = provider;
  }

  @GetMapping("/api/valuation/context")
  public QualifiedRateFileProvider.Context context(
      @RequestParam String instrumentId,
      @RequestParam String currency,
      @RequestParam Purpose purpose) {
    return provider.context(instrumentId, currency, purpose);
  }
}
