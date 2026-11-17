package ru.orbita.control;

import java.io.IOException;
import java.security.*;
import java.util.HexFormat;
import org.springframework.stereotype.Component;
import ru.orbita.copy.*;

/**
 * Bytecode fingerprint invalidates shadow qualification whenever the installed calculation changes.
 */
@Component
public class AlgorithmVersion {
  private final String fingerprint;

  public AlgorithmVersion() {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      var types = new java.util.TreeMap<String, Class<?>>();
      for (Class<?> type :
          new Class<?>[] {
            Rational.class,
            CopyCalculation.class,
            TradePolicy.class,
            PositionRecovery.class,
            CopyService.class,
            PositionRecoveryService.class,
            TradingService.class,
            ControlService.class,
            CapitalCandidateService.class,
            TimeAuthority.class,
            SourceCycles.class,
            ManualReconciliationService.class,
            AcceptanceService.class,
            ReconciliationService.class,
            DurableSourceFeedJournal.class,
            ClockEvidence.class,
            ru.orbita.broker.BrokerGateway.class,
            ru.orbita.broker.OrderSdkEvidence.class,
            ru.orbita.broker.BrokerReceiptClock.class,
            ru.orbita.broker.BrokerQualifications.class,
            ru.orbita.broker.BrokerQualificationSettings.class,
            ru.orbita.broker.TInvestBrokerGateway.class,
            ru.orbita.broker.SourceFeedJournal.class,
            ru.orbita.time.QualifiedClockEvidenceFile.class,
            ru.orbita.valuation.RubValuation.class,
            ru.orbita.valuation.ExactRub.class,
            ru.orbita.valuation.QualifiedRateProvider.class,
            ru.orbita.valuation.QualifiedRateFileProvider.class
          }) collect(type, types);
      // The stream parser remains package-private to its broker module.
      collect(
          Class.forName("ru.orbita.broker.SourceExecutionFeed", false, getClass().getClassLoader()),
          types);
      for (Class<?> type : types.values()) {
        try (var input =
            type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
          if (input == null) throw new IllegalStateException("Missing calculation artifact");
          digest.update(type.getName().getBytes(java.nio.charset.StandardCharsets.UTF_8));
          digest.update(input.readAllBytes());
        }
      }
      fingerprint = HexFormat.of().formatHex(digest.digest());
    } catch (IOException | NoSuchAlgorithmException | ClassNotFoundException e) {
      throw new IllegalStateException("Cannot identify calculation artifact", e);
    }
  }

  private static void collect(Class<?> type, java.util.Map<String, Class<?>> types) {
    for (Class<?> member : type.getNestHost().getNestMembers())
      types.putIfAbsent(member.getName(), member);
  }

  public String current() {
    return fingerprint;
  }
}
