package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.orbita.broker.BrokerGateway.Side.*;

import java.math.BigDecimal;
import java.time.*;
import org.junit.jupiter.api.Test;

class TradePolicyTest {
  @Test
  void wholeConfirmedAgeRangeMustFitWithinSixtySeconds() {
    Instant execution = Instant.parse("2026-11-15T00:00:00Z");
    assertThat(TradePolicy.age(execution, execution.plusMillis(59800), Duration.ofMillis(500)))
        .isEqualTo(TradePolicy.Age.EXPIRED);
    assertThat(TradePolicy.age(execution, execution.plusMillis(59500), Duration.ofMillis(500)))
        .isEqualTo(TradePolicy.Age.ALLOWED);
    assertThat(TradePolicy.age(execution, execution.plusSeconds(1), null))
        .isEqualTo(TradePolicy.Age.WAIT_FOR_TIME);
    assertThat(TradePolicy.age(execution.plusSeconds(2), execution, Duration.ZERO))
        .isEqualTo(TradePolicy.Age.FUTURE_TIMESTAMP);
  }

  @Test
  void priceStepCannotExceedTheHalfPercentDeterioration() {
    assertThat(TradePolicy.limit(BUY, new BigDecimal("101"), new BigDecimal("0.1")))
        .isEqualByComparingTo("101.5");
    assertThat(TradePolicy.limit(SELL, new BigDecimal("101"), new BigDecimal("0.1")))
        .isEqualByComparingTo("100.5");
    assertThat(
            TradePolicy.affordable(
                5,
                10,
                new BigDecimal("100"),
                new BigDecimal("3100"),
                new BigDecimal("50"),
                new BigDecimal("100")))
        .isEqualTo(2);
  }

  @Test
  void buyAffordabilityUsesCommissionForTheVolumeThatFits() {
    long affordable =
        TradePolicy.affordable(
            1000,
            1,
            new BigDecimal("100"),
            new BigDecimal("150"),
            new BigDecimal("100"),
            BigDecimal.ZERO);

    assertThat(affordable).isEqualTo(1);
    assertThat(TradePolicy.commissionForLots(1000, affordable, new BigDecimal("100")))
        .isEqualByComparingTo("0.10");
  }
}
