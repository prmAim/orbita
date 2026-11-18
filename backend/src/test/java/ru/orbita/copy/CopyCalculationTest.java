package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class CopyCalculationTest {
  @Test
  void fractionalContributionsAccumulateWithinOneSourceOrderAndKeepTheAcceptedCoefficient() {
    var calculation = new CopyCalculation();
    var first =
        calculation.accept(
            "trade-1",
            "order-1",
            new BigDecimal("1"),
            new BigDecimal("0.4"),
            new BigDecimal("100"),
            Instant.EPOCH);
    var second =
        calculation.accept(
            "trade-2",
            "order-1",
            new BigDecimal("1"),
            new BigDecimal("0.7"),
            new BigDecimal("110"),
            Instant.EPOCH.plusSeconds(70));
    var duplicate =
        calculation.accept(
            "trade-1",
            "order-1",
            new BigDecimal("1"),
            new BigDecimal("9"),
            new BigDecimal("999"),
            Instant.EPOCH);
    assertThat(first.newLots()).isZero();
    assertThat(second.newLots()).isEqualTo(1);
    assertThat(second.referencePrice()).isEqualByComparingTo("110");
    assertThat(duplicate.duplicate()).isTrue();
    assertThat(calculation.targetLots("order-1")).isEqualTo(1);
    assertThat(calculation.targetLots("order-2")).isZero();
  }

  @Test
  void fiftyLotsComeFromTenSourceLotsAtFiveAndSubsequentResourceSkipCannotIncreaseNextIncrement() {
    var calculation = new CopyCalculation();
    assertThat(
            calculation
                .accept(
                    "first",
                    "order",
                    new BigDecimal("10"),
                    new BigDecimal("5"),
                    new BigDecimal("100"),
                    Instant.EPOCH)
                .newLots())
        .isEqualTo(50);
    // Forty-five executed and five permanently skipped consume the first target; only the newly
    // formed ten are due.
    assertThat(
            calculation
                .accept(
                    "second",
                    "order",
                    new BigDecimal("2"),
                    new BigDecimal("5"),
                    new BigDecimal("100"),
                    Instant.EPOCH)
                .newLots())
        .isEqualTo(10);
  }

  @Test
  void skippedTenLotsRemainInSameSourceOrderTargetWhenTwoMoreLotsArrive() {
    var calculation = new CopyCalculation();
    var first =
        calculation.accept(
            "first-trade",
            "same-source-order",
            BigDecimal.TEN,
            BigDecimal.ONE,
            new BigDecimal("100"),
            Instant.EPOCH);
    var next =
        calculation.accept(
            "next-trade",
            "same-source-order",
            BigDecimal.valueOf(2),
            BigDecimal.ONE,
            new BigDecimal("110"),
            Instant.EPOCH.plusSeconds(1));

    assertThat(first.newLots()).isEqualTo(10);
    assertThat(next.newLots()).isEqualTo(2);
    assertThat(next.targetLots()).isEqualTo(12);
  }

  @Test
  void twoPointSevenTwiceMakesTwoThenThreeAndRevaluationDoesNotChangeOldContributions() {
    var fractional = new CopyCalculation();
    assertThat(
            fractional
                .accept(
                    "a", "o", BigDecimal.ONE, new BigDecimal("2.7"), BigDecimal.TEN, Instant.EPOCH)
                .newLots())
        .isEqualTo(2);
    assertThat(
            fractional
                .accept(
                    "b", "o", BigDecimal.ONE, new BigDecimal("2.7"), BigDecimal.TEN, Instant.EPOCH)
                .newLots())
        .isEqualTo(3);
    var revalued = new CopyCalculation();
    revalued.accept("a", "o", BigDecimal.ONE, new BigDecimal("2"), BigDecimal.TEN, Instant.EPOCH);
    revalued.accept("b", "o", BigDecimal.ONE, new BigDecimal("3"), BigDecimal.TEN, Instant.EPOCH);
    assertThat(revalued.targetLots("o")).isEqualTo(5);
  }

  @Test
  void fractionalSixTenthsFromTwoDifferentOrdersNeverCombine() {
    var calculation = new CopyCalculation();
    calculation.accept(
        "a", "first-order", BigDecimal.ONE, new BigDecimal("0.6"), BigDecimal.TEN, Instant.EPOCH);
    calculation.accept(
        "b", "second-order", BigDecimal.ONE, new BigDecimal("0.6"), BigDecimal.TEN, Instant.EPOCH);
    assertThat(calculation.targetLots("first-order")).isZero();
    assertThat(calculation.targetLots("second-order")).isZero();
    assertThat(
            calculation
                .accept(
                    "c",
                    "first-order",
                    BigDecimal.ONE,
                    new BigDecimal("0.6"),
                    new BigDecimal("110"),
                    Instant.EPOCH.plusSeconds(90))
                .newLots())
        .isEqualTo(1);
  }
}
