package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class PositionRecoveryTest {
  @Test
  void partialSecondBuyIsCancelledBeforeOneSixteenLotSale() {
    var result =
        PositionRecovery.recover(
            List.of(
                new PositionRecovery.Buy("first", new BigDecimal("10"), new BigDecimal("5")),
                new PositionRecovery.Buy("second", new BigDecimal("10"), new BigDecimal("3"))),
            new BigDecimal("10"),
            56,
            true,
            true);
    assertThat(result.targetLots()).isEqualTo(40);
    assertThat(result.sellLots()).isEqualTo(16);
    assertThat(result.retainedContributions())
        .containsExactly(new BigDecimal("25"), new BigDecimal("15"));
  }

  @Test
  void delayedBuyRetainsOnlySixSourceLotsAtItsOriginalCoefficient() {
    var result =
        PositionRecovery.recover(
            List.of(new PositionRecovery.Buy("buy", new BigDecimal("10"), new BigDecimal("5"))),
            new BigDecimal("4"),
            0,
            true,
            true);
    assertThat(result.buyLots()).isEqualTo(30);
    assertThat(result.sellLots()).isZero();
  }

  @Test
  void twoUnsentBuysRetainTheirOwnHistoricalCoefficients() {
    var result =
        PositionRecovery.recover(
            List.of(
                new PositionRecovery.Buy("first", new BigDecimal("10"), new BigDecimal("5")),
                new PositionRecovery.Buy("second", new BigDecimal("10"), new BigDecimal("3"))),
            new BigDecimal("10"),
            0,
            true,
            true);
    assertThat(result.buyLots()).isEqualTo(40);
    assertThat(result.retainedContributions())
        .containsExactly(new BigDecimal("25"), new BigDecimal("15"));
  }

  @Test
  void partialCorrectionAllocatesOneActualHoldingAndPermanentlyLeavesUnfilledLots() {
    assertThat(PositionRecovery.allocateFinalCorrection(25, 15, 10, 6)).containsExactly(29L, 15L);
    assertThat(PositionRecovery.allocateFinalCorrection(25, 15, 10, 10)).containsExactly(25L, 15L);
    assertThat(PositionRecovery.allocateFinalCorrection(25, 15, 10, 0)).containsExactly(35L, 15L);
  }

  @Test
  void equalPositionsWithoutProvenSequenceCannotAuthorizeCorrection() {
    var result =
        PositionRecovery.recover(
            List.of(new PositionRecovery.Buy("buy", BigDecimal.TEN, new BigDecimal("5"))),
            BigDecimal.TEN,
            0,
            false,
            true);
    assertThat(result.disposition()).isEqualTo("WAIT_FOR_RECONCILIATION");
    assertThat(result.buyLots()).isZero();
    assertThat(result.sellLots()).isZero();
  }

  @Test
  void repeatingRationalSharesAreRoundedOncePerOriginalSourceOrder() {
    var sameOrder =
        List.of(
            new PositionRecovery.Buy("a", "one-order", BigDecimal.ONE, BigDecimal.ONE),
            new PositionRecovery.Buy("b", "one-order", BigDecimal.ONE, BigDecimal.ONE),
            new PositionRecovery.Buy("c", "one-order", BigDecimal.ONE, BigDecimal.ONE));
    assertThat(PositionRecovery.recover(sameOrder, BigDecimal.ONE, 0, true, true).targetLots())
        .isEqualTo(2);
    var separateOrders =
        List.of(
            new PositionRecovery.Buy("a", BigDecimal.ONE, BigDecimal.ONE),
            new PositionRecovery.Buy("b", BigDecimal.ONE, BigDecimal.ONE),
            new PositionRecovery.Buy("c", BigDecimal.ONE, BigDecimal.ONE));
    assertThat(PositionRecovery.recover(separateOrders, BigDecimal.ONE, 0, true, true).targetLots())
        .isZero();
  }
}
