package ru.orbita.copy;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import ru.orbita.broker.BrokerGateway.*;
import ru.orbita.broker.OrderSdkEvidence.Source;

class OrderEvidenceFixturesTest {
  private final OrderRequest request =
      new OrderRequest("mirror", UUID.randomUUID(), "instrument", Side.BUY, 3, BigDecimal.TEN);

  @ParameterizedTest
  @EnumSource(
      value = OrderStatus.class,
      names = {"REJECTED", "CANCELLED"})
  void terminalPairDescribesOneAttemptThroughTwoIndependentSources(OrderStatus status) {
    var stream =
        OrderEvidenceFixtures.terminalStream(request, status, "exchange", 10, Instant.EPOCH);
    var unary = OrderEvidenceFixtures.unary(request, status, "exchange", Instant.EPOCH);
    for (var observation : java.util.List.of(stream, unary)) {
      var raw = observation.sdkEvidence();
      assertThat(observation.requestId()).isEqualTo(request.requestId());
      assertThat(raw.responseRequestId()).isEqualTo(request.requestId().toString());
      assertThat(raw.responseInstrumentId()).isEqualTo(request.instrumentId());
      assertThat(raw.directionValue()).isEqualTo(1);
      assertThat(raw.executionStatusValue()).isEqualTo(status == OrderStatus.REJECTED ? 2 : 3);
      assertThat(observation.status()).isEqualTo(status);
      assertThat(raw.requestedLots()).isEqualTo(request.lots());
      assertThat(raw.executedLots()).isZero();
      assertThat(raw.trades()).isEmpty();
      assertThat(raw.stages()).isEmpty();
    }
    var s = stream.sdkEvidence();
    var u = unary.sdkEvidence();
    assertThat(s.source()).isEqualTo(Source.ORDER_STATE_STREAM);
    assertThat(u.source()).isEqualTo(Source.GET_ORDER_STATE);
    assertThat(s.responseAccountId()).isEqualTo(request.accountId());
    assertThat(u.responseAccountId()).isNull();
    assertThat(s.responseOrderId()).isNotEqualTo(s.tradeOrderId());
    assertThat(s.tradeOrderId()).isEqualTo(u.responseOrderId());
    assertThat(s.lotSize()).isEqualTo(10);
    assertThat(s.statusInfoPresent()).isTrue();
    assertThat(s.causeValue()).isEqualTo(2);
    assertThat(s.remainingLots()).isZero();
    assertThat(s.cancelledLots()).isEqualTo(request.lots());
    assertThat(u.statusInfoPresent()).isFalse();
  }

  @Test
  void rawStreamKeepsInconsistentVolumeAsSupplied() {
    var raw =
        OrderEvidenceFixtures.stream(
                request, OrderStatus.CANCELLED, 0, 0, 2, 2, "exchange", 10, Instant.EPOCH)
            .sdkEvidence();
    assertThat(raw.requestedLots()).isEqualTo(3);
    assertThat(raw.cancelledLots()).isEqualTo(2);
  }
}
