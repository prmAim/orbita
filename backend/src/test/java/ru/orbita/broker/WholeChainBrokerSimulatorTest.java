package ru.orbita.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WholeChainBrokerSimulatorTest {
  @TempDir Path directory;

  @Test
  void realSdkReadsQualifiedCapitalResourcesAndNaturalSourceExecutionHistory() throws Exception {
    try (var bank = WholeChainBrokerSimulator.open(directory);
        var gateway = bank.gateway(Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      gateway.verifyAccounts();
      var at = Instant.now();
      bank.emitSource(
          "natural-buy", "source-order-1", BrokerGateway.Side.BUY, 3, new BigDecimal("100"), at);
      assertThat(gateway.capital(WholeChainBrokerSimulator.SOURCE).compositionConfirmed()).isTrue();
      assertThat(gateway.capital(WholeChainBrokerSimulator.MIRROR).totalRub())
          .isEqualByComparingTo("10000");
      var request =
          new BrokerGateway.OrderRequest(
              WholeChainBrokerSimulator.MIRROR,
              UUID.randomUUID(),
              WholeChainBrokerSimulator.INSTRUMENT,
              BrokerGateway.Side.BUY,
              2,
              new BigDecimal("100.50"));
      var resources = gateway.resources(request);
      assertThat(resources.qualified()).isTrue();
      assertThat(resources.preliminaryCommission()).isEqualByComparingTo("1");
      assertThat(resources.additionalCommissionReserve()).isEqualByComparingTo("1");
      assertThat(
              gateway.bestOppositePrice(
                  WholeChainBrokerSimulator.INSTRUMENT, BrokerGateway.Side.BUY))
          .hasValue(new BigDecimal("100.000000000"));
      var history = gateway.history(at.minusSeconds(1), at.plusSeconds(1));
      assertThat(history.complete()).isTrue();
      assertThat(history.executions())
          .singleElement()
          .satisfies(
              e -> {
                assertThat(e.tradeId()).isEqualTo("natural-buy");
                assertThat(e.lots()).isEqualByComparingTo("3");
              });
      assertThat(gateway.positions(WholeChainBrokerSimulator.SOURCE).positions())
          .singleElement()
          .satisfies(p -> assertThat(p.totalPieces()).isEqualByComparingTo("3"));
      assertThat(bank.audit()).allSatisfy(r -> assertThat(r.remoteAddress()).contains("127.0.0.1"));
      assertThat(bank.audit())
          .extracting(WholeChainBrokerSimulator.Rpc::method)
          .contains(
              "GetAccounts",
              "GetPortfolio",
              "GetPositions",
              "GetOperationsByCursor",
              "GetInstrumentBy",
              "GetMaxLots",
              "GetOrderPrice",
              "GetOrderBook");
      assertThat(bank.calls("PostOrder")).isEmpty();
    }
  }

  @Test
  void realPostPartialFillCancellationAndCommissionShareOneAuditedBankOrder() throws Exception {
    try (var bank = WholeChainBrokerSimulator.open(directory);
        var gateway = bank.gateway(Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      bank.setNextPostFill(2);
      var request =
          new BrokerGateway.OrderRequest(
              WholeChainBrokerSimulator.MIRROR,
              UUID.randomUUID(),
              WholeChainBrokerSimulator.INSTRUMENT,
              BrokerGateway.Side.BUY,
              4,
              new BigDecimal("100.50"));
      assertThat(gateway.post(request).status())
          .isEqualTo(BrokerGateway.OrderStatus.PARTIALLY_FILLED);
      assertThat(gateway.reconcile(request).fills())
          .singleElement()
          .satisfies(f -> assertThat(f.lots()).isEqualByComparingTo("2"));
      bank.fillOrder(request.requestId(), 1, Instant.now());
      assertThat(gateway.reconcile(request).lotsExecuted()).isEqualTo(3);
      assertThat(gateway.cancel(request, bank.brokerOrderId(request.requestId())).status())
          .isEqualTo(BrokerGateway.OrderStatus.CANCELLED);
      assertThat(gateway.commission(request).qualified()).isTrue();
      assertThat(gateway.commission(request).amount()).isEqualByComparingTo("1");
      assertThat(bank.calls("PostOrder")).hasSize(1);
      assertThat(bank.calls("CancelOrder")).hasSize(1);
      assertThat(gateway.mirrorHistory(Instant.now().minusSeconds(60), Instant.now()).executions())
          .hasSize(2);
      assertThat(bank.position(WholeChainBrokerSimulator.MIRROR)).isEqualTo(3);
      bank.setResources(BigDecimal.ZERO, 0, 0);
      assertThat(gateway.resources(request).buyMaxLots()).isZero();
      assertThat(gateway.resources(request).ownMoney()).isEqualByComparingTo("0");
    }
  }
}
