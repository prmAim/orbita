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
  void realSdkDistinguishesSecurityIdentityAndQuotes() throws Exception {
    try (var bank = WholeChainBrokerSimulator.open(directory);
        var gateway = bank.gateway(Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      assertThat(gateway.instrument("[ТЕСТ] SBER").id()).isEqualTo("[ТЕСТ] SBER");
      assertThat(gateway.instrument("[ТЕСТ] GAZP").id()).isEqualTo("[ТЕСТ] GAZP");
      assertThat(gateway.bestOppositePrice("[ТЕСТ] SBER", BrokerGateway.Side.BUY))
          .contains(new BigDecimal("100.000000000"));
      assertThat(gateway.bestOppositePrice("[ТЕСТ] GAZP", BrokerGateway.Side.SELL))
          .contains(new BigDecimal("150.000000000"));
      var request =
          new BrokerGateway.OrderRequest(
              WholeChainBrokerSimulator.MIRROR,
              UUID.randomUUID(),
              "[ТЕСТ] GAZP",
              BrokerGateway.Side.BUY,
              2,
              new BigDecimal("150.45"));
      assertThat(gateway.resources(request).instrument().id()).isEqualTo("[ТЕСТ] GAZP");
      assertThat(gateway.resources(request).qualified()).isTrue();
    }
  }

  @Test
  void realOrdersResourcesPositionsAndHistoryStayWithinTheirSecurity() throws Exception {
    try (var bank = WholeChainBrokerSimulator.open(directory);
        var gateway = bank.gateway(Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var sber =
          new BrokerGateway.OrderRequest(
              WholeChainBrokerSimulator.MIRROR,
              UUID.randomUUID(),
              "[ТЕСТ] SBER",
              BrokerGateway.Side.BUY,
              4,
              new BigDecimal("100.30"));
      var gazp =
          new BrokerGateway.OrderRequest(
              WholeChainBrokerSimulator.MIRROR,
              UUID.randomUUID(),
              "[ТЕСТ] GAZP",
              BrokerGateway.Side.BUY,
              3,
              new BigDecimal("150.45"));
      bank.setNextPostFill(0);
      assertThat(gateway.post(sber).status()).isEqualTo(BrokerGateway.OrderStatus.NEW);
      assertThat(gateway.post(gazp).status()).isEqualTo(BrokerGateway.OrderStatus.FILLED);
      bank.fillOrder(sber.requestId(), 2, Instant.now());
      assertThat(gateway.cancel(sber, bank.brokerOrderId(sber.requestId())).lotsExecuted())
          .isEqualTo(2);
      assertThat(gateway.reconcile(gazp).status()).isEqualTo(BrokerGateway.OrderStatus.FILLED);
      assertThat(gateway.reconcile(gazp).lotsExecuted()).isEqualTo(3);
      var positions =
          gateway.positions(WholeChainBrokerSimulator.MIRROR).positions().stream()
              .collect(
                  java.util.stream.Collectors.toMap(
                      BrokerGateway.Position::instrumentId, BrokerGateway.Position::totalPieces));
      assertThat(positions).containsOnlyKeys("[ТЕСТ] SBER", "[ТЕСТ] GAZP");
      assertThat(positions.get("[ТЕСТ] SBER")).isEqualByComparingTo("2");
      assertThat(positions.get("[ТЕСТ] GAZP")).isEqualByComparingTo("3");
      var history =
          gateway.mirrorHistory(Instant.now().minusSeconds(60), Instant.now()).executions();
      assertThat(history).hasSize(2);
      assertThat(history)
          .anySatisfy(
              e -> {
                assertThat(e.instrumentId()).isEqualTo("[ТЕСТ] SBER");
                assertThat(e.lots()).isEqualByComparingTo("2");
              });
      assertThat(history)
          .anySatisfy(
              e -> {
                assertThat(e.instrumentId()).isEqualTo("[ТЕСТ] GAZP");
                assertThat(e.lots()).isEqualByComparingTo("3");
              });
      bank.setResources(BigDecimal.ZERO, 0, 0);
      assertThat(gateway.resources(sber).buyMaxLots()).isZero();
      assertThat(gateway.resources(gazp).ownMoney()).isEqualByComparingTo("9350");
      assertThat(gateway.resources(gazp).buyMaxLots()).isEqualTo(62);
      assertThat(gateway.capital(WholeChainBrokerSimulator.MIRROR).totalRub())
          .isEqualByComparingTo("10000");
    }
  }

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
