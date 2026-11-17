package ru.orbita.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.stub.StreamObserver;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import ru.orbita.broker.BrokerGateway.*;
import ru.tinkoff.piapi.contract.v1.*;

/** SDK 1.51 over loopback TLS; this deliberately scripted broker is not production evidence. */
class BuyResourceRejectionTlsTest {
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({"2,BUY,true", "3,BUY,true", "2,SELL,false", "3,SELL,false"})
  void independentlyReadRequestUuidAndExactStreamQualifyOnlyBuy(
      int status, Side side, boolean qualifies) throws Exception {
    var request =
        new OrderRequest("mirror", UUID.randomUUID(), "instrument-1", side, 3, BigDecimal.TEN);
    int direction = side == Side.BUY ? 1 : 2;
    var received = new LinkedBlockingQueue<OrderEvidence>();
    var orders =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          @Override
          public void getOrderState(GetOrderStateRequest lookup, StreamObserver<OrderState> out) {
            assertThat(lookup.getAccountId()).isEqualTo("mirror");
            assertThat(lookup.getOrderId()).isEqualTo(request.requestId().toString());
            assertThat(lookup.getOrderIdType()).isEqualTo(OrderIdType.ORDER_ID_TYPE_REQUEST);
            out.onNext(
                OrderState.newBuilder()
                    .setOrderRequestId(lookup.getOrderId())
                    .setOrderId("exchange-id")
                    .setInstrumentUid("instrument-1")
                    .setDirectionValue(direction)
                    .setExecutionReportStatusValue(status)
                    .setLotsRequested(3)
                    .setLotsExecuted(0)
                    .build());
            out.onCompleted();
          }
        };
    var streams =
        new OrdersStreamServiceGrpc.OrdersStreamServiceImplBase() {
          @Override
          public void orderStateStream(
              OrderStateStreamRequest subscription, StreamObserver<OrderStateStreamResponse> out) {
            assertThat(subscription.getAccountsList()).containsExactly("mirror");
            out.onNext(
                OrderStateStreamResponse.newBuilder()
                    .setSubscription(MirrorOrderFeedTlsTest.ack("mirror"))
                    .build());
            out.onNext(
                OrderStateStreamResponse.newBuilder()
                    .setOrderState(
                        OrderStateStreamResponse.OrderState.newBuilder()
                            .setAccountId("mirror")
                            .setOrderRequestId(request.requestId().toString())
                            .setOrderId("internal-id")
                            .setTradeOrderId("exchange-id")
                            .setInstrumentUid("instrument-1")
                            .setDirectionValue(direction)
                            .setExecutionReportStatusValue(status)
                            .setStatusInfoValue(2)
                            .setLotsRequested(3)
                            .setLotsExecuted(0)
                            .setLotsLeft(0)
                            .setLotsCancelled(3)
                            .setLotSize(10))
                    .build());
          }

          @Override
          public void tradesStream(
              TradesStreamRequest subscription, StreamObserver<TradesStreamResponse> out) {
            out.onNext(
                TradesStreamResponse.newBuilder()
                    .setSubscription(MirrorOrderFeedTlsTest.ack(subscription.getAccounts(0)))
                    .build());
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, orders, streams);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS);
        var feed = gateway.observe(MirrorOrderFeedTlsTest.journal(received))) {
      var stream = received.poll(5, TimeUnit.SECONDS);
      assertThat(stream).isNotNull();
      var unary = gateway.reconcile(request);
      var result = BuyResourceRejectionClassifier.classify(request, List.of(stream, unary), true);
      assertThat(result.isPresent()).isEqualTo(qualifies);
      assertThat(stream.sdkEvidence().responseOrderId()).isEqualTo("internal-id");
      assertThat(unary.brokerOrderId()).isEqualTo("exchange-id");
      assertThat(unary.status())
          .isEqualTo(status == 2 ? OrderStatus.REJECTED : OrderStatus.CANCELLED);
      if (qualifies) {
        assertThat(result.orElseThrow().status()).isEqualTo(unary.status());
        assertThat(result.orElseThrow().reason()).isEqualTo(RejectionReason.INSUFFICIENT_RESOURCES);
      }
    }
  }
}
