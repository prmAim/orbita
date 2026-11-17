package ru.orbita.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Clock;
import java.util.UUID;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import ru.tinkoff.piapi.contract.v1.*;

class OrderSdkTlsTest {
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({
    "BUY,false,false",
    "SELL,false,false",
    "BUY,true,false",
    "SELL,true,false",
    "BUY,false,true",
    "BUY,true,true"
  })
  void reconciliationPreservesExecutionsAndRawContradictions(
      BrokerGateway.Side side, boolean mismatch, boolean invalidDate) throws Exception {
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          @Override
          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            out.onNext(
                OrderState.newBuilder()
                    .setOrderId("bank-order")
                    .setOrderRequestId(request.getOrderId())
                    .setInstrumentUid(mismatch ? "another-instrument" : "instrument-1")
                    .setDirection(
                        side == BrokerGateway.Side.BUY
                            ? OrderDirection.ORDER_DIRECTION_BUY
                            : OrderDirection.ORDER_DIRECTION_SELL)
                    .setLotsRequested(3)
                    .setLotsExecuted(2)
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED)
                    .addStages(
                        OrderStage.newBuilder()
                            .setTradeId("filled-before-cancel")
                            .setQuantity(2)
                            .setPrice(MoneyValue.newBuilder().setCurrency("rub").setUnits(10))
                            .setExecutionTime(
                                com.google.protobuf.Timestamp.newBuilder().setSeconds(1800000000)))
                    .setOrderDate(
                        com.google.protobuf.Timestamp.newBuilder()
                            .setSeconds(1799999999)
                            .setNanos(invalidDate ? -1 : 0))
                    .build());
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var request = request(side);
      var result = gateway.reconcile(request);
      assertThat(result.status())
          .isEqualTo(
              mismatch ? BrokerGateway.OrderStatus.UNKNOWN : BrokerGateway.OrderStatus.CANCELLED);
      assertThat(result.sdkEvidence().executedLots()).isEqualTo(2);
      assertThat(result.sdkEvidence().responseInstrumentId())
          .isEqualTo(mismatch ? "another-instrument" : "instrument-1");
      assertThat(result.sdkEvidence().responseRequestId())
          .isEqualTo(request.requestId().toString());
      assertThat(result.sdkEvidence().executionStatusValue()).isEqualTo(3);
      assertThat(result.sdkEvidence().stages())
          .extracting(OrderStage::getTradeId)
          .containsExactly("filled-before-cancel");
      assertThat(result.sdkEvidence().eventAt())
          .isEqualTo(invalidDate ? null : java.time.Instant.ofEpochSecond(1799999999));
      assertThat(result.sdkEvidence().receivedAt()).isEqualTo(result.observedAt());
      assertThat(result.sdkEvidence().sdkVersion()).isEqualTo("1.51");
      assertThat(result.sdkEvidence().classifierVersion()).isEqualTo("f04-a1-v1");
      assertThat(result.sdkEvidence().remainingLots()).isNull();
      assertThat(result.sdkEvidence().cancelledLots()).isNull();
      if (!mismatch) {
        assertThat(result.lotsExecuted()).isEqualTo(2);
        assertThat(result.fills())
            .extracting(BrokerGateway.Fill::tradeId)
            .containsExactly("filled-before-cancel");
        assertThat(result.rejectionReason()).isEqualTo(BrokerGateway.RejectionReason.OTHER);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void lookupAndCancellationWithoutTrailersStayUnknown(boolean cancelling) throws Exception {
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          @Override
          public void getOrderState(GetOrderStateRequest request, StreamObserver<OrderState> out) {
            out.onError(Status.NOT_FOUND.asRuntimeException());
          }

          @Override
          public void cancelOrder(
              CancelOrderRequest request, StreamObserver<CancelOrderResponse> out) {
            out.onError(Status.UNAVAILABLE.asRuntimeException());
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var request = request(BrokerGateway.Side.BUY);
      var result = cancelling ? gateway.cancel(request, "bank-order") : gateway.reconcile(request);
      assertThat(result.status()).isEqualTo(BrokerGateway.OrderStatus.UNKNOWN);
      assertThat(result.rejectionReason()).isEqualTo(BrokerGateway.RejectionReason.NONE);
      assertThat(result.sdkEvidence().source())
          .isEqualTo(
              cancelling
                  ? OrderSdkEvidence.Source.CANCEL_ORDER
                  : OrderSdkEvidence.Source.GET_ORDER_STATE);
      assertThat(result.sdkEvidence().transport().grpcStatus())
          .isEqualTo(cancelling ? "UNAVAILABLE" : "NOT_FOUND");
      assertThat(result.sdkEvidence().transport().errorCode()).isNull();
      assertThat(result.sdkEvidence().transport().trackingId()).isNull();
      assertThat(result.sdkEvidence().executedLots()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void postKeepsSuppliedRequestUuidAndDoesNotQualifyAnotherAttempt(boolean mismatch)
      throws Exception {
    var other = UUID.randomUUID().toString();
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          @Override
          public void postOrder(PostOrderRequest request, StreamObserver<PostOrderResponse> out) {
            out.onNext(
                PostOrderResponse.newBuilder()
                    .setOrderId("bank-order")
                    .setOrderRequestId(mismatch ? other : request.getOrderId())
                    .setInstrumentUid(request.getInstrumentId())
                    .setDirection(request.getDirection())
                    .setLotsRequested(request.getQuantity())
                    .setExecutionReportStatus(
                        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW)
                    .build());
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var request = request(BrokerGateway.Side.BUY);
      var result = gateway.post(request);
      assertThat(result.status())
          .isEqualTo(mismatch ? BrokerGateway.OrderStatus.UNKNOWN : BrokerGateway.OrderStatus.NEW);
      assertThat(result.requestId()).isEqualTo(request.requestId());
      assertThat(result.sdkEvidence().responseRequestId())
          .isEqualTo(mismatch ? other : request.requestId().toString());
    }
  }

  private BrokerGateway.OrderRequest request(BrokerGateway.Side side) {
    return new BrokerGateway.OrderRequest(
        "mirror", UUID.randomUUID(), "instrument-1", side, 3, BigDecimal.TEN);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "30098",
        "30034",
        "30042",
        "30052",
        "30079",
        "30255",
        "quota",
        "price-limit",
        "new-error"
      })
  void transportErrorNeverQualifiesBusinessReasonAndRetainsSdkFields(String code) throws Exception {
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          @Override
          public void postOrder(PostOrderRequest request, StreamObserver<PostOrderResponse> out) {
            var trailers = new Metadata();
            trailers.put(
                Metadata.Key.of("message", Metadata.ASCII_STRING_MARSHALLER),
                "trading halted sandbox margin quota price-limit");
            trailers.put(
                Metadata.Key.of("x-tracking-id", Metadata.ASCII_STRING_MARSHALLER),
                "tracking-not-order");
            trailers.put(
                Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                "Bearer synthetic-secret");
            out.onError(Status.INVALID_ARGUMENT.withDescription(code).asRuntimeException(trailers));
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var request =
          new BrokerGateway.OrderRequest(
              "mirror",
              UUID.randomUUID(),
              "instrument-1",
              BrokerGateway.Side.BUY,
              3,
              BigDecimal.TEN);
      var result = gateway.post(request);
      assertThat(result.status()).isEqualTo(BrokerGateway.OrderStatus.UNKNOWN);
      assertThat(result.rejectionReason()).isEqualTo(BrokerGateway.RejectionReason.NONE);
      assertThat(result.brokerOrderId()).isNull();
      assertThat(result.sdkEvidence()).isNotNull();
      assertThat(result.sdkEvidence().source()).isEqualTo(OrderSdkEvidence.Source.POST_ORDER);
      assertThat(result.sdkEvidence().executionStatusValue()).isNull();
      assertThat(result.sdkEvidence().executedLots()).isNull();
      assertThat(result.sdkEvidence().transport().grpcStatus()).isEqualTo("INVALID_ARGUMENT");
      assertThat(result.sdkEvidence().transport().errorCode()).isEqualTo(code);
      assertThat(result.sdkEvidence().transport().description())
          .isEqualTo("trading halted sandbox margin quota price-limit");
      assertThat(result.sdkEvidence().transport().trackingId()).isEqualTo("tracking-not-order");
      assertThat(result.sdkEvidence().toString()).doesNotContain("synthetic-secret");
    }
  }

  @ParameterizedTest
  @CsvSource({"4,NEW,NONE", "3,CANCELLED,OTHER", "2,REJECTED,OTHER", "991,UNKNOWN,NONE"})
  void postRetainsRawStatusAndIndependentOutcome(
      int rawStatus, BrokerGateway.OrderStatus expected, BrokerGateway.RejectionReason reason)
      throws Exception {
    var broker =
        new OrdersServiceGrpc.OrdersServiceImplBase() {
          @Override
          public void postOrder(PostOrderRequest request, StreamObserver<PostOrderResponse> out) {
            out.onNext(
                PostOrderResponse.newBuilder()
                    .setOrderId("bank-order")
                    .setInstrumentUid(request.getInstrumentId())
                    .setDirection(request.getDirection())
                    .setLotsRequested(request.getQuantity())
                    .setExecutionReportStatusValue(rawStatus)
                    .build());
            out.onCompleted();
          }
        };
    try (var server = BrokerTlsTestSupport.open(directory, broker);
        var gateway =
            BrokerTlsTestSupport.gateway(
                server.port(), Clock.systemUTC(), BrokerTlsTestSupport.NO_EVENTS)) {
      var request =
          new BrokerGateway.OrderRequest(
              "mirror",
              UUID.randomUUID(),
              "instrument-1",
              BrokerGateway.Side.BUY,
              3,
              BigDecimal.TEN);
      var result = gateway.post(request);
      assertThat(result.status()).isEqualTo(expected);
      assertThat(result.rejectionReason()).isEqualTo(reason);
      assertThat(result.sdkEvidence()).isNotNull();
      assertThat(result.sdkEvidence().executionStatusValue()).isEqualTo(rawStatus);
      assertThat(result.sdkEvidence().responseRequestId()).isNull();
      assertThat(result.sdkEvidence().responseAccountId()).isNull();
      assertThat(result.sdkEvidence().statusInfoPresent()).isFalse();
      assertThat(result.sdkEvidence().causeValue()).isNull();
    }
  }
}
