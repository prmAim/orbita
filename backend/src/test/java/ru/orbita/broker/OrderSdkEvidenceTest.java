package ru.orbita.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Metadata;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import ru.ttech.piapi.core.connector.exception.ServiceRuntimeException;

class OrderSdkEvidenceTest {
  @Test
  void diagnosticFieldsAreBoundedSingleLineAndDoNotRetainEchoedCredentials() {
    var transport =
        new OrderSdkEvidence.Transport(
            "INVALID_ARGUMENT",
            "30098\r\n\u001b",
            "failure\nAuthorization: Bearer secret-value; token=other-secret; " + "x".repeat(1000),
            "track\n" + "t".repeat(200));
    assertThat(transport.errorCode()).isEqualTo("30098");
    assertThat(transport.description())
        .hasSizeLessThanOrEqualTo(512)
        .doesNotContain("secret-value", "other-secret", "\n", "\r");
    assertThat(transport.trackingId()).hasSize(128).doesNotContain("\n");
  }

  @Test
  void controlsCannotHideAuthorizationAndBasicCredentialsAreAlsoRedacted() {
    var transport =
        new OrderSdkEvidence.Transport(
            "INTERNAL",
            null,
            "Author\u0000ization: Basic c3ludGhldGljOnNlY3JldA==; Bearer another-secret",
            null);
    assertThat(transport.description())
        .doesNotContain("c3ludGhldGljOnNlY3JldA==", "another-secret", "\u0000");
  }

  @Test
  void missingTrailersAndErrorCodeStayAbsentInsteadOfNumericZero() {
    var evidence =
        OrderSdkEvidence.transport(
            new ServiceRuntimeException(Status.UNAVAILABLE.asRuntimeException()));
    assertThat(evidence.grpcStatus()).isEqualTo("UNAVAILABLE");
    assertThat(evidence.errorCode()).isNull();
    assertThat(evidence.description()).isEqualTo("Unknown error");
    assertThat(evidence.trackingId()).isNull();
  }

  @Test
  void nestedSdkFailurePreservesSeparateFieldsWithoutInventingNumericCode() {
    var trailers = new Metadata();
    trailers.put(Metadata.Key.of("message", Metadata.ASCII_STRING_MARSHALLER), "quota exhausted");
    trailers.put(Metadata.Key.of("x-tracking-id", Metadata.ASCII_STRING_MARSHALLER), "trace-1");
    var sdk =
        new ServiceRuntimeException(
            Status.RESOURCE_EXHAUSTED.withDescription("not-numeric").asRuntimeException(trailers));
    assertThat(sdk.parseErrorCode()).isZero();
    var evidence = OrderSdkEvidence.transport(new IllegalStateException(sdk));
    assertThat(evidence.grpcStatus()).isEqualTo("RESOURCE_EXHAUSTED");
    assertThat(evidence.errorCode()).isEqualTo("not-numeric");
    assertThat(evidence.description()).isEqualTo("quota exhausted");
    assertThat(evidence.trackingId()).isEqualTo("trace-1");
  }
}
