package ru.orbita.broker;

import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import ru.tinkoff.piapi.contract.v1.*;
import ru.ttech.piapi.core.connector.*;

/** Shared real SDK/TLS simulator fixture; certificates and token are synthetic and local only. */
final class BrokerTlsTestSupport {
  static Simulator open(Path directory, BindableService... services) throws Exception {
    Path key = directory.resolve("key.pem"),
        cert = directory.resolve("cert.pem"),
        store = directory.resolve("simulator.p12");
    var executable =
        Path.of(
            System.getProperty("java.home"),
            "bin",
            System.getProperty("os.name").startsWith("Windows") ? "keytool.exe" : "keytool");
    var generation =
        new ProcessBuilder(
                executable.toString(),
                "-genkeypair",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-alias",
                "simulator",
                "-storetype",
                "PKCS12",
                "-keystore",
                store.toString(),
                "-storepass",
                "synthetic-password",
                "-keypass",
                "synthetic-password",
                "-dname",
                "CN=localhost",
                "-ext",
                "SAN=IP:127.0.0.1",
                "-validity",
                "1",
                "-noprompt")
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start();
    if (!generation.waitFor(20, TimeUnit.SECONDS) || generation.exitValue() != 0) {
      generation.destroyForcibly();
      throw new IllegalStateException("Offline certificate generation failed");
    }
    var keys = java.security.KeyStore.getInstance("PKCS12");
    try (var in = Files.newInputStream(store)) {
      keys.load(in, "synthetic-password".toCharArray());
    }
    var base64 = Base64.getMimeEncoder(64, new byte[] {'\n'});
    Files.writeString(
        key,
        "-----BEGIN PRIVATE KEY-----\n"
            + base64.encodeToString(
                keys.getKey("simulator", "synthetic-password".toCharArray()).getEncoded())
            + "\n-----END PRIVATE KEY-----\n");
    Files.writeString(
        cert,
        "-----BEGIN CERTIFICATE-----\n"
            + base64.encodeToString(keys.getCertificate("simulator").getEncoded())
            + "\n-----END CERTIFICATE-----\n");
    var server =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .useTransportSecurity(cert.toFile(), key.toFile());
    if (Arrays.stream(services).noneMatch(s -> s instanceof UsersServiceGrpc.UsersServiceImplBase))
      server.addService(
          new UsersServiceGrpc.UsersServiceImplBase() {
            @Override
            public void getAccounts(
                GetAccountsRequest request, StreamObserver<GetAccountsResponse> out) {
              out.onNext(
                  GetAccountsResponse.newBuilder()
                      .addAccounts(
                          Account.newBuilder()
                              .setId("source")
                              .setStatus(AccountStatus.ACCOUNT_STATUS_OPEN)
                              .setAccessLevel(AccessLevel.ACCOUNT_ACCESS_LEVEL_READ_ONLY))
                      .addAccounts(
                          Account.newBuilder()
                              .setId("mirror")
                              .setStatus(AccountStatus.ACCOUNT_STATUS_OPEN)
                              .setAccessLevel(AccessLevel.ACCOUNT_ACCESS_LEVEL_FULL_ACCESS))
                      .build());
              out.onCompleted();
            }
          });
    for (var service : services) server.addService(service);
    return new Simulator(server.build().start());
  }

  static TInvestBrokerGateway gateway(int port, Clock clock, BrokerConnectivityEvents events)
      throws Exception {
    var properties = new Properties();
    properties.setProperty("target", "127.0.0.1:" + port);
    properties.setProperty("token", "synthetic-offline-token");
    properties.setProperty("connection.ssl.secure-internal", "false");
    properties.setProperty("connection.ssl.insecure", "true");
    var factory = ServiceStubFactory.create(ConnectorConfiguration.loadFromProperties(properties));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    var channel = factory.getChannel();
    var state = channel.getState(true);
    while (state != ConnectivityState.READY) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        channel.shutdownNow();
        throw new IllegalStateException("Local TLS handshake timed out");
      }
      var changed = new CountDownLatch(1);
      channel.notifyWhenStateChanged(state, changed::countDown);
      if (!changed.await(remaining, TimeUnit.NANOSECONDS)) {
        channel.shutdownNow();
        throw new IllegalStateException("Local TLS channel not ready");
      }
      state = channel.getState(true);
    }
    return new TInvestBrokerGateway(
        factory, "source", "mirror", clock, BrokerQualifications.unconfirmed(), events);
  }

  static final BrokerConnectivityEvents NO_EVENTS =
      new BrokerConnectivityEvents() {
        public void connectionLost(Cause cause) {}

        public void readSucceeded() {}

        public void restoredCopyOpened() {}
      };

  static final class Simulator implements AutoCloseable {
    final Server server;

    Simulator(Server server) {
      this.server = server;
    }

    int port() {
      return server.getPort();
    }

    public void close() throws Exception {
      if (!server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS))
        throw new IllegalStateException("TLS simulator did not stop");
    }
  }
}
