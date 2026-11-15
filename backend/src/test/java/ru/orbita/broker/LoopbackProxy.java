package ru.orbita.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/** Local TCP relay; reset only after the simulator has accepted the SDK request. */
final class LoopbackProxy implements AutoCloseable {
  private final ServerSocket listener = new ServerSocket();
  private final int upstream;
  private Socket client;
  private Socket server;
  private final Thread acceptor;

  LoopbackProxy(int upstream) throws IOException {
    this.upstream = upstream;
    listener.bind(new InetSocketAddress("127.0.0.1", 0));
    acceptor = Thread.startVirtualThread(this::accept);
  }

  int port() {
    return listener.getLocalPort();
  }

  private void accept() {
    try {
      while (!listener.isClosed()) {
        Socket downstream = listener.accept();
        Socket upstreamSocket = new Socket("127.0.0.1", upstream);
        synchronized (this) {
          client = downstream;
          server = upstreamSocket;
        }
        Thread.startVirtualThread(() -> relay(downstream, upstreamSocket));
        Thread.startVirtualThread(() -> relay(upstreamSocket, downstream));
      }
    } catch (IOException error) {
      if (!listener.isClosed()) throw new IllegalStateException("Simulator relay failed", error);
    }
  }

  private static void relay(Socket from, Socket to) {
    try (from;
        to) {
      from.getInputStream().transferTo(to.getOutputStream());
    } catch (IOException expectedOnDisconnect) {
      /* Reset closes both forwarding directions. */
    }
  }

  synchronized void loseReply() throws IOException {
    client.setSoLinger(true, 0);
    client.close();
    server.close();
  }

  @Override
  public void close() throws Exception {
    listener.close();
    acceptor.join(5000);
    if (client != null) client.close();
    if (server != null) server.close();
  }
}
