package ru.orbita.broker;

/**
 * Typed connectivity observations, separate from subscription readiness and business qualification.
 */
public interface BrokerConnectivityEvents {
  enum Cause {
    TRANSPORT_UNAVAILABLE("соединение с брокером недоступно"),
    RPC_DEADLINE_EXCEEDED("не получен ответ брокера в срок"),
    STREAM_CLOSED("поток брокера завершён"),
    HEARTBEAT_TIMEOUT("превышен срок ожидания сообщений брокера");
    private final String ownerText;

    Cause(String ownerText) {
      this.ownerText = ownerText;
    }

    public String ownerText() {
      return ownerText;
    }
  }

  void connectionLost(Cause cause);

  void readSucceeded();

  void restoredCopyOpened();
}
