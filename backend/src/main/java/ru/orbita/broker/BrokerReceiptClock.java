package ru.orbita.broker;

import java.util.UUID;

/** One JVM-local interval domain, shared by request guards and raw broker receipt observations. */
public final class BrokerReceiptClock {
  private static final String DOMAIN = UUID.randomUUID().toString();

  private BrokerReceiptClock() {}

  public static String domain() {
    return DOMAIN;
  }

  public static long nanoTime() {
    return System.nanoTime();
  }
}
