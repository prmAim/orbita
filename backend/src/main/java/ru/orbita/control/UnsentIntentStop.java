package ru.orbita.control;

/** Joins the owner's command transaction; performs no external trading action. */
public interface UnsentIntentStop {
  void discardUnsent(String reason);
}
