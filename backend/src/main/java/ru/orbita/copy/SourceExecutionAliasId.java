package ru.orbita.copy;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class SourceExecutionAliasId implements Serializable {
  @Column(name = "account_id", nullable = false)
  String accountId;

  @Column(name = "alias_trade_id", nullable = false)
  String aliasTradeId;

  protected SourceExecutionAliasId() {}

  SourceExecutionAliasId(String accountId, String aliasTradeId) {
    this.accountId = accountId;
    this.aliasTradeId = aliasTradeId;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof SourceExecutionAliasId that)) return false;
    return Objects.equals(accountId, that.accountId)
        && Objects.equals(aliasTradeId, that.aliasTradeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountId, aliasTradeId);
  }
}
