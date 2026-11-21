package ru.orbita.copy;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "source_execution_alias")
public class SourceExecutionAliasEntity {
  @EmbeddedId SourceExecutionAliasId id;

  @Column(nullable = false)
  String executionTradeId;

  @Column(nullable = false)
  String canonicalTradeId;

  @Column(nullable = false, length = 64)
  String fingerprint;

  protected SourceExecutionAliasEntity() {}

  SourceExecutionAliasEntity(
      String accountId,
      String aliasTradeId,
      String executionTradeId,
      String canonicalTradeId,
      String fingerprint) {
    this.id = new SourceExecutionAliasId(accountId, aliasTradeId);
    this.executionTradeId = executionTradeId;
    this.canonicalTradeId = canonicalTradeId;
    this.fingerprint = fingerprint;
  }
}
