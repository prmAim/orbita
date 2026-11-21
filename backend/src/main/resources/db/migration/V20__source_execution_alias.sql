create table source_execution_alias (
  account_id text not null,
  alias_trade_id text not null,
  execution_trade_id text not null,
  canonical_trade_id text not null,
  fingerprint varchar(64) not null check (fingerprint ~ '^[0-9a-f]{64}$'),
  primary key (account_id, alias_trade_id),
  foreign key (account_id, execution_trade_id)
    references source_execution(account_id, trade_id)
    on delete cascade
);
create index source_execution_alias_canonical_idx
  on source_execution_alias(account_id, canonical_trade_id, execution_trade_id);
create index source_execution_alias_execution_idx
  on source_execution_alias(account_id, execution_trade_id);
