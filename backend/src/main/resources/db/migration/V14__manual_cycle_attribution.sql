alter table mirror_trade_receipt add column source_cycle_id text;
create index mirror_trade_receipt_cycle_idx on mirror_trade_receipt(source_cycle_id);
