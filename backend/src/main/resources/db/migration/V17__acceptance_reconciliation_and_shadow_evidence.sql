alter table copy_intent add column acceptance_order_reconciled boolean not null default false;
alter table copy_intent add column acceptance_reconciled boolean not null default false;
alter table shadow_check add column independent_history text;
alter table shadow_check add column replay_contribution numeric(150,100);
alter table shadow_check add column replay_new_lots bigint;
