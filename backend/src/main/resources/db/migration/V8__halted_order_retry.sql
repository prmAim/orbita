alter table copy_intent add column first_best_opposite numeric(38,9);
alter table copy_intent add column retry_of uuid references copy_intent(id);
create index copy_intent_retry_idx on copy_intent(retry_of);
