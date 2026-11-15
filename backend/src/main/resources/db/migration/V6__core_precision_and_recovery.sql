-- Existing accepted decimal coefficients remain the exact historical value they recorded.
-- New acceptances retain the original capital ratio, without decimal rounding before floor.
alter table control_state add column ratio_numerator numeric(200,0);
alter table control_state add column ratio_denominator numeric(200,0);
alter table control_state add column observation_boundary_trusted boolean not null default false;
alter table control_state add column resume_boundary_trusted boolean not null default false;
update control_state set ratio_numerator=coefficient*power(10::numeric,50),ratio_denominator=power(10::numeric,50) where coefficient is not null;
alter table control_state add constraint control_ratio_valid check((ratio_numerator is null and ratio_denominator is null) or (ratio_numerator>=0 and ratio_denominator>0));

alter table source_execution add column ratio_numerator numeric(200,0);
alter table source_execution add column ratio_denominator numeric(200,0);
alter table source_execution add column received_sequence bigint not null default 0;
alter table source_execution alter column contribution type numeric(160,100);
update source_execution set ratio_numerator=coefficient*power(10::numeric,50),ratio_denominator=power(10::numeric,50) where coefficient is not null;
update source_execution e set received_sequence=coalesce((select min(j.id) from journal_entry j where j.category='SOURCE_RECEIVED' and j.detail=e.id),e.accepted_sequence,0);
alter table source_execution add constraint source_ratio_valid check((ratio_numerator is null and ratio_denominator is null) or (ratio_numerator>=0 and ratio_denominator>0));

alter table incoming_receipt add column first_earliest timestamptz;
alter table incoming_receipt add column first_latest timestamptz;
alter table incoming_receipt add constraint receipt_time_ordered check((first_earliest is null and first_latest is null) or first_earliest<=first_latest);

alter table copy_intent add column requested_lots bigint not null default 0;
alter table copy_intent add column priority_sequence bigint not null default 0;
alter table copy_intent add column cancellation_pending boolean not null default false;
alter table copy_intent add column unknown_notification uuid;
alter table copy_intent add column permitted_rub_limit numeric(38,9);
update copy_intent i set priority_sequence=e.received_sequence from source_execution e where e.id=i.execution_id;
update copy_intent i set requested_lots=coalesce((select substring(j.detail from 'lots=([0-9]+)')::bigint from journal_entry j where j.category='SEND_STARTED' and j.detail like i.id::text||' lots=%' order by j.id limit 1),case when i.status in ('SEND_STARTED','ACTIVE','UNKNOWN','CANCEL_STARTED') then i.lots-i.skipped_lots else 0 end);
update copy_intent set cancellation_pending=true where status='CANCEL_STARTED';

create table mirror_trade_receipt (
 id text primary key,instrument_id text not null,side text not null,lots bigint not null check(lots>0),executed_at timestamptz not null,
 manual_lots_applied bigint not null default 0,copied_lots_applied bigint not null default 0
);
create index mirror_trade_receipt_instrument_idx on mirror_trade_receipt(instrument_id,executed_at,id);
