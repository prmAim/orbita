create sequence journal_seq increment 1;
create table incoming_receipt (id text primary key,first_received timestamptz not null);
create table control_state (
 id integer primary key check(id=1), version bigint not null default 0, mode text not null default 'NOT_STARTED',
 coefficient numeric(100,50), observation_boundary timestamptz, resume_boundary timestamptz,
 automatic_boundary timestamptz, permit_limit numeric(38,9), permit_issued_at timestamptz,
 first_acceptance boolean not null default true, live_accepted boolean not null default false,
 reconciled boolean not null default false, feed_ready boolean not null default false,shadow_version text,
 shadow_buy_checked boolean not null default false,shadow_sell_checked boolean not null default false,recalculation_paused boolean not null default false,
 attempt_id uuid, attempt_kind text, generation bigint not null default 0, completed_due date,
 retry_at timestamptz, last_error text, pause_started_at timestamptz, failed_attempts integer not null default 0,
 check(coefficient is null or coefficient>=0),check(permit_limit is null or permit_limit>0)
);
insert into control_state(id) values(1);
create table instrument_state (
 instrument_id text primary key,version bigint not null default 0,paused boolean not null default false,
 waiting boolean not null default false,reconciled boolean not null default false,reason text
);
create table journal_entry (
 id bigint primary key default nextval('journal_seq'),occurred_at timestamptz not null,
 category text not null,instrument text,detail text not null
);
create index journal_entry_time_idx on journal_entry(occurred_at,id);
create table source_execution (
 id text primary key,account_id text not null,trade_id text not null,source_order_id text not null,
 instrument_id text not null,side text not null check(side in ('BUY','SELL')),lots numeric(38,9) not null check(lots>0),
 price numeric(38,9) not null check(price>0),currency text not null,executed_at timestamptz,received_at timestamptz not null,
 coefficient numeric(100,50),contribution numeric(120,50),disposition text not null,new_lots bigint not null default 0,requires_recovery boolean not null default false,accepted_sequence bigint not null default 0,
 unique(account_id,trade_id)
);
create index source_execution_order_idx on source_execution(source_order_id,id);
create index source_execution_pending_idx on source_execution(disposition,received_at,id);
create table copy_intent (
 id uuid primary key,version bigint not null default 0,execution_id text not null references source_execution(id),
 instrument_id text not null,source_order_id text not null,side text not null,currency text not null,
 lots bigint not null check(lots>0),reference_price numeric(38,9) not null,executed_at timestamptz not null,
 created_at timestamptz not null,status text not null,limit_price numeric(38,9),reserved_money numeric(38,9) not null,
 commission_reserve numeric(38,9) not null,reserved_lots bigint not null default 0,
 executed_lots bigint not null default 0,skipped_lots bigint not null default 0,broker_order_id text,
 sent_at timestamptz,deadline timestamptz,reason text,full_close boolean not null default false,
 recovery_first_buy text,recovery_second_buy text,recovery_first_target bigint not null default 0,recovery_second_target bigint not null default 0,
 acceptance_attempt boolean not null default false,actual_commission numeric(38,9),
 check(executed_lots>=0 and skipped_lots>=0 and executed_lots+skipped_lots<=lots)
);
create index copy_intent_work_idx on copy_intent(status,created_at,id);
create index copy_intent_instrument_idx on copy_intent(instrument_id,status);
create table mirror_position (
 instrument_id text primary key,version bigint not null default 0,copied_lots bigint not null default 0,
 manual_lots bigint not null default 0,broker_lots numeric(38,9),broker_read_at timestamptz,check(copied_lots>=0 and manual_lots>=0)
);
create table broker_fill (
 id text primary key,intent_id uuid not null references copy_intent(id),trade_id text not null,
 lots numeric(38,9) not null,price numeric(38,9) not null,currency text not null,executed_at timestamptz not null,
 unique(intent_id,trade_id)
);
create index broker_fill_intent_idx on broker_fill(intent_id);
create table holding_allocation (
 source_execution_id text primary key references source_execution(id),instrument_id text not null,lots bigint not null check(lots>=0)
);
create index holding_allocation_instrument_idx on holding_allocation(instrument_id);
create table shadow_check (
 id uuid primary key,execution_id text not null references source_execution(id),algorithm_version text not null,
 side text not null,checked_at timestamptz not null,execution_matches boolean not null,calculation_matches boolean not null,detail text not null
);
create index shadow_check_version_idx on shadow_check(algorithm_version,side,checked_at,id);
