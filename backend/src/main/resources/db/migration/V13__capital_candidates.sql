create table capital_candidate (
    id uuid primary key,
    generation bigint not null,
    kind text not null check(kind in ('START','MANUAL','MANDATORY')),
    process_id uuid not null,
    source_capital numeric not null check(source_capital>0),
    mirror_capital numeric not null check(mirror_capital>=0),
    coefficient numeric(100,50) not null,
    ratio_numerator numeric(200,0) not null check(ratio_numerator>=0),
    ratio_denominator numeric(200,0) not null check(ratio_denominator>0),
    first_request_at timestamptz not null,
    first_request_nanos bigint not null,
    source_reply_nanos bigint not null,
    mirror_reply_nanos bigint not null,
    created_at timestamptz not null default clock_timestamp(),
    status text not null default 'UNACCEPTED' check(status in ('UNACCEPTED','QUALIFIED','REJECTED')),
    commit_ack_elapsed_nanos bigint,
    persistence_earliest timestamptz,
    persistence_latest timestamptz,
    persistence_earliest_seconds bigint,
    persistence_earliest_nano integer check(persistence_earliest_nano between 0 and 999999999),
    persistence_latest_seconds bigint,
    persistence_latest_nano integer check(persistence_latest_nano between 0 and 999999999),
    qualified_at timestamptz,
    qualification_error text
);
create index capital_candidate_pending on capital_candidate(created_at,id) where status='UNACCEPTED';
alter table control_state add column active_capital_candidate_id uuid references capital_candidate(id);
