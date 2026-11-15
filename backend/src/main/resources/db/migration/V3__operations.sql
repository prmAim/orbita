create table owner_credential (
    singleton boolean primary key default true check (singleton),
    password_hash text,
    generation bigint not null default 0,
    changed_at timestamptz not null default now()
);
insert into owner_credential(singleton) values(true);

create table notification (
    id uuid primary key,
    category text not null,
    instrument text not null,
    occurred_at timestamptz not null,
    owner_text text not null,
    status text not null check(status in ('PENDING','SENDING','DELIVERED','IGNORED')),
    automatic_failures integer not null default 0 check(automatic_failures >= 0),
    next_attempt_at timestamptz,
    telegram_block_until timestamptz,
    claim_id uuid,
    claim_started_at timestamptz,
    claim_kind text check(claim_kind in ('AUTOMATIC','MANUAL')),
    last_result text,
    resolved_at timestamptz,
    version bigint not null default 0
);
create index notification_pending_due on notification(next_attempt_at, occurred_at, id) where status='PENDING';
create table notification_attempt (
    id uuid primary key,
    notification_id uuid not null references notification(id),
    kind text not null check(kind in ('AUTOMATIC','MANUAL')),
    started_at timestamptz not null,
    completed_at timestamptz,
    result text,
    detail text
);
create index notification_attempt_event on notification_attempt(notification_id,started_at,id);

create table backup_version (
    month date primary key,
    created_at timestamptz,
    verified_at timestamptz,
    transferred_at timestamptz,
    local_path text,
    sha256 text,
    status text not null,
    next_attempt_at timestamptz,
    last_error text
);
