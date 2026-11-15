create table migration_verification (
    id uuid primary key,
    checked_at timestamptz not null,
    manifest_hash text not null,
    execution_id text not null,
    journal_matches boolean not null,
    settings_match boolean not null,
    execution_matches boolean not null,
    calculation_matches boolean not null,
    detail text not null
);
create index migration_verification_time on migration_verification(checked_at,id);
