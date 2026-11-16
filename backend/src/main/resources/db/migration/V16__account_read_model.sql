-- Display-only observations and projections, isolated from trading state.
create table account_read_document (
 account_id text not null, kind text not null, id text not null,
 payload jsonb not null, instrument_id text, ticker text, name text,
 side text, status text, origin text, state text, attention boolean not null default false,
 event_at timestamptz, lots numeric, executed_lots numeric, execution_price numeric,
 current boolean not null default true, observed_at timestamptz not null,
 primary key(account_id,kind,id)
);
create index account_read_page_idx on account_read_document(account_id,kind,current,event_at,id);
create index account_read_instrument_idx on account_read_document(account_id,kind,instrument_id,event_at,id);
create index account_read_filter_idx on account_read_document(account_id,kind,origin,status,side,id);
create table account_read_freshness (
 account_id text not null, section text not null, status text not null,
 last_attempt_at timestamptz, last_success_at timestamptz,
 from_inclusive timestamptz, through_inclusive timestamptz,
 complete boolean not null, detail text not null,
 primary key(account_id,section)
);
create table account_read_revision (account_id text primary key, local_revision text not null);
