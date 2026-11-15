alter table control_state add column permit_id uuid;
alter table control_state add column permit_epoch bigint not null default 0;
alter table control_state add column observation_activated boolean not null default false;
update control_state set permit_id=gen_random_uuid() where permit_limit is not null;
update control_state set observation_activated=true where observation_boundary is not null;
alter table copy_intent add column consumed_permit_id uuid;
alter table copy_intent add column consumed_permit_epoch bigint not null default 0;
alter table copy_intent add column control_generation bigint not null default 0;
alter table copy_intent add column permitted_issued_at timestamptz;
