alter table notification add column resolution_category text not null default 'RESOLVED';

create table broker_connectivity (
    singleton boolean primary key default true check (singleton),
    loss_notification_id uuid references notification(id),
    lost_at timestamptz,
    last_recovered_at timestamptz,
    restore_session uuid,
    check ((loss_notification_id is null) = (lost_at is null))
);
insert into broker_connectivity(singleton) values(true);
