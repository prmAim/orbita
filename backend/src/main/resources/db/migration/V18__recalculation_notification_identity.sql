alter table control_state
    add column recalculation_error_notification_id uuid;

alter table notification
    add column retry_annotated_notification_on_completion boolean not null default false;
