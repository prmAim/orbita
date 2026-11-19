alter table control_state
    add column recalculation_error_notification_id uuid,
    add column last_recalculation_notification_reason text;

alter table notification
    add column retry_annotated_notification_on_completion boolean not null default false;

with recovery_boundary as (
    select coalesce(max(journal_id), 0) as journal_id
      from notification
     where category = 'RECALCULATION_RECOVERED'
), active_pause as (
    select id, pause_started_at
      from control_state
     where recalculation_paused and failed_attempts > 0 and pause_started_at is not null
), matching_errors as (
    select p.id as state_id,
           n.id as notification_id,
           n.journal_id,
           row_number() over (partition by p.id order by n.journal_id) as sequence
      from active_pause p
      cross join recovery_boundary r
      join notification n
        on n.category = 'RECALCULATION_ERROR'
       and ((r.journal_id > 0 and n.journal_id > r.journal_id)
            or (r.journal_id = 0 and n.occurred_at = p.pause_started_at))
), first_error as (
    select state_id, notification_id, journal_id
      from matching_errors
     where sequence = 1
), latest_error as (
    select f.state_id,
           f.notification_id as first_notification_id,
           j.detail,
           row_number() over (partition by f.state_id order by n.journal_id desc) as sequence
      from first_error f
      join notification n
        on n.category = 'RECALCULATION_ERROR'
       and n.journal_id >= f.journal_id
      join journal_entry j on j.id = n.journal_id
)
update control_state c
   set recalculation_error_notification_id = f.notification_id,
       last_recalculation_notification_reason =
           case
               when split_part(l.detail, E'\n', 1) like '%; обязательный пересчёт приостановлен'
               then regexp_replace(split_part(l.detail, E'\n', 1), '; обязательный пересчёт приостановлен$', '')
               else null
           end
  from first_error f
  join latest_error l on l.state_id = f.state_id and l.sequence = 1
 where c.id = f.state_id;
