alter table control_state
    add column recalculation_error_notification_id uuid,
    add column recalculation_pause_journal_boundary bigint;

alter table notification
    add column retry_annotated_notification_on_completion boolean not null default false;

with pause_boundary as (
    select greatest(
               coalesce((select max(id) from journal_entry where category in ('MANUAL_STOP', 'COEFFICIENT_SAVED')), 0),
               coalesce((select max(journal_id) from notification where category = 'RECALCULATION_RECOVERED'), 0)
           ) as journal_id
)
update control_state c
   set recalculation_pause_journal_boundary = b.journal_id
  from pause_boundary b
 where c.recalculation_paused
   and c.failed_attempts > 0
   and c.pause_started_at is not null;

with active_pause as (
    select id, recalculation_pause_journal_boundary
      from control_state
     where recalculation_paused
       and failed_attempts > 0
       and pause_started_at is not null
), first_error as (
    select p.id as state_id,
           n.id as notification_id,
           row_number() over (partition by p.id order by n.journal_id) as sequence
      from active_pause p
      join notification n
        on n.category = 'RECALCULATION_ERROR'
       and n.journal_id > p.recalculation_pause_journal_boundary
)
update control_state c
   set recalculation_error_notification_id = f.notification_id
  from first_error f
 where c.id = f.state_id
   and f.sequence = 1;
