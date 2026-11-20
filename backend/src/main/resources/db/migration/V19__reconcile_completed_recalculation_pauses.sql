-- V17 recorded the recovery notice separately but did not link it to the first error.
-- Journal IDs preserve commit order when the wall clock moves backwards.
with journal_events as (
    select j.id as journal_id,
           j.category as journal_category,
           n.id as notification_id,
           n.category as notification_category,
           n.occurred_at,
           j.detail
      from journal_entry j
      left join notification n on n.journal_id = j.id
     where j.category in ('MANUAL_STOP', 'COEFFICIENT_SAVED')
        or n.category in ('RECALCULATION_ERROR', 'RECALCULATION_RECOVERED')
), sequenced_events as (
    select e.*,
           coalesce(
               max(case
                       when e.journal_category in ('MANUAL_STOP', 'COEFFICIENT_SAVED')
                         or e.notification_category = 'RECALCULATION_RECOVERED'
                       then e.journal_id
                   end)
               over (order by e.journal_id rows between unbounded preceding and 1 preceding),
               0
           ) as pause_boundary
      from journal_events e
), first_error_per_pause as (
    select distinct on (pause_boundary)
           pause_boundary,
           notification_id
      from sequenced_events
     where notification_category = 'RECALCULATION_ERROR'
     order by pause_boundary, journal_id
), recovery_pairs as (
    select recovery.occurred_at as recovered_at,
           regexp_replace(
               left(
                   recovery.detail,
                   position(E'\nИдентификатор уведомления:' in recovery.detail) - 1
               ),
               E'\n[^\n]*$',
               ''
           ) as resolution_text,
           first_error.notification_id as error_id
      from sequenced_events recovery
      join first_error_per_pause first_error
        on first_error.pause_boundary = recovery.pause_boundary
     where recovery.notification_category = 'RECALCULATION_RECOVERED'
       and position(E'\nИдентификатор уведомления:' in recovery.detail) > 0
), annotated_journal as (
    update journal_entry error_journal
       set detail = replace(
               error_journal.detail,
               E'\nИдентификатор уведомления:',
               E'\n' || pairs.resolution_text || E'\nИдентификатор уведомления:'
           )
      from recovery_pairs pairs
      join notification error_event on error_event.id = pairs.error_id
     where error_journal.id = error_event.journal_id
       and error_event.status in ('PENDING', 'SENDING')
       and error_event.resolved_at is null
       and position(E'\nИдентификатор уведомления:' in error_journal.detail) > 0
       and pairs.resolution_text is not null
    returning error_journal.id
)
update notification error_event
   set resolved_at = pairs.recovered_at,
       resolution_text = pairs.resolution_text,
       retry_annotated_notification_on_completion = true
  from recovery_pairs pairs,
       annotated_journal
 where error_event.id = pairs.error_id
   and annotated_journal.id = error_event.journal_id
   and error_event.status in ('PENDING', 'SENDING')
   and error_event.resolved_at is null;

create index notification_sending_recovery_idx
    on notification (occurred_at, id)
    where status = 'SENDING';
