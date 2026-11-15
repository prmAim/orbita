begin;
update control_state set mode='RESTORED',generation=generation+1,reconciled=false,feed_ready=false,automatic_boundary=null,permit_issued_at=null,permit_limit=null,attempt_id=null,attempt_kind=null,retry_at=null;
update notification set status='IGNORED',claim_id=null,claim_started_at=null,claim_kind=null,next_attempt_at=null where status in ('PENDING','SENDING');
update notification_attempt set completed_at=now(),result='RESTORED_COPY',detail='Очередь восстановленной копии не отправляется' where completed_at is null;
update owner_credential set password_hash=null,generation=generation+1,changed_at=now();
-- This batch precedes Flyway upgrades: old archives have neither permit identity nor connectivity.
do $restore_safeguards$
begin
    if exists(select 1 from information_schema.columns where table_schema='public' and table_name='control_state' and column_name='permit_id') then
        execute 'update public.control_state set permit_id=null';
    end if;
    if exists(select 1 from information_schema.columns where table_schema='public' and table_name='control_state' and column_name='permit_epoch') then
        execute 'update public.control_state set permit_epoch=permit_epoch+1';
    end if;
    if to_regclass('public.broker_connectivity') is not null then
        execute 'update public.broker_connectivity set loss_notification_id=null,lost_at=null,last_recovered_at=null,restore_session=null where singleton';
    end if;
end;
$restore_safeguards$;
insert into journal_entry(occurred_at,category,detail) values(now(),'BACKUP_RESTORED','Только чтение и сверка; допуск и прежняя очередь отменены');
commit;
