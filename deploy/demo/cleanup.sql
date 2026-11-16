begin;
set local lock_timeout='5s';
select pg_advisory_xact_lock(hashtextextended('orbita.interface-demo',0));

do $demo_cleanup$
declare
    captured boolean;
    restored timestamptz;
    column_list text;
    item text[];
    manifest_version integer;
    schema_version text;
    key_expression text;
    ambiguous boolean;
begin
    if current_setting('orbita.demo.expected_database',true) is distinct from current_database()
       or current_setting('orbita.demo.expected_role',true) is distinct from current_user
       or current_setting('orbita.demo.offline_confirmed',true) is distinct from 'broker+telegram+worker+backup-disabled;accounts-empty'
       or current_user<>session_user
       or not exists(select 1 from pg_database where datname=current_database() and pg_get_userbyid(datdba)=current_user) then
        raise exception 'Demo cleanup requires the selected offline database owner and explicit attestation';
    end if;
    -- Repeated Remove on a database without demonstration metadata has no effects.
    if to_regnamespace('orbita_demo_preview') is null then return; end if;
    if to_regclass('orbita_demo_preview.batch') is null
       or to_regclass('orbita_demo_preview.owned') is null
       or to_regclass('orbita_demo_preview.baseline') is null
       or to_regclass('orbita_demo_preview.instrument') is null then
        raise exception 'Demo manifest is incomplete; no cleanup was performed';
    end if;
    perform 1 from public.control_state where id=1 for update;
    select b.captured,b.restored_at,b.version into captured,restored,manifest_version from orbita_demo_preview.batch b where b.singleton and b.version in (1,2) for update;
    if not found or captured is distinct from true then
        raise exception 'Demo ownership must be captured after all harness workers close; no cleanup was performed';
    end if;
    select version into schema_version from public.flyway_schema_history where success order by installed_rank desc limit 1;
    if manifest_version=1 and schema_version is distinct from '15' then
        raise exception 'Remove the legacy version 1 preview on schema 15 before upgrading';
    elsif manifest_version=2 and (schema_version is distinct from '16' or to_regclass('public.account_read_document') is null
       or to_regclass('public.account_read_freshness') is null or to_regclass('public.account_read_revision') is null) then
        raise exception 'Version 2 preview requires qualified schema 16 read tables';
    end if;
    -- Only the unfinished harness teardown may restore its snapshot. A later Remove must
    -- preserve subsequent owner state instead of replaying an old control snapshot.
    if restored is null then
        select string_agg(quote_ident(attname),',' order by attnum) into column_list
          from pg_attribute where attrelid='public.control_state'::regclass and attnum>0 and not attisdropped;
        execute format('update public.control_state set (%s)=(select %s from jsonb_populate_record(null::public.control_state,(select control_before from orbita_demo_preview.batch where singleton))) where id=1',column_list,column_list);
        select string_agg(quote_ident(attname),',' order by attnum) into column_list
          from pg_attribute where attrelid='public.broker_connectivity'::regclass and attnum>0 and not attisdropped;
        execute format('update public.broker_connectivity set (%s)=(select %s from jsonb_populate_record(null::public.broker_connectivity,(select connectivity_before from orbita_demo_preview.batch where singleton))) where singleton',column_list,column_list);
    end if;
    if exists(select 1 from public.control_state where id=1 and
        (mode not in ('NOT_STARTED','STOPPED','STOPPING','RESTORED') or live_accepted or automatic_boundary is not null
         or permit_limit is not null or permit_issued_at is not null or permit_id is not null or attempt_id is not null)) then
        raise exception 'Demo cleanup refuses an active or trading-qualified application';
    end if;
    -- New real activity overlapping demonstration holdings makes attribution ambiguous.
    if exists(select 1 from public.source_execution e join orbita_demo_preview.instrument i on i.instrument_id=e.instrument_id
              where not exists(select 1 from orbita_demo_preview.owned o where o.relation='source_execution' and o.row_id=e.id))
       or exists(select 1 from public.mirror_trade_receipt r join orbita_demo_preview.instrument i on i.instrument_id=r.instrument_id
              where not exists(select 1 from orbita_demo_preview.owned o where o.relation='mirror_trade_receipt' and o.row_id=r.id)) then
        raise exception 'Demo cleanup found unrelated new trading facts on a demonstration instrument';
    end if;

    if manifest_version=2 then
        -- These statements are planned only in v2/schema16. A v1/schema15 removal
        -- never resolves an absent account_read_* relation or row_snapshot column.
        foreach item slice 1 in array array[
            array['account_read_document','jsonb_build_array(t.account_id,t.kind,t.id)::text'],
            array['account_read_freshness','jsonb_build_array(t.account_id,t.section)::text'],
            array['account_read_revision','jsonb_build_array(t.account_id)::text']
        ] loop
            key_expression=item[2];
            execute format('select exists(select 1 from orbita_demo_preview.baseline b where b.relation=%L and b.row_snapshot is not null and not exists(select 1 from public.%I t where %s=b.row_id and to_jsonb(t)=b.row_snapshot))',item[1],item[1],key_expression) into ambiguous;
            if ambiguous then raise exception 'Demo cleanup refuses changed baseline read data: %',item[1]; end if;
            if item[1]<>'account_read_revision' then
                execute format('select exists(select 1 from orbita_demo_preview.owned o where o.relation=%L and o.row_snapshot is not null %s and not exists(select 1 from public.%I t where %s=o.row_id and to_jsonb(t)=o.row_snapshot))',item[1],case when item[1]='account_read_document' then 'and o.row_snapshot->>''kind'' in (''SECURITY'',''CASH'',''BANK_ORDER'',''BANK_FILL'')' else '' end,item[1],key_expression) into ambiguous;
                if ambiguous then raise exception 'Demo cleanup refuses changed captured broker observations: %',item[1]; end if;
            end if;
        end loop;
        if exists(
            select 1 from public.account_read_document d
             where d.kind in ('SECURITY','CASH','BANK_ORDER','BANK_FILL')
               and d.account_id in (select o.row_id::jsonb->>0 from orbita_demo_preview.owned o where o.relation in ('account_read_document','account_read_freshness','account_read_revision'))
               and not exists(select 1 from orbita_demo_preview.owned o where o.relation='account_read_document' and o.row_id=jsonb_build_array(d.account_id,d.kind,d.id)::text)
               and not exists(select 1 from orbita_demo_preview.baseline b where b.relation='account_read_document' and b.row_id=jsonb_build_array(d.account_id,d.kind,d.id)::text)
        ) or exists(
            select 1 from public.account_read_freshness f
             where f.account_id in (select o.row_id::jsonb->>0 from orbita_demo_preview.owned o where o.relation in ('account_read_document','account_read_freshness','account_read_revision'))
               and not exists(select 1 from orbita_demo_preview.owned o where o.relation='account_read_freshness' and o.row_id=jsonb_build_array(f.account_id,f.section)::text)
               and not exists(select 1 from orbita_demo_preview.baseline b where b.relation='account_read_freshness' and b.row_id=jsonb_build_array(f.account_id,f.section)::text)
        ) then raise exception 'Demo cleanup found unrelated new broker read data on a preview account'; end if;

        -- Offline owner actions can rebuild projections after capture. Adopt only
        -- derived records in already captured account/instrument scopes, never raw facts.
        insert into orbita_demo_preview.owned(relation,row_id)
          select 'account_read_document',jsonb_build_array(d.account_id,d.kind,d.id)::text
            from public.account_read_document d join orbita_demo_preview.instrument i on i.instrument_id=d.instrument_id
           where d.kind in ('ASSET','ORDER','OPERATION','FILL')
             and d.account_id in (select o.row_id::jsonb->>0 from orbita_demo_preview.owned o where o.relation in ('account_read_document','account_read_freshness','account_read_revision'))
             and not exists(select 1 from orbita_demo_preview.baseline b where b.relation='account_read_document' and b.row_id=jsonb_build_array(d.account_id,d.kind,d.id)::text)
          on conflict do nothing;
        insert into orbita_demo_preview.owned(relation,row_id)
          select 'account_read_revision',jsonb_build_array(r.account_id)::text from public.account_read_revision r
           where r.account_id in (select o.row_id::jsonb->>0 from orbita_demo_preview.owned o where o.relation='account_read_document')
             and not exists(select 1 from orbita_demo_preview.baseline b where b.relation='account_read_revision' and b.row_id=jsonb_build_array(r.account_id)::text)
          on conflict do nothing;
        foreach item slice 1 in array array[
            array['account_read_document','jsonb_build_array(t.account_id,t.kind,t.id)::text'],
            array['account_read_freshness','jsonb_build_array(t.account_id,t.section)::text'],
            array['account_read_revision','jsonb_build_array(t.account_id)::text']
        ] loop
            execute format('delete from public.%I t using orbita_demo_preview.owned o where o.relation=%L and o.row_id=%s',item[1],item[1],item[2]);
        end loop;
    end if;

    -- Later manual retries and explicit instrument pause actions belong to these exact
    -- demonstration objects. Other later journal rows, including PROCESS_STARTED, survive.
    insert into orbita_demo_preview.owned(relation,row_id)
      select 'notification',n.id::text from public.notification n join orbita_demo_preview.instrument i on i.instrument_id=n.instrument
       where n.category='INSTRUMENT_PAUSED' and not exists(select 1 from orbita_demo_preview.baseline b where b.relation='notification' and b.row_id=n.id::text)
      on conflict do nothing;
    insert into orbita_demo_preview.owned(relation,row_id)
      select 'notification_attempt',a.id::text from public.notification_attempt a join orbita_demo_preview.owned o on o.relation='notification' and o.row_id=a.notification_id::text
      on conflict do nothing;
    insert into orbita_demo_preview.owned(relation,row_id)
      select 'journal_entry',n.journal_id::text from public.notification n join orbita_demo_preview.owned o on o.relation='notification' and o.row_id=n.id::text
      on conflict do nothing;
    insert into orbita_demo_preview.owned(relation,row_id)
      select 'journal_entry',j.id::text from public.journal_entry j join orbita_demo_preview.instrument i on i.instrument_id=j.instrument
       where j.category='INSTRUMENT_PAUSED' and not exists(select 1 from orbita_demo_preview.baseline b where b.relation='journal_entry' and b.row_id=j.id::text)
      on conflict do nothing;
    insert into orbita_demo_preview.owned(relation,row_id)
      select 'copy_intent',c.id::text from public.copy_intent c join orbita_demo_preview.owned o on o.relation='source_execution' and o.row_id=c.execution_id
      on conflict do nothing;
    insert into orbita_demo_preview.owned(relation,row_id)
      select 'broker_fill',f.id from public.broker_fill f join orbita_demo_preview.owned o on o.relation='copy_intent' and o.row_id=f.intent_id::text
      on conflict do nothing;
    insert into orbita_demo_preview.owned(relation,row_id)
      select 'shadow_check',s.id::text from public.shadow_check s join orbita_demo_preview.owned o on o.relation='source_execution' and o.row_id=s.execution_id
      on conflict do nothing;
    insert into orbita_demo_preview.owned(relation,row_id)
      select 'holding_allocation',h.source_execution_id from public.holding_allocation h join orbita_demo_preview.owned o on o.relation='source_execution' and o.row_id=h.source_execution_id
      on conflict do nothing;
    insert into orbita_demo_preview.owned(relation,row_id)
      select 'migration_verification',m.id::text from public.migration_verification m join orbita_demo_preview.owned o on o.relation='source_execution' and o.row_id=m.execution_id
      on conflict do nothing;
    insert into orbita_demo_preview.owned(relation,row_id)
      select 'instrument_state',s.instrument_id from public.instrument_state s join orbita_demo_preview.instrument i using(instrument_id)
       where not exists(select 1 from orbita_demo_preview.baseline b where b.relation='instrument_state' and b.row_id=s.instrument_id)
      on conflict do nothing;

    foreach item slice 1 in array array[
        array['notification_attempt','id'],array['notification','id'],
        array['broker_fill','id'],array['shadow_check','id'],array['holding_allocation','source_execution_id'],
        array['migration_verification','id'],array['copy_intent','id'],array['source_execution','id'],
        array['incoming_receipt','id'],array['mirror_trade_receipt','id'],array['mirror_position','instrument_id'],
        array['instrument_state','instrument_id'],array['capital_candidate','id'],array['journal_entry','id']
    ] loop
        execute format('delete from public.%I t using orbita_demo_preview.owned o where o.relation=%L and o.row_id=t.%I::text',item[1],item[1],item[2]);
    end loop;
    -- No password, session, backup version or sequence is rewritten by removal.
    drop schema orbita_demo_preview cascade;
end;
$demo_cleanup$;
commit;
