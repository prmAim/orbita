-- One authoritative owner text, visible in the journal and sent unchanged by the outbox.
alter table notification add column journal_id bigint;
update notification set journal_id=nextval('journal_seq');
insert into journal_entry(id,occurred_at,category,instrument,detail)
select journal_id,occurred_at,'NOTIFICATION_'||category,instrument,owner_text from notification;
alter table notification alter column journal_id set not null;
alter table notification add constraint notification_journal_fk foreign key(journal_id) references journal_entry(id);
alter table notification add constraint notification_journal_unique unique(journal_id);
alter table notification drop column owner_text;
