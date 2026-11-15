-- Existing attempts have no independently recorded payload; do not invent historical evidence.
alter table notification_attempt add column payload text;
alter table notification add column resolution_text text;
