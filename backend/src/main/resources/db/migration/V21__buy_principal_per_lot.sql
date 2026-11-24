alter table copy_intent
  add column buy_principal_per_lot numeric(38,9) check (buy_principal_per_lot > 0);
