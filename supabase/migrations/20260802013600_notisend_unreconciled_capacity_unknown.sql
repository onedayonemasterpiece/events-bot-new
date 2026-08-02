-- An unreconciled NotiSend provider counter is unknown, not zero. Routing was
-- already fail-closed; this follow-up also prevents operator reports from
-- presenting a fictional 0/200 balance before the first dashboard snapshot.

begin;

alter table email_control.recommendation_capacity
  drop constraint if exists recommendation_capacity_provider_used_chk;

alter table email_control.recommendation_capacity
  alter column provider_used_count drop not null,
  alter column provider_used_count drop default;

update email_control.recommendation_capacity
   set provider_used_count = null
 where provider_reconciled_at is null;

alter table email_control.recommendation_capacity
  add constraint recommendation_capacity_provider_used_chk
  check (provider_used_count is null or provider_used_count between 0 and capacity);

alter table email_control.recommendation_capacity
  add constraint recommendation_capacity_provider_reconciliation_value_chk
  check (
    (provider_reconciled_at is null and provider_used_count is null)
    or (provider_reconciled_at is not null and provider_used_count is not null)
  );

comment on column email_control.recommendation_capacity.provider_used_count is
  'Actual provider-reported unique recipients in the current billing period; NULL until an operator reconciliation succeeds.';

commit;
