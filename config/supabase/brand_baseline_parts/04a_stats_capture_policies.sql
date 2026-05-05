drop policy if exists "brand viewers read account stats" on wechat_stats_account_snapshots;
create policy "brand viewers read account stats"
on wechat_stats_account_snapshots for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators manage account stats" on wechat_stats_account_snapshots;
create policy "brand operators manage account stats"
on wechat_stats_account_snapshots for all
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']))
with check (brand_has_role(array['owner', 'admin', 'operator']));

drop policy if exists "brand viewers read stats capture runs" on wechat_stats_capture_runs;
create policy "brand viewers read stats capture runs"
on wechat_stats_capture_runs for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators manage stats capture runs" on wechat_stats_capture_runs;
create policy "brand operators manage stats capture runs"
on wechat_stats_capture_runs for all
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']))
with check (brand_has_role(array['owner', 'admin', 'operator']));
