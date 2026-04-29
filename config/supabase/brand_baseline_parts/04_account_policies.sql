drop policy if exists "brand members can read own row" on brand_members;
create policy "brand members can read own row"
on brand_members for select
to authenticated
using (user_id = auth.uid() or brand_has_role(array['owner', 'admin']));

drop policy if exists "brand owners manage members" on brand_members;
create policy "brand owners manage members"
on brand_members for all
to authenticated
using (brand_has_role(array['owner']))
with check (brand_has_role(array['owner']));

drop policy if exists "brand viewers read accounts" on matrix_accounts;
create policy "brand viewers read accounts"
on matrix_accounts for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators manage accounts" on matrix_accounts;
create policy "brand operators manage accounts"
on matrix_accounts for all
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']))
with check (brand_has_role(array['owner', 'admin', 'operator']));

drop policy if exists "brand viewers read account platforms" on account_platforms;
create policy "brand viewers read account platforms"
on account_platforms for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators manage account platforms" on account_platforms;
create policy "brand operators manage account platforms"
on account_platforms for all
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']))
with check (brand_has_role(array['owner', 'admin', 'operator']));

drop policy if exists "brand viewers read browser profiles" on browser_profiles;
create policy "brand viewers read browser profiles"
on browser_profiles for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators manage browser profiles" on browser_profiles;
create policy "brand operators manage browser profiles"
on browser_profiles for all
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']))
with check (brand_has_role(array['owner', 'admin', 'operator']));

drop policy if exists "brand viewers read tasks" on automation_tasks;
create policy "brand viewers read tasks"
on automation_tasks for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators manage tasks" on automation_tasks;
create policy "brand operators manage tasks"
on automation_tasks for all
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']))
with check (brand_has_role(array['owner', 'admin', 'operator']));

drop policy if exists "brand viewers read stats" on video_stats_snapshots;
create policy "brand viewers read stats"
on video_stats_snapshots for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators import stats" on video_stats_snapshots;
create policy "brand operators import stats"
on video_stats_snapshots for insert
to authenticated
with check (brand_has_role(array['owner', 'admin', 'operator']));
