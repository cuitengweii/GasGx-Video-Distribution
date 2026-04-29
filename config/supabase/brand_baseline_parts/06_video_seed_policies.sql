drop policy if exists "brand viewers read video matrix assets" on video_matrix_assets;
create policy "brand viewers read video matrix assets"
on video_matrix_assets for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators manage video matrix assets" on video_matrix_assets;
create policy "brand operators manage video matrix assets"
on video_matrix_assets for all
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']))
with check (brand_has_role(array['owner', 'admin', 'operator']));

drop policy if exists "brand viewers read video matrix jobs" on video_matrix_jobs;
create policy "brand viewers read video matrix jobs"
on video_matrix_jobs for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators manage video matrix jobs" on video_matrix_jobs;
create policy "brand operators manage video matrix jobs"
on video_matrix_jobs for all
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']))
with check (brand_has_role(array['owner', 'admin', 'operator']));

drop policy if exists "brand admins read seed runs" on app_seed_runs;
create policy "brand admins read seed runs"
on app_seed_runs for select
to authenticated
using (brand_has_role(array['owner', 'admin']));

drop policy if exists "brand admins manage seed runs" on app_seed_runs;
create policy "brand admins manage seed runs"
on app_seed_runs for all
to authenticated
using (brand_has_role(array['owner', 'admin']))
with check (brand_has_role(array['owner', 'admin']));
