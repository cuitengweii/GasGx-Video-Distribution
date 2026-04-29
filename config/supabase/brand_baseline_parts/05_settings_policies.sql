drop policy if exists "brand admins manage robot configs" on ai_robot_configs;
create policy "brand admins manage robot configs"
on ai_robot_configs for all
to authenticated
using (brand_has_role(array['owner', 'admin']))
with check (brand_has_role(array['owner', 'admin']));

drop policy if exists "brand operators read robot configs" on ai_robot_configs;
create policy "brand operators read robot configs"
on ai_robot_configs for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']));

drop policy if exists "brand viewers read robot messages" on ai_robot_messages;
create policy "brand viewers read robot messages"
on ai_robot_messages for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators manage robot messages" on ai_robot_messages;
create policy "brand operators manage robot messages"
on ai_robot_messages for all
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']))
with check (brand_has_role(array['owner', 'admin', 'operator']));

drop policy if exists "brand viewers read settings" on brand_settings;
create policy "brand viewers read settings"
on brand_settings for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand admins manage settings" on brand_settings;
create policy "brand admins manage settings"
on brand_settings for all
to authenticated
using (brand_has_role(array['owner', 'admin']))
with check (brand_has_role(array['owner', 'admin']));

drop policy if exists "brand viewers read migrations" on schema_migrations;
create policy "brand viewers read migrations"
on schema_migrations for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand admins manage migrations" on schema_migrations;
create policy "brand admins manage migrations"
on schema_migrations for all
to authenticated
using (brand_has_role(array['owner', 'admin']))
with check (brand_has_role(array['owner', 'admin']));

drop policy if exists "brand viewers read app settings" on app_settings;
create policy "brand viewers read app settings"
on app_settings for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand admins manage app settings" on app_settings;
create policy "brand admins manage app settings"
on app_settings for all
to authenticated
using (brand_has_role(array['owner', 'admin']))
with check (brand_has_role(array['owner', 'admin']));

drop policy if exists "brand viewers read analytics items" on analytics_items;
create policy "brand viewers read analytics items"
on analytics_items for select
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator', 'viewer']));

drop policy if exists "brand operators manage analytics items" on analytics_items;
create policy "brand operators manage analytics items"
on analytics_items for all
to authenticated
using (brand_has_role(array['owner', 'admin', 'operator']))
with check (brand_has_role(array['owner', 'admin', 'operator']));
