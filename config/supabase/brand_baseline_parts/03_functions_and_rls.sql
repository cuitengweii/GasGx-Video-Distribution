create or replace function dashboard_summary()
returns table (
    accounts bigint,
    platforms bigint,
    running_tasks bigint,
    failed_tasks bigint,
    unsupported_tasks bigint,
    remaining_material_videos bigint,
    views bigint,
    likes bigint,
    comments bigint,
    messages bigint
)
language sql
stable
as $$
    select
        (select count(*) from matrix_accounts) as accounts,
        (select count(*) from account_platforms where enabled = 1) as platforms,
        (select count(*) from automation_tasks where status in ('pending', 'running')) as running_tasks,
        (select count(*) from automation_tasks where status = 'failed') as failed_tasks,
        (select count(*) from automation_tasks where status = 'unsupported') as unsupported_tasks,
        0::bigint as remaining_material_videos,
        coalesce((select sum(views) from video_stats_snapshots), 0)::bigint as views,
        coalesce((select sum(likes) from video_stats_snapshots), 0)::bigint as likes,
        coalesce((select sum(comments) from video_stats_snapshots), 0)::bigint as comments,
        coalesce((select sum(messages) from video_stats_snapshots), 0)::bigint as messages
$$;

create table if not exists brand_members (
    user_id uuid primary key references auth.users(id) on delete cascade,
    role text not null check (role in ('owner', 'admin', 'operator', 'viewer')),
    created_at bigint not null,
    updated_at bigint not null
);

create or replace function brand_current_role()
returns text
language sql
security definer
set search_path = public
stable
as $$
    select role from brand_members where user_id = auth.uid()
$$;

create or replace function brand_has_role(allowed_roles text[])
returns boolean
language sql
security definer
set search_path = public
stable
as $$
    select coalesce(brand_current_role() = any(allowed_roles), false)
$$;

alter table matrix_accounts enable row level security;
alter table account_platforms enable row level security;
alter table browser_profiles enable row level security;
alter table automation_tasks enable row level security;
alter table video_stats_snapshots enable row level security;
alter table ai_robot_configs enable row level security;
alter table ai_robot_messages enable row level security;
alter table brand_settings enable row level security;
alter table schema_migrations enable row level security;
alter table app_settings enable row level security;
alter table analytics_items enable row level security;
alter table video_matrix_assets enable row level security;
alter table video_matrix_jobs enable row level security;
alter table app_seed_runs enable row level security;
alter table brand_members enable row level security;
