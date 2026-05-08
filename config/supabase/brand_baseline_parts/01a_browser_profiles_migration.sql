do $$
begin
    if exists (
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = 'browser_profiles'
          and column_name = 'account_platform_id'
    ) then
        execute '
            update browser_profiles bp
            set account_id = ap.account_id
            from account_platforms ap
            where bp.account_id is null
              and bp.account_platform_id = ap.id
        ';
    end if;
end $$;

with ranked_browser_profiles as (
    select id, row_number() over (partition by account_id order by updated_at desc, id asc) as rn
    from browser_profiles
    where account_id is not null
)
update browser_profiles bp
set account_id = null
from ranked_browser_profiles ranked
where bp.id = ranked.id
  and ranked.rn > 1;

do $$
begin
    if not exists (
        select 1 from pg_constraint
        where conrelid = 'browser_profiles'::regclass
          and conname = 'browser_profiles_account_id_fkey'
    ) then
        alter table browser_profiles
        add constraint browser_profiles_account_id_fkey
        foreign key (account_id) references matrix_accounts(id) on delete cascade not valid;
    end if;

    if not exists (
        select 1 from pg_constraint
        where conrelid = 'browser_profiles'::regclass
          and conname = 'browser_profiles_account_id_key'
    ) then
        alter table browser_profiles
        add constraint browser_profiles_account_id_key unique (account_id);
    end if;
end $$;
