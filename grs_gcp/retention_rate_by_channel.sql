--Weekly: Retention Rate by Custom Insurance Segments
with
date_range as (
  select
  date('2024-02-26') as report_start
  ,date_add(current_date(), interval -1 day) as report_end
)
,dates as (
  select
  CalendarDay as dt
  ,FiscalWeekStartDate as week_start
  ,FiscalWeekEndDate as week_end
  from gcp-wow-ent-im-wowx-cust-prod.adp_wowx_dm_masterdata_view.dim_date_v d
  join date_range r on d.CalendarDay between r.report_start and r.report_end
)
,bq_policies as (
  select
  *
  ,case
    when date_diff((select report_end from date_range),start_date,DAY) >= coolingoff_days
      then if(date_diff(end_date,start_date,DAY) <= coolingoff_days,1,0)
    else null
  end as coolingoff_cancelled
  ,case
    when date_diff((select report_end from date_range),start_date,DAY) >= 30
      then if(date_diff(end_date,start_date,DAY) <= 30,1,0)
    else null
  end as mtc_30d
  ,case
    when date_diff((select report_end from date_range),start_date,DAY) >= 60
      then if(date_diff(end_date,start_date,DAY) <= 60,1,0)
    else null
  end as mtc_60d
  ,case
    when date_diff((select report_end from date_range),start_date,DAY) >= 90
      then if(date_diff(end_date,start_date,DAY) <= 90,1,0)
    else null
  end as mtc_90d
  ,case --adjust for cooling-off
    when (select report_end from date_range) > first_renewal_date_plus_coolingoff and end_date >= first_renewal_date
      then if(end_date > first_renewal_date_plus_coolingoff,1,0)
    else null
  end as renewed
  ,case --adjust for cooling-off
    when (select report_end from date_range) > first_renewal_date_plus_coolingoff
      then if(end_date > first_renewal_date_plus_coolingoff,1,0)
    else null
  end as retained
  from (
    select
    *
    ,date_add(first_renewal_date, interval coolingoff_days DAY) first_renewal_date_plus_coolingoff
    from (
      select
      lower(SourceProduct) as product
      ,case when SourceProduct in ('CAR','HOME') then substr(PolicyIdentifier,4,100) else PolicyIdentifier end as pid
      ,policypurchasedateestimated as purchase_date
      ,week_start as purchase_week
      ,policystartdateestimated as start_date
      ,coalesce(policyenddateestimated,date('2099-12-31')) as end_date
      ,policyrenewaldateestimated as recorded_renewal_date
      ,PolicyCoolingOffPeriodDays as coolingoff_days
      ,date_add(policyrenewaldateestimated, INTERVAL -(extract(year from policyrenewaldateestimated) - extract(year from policystartdateestimated)) +1 YEAR) as first_renewal_date,
      from gcp-wow-rwds-ai-insurance-prod.analytics_layer.master_data p
      join dates d
        on p.PolicyPurchaseDate = d.dt
      where (SourceProduct = 'CAR' or (SourceProduct = 'HOME' and ProductSubType in ('Combined Home & Contents','Content Only Cover','Home Only Cover')))
    )
  )
)
,grs_policies as (
  select
  * except(hit_num)
  from (
    select
    lower(eVar7) as product
    ,post_evar61 as pid
    ,mcvisid as adobe_id
    ,date_time
    ,visit_num
    ,m.channel_name
    ,va_closer_detail as channel_detail
    ,row_number() over(partition by evar61 order by date_time) as hit_num
    ,visit_referrer
    ,post_evar46 as utm_param
    ,pagename
    from gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.grs_adobe a
    join dates d
      on date(a.date_time) = d.dt
    left join gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.dim_grs_marketing_channel m
      on cast(a.va_closer_id as numeric) = m.channel_id
    where 1=1
    and date(a.date_time) between (select report_start from date_range) and (select report_end from date_range)
    and post_evar61 is not null
    and pagename in ('ev-ins:car:quote:thanks','ev-ins:home:quote:thanks')
  )
  where hit_num = 1 --only take the first hit for purchase
)
,channel_assignment as (
  select
  product
  ,pid
  ,date(date_time) as purchase_date
  ,channel_name as default_channel
  ,channel_detail
  ,case 
    when (
      net.host(visit_referrer) like '%comparethemarket%'
      or
      net.host(visit_referrer) like '%choosi%'
      or
      net.host(visit_referrer) like '%iselect%'
      or
      lower(utm_param) like '%utm:ctm%'
      or
      lower(utm_param) like '%utm:comparethemarket%'
      or
      lower(utm_param) like '%iselect%'
      or
      lower(utm_param) like '%utm:choosi%'
    ) then 'aggregator'
    when channel_name = 'direct' then 'direct'
    when (
      channel_name = 'email'
      or
      lower(utm_param) like '%ser-%'
      or
      lower(utm_param) like '%email%'
    ) then 'email'
    when (
      channel_name = 'paid search'
      and (
        lower(utm_param) like '%pmax%'
        or
        lower(utm_param) like '%performance_max%'
        )
    ) then 'pmax'
    when channel_name = 'paid search' then 'paid search'
    when (
      lower(utm_param) like '%utm:rewards%'
      or
      lower(utm_param) like '%utm:discoverpage%'
    ) then 'everyday'
    when (
        channel_name = 'social'
        or
        lower(utm_param) like '%facebook%'
        or
        lower(utm_param) like '%tiktok%'
        or
        lower(utm_param) like '%youtube%'
        or
        lower(utm_param) like '%instagram%'
        or
        lower(utm_param) like '%bvod%'
    ) then 'social'
    when channel_name = 'organic search' then 'organic search'
    else 'other'
  end as custom_channel
  from grs_policies
)
select
b.product
,b.purchase_week
-- ,g.default_channel
,g.custom_channel
,count(*) purchase_cnt
,sum(coolingoff_cancelled) coolingoff_cancelled
,sum(mtc_30d) mtc_30d
,sum(mtc_60d) mtc_60d
,sum(mtc_90d) mtc_90d
,sum(renewed) renewed
,sum(retained) retained
,sum(coolingoff_cancelled)*1.0/count(coolingoff_cancelled) coolingoff_cancel_rate
,sum(mtc_30d)*1.0/count(mtc_30d) mtc_30d_rate
,sum(mtc_60d)*1.0/count(mtc_60d) mtc_60d_rate
,sum(mtc_90d)*1.0/count(mtc_90d) mtc_90d_rate
,sum(renewed)*1.0/count(renewed) renewal_rate
,sum(retained)*1.0/count(retained) retention_rate
from bq_policies b
join channel_assignment g
  on b.pid = g.pid and b.product = g.product
group by 1,2,3
order by 1,2,3
;

--Monthly: Retention Rate by Custom Insurance Segments
with
date_range as (
  select
  date('2024-03-01') as report_start
  ,date_add(current_date(), interval -1 day) as report_end
)
,dates as (
  select
  CalendarDay as dt
  from gcp-wow-ent-im-wowx-cust-prod.adp_wowx_dm_masterdata_view.dim_date_v d
  join date_range r on d.CalendarDay between r.report_start and r.report_end
)
,bq_policies as (
  select
  *
  ,case
    when date_diff((select report_end from date_range),start_date,DAY) >= coolingoff_days
      then if(date_diff(end_date,start_date,DAY) <= coolingoff_days,1,0)
    else null
  end as coolingoff_cancelled
  ,case
    when date_diff((select report_end from date_range),start_date,DAY) >= 30
      then if(date_diff(end_date,start_date,DAY) <= 30,1,0)
    else null
  end as mtc_30d
  ,case
    when date_diff((select report_end from date_range),start_date,DAY) >= 60
      then if(date_diff(end_date,start_date,DAY) <= 60,1,0)
    else null
  end as mtc_60d
  ,case
    when date_diff((select report_end from date_range),start_date,DAY) >= 90
      then if(date_diff(end_date,start_date,DAY) <= 90,1,0)
    else null
  end as mtc_90d
  ,case --adjust for cooling-off
    when (select report_end from date_range) > first_renewal_date_plus_coolingoff and end_date >= first_renewal_date
      then if(end_date > first_renewal_date_plus_coolingoff,1,0)
    else null
  end as renewed
  ,case --adjust for cooling-off
    when (select report_end from date_range) > first_renewal_date_plus_coolingoff
      then if(end_date > first_renewal_date_plus_coolingoff,1,0)
    else null
  end as retained
  from (
    select
    *
    ,date_add(first_renewal_date, interval coolingoff_days DAY) first_renewal_date_plus_coolingoff
    from (
      select
      lower(SourceProduct) as product
      ,case when SourceProduct in ('CAR','HOME') then substr(PolicyIdentifier,4,100) else PolicyIdentifier end as pid
      ,policypurchasedateestimated as purchase_date
      ,date_trunc(policypurchasedateestimated,month) as purchase_month
      ,policystartdateestimated as start_date
      ,coalesce(policyenddateestimated,date('2099-12-31')) as end_date
      ,policyrenewaldateestimated as recorded_renewal_date
      ,PolicyCoolingOffPeriodDays as coolingoff_days
      ,date_add(policyrenewaldateestimated, INTERVAL -(extract(year from policyrenewaldateestimated) - extract(year from policystartdateestimated)) +1 YEAR) as first_renewal_date,
      from gcp-wow-rwds-ai-insurance-prod.analytics_layer.master_data p
      join dates d
        on p.PolicyPurchaseDate = d.dt
      where (SourceProduct = 'CAR' or (SourceProduct = 'HOME' and ProductSubType in ('Combined Home & Contents','Content Only Cover','Home Only Cover')))
    )
  )
)
,grs_policies as (
  select
  * except(hit_num)
  from (
    select
    lower(eVar7) as product
    ,post_evar61 as pid
    ,mcvisid as adobe_id
    ,date_time
    ,visit_num
    ,m.channel_name
    ,va_closer_detail as channel_detail
    ,row_number() over(partition by evar61 order by date_time) as hit_num
    ,visit_referrer
    ,post_evar46 as utm_param
    ,pagename
    from gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.grs_adobe a
    join dates d
      on date(a.date_time) = d.dt
    left join gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.dim_grs_marketing_channel m
      on cast(a.va_closer_id as numeric) = m.channel_id
    where 1=1
    and date(a.date_time) between (select report_start from date_range) and (select report_end from date_range)
    and post_evar61 is not null
    and pagename in ('ev-ins:car:quote:thanks','ev-ins:home:quote:thanks')
  )
  where hit_num = 1 --only take the first hit for purchase
)
,channel_assignment as (
  select
  product
  ,pid
  ,date(date_time) as purchase_date
  ,channel_name as default_channel
  ,channel_detail
  ,case 
    when (
      net.host(visit_referrer) like '%comparethemarket%'
      or
      net.host(visit_referrer) like '%choosi%'
      or
      net.host(visit_referrer) like '%iselect%'
      or
      lower(utm_param) like '%utm:ctm%'
      or
      lower(utm_param) like '%utm:comparethemarket%'
      or
      lower(utm_param) like '%iselect%'
      or
      lower(utm_param) like '%utm:choosi%'
    ) then 'aggregator'
    when channel_name = 'direct' then 'direct'
    when (
      channel_name = 'email'
      or
      lower(utm_param) like '%ser-%'
      or
      lower(utm_param) like '%email%'
    ) then 'email'
    when (
      channel_name = 'paid search'
      and (
        lower(utm_param) like '%pmax%'
        or
        lower(utm_param) like '%performance_max%'
        )
    ) then 'pmax'
    when channel_name = 'paid search' then 'paid search'
    when (
      lower(utm_param) like '%utm:rewards%'
      or
      lower(utm_param) like '%utm:discoverpage%'
    ) then 'everyday'
    when (
        channel_name = 'social'
        or
        lower(utm_param) like '%facebook%'
        or
        lower(utm_param) like '%tiktok%'
        or
        lower(utm_param) like '%youtube%'
        or
        lower(utm_param) like '%instagram%'
        or
        lower(utm_param) like '%bvod%'
    ) then 'social'
    when channel_name = 'organic search' then 'organic search'
    else 'other'
  end as custom_channel
  from grs_policies
)
select
b.product
,b.purchase_month
-- ,g.default_channel
,g.custom_channel
,count(*) purchase_cnt
,sum(coolingoff_cancelled) coolingoff_cancelled
,sum(mtc_30d) mtc_30d
,sum(mtc_60d) mtc_60d
,sum(mtc_90d) mtc_90d
,sum(renewed) renewed
,sum(retained) retained
,sum(coolingoff_cancelled)*1.0/count(coolingoff_cancelled) coolingoff_cancel_rate
,sum(mtc_30d)*1.0/count(mtc_30d) mtc_30d_rate
,sum(mtc_60d)*1.0/count(mtc_60d) mtc_60d_rate
,sum(mtc_90d)*1.0/count(mtc_90d) mtc_90d_rate
,sum(renewed)*1.0/count(renewed) renewal_rate
,sum(retained)*1.0/count(retained) retention_rate
from bq_policies b
join channel_assignment g
  on b.pid = g.pid and b.product = g.product
group by 1,2,3
order by 1,2,3
;
