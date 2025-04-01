--Default Marketing Channel
with
dates as (
  select
  date('2025-02-01') as start_date
  ,date('2025-02-28') as end_date
)
,policy_purchase_hits as (
  select
  * except(hit_num)
  from (
    select
    lower(eVar7) as product
    ,evar61 as policy_id
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
    on date(a.date_time) between d.start_date and d.end_date
    left join gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.dim_grs_marketing_channel m
    on cast(a.va_closer_id as numeric) = m.channel_id
    where 1=1
    and post_evar61 is not null
    and pagename in ('ev-ins:car:quote:thanks','ev-ins:home:quote:thanks','ev-ins:quoteline:travel:thankyou')
  )
  where hit_num = 1 --only take the first hit for purchase
)
select
product
,channel_name
,count(*) purchase_cnt
from policy_purchase_hits
group by 1,2
order by 1,2
;


--Insurance Custom Segments
with
dates as (
  select
  date('2025-02-01') as start_date
  ,date('2025-02-28') as end_date
)
,policy_purchase_hits as (
  select
  * except(hit_num)
  from (
    select
    lower(eVar7) as product
    ,evar61 as policy_id
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
    on date(a.date_time) between d.start_date and d.end_date
    left join gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.dim_grs_marketing_channel m
    on cast(a.va_closer_id as numeric) = m.channel_id
    where 1=1
    and post_evar61 is not null
    and pagename in ('ev-ins:car:quote:thanks','ev-ins:home:quote:thanks','ev-ins:quoteline:travel:thankyou')
  )
  where hit_num = 1 --only take the first hit for purchase
)
,channel_assignment as (
  select
  product
  ,policy_id
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
  from policy_purchase_hits
)
select
product
,custom_channel
,count(*) purchase_cnt
from channel_assignment
group by 1,2
order by 1,2
;


--Adobe and BigQuery Policy ID Matching
with
dates as (
  select
  date('2024-11-01') as start_date
  ,date('2025-01-31') as end_date
)
,bq_policies as (
  select distinct
  lower(SourceProduct) as product
  ,case when SourceProduct in ('CAR','HOME') then substr(PolicyIdentifier,4,100) else PolicyIdentifier end as pid
  from gcp-wow-rwds-ai-insurance-prod.analytics_layer.master_data p
  join dates d
    on p.PolicyPurchaseDate between d.start_date and d.end_date
  where (SourceProduct = 'CAR' or (SourceProduct = 'HOME' and ProductSubType in ('Combined Home & Contents','Content Only Cover','Home Only Cover')) or SourceProduct = 'TRAVEL')
)
,grs_policies as (
  select
  * except(hit_num)
  from (
    select
    lower(eVar7) as product
    ,evar61 as pid
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
      on date(a.date_time) between d.start_date and d.end_date
    left join gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.dim_grs_marketing_channel m
      on cast(a.va_closer_id as numeric) = m.channel_id
    where 1=1
    and evar61 is not null
    and evar7 in ('car','home','landlord','pet','travel')
    and pagename in ('ev-ins:car:quote:thanks','ev-ins:home:quote:thanks','ev-ins:quoteline:travel:thankyou')
  )
  where hit_num = 1 --only take the first hit for purchase
)
,callcentre_policies as (
  select 'car' as product
  ,substr(policy_id,4,100) as pid
  from gcp-wow-rwds-ai-insurance-dev.tony.callcentre_car_novjan

  union all

  select 'home' as product
  ,substr(policy_id,4,100) as pid
  from gcp-wow-rwds-ai-insurance-dev.tony.callcentre_home_novjan
)
select
coalesce(b.product,c.product,g.product) as product
,sum(case when b.pid is not null then 1 else 0 end) as bq_sales
,sum(case when b.pid is not null and c.pid is null then 1 else 0 end) as bq_nonhub_sales
,sum(case when b.pid is not null and c.pid is not null then 1 else 0 end) as bq_hub_sales
,sum(case when g.pid is not null then 1 else 0 end) as grs_sales
,sum(case when b.pid is not null and g.pid is not null then 1 else 0 end) as both_sales
,sum(case when b.pid is not null and c.pid is null and g.pid is not null then 1 else 0 end) as nonhub_both_sales
,sum(case when b.pid is not null and g.pid is null then 1 else 0 end) as bq_only_sales
,sum(case when b.pid is not null and c.pid is null and g.pid is null then 1 else 0 end) as bq_nonhub_only_sales
,sum(case when b.pid is null and g.pid is not null then 1 else 0 end) as grs_only_sales
from bq_policies b
left join callcentre_policies c
on b.pid = c.pid
full outer join grs_policies g
on b.pid = g.pid and b.product = g.product
group by 1
order by 1
;