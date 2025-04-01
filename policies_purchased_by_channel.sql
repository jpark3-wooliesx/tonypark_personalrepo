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