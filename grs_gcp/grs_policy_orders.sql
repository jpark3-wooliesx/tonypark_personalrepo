--the below script is run daily to populate the grs_policy_orders table with the previous 7 days of data

declare start_date_var date default current_date('Australia/Sydney')-8;
declare end_date_var date default current_date('Australia/Sydney')-1;

--remove previous 7 days data (which will be then be replaced)
delete from gcp-wow-rwds-ai-insurance-dev.tony.grs_policy_orders
  where date(date_time) between start_date_var and end_date_var
;

--insert new data for the previous 7 days
insert into gcp-wow-rwds-ai-insurance-dev.tony.grs_policy_orders
select
* except(rnum)
from (
  select
  a.*
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
    when default_channel = 'direct' then 'direct'
    when (
      default_channel = 'email'
      or
      lower(utm_param) like '%ser-%'
      or
      lower(utm_param) like '%email%'
    ) then 'email'
    when (
      default_channel = 'paid search'
      and (
        lower(utm_param) like '%pmax%'
        or
        lower(utm_param) like '%performance_max%'
        )
    ) then 'pmax'
    when default_channel = 'paid search' then 'paid search'
    when (
      lower(utm_param) like '%utm:rewards%'
      or
      lower(utm_param) like '%utm:discoverpage%'
    ) then 'everyday'
    when (
        default_channel = 'social'
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
    when default_channel = 'organic search' then 'organic search'
    else 'other'
  end as custom_channel
  ,v.device as device_type
  ,Mobile_Device_Type as os_type
  ,os_name as os_version
  ,row_number() over(partition by a.pid order by a.date_time, v.start_time) as rnum
  from (
    select
    date_time
    ,lower(eVar7) as product
    ,case
      when eVar7 = 'car' then concat('MOT',post_evar61)
      when eVar7 = 'home' then concat('HOM',post_evar61)
      else post_evar61
    end as pid
    ,mcvisid as adobe_id
    ,concat(cast(post_visid_high as string),cast(post_visid_low as string)) visitor_id
    ,visit_num
    ,visit_referrer
    ,post_evar46 as utm_param
    ,m.channel_name as default_channel
    ,va_closer_detail as default_channel_detail
    from gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.grs_adobe a
    left join gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.dim_grs_marketing_channel m
      on cast(a.va_closer_id as numeric) = m.channel_id
    where 1=1
    and date(a.date_time) between start_date_var and end_date_var
    and post_evar61 is not null
    and pagename in ('ev-ins:car:quote:thanks','ev-ins:home:quote:thanks','ev-ins:quoteline:travel:thankyou') --need to add pet once solution found
  ) a
  left join gcp-wow-rwds-ai-data-prod.grs.grs_nonsm_visits v
    on concat(a.visitor_id,'_',a.visit_num) = v.visit_id
)
where rnum = 1 --remove duplicates
order by date_time
;

/*

--The below script was run as a once-off to populate historical policy purchases from the GRS table

declare start_date_var date default '2024-01-01';
declare end_date_var date default '2025-04-08';

-- Adobe GRS data available from
-- Car & Home: 2024-02-22
-- Pet: 2023-03-23
-- Travel: 2024-01-18

create or replace table gcp-wow-rwds-ai-insurance-dev.tony.grs_policy_orders as

select
* except(rnum)
from (
  select
  a.*
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
    when default_channel = 'direct' then 'direct'
    when (
      default_channel = 'email'
      or
      lower(utm_param) like '%ser-%'
      or
      lower(utm_param) like '%email%'
    ) then 'email'
    when (
      default_channel = 'paid search'
      and (
        lower(utm_param) like '%pmax%'
        or
        lower(utm_param) like '%performance_max%'
        )
    ) then 'pmax'
    when default_channel = 'paid search' then 'paid search'
    when (
      lower(utm_param) like '%utm:rewards%'
      or
      lower(utm_param) like '%utm:discoverpage%'
    ) then 'everyday'
    when (
        default_channel = 'social'
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
    when default_channel = 'organic search' then 'organic search'
    else 'other'
  end as custom_channel
  ,v.device as device_type
  ,Mobile_Device_Type as os_type
  ,os_name as os_version
  ,row_number() over(partition by a.pid order by a.date_time, v.start_time) as rnum
  from (
    select
    date_time
    ,lower(eVar7) as product
    ,case
      when eVar7 = 'car' then concat('MOT',post_evar61)
      when eVar7 = 'home' then concat('HOM',post_evar61)
      else post_evar61
    end as pid
    ,mcvisid as adobe_id
    ,concat(cast(post_visid_high as string),cast(post_visid_low as string)) visitor_id
    ,visit_num
    ,visit_referrer
    ,post_evar46 as utm_param
    ,m.channel_name as default_channel
    ,va_closer_detail as default_channel_detail
    from gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.grs_adobe a
    left join gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.dim_grs_marketing_channel m
      on cast(a.va_closer_id as numeric) = m.channel_id
    where 1=1
    and date(a.date_time) between start_date_var and end_date_var
    and post_evar61 is not null
    and pagename in ('ev-ins:car:quote:thanks','ev-ins:home:quote:thanks','ev-ins:quoteline:travel:thankyou') --need to add pet once solution found
  ) a
  left join gcp-wow-rwds-ai-data-prod.grs.grs_nonsm_visits v
    on concat(a.visitor_id,'_',a.visit_num) = v.visit_id
)
where rnum = 1 --remove duplicates
order by date_time
;

*/