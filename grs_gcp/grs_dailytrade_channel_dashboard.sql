--monthly refresh used to populate daily trade channel retention dashboard

declare report_start date default date('2024-03-01');
declare report_end date default last_day(date_sub(current_date('Australia/Sydney'), interval 1 month));

create or replace table gcp-wow-rwds-ai-insurance-dev.analytics_layer.grs_retention_channel_pivot as
with
retention_calcs as (
  select
  *
  ,case
    when date_diff(report_end,start_date,DAY) >= coolingoff_days
      then if(date_diff(end_date,start_date,DAY) <= coolingoff_days,1,0)
    else null
  end as coolingoff_cancelled
  ,case
    when date_diff(report_end,start_date,DAY) >= 30
      then if(date_diff(end_date,start_date,DAY) <= 30,1,0)
    else null
  end as mtc_30d
  ,case
    when date_diff(report_end,start_date,DAY) >= 60
      then if(date_diff(end_date,start_date,DAY) <= 60,1,0)
    else null
  end as mtc_60d
  ,case
    when date_diff(report_end,start_date,DAY) >= 90
      then if(date_diff(end_date,start_date,DAY) <= 90,1,0)
    else null
  end as mtc_90d
  ,case --adjust for cooling-off
    when report_end > first_renewal_date_plus_coolingoff and end_date >= first_renewal_date
      then if(end_date > first_renewal_date_plus_coolingoff,1,0)
    else null
  end as renewed
  ,case --adjust for cooling-off
    when report_end > first_renewal_date_plus_coolingoff
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
      ,policypurchasedateestimated as purchase_date
      ,date_trunc(policypurchasedateestimated,month) as purchase_month
      ,policystartdateestimated as start_date
      ,date_trunc(policystartdateestimated,month) as start_month
      ,coalesce(policyenddateestimated,date('2099-12-31')) as end_date
      ,policyrenewaldateestimated as recorded_renewal_date
      ,PolicyCoolingOffPeriodDays as coolingoff_days
      ,date_add(policyrenewaldateestimated, INTERVAL -(extract(year from policyrenewaldateestimated) - extract(year from policystartdateestimated)) +1 YEAR) as first_renewal_date
      ,g.utm_param
      ,g.custom_channel
      ,g.default_channel
      ,g.default_channel_detail
      from gcp-wow-rwds-ai-insurance-prod.analytics_layer.master_data p
      join gcp-wow-rwds-ai-insurance-dev.analytics_layer.grs_policy_orders g
        on p.PolicyIdentifier = g.pid
      where p.policystartdateestimated between report_start and report_end
      and SourceProduct in ('CAR','HOME')
    )
  )
)
select
product
,purchase_month
,start_month
,utm_param
,custom_channel
,default_channel
,count(*) purchased
,sum(coolingoff_cancelled) coolingoff_cancelled
,count(coolingoff_cancelled) coolingoff_base
,sum(mtc_30d) mtc_30d
,count(mtc_30d) mtc_30d_base
,sum(mtc_60d) mtc_60d
,count(mtc_60d) mtc_60d_base
,sum(mtc_90d) mtc_90d
,count(mtc_90d) mtc_90d_base
,sum(renewed) renewed
,count(renewed) - sum(renewed) lapsed
,count(renewed) renewal_reached
,sum(retained) retained
,count(retained) - sum(retained) churned
,count(retained) retention_base
-- ,sum(coolingoff_cancelled)*1.0/count(coolingoff_cancelled) coolingoff_cancel_rate
-- ,sum(mtc_30d)*1.0/count(mtc_30d) mtc_30d_rate
-- ,sum(mtc_60d)*1.0/count(mtc_60d) mtc_60d_rate
-- ,sum(mtc_90d)*1.0/count(mtc_90d) mtc_90d_rate
-- ,sum(renewed)*1.0/count(renewed) renewal_rate
-- ,sum(retained)*1.0/count(retained) retention_rate
from retention_calcs
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
;
