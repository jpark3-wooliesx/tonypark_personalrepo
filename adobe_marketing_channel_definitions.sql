--Adobe Marketing Channel Definitions

/*

variable to column conversions:

site_app_name: evar2, post_evar2
site_app_platform: evar3, post_evar3
deeplink_uri: evar191, post_evar191(?)
utm_parameters: post_evar46

users linked through: mcvisid
sessions separated through: visit_num

search engine: visit_search_engine, search_engine, post_search_engine
    note: values are numbers as strings e.g 0 (believe no search engine), 57 (likely Google), 284 (the next most common)

1. Paid Search

deeplink_uri like '%utm_%'
and
(deeplink_uri like '%cpc%'
or
deeplink_uri like '%sem%'
or
deeplink_uri like '%ppc%'
)
and site_app_name like '%Supermarkets%'
and site_app_platform like '%App%'

2. Email

deeplink_uri like '%utm_%'
and
deeplink_uri like '%email%'
and
site_app_name like '%Supermarkets%'
and
site_app_platform like '%App%'

3. Email

deeplink_uri like '%utm_%'
and
utm_parameters like '%email%'
and
site_app_name like '%Supermarkets%'
and
site_app_platform like '%App%'

4. Social

deeplink_uri like '%utm_%'
and
utm_parameters like '%social%'
and
site_app_name like '%Supermarkets%'
and
site_app_platform like '%App%'

5. Paid Search

utm_parameters like '%paidsearch%'
or
utm_parameters like '%paid search%'
or
utm_parameters like '%paid search%'
or
utm_parameters like '%paid search%'
or
utm_parameters like '%search%'
or
utm_parameters = 'cpc'

6. Paid Search

REGEXP_CONTAINS(your_url_field, r'cvosrc=.*ppc\..*')
or
REGEXP_CONTAINS(your_url_field, r'cvosrc=.*cse\..*')

7. Paid Search

REGEXP_CONTAINS(your_url_field, r'cmpid=smsm\..*')

8.Paid Search

--Matches Adobe's Paid Search Detection Rules

9.Organic Search

deeplink_uri is not null
and
deeplink_uri not like '%utm_%'
and
deeplink_uri like '%referrer_domain=%'
and
deeplink_uri not like '%not_available%'
and
site_app_name like '%Supermarkets%'
and
site_app_platform like '%App%'

10. Organic Search

utm_parameters like '%organic%'

11. Organic Search

--Matches Adobe's Organic Search Detection Rules
or

12. Email

utm_parameters like '%email%'
or
utm_parameters like '%lifecycle%'
or
utm_parameters like '%gmail%'
or
utm_parameters like '%email%'

13. Email

REGEXP_CONTAINS(your_url_field, r'cvosrc=.*email\.')

14. Display

utm_parameters like '%display%'
or
utm_parameters like '%programmatic%'
or
utm_parameters like '%spotify%'

15. Display

REGEXP_CONTAINS(query_string_parameter, r'cvosrc=.*display.*')

16. Display

REGEXP_CONTAINS(query_string_parameter, r'cmpid=smd.*')

17. Video

utm_parameters like '%video%'

18. Audio

utm_parameters like '%audio%'

19. Affiliate

utm_parameters like '%affiliate%'
or
utm_parameters like '%impact%'

20. Affiliate

REGEXP_CONTAINS(query_string_parameter, r'cvosrc=.*affiliate.*')

21. Social

utm_parameters like '%social%'
or
utm_parameters like '%social_post_paid%'
or
utm_parameters like '%social%'
or
utm_parameters like '%social%'
or
utm_parameters like '%facebook%'

22. Social

REGEXP_CONTAINS(your_url_field, r'cvosrc=social.*')

23. Social

REGEXP_CONTAINS(your_url_field, r'cmpid=smf.*')

24.Social

net.host(visit_referrer) like '%facebook%'
or
net.host(visit_referrer) like '%linkedin.com%'
or
net.host(visit_referrer) like '%twitter.com%'
or
net.host(visit_referrer) like '%plus.google.com%'

25. Mobile Web Journey

utm_parameters like '%Mobile web%'
or
utm_parameters like '%journeys%'
or
utm_parameters like '%mobile%20web%'

26. In Store

utm_parameters like '%Instore%'
or
utm_parameters like '%in-store%'

27. Partner

utm_parameters like '%partner%'

28. QR Code

utm_parameters like '%qr-code%'

29. Referring Domains

utm_parameters like '%referral%'
or 
utm_parameters like '%yoomroo%'
or
utm_parameters like '%swaven%'
or
utm_parameters like '%skedlink%'
or
utm_parameters like '%mamamia.com.au%'

30. Push Notifications

utm_parameters like '%push%'
or
deeplink_uri like '%=push%'

31. Untracked

utm_parameters is not null

32. Referring Domains

visit_referrer is not null

33. Direct Traffic

visit_referrer is null
and
(is first hit of visit)

34. Direct Traffic

Referrer Matched Internal URL Filters
and
(is first hit of visit)

*/

--Testing out the channel attribution

with
sample_users as (
  select
  distinct mcvisid
  from gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.grs_adobe
  WHERE
  DATE(date_time) BETWEEN '2025-01-01' and '2025-01-31'
  AND evar2 like '%Insurance%'
  limit 100
)
,sample_hits as (
  select
  *
  ,row_number() over(partition by mcvisid,visit_num order by date_time) hit_num
  from gcp-wow-food-wlx-digaspt-dev.prod_adobe_data.grs_adobe g
  join sample_users
    using(mcvisid)
  WHERE
  DATE(date_time) BETWEEN '2025-01-01' and '2025-01-31'
)
select
mcvisid
,visit_num
,hit_num
,date_time
,page_url
,evar2
,evar3
,post_evar46
,evar191
,visit_referrer
,CASE
    WHEN evar191 like '%utm_%'
    and (evar191 like '%cpc%'
         or evar191 like '%sem%'
         or evar191 like '%ppc%')
    and evar2 like '%Supermarkets%'
    and evar3 like '%App%'
    THEN 'Paid Search'  -- 1. Paid Search

    WHEN evar191 like '%utm_%'
    and evar191 like '%email%'
    and evar2 like '%Supermarkets%'
    and evar3 like '%App%'
    THEN 'Email'  -- 2. Email

    WHEN evar191 like '%utm_%'
    and post_evar46 like '%email%'
    and evar2 like '%Supermarkets%'
    and evar3 like '%App%'
    THEN 'Email'  -- 3. Email

    WHEN evar191 like '%utm_%'
    and post_evar46 like '%social%'
    and evar2 like '%Supermarkets%'
    and evar3 like '%App%'
    THEN 'Social'  -- 4. Social

    WHEN post_evar46 like '%paidsearch%'
    or post_evar46 like '%paid search%'
    or post_evar46 like '%paid search%'
    or post_evar46 like '%paid search%'
    or post_evar46 like '%search%'
    or post_evar46 = 'cpc'
    THEN 'Paid Search'  -- 5. Paid Search

    WHEN REGEXP_CONTAINS(page_url, r'cvosrc=.*ppc\..*')
    or REGEXP_CONTAINS(page_url, r'cvosrc=.*cse\..*')
    THEN 'Paid Search'  -- 6. Paid Search

    WHEN REGEXP_CONTAINS(page_url, r'cmpid=smsm\..*')
    THEN 'Paid Search'  -- 7. Paid Search

    -- 8. Paid Search
    -- Matches Adobe's Paid Search Detection Rules
    -- (You'll need to implement these Adobe rules here)

    WHEN evar191 is not null
    and evar191 not like '%utm_%'
    and evar191 like '%referrer_domain=%'
    and evar191 not like '%not_available%'
    and evar2 like '%Supermarkets%'
    and evar3 like '%App%'
    THEN 'Organic Search'  -- 9. Organic Search

    WHEN post_evar46 like '%organic%'
    THEN 'Organic Search'  -- 10. Organic Search

    -- 11. Organic Search
    -- Matches Adobe's Organic Search Detection Rules
    -- (You'll need to implement these Adobe rules here)

    WHEN post_evar46 like '%email%'
    or post_evar46 like '%lifecycle%'
    or post_evar46 like '%gmail%'
    or post_evar46 like '%email%'
    THEN 'Email'  -- 12. Email

    WHEN REGEXP_CONTAINS(page_url, r'cvosrc=.*email\.')
    THEN 'Email'  -- 13. Email

    WHEN post_evar46 like '%display%'
    or post_evar46 like '%programmatic%'
    or post_evar46 like '%spotify%'
    THEN 'Display'  -- 14. Display

    WHEN REGEXP_CONTAINS(page_url, r'cvosrc=.*display.*')
    THEN 'Display'  -- 15. Display

    WHEN REGEXP_CONTAINS(page_url, r'cmpid=smd.*')
    THEN 'Display'  -- 16. Display

    WHEN post_evar46 like '%video%'
    THEN 'Video'  -- 17. Video

    WHEN post_evar46 like '%audio%'
    THEN 'Audio'  -- 18. Audio

    WHEN post_evar46 like '%affiliate%'
    or post_evar46 like '%impact%'
    THEN 'Affiliate'  -- 19. Affiliate

    WHEN REGEXP_CONTAINS(page_url, r'cvosrc=.*affiliate.*')
    THEN 'Affiliate'  -- 20. Affiliate

    WHEN post_evar46 like '%social%'
    or post_evar46 like '%social_post_paid%'
    or post_evar46 like '%social%'
    or post_evar46 like '%social%'
    or post_evar46 like '%facebook%'
    THEN 'Social'  -- 21. Social

    WHEN REGEXP_CONTAINS(page_url, r'cvosrc=social.*')
    THEN 'Social'  -- 22. Social

    WHEN REGEXP_CONTAINS(page_url, r'cmpid=smf.*')
    THEN 'Social'  -- 23. Social

    WHEN net.host(visit_referrer) like '%facebook%'
    or net.host(visit_referrer) like '%linkedin.com%'
    or net.host(visit_referrer) like '%twitter.com%'
    or net.host(visit_referrer) like '%plus.google.com%'
    THEN 'Social'  -- 24. Social

    WHEN post_evar46 like '%Mobile web%'
    or post_evar46 like '%journeys%'
    or post_evar46 like '%mobile%20web%'
    THEN 'Mobile Web Journey'  -- 25. Mobile Web Journey

    WHEN post_evar46 like '%Instore%'
    or post_evar46 like '%in-store%'
    THEN 'In Store'  -- 26. In Store

    WHEN post_evar46 like '%partner%'
    THEN 'Partner'  -- 27. Partner

    WHEN post_evar46 like '%qr-code%'
    THEN 'QR Code'  -- 28. QR Code

    WHEN post_evar46 like '%referral%'
    or post_evar46 like '%yoomroo%'
    or post_evar46 like '%swaven%'
    or post_evar46 like '%skedlink%'
    or post_evar46 like '%mamamia.com.au%'
    THEN 'Referring Domains'  -- 29. Referring Domains

    WHEN post_evar46 like '%push%'
    or evar191 like '%=push%'
    THEN 'Push Notifications'  -- 30. Push Notifications

    WHEN post_evar46 is not null
    THEN 'Untracked'  -- 31. Untracked

    WHEN visit_referrer is not null
    THEN 'Referring Domains'  -- 32. Referring Domains

    WHEN visit_referrer is null
    and hit_num = 1
    THEN 'Direct Traffic'  -- 33. Direct Traffic

    WHEN 
    -- your_condition_for_referrer_matched_internal_url_filters -- Replace with your filter logic
    hit_num = 1
    THEN 'Direct Traffic'  -- 34. Direct Traffic

    ELSE 'Other'  -- Default case for anything that doesn't match
END as channel
from sample_hits
order by 1,2,3,4
;