--Adobe Marketing Channel Definitions

/*

variable to column conversions:

site_app_name: evar2, post_evar2
site_app_platform: evar3, post_evar3
deeplink_uri: evar191, post_evar191(?)
utm_parameters: post_evar46



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


