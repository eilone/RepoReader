import re
import streamlit as st

def extract_active_sources_refs(sql_text, remove_commented=False):
    # Find all instances of source and ref
    sources_matches = re.finditer(r"{{\s*source\('([^']+)',\s*'([^']+)'\)\s*}}", sql_text)
    refs_matches = re.finditer(r"{{\s*ref\(\s*'([^']+)'\s*\)\s*}}", sql_text)

    if remove_commented:
        # Extract the relevant values from the matches and filter out those that are commented
        sources_list = [match.group(2) for match in sources_matches if
                        not re.search(r"--\s*{{\s*source", sql_text[max(0, match.start() - 10):match.end()])]
        refs_list = [match.group(1) for match in refs_matches if
                     not re.search(r"--\s*{{\s*ref", sql_text[max(0, match.start() - 10):match.end()])]

    else:
        st.warning("WARNING: The SQL file contains commented sources and/or refs. ")
        # Extract the relevant values from the matches
        sources_list = [match.group(2) for match in sources_matches]
        refs_list = [match.group(1) for match in refs_matches]

    return {'sources': list(set(sources_list)), 'refs': list(set(refs_list))}

sql = """
{{
     config(
             materialized='table',
             schema='agg',
             partition_by={ "field": "purchase_date","data_type": "date"},
             cluster_by=['vertical','purchase_platform','application'],
             labels = {'agg': 'billings_by_date'}
            )
  }}

-- dbt run --target stg --models billings_by_date
--
--with refunds as(
--   select
--        date(a.payments.refund_date) as refund_date,
--        coalesce(a.vertical,'bug') as vertical,
--        coalesce(a.platform,'bug') as platform,
--        coalesce(a.application,'bug') as application,
--        coalesce(a.app_real_world_name,'bug') as app_real_world_name,
--        coalesce(a.payments.purchase_platform,'bug') as purchase_platform,
--        coalesce(a.subscription.current_subscription_store_country,'bug') as current_subscription_store_country,
--        coalesce(a.subscription.subscription_duration,'bug') as subscription_duration,
--        coalesce(a.payments.month0_late_or_renewal,'bug') as month0_late_or_renewal,
--        coalesce(a.payments.month0_year0_backlog,'bug') as month0_year0_backlog,
--        case
--            when a.subscription.product_category = 'asset' then 'asset'
--            when a.griffin.griffin_app_name like '%.cn' then 'CN'
--            when a.source_table = 'payments' then 'web_payments'
--            else 'app_store'
--        end as payment_source,
--        a.subscription.product_category,
--        case
--            when subs.subscription.had_trial = true then true
--            else false
--        end as had_trial,
--        case
--            when a.transaction.is_trial_period = true then true
--            else false
--        end as is_trial_period,
--        coalesce(a.subscription.trial_length,'non_trial_product') as trial_length,
--        case when a.projected.adjusted_actual_proceeds_alloc > 0 then true else false end as is_paid,
--        case when a.payments.actual_proceeds_alloc > 0 then true else false end as is_paid_wopt,
--        coalesce(subs.subscription.is_sol, false) as is_sol,
--        count(*) as refunds
--   from {{ref('unified_transactions_revenue_allocation') }} as a
--   --from `ltx-dwh-prod-processed.subscriptions.unified_transactions_revenue_allocation` as a
--    left join {{source('subscriptions','unified_subscriptions') }} as subs
--   --left join `ltx-dwh-prod-processed.subscriptions.unified_subscriptions` as subs
--        on subs.lt_subscription_id = a.lt_subscription_id
--  where a.payments.was_refund = true
--  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
--)

--with transactions as
(
    select
        date(a.purchase_date) as purchase_date,
        coalesce(a.vertical,'bug') as vertical,
        coalesce(a.platform,'bug') as platform,
        coalesce(a.application,'bug') as application,
        coalesce(a.app_real_world_name,'bug') as app_real_world_name,
        coalesce(a.payments.purchase_platform,'bug') as purchase_platform,
        coalesce(a.subscription.current_subscription_store_country,'bug') as current_subscription_store_country,
        coalesce(a.subscription.subscription_duration,'bug') as subscription_duration,
        coalesce(a.payments.month0_late_or_renewal,'bug') as month0_late_or_renewal,
        coalesce(a.payments.month0_year0_backlog,'bug') as month0_year0_backlog,
        a.payments.payment_origin,
        a.subscription.product_category,
        case
            when subs.subscription.had_trial = true then true
            else false
        end as had_trial,
        case
            when a.transaction.is_trial_period = true then true
            else false
        end as is_trial_period,
        coalesce(a.subscription.trial_length,'non_trial_product') as trial_length,
--        case when a.projected.adjusted_actual_proceeds_alloc > 0 then true else false end as is_paid,
--        case when a.payments.actual_proceeds_alloc > 0 then true else false end as is_paid_wopt,
        coalesce(subs.subscription.is_sol, false) is_sol,
        sum(a.payments.actual_proceeds_alloc) as revenue, -- without projected trials
        sum(a.projected.adjusted_actual_proceeds_alloc) as adjusted_revenue,
        sum(a.payments.actual_gross_proceeds_alloc) as gross_revenue,
        count(distinct a.lt_transaction_id) as transactions,
        cast(sum (a.projected.adjusted_pct_to_pay) as int) as paid_transactions,
        count(distinct case when a.payments.was_refund = true then a.lt_transaction_id end) as refunded_transactions,
        count(distinct case when a.payments.was_refund = true or a.payments.actual_proceeds_alloc>0 then a.lt_transaction_id end) as eligible_refund_transactions
    from
        {{source('subscriptions','unified_transactions_revenue_allocation') }} as a
--        `ltx-dwh-prod-processed.subscriptions.unified_transactions_revenue_allocation` as a
    left join
        {{source('subscriptions','unified_subscriptions') }} as subs
--        `ltx-dwh-prod-processed.subscriptions.unified_subscriptions` as subs
        on subs.lt_subscription_id = a.lt_subscription_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)

--select
--        coalesce(transactions.purchase_date, refunds.refund_date) as dt,
--        coalesce(transactions.vertical, refunds.vertical) as vertical,
--        coalesce(transactions.platform, refunds.platform) as platform,
--        coalesce(transactions.application, refunds.application) as application,
--        coalesce(transactions.app_real_world_name, refunds.app_real_world_name) as app_real_world_name,
--        coalesce(transactions.purchase_platform,refunds.purchase_platform) as purchase_platform,
--        coalesce(transactions.current_subscription_store_country, refunds.current_subscription_store_country)
--            as current_subscription_store_country,
--        coalesce(transactions.subscription_duration, refunds.subscription_duration) as subscription_duration,
--        coalesce(transactions.month0_late_or_renewal, refunds.month0_late_or_renewal) as month0_late_or_renewal,
--        coalesce(transactions.month0_year0_backlog, refunds.month0_year0_backlog) as month0_year0_backlog,
--        coalesce(transactions.payment_source, refunds.payment_source) as payment_source,
--        coalesce(transactions.product_category, refunds.product_category) as product_category,
--        coalesce(transactions.had_trial, refunds.had_trial) as had_trial,
--        coalesce(transactions.is_trial_period, refunds.is_trial_period) as is_trial_period,
--        coalesce(transactions.trial_length, refunds.trial_length) as trial_length,
--        coalesce(transactions.is_paid, refunds.is_paid) as is_paid,
--        coalesce(transactions.is_paid_wopt, refunds.is_paid_wopt) as is_paid_wopt,
--        coalesce(transactions.is_sol,refunds.is_sol) as is_sol,
--        coalesce(revenue_wopt, 0) as revenue_wopt,
--        coalesce(revenue, 0) as revenue,
--        coalesce(gross_proceeds, 0) as gross_proceeds,
--        coalesce(transactions.transactions, 0) as transactions,
--        coalesce(refunds.refunds, 0) as refunds,
--        coalesce(transactions.refunded_transactions, 0) as refunded_transactions_by_purchase_date,
--        coalesce(transactions.eligible_refund_transactions, 0) as eligible_refund_transactions_by_purchase_date,

--        sum(actual_proceeds) over(partition by
--            date_trunc(transactions.purchase_date,year),
--            transactions.vertical,
--            transactions.license_platforms,
--            transactions.license_applications,
--            transactions.license_app_real_world_name,
--            transactions.purchase_platform,
--            transactions.current_subscription_store_country,
--            transactions.subscription_duration,
--            transactions.month0_late_or_renewal,
--            transactions.month0_year0_backlog,
--            transactions.payment_source,
--            transactions.had_trial,
--            transactions.is_trial_period,
--            transactions.trial_length,
--            transactions.is_paid,
--            transactions.is_sol
--            order by transactions.purchase_date rows between unbounded preceding and current row)     as ytd_rev,
--        sum(actual_proceeds) over(partition by
--            date_trunc(transactions.purchase_date,month),
--            transactions.vertical,
--            transactions.license_platforms,
--            transactions.license_applications,
--            transactions.license_app_real_world_name,
--            transactions.purchase_platform,
--            transactions.current_subscription_store_country,
--            transactions.subscription_duration,
--            transactions.month0_late_or_renewal,
--            transactions.month0_year0_backlog,
--            transactions.payment_source,
--            transactions.had_trial,
--            transactions.is_trial_period,
--            transactions.trial_length,
--            transactions.is_paid,
--            transactions.is_sol
--        order by purchase_date rows between unbounded preceding and current row) as mtd_rev
--from
--    transactions
--full join
--    refunds
--    on refunds.refund_date = transactions.purchase_date
--    and refunds.vertical = transactions.vertical
--    and refunds.platform = transactions.platform
--    and refunds.application = transactions.application
--    and refunds.app_real_world_name = transactions.app_real_world_name
--    and refunds.purchase_platform = transactions.purchase_platform
--    and refunds.current_subscription_store_country = transactions.current_subscription_store_country
--    and refunds.subscription_duration = transactions.subscription_duration
--    and refunds.month0_late_or_renewal = transactions.month0_late_or_renewal
--    and refunds.month0_year0_backlog = transactions.month0_year0_backlog
--    and refunds.payment_source = transactions.payment_source
--    and refunds.product_category = transactions.product_category
--    and refunds.had_trial = transactions.had_trial
--    and refunds.is_trial_period = transactions.is_trial_period
--    and refunds.trial_length = transactions.trial_length
--    and refunds.is_paid = transactions.is_paid
--    and refunds.is_paid_wopt = transactions.is_paid_wopt
--    and refunds.is_sol = transactions.is_sol

--Tests:
--
---- Check the refunds of billings_by_date agg table, the result should be 0, other result is a bug
--with had_trial_transactions as(
--    select
--           lt_subscription_id,
--           transaction.is_trial_period is_trial_period
--      from `ltx-dwh-prod-processed.subscriptions.unified_transactions`
--     where transaction.is_trial_period = true
--     group by 1,2
--)
--,billings_by_date_sample as
--(
--   select * from `ltx-dwh-prod-processed.agg.billings_by_date` tablesample system (0.000001 PERCENT)
--    where refunds > 0
--)
--,unified_transactions_records as
--(
--   select
--        date(a.payments.full_refund_date) as refund_date,
--        case
--            when a.license_applications in('facetune2, facetune2_android','facetune2_android','facetune2','facetune2_android_china','ftvideo','facetune')
--            then 'retouch/facetune'
--            when a.license_applications in('videoleap, videoleap_android', 'videoleap_android','videoleap')
--            then 'video'
--            when a.license_applications in('editor','photoleap, editor')
--            then 'photo'
--            when a.license_applications in('antares_web, antares, antares_android', 'quickart',
--                                           'lightwave','antares_android','antares_web','antares','venus','quickshot_android','quickshot','pixaloop_android','phoenix','vega','bambi')
--            then 'legacy'
--            else 'bug'
--        end as vertical,
--        case
--            when coalesce(a.license_app_real_world_name,a.license_applications) = 'facetune' then 'Facetune2 Web' -- we can't identify what kind of facetune app is it from web payments
--            when a.license_app_real_world_name is null and a.license_applications = 'photoleap, editor' and a.payments.purchase_platform = 'web' then 'Photoleap Web'
--            else coalesce(a.license_app_real_world_name,'bug')
--        end as license_app_real_world_name,
--        coalesce(a.license_applications,'bug') as license_applications,
--        coalesce(a.license_platforms,'bug') as license_platforms,
--        coalesce(a.subscription.current_subscription_store_country,'bug') as current_subscription_store_country,
--        case
--            when a.subscription.subscription_duration in('P1Y','P365D','1y') then '1y'
--            when a.subscription.subscription_duration in('P1M','P30D','1m') then '1m'
--            when a.subscription.subscription_duration = 'l' then 'otp'
--            else 'other'
--        end as subscription_duration,
--        case
--            when a.payments.month0_late_or_renewal = 'month0' or a.source_table = 'ios_assets_transactions_state'
--            then 'month0'
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) = date_trunc(date(a.purchase_date), year)
--            then concat('year0 - ', a.payments.month0_late_or_renewal)
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) <> date_trunc(date(a.purchase_date), year)
--            then concat('backlog - ', a.payments.month0_late_or_renewal)
--            else 'bug'
--        end as month0_year0_backlog,
--        case
--            when a.subscription.product_category = 'asset' then 'asset'
--            when a.griffin.griffin_app_name like '%.cn' then 'CN'
--            when a.source_table = 'payments' then 'web_payments'
--            else 'app_store'
--        end as payment_source,
--        case
--            when a.transaction.is_trial_period = true then true
--            else false
--        end as is_trial,
--        case
--            when had_trial_transactions.is_trial_period = true then true
--            else false
--        end as had_trial, -- if the subscriber had a subscription (by global_subscription_id) it will be true for all dates)
--        case
--            when a.subscription.trial_length in('7d','1w') then '1w'
--            when a.subscription.is_trial_product = false then 'non_trial_product'
--            else a.subscription.trial_length
--        end as trial_length,
--        date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) as attribution_year,
--        case when a.payments.gross_proceeds > 0 then true else false end as is_paid,
--        coalesce(subs.subscription.is_sol, false) as is_sol,
--        refunds,
--        count(*) as cnt_refunds
--    from billings_by_date_sample
--    join `ltx-dwh-prod-processed.subscriptions.unified_transactions` as a
--      on case
--            when a.license_applications in('facetune2, facetune2_android','facetune2_android','facetune2','facetune2_android_china','ftvideo','facetune')
--            then 'retouch/facetune'
--            when a.license_applications in('videoleap, videoleap_android', 'videoleap_android','videoleap')
--            then 'video'
--            when a.license_applications in('editor','photoleap, editor')
--            then 'photo'
--            when a.license_applications in('antares_web, antares, antares_android', 'quickart','lightwave','antares_android','antares_web',
--                 'antares','venus','quickshot_android','quickshot','pixaloop_android','phoenix','vega','bambi')
--            then 'legacy'
--            else 'bug'
--         end = billings_by_date_sample.vertical
--     and case
--            when coalesce(a.license_app_real_world_name,a.license_applications) = 'facetune' then 'Facetune2 Web' -- we can't identify what kind of facetune app is it from web payments
--            when a.license_app_real_world_name is null and a.license_applications = 'photoleap, editor' and a.payments.purchase_platform = 'web' then 'Photoleap Web'
--            else coalesce(a.license_app_real_world_name,'bug')
--        end = billings_by_date_sample.license_app_real_world_name
--        and a.license_applications = billings_by_date_sample.license_applications
--        and a.license_platforms = billings_by_date_sample.license_platforms
--        and a.subscription.current_subscription_store_country = billings_by_date_sample.current_subscription_store_country
--     and case
--            when a.subscription.subscription_duration in('P1Y','P365D','1y') then '1y'
--            when a.subscription.subscription_duration in('P1M','P30D','1m') then '1m'
--            when a.subscription.subscription_duration = 'l' then 'otp'
--            else 'other'
--        end  = billings_by_date_sample.subscription_duration
--     and case
--            when a.payments.month0_late_or_renewal = 'month0' or a.source_table = 'ios_assets_transactions_state'
--            then 'month0'
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) = date_trunc(date(a.purchase_date), year)
--            then concat('year0 - ', a.payments.month0_late_or_renewal)
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) <> date_trunc(date(a.purchase_date), year)
--            then concat('backlog - ', a.payments.month0_late_or_renewal)
--            else 'bug'
--        end = billings_by_date_sample.month0_year0_backlog
--     and case
--            when a.subscription.product_category = 'asset' then 'asset'
--            when a.griffin.griffin_app_name like '%.cn' then 'CN'
--            when a.source_table = 'payments' then 'web_payments'
--            else 'app_store'
--        end = billings_by_date_sample.payment_source
--     and  case
--            when a.transaction.is_trial_period = true then true
--            else false
--        end  = billings_by_date_sample.is_trial
--     and  case
--            when a.subscription.trial_length in('7d','1w') then '1w'
--            when a.subscription.is_trial_product = false then 'non_trial_product'
--            else a.subscription.trial_length
--        end = billings_by_date_sample.trial_length
--     and  date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year)  = date_trunc(billings_by_date_sample.attribution_year, year)
--     and (a.payments.gross_proceeds > 0 ) = billings_by_date_sample.is_paid
--     and date(a.payments.full_refund_date) = date(billings_by_date_sample.purchase_date)
--    left join had_trial_transactions
--         on had_trial_transactions.lt_subscription_id = a.lt_subscription_id
--    left join `ltx-dwh-prod-processed.subscriptions.unified_subscriptions` as subs
--         on subs.lt_subscription_id = a.lt_subscription_id
--   where coalesce(subs.subscription.is_sol, false) = billings_by_date_sample.is_sol
--     and case
--          when had_trial_transactions.is_trial_period = true then true
--            else false
--          end = billings_by_date_sample.had_trial
--    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
--)
--select sum(cnt_refunds-refunds)
--  from unified_transactions_records;
--
--
--
---- Check the transactions, actual_proceeds and gross_proceeds of billings_by_date agg table, the result shoulld be 0, other result is a bug
--with had_trial_transactions as(
--    select
--           lt_subscription_id,
--           transaction.is_trial_period is_trial_period
--      from `ltx-dwh-prod-processed.subscriptions.unified_transactions`
--     where transaction.is_trial_period = true
--     group by 1,2
--)
--,billings_by_date_sample as
--(
--   select * from `ltx-dwh-prod-processed.agg.billings_by_date` tablesample system (0.000001 PERCENT)
--    where coalesce(transactions,0) > 0
--)
--,unified_transactions_records as
--(
--   select
--        date(a.purchase_date) as purchase_date,
--        case
--            when a.license_applications in('facetune2, facetune2_android','facetune2_android','facetune2','facetune2_android_china','ftvideo','facetune')
--            then 'retouch/facetune'
--            when a.license_applications in('videoleap, videoleap_android', 'videoleap_android','videoleap')
--            then 'video'
--            when a.license_applications in('editor','photoleap, editor')
--            then 'photo'
--            when a.license_applications in('antares_web, antares, antares_android', 'quickart','lightwave','antares_android',
--                                           'antares_web','antares','venus','quickshot_android','quickshot','pixaloop_android','phoenix','vega','bambi')
--            then 'legacy'
--            else 'bug'
--        end as vertical,
--        case
--            when coalesce(a.license_app_real_world_name,a.license_applications) = 'facetune' then 'Facetune2 Web' -- we can't identify what kind of facetune app is it from web payments
--            when a.license_app_real_world_name is null and a.license_applications = 'photoleap, editor' and a.payments.purchase_platform = 'web' then 'Photoleap Web'
--            else coalesce(a.license_app_real_world_name,'bug')
--        end as license_app_real_world_name,
--        coalesce(a.license_applications,'bug') as license_applications,
--        coalesce(a.license_platforms,'bug') as license_platforms,
--        coalesce(a.subscription.current_subscription_store_country,'bug') as app_store_country,
--        case
--            when a.subscription.subscription_duration in('P1Y','P365D','1y') then '1y'
--            when a.subscription.subscription_duration in('P1M','P30D','1m') then '1m'
--            when a.subscription.subscription_duration = 'l' then 'otp'
--            else 'other'
--        end as subscription_duration,
--        case
--            when a.payments.month0_late_or_renewal = 'month0' or a.source_table = 'ios_assets_transactions_state'
--            then 'month0'
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) = date_trunc(date(a.purchase_date), year)
--            then concat('year0 - ', a.payments.month0_late_or_renewal)
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) <> date_trunc(date(a.purchase_date), year)
--            then concat('backlog - ', a.payments.month0_late_or_renewal)
--            else 'bug'
--        end as month0_year0_backlog,
--        case
--            when a.subscription.product_category = 'asset' then 'asset'
--            when a.griffin.griffin_app_name like '%.cn' then 'CN'
--            when a.source_table = 'payments' then 'web_payments'
--            else 'app_store'
--        end as payment_source,
--        case
--            when a.transaction.is_trial_period = true then true
--            else false
--        end as is_trial,
--        case
--            when had_trial_transactions.is_trial_period = true then true
--            else false
--        end as had_trial, -- if the subscriber had a subscription (by global_subscription_id) it will be true for all dates)
--        case
--            when a.subscription.trial_length in('7d','1w') then '1w'
--            when a.subscription.is_trial_product = false then 'non_trial_product'
--            else a.subscription.trial_length
--        end as trial_length,
--        date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) as attribution_year,
--        case when a.payments.gross_proceeds > 0 then true else false end as is_paid,
--        coalesce(subs.subscription.is_sol, false) as is_sol,
--        actual_proceeds,
--        gross_proceeds,
--        transactions,
--        sum(a.payments.actual_proceeds) as actual_proceeds_sum,
--        sum(a.payments.gross_proceeds) as gross_proceeds_sum,
--        count(distinct a.lt_transaction_id) as transactions_cnt
--     from billings_by_date_sample
--     join `ltx-dwh-prod-processed.subscriptions.unified_transactions` as a
--       on case
--            when a.license_applications in('facetune2, facetune2_android','facetune2_android','facetune2','facetune2_android_china','ftvideo','facetune')
--            then 'retouch/facetune'
--            when a.license_applications in('videoleap, videoleap_android', 'videoleap_android','videoleap')
--            then 'video'
--            when a.license_applications in('editor','photoleap, editor')
--            then 'photo'
--            when a.license_applications in('antares_web, antares, antares_android', 'quickart','lightwave','antares_android','antares_web',
--                                           'antares','venus','quickshot_android','quickshot','pixaloop_android','phoenix','vega','bambi')
--            then 'legacy'
--            else 'bug'
--        end = billings_by_date_sample.vertical
--     and case
--            when coalesce(a.license_app_real_world_name,a.license_applications) = 'facetune' then 'Facetune2 Web' -- we can't identify what kind of facetune app is it from web payments
--            when a.license_app_real_world_name is null and a.license_applications = 'photoleap, editor' and a.payments.purchase_platform = 'web' then 'Photoleap Web'
--            else coalesce(a.license_app_real_world_name,'bug')
--         end = billings_by_date_sample.license_app_real_world_name
--     and a.license_applications = billings_by_date_sample.license_applications
--     and a.license_platforms = billings_by_date_sample.license_platforms
--     and a.subscription.current_subscription_store_country = billings_by_date_sample.current_subscription_store_country
--     and case
--            when a.subscription.subscription_duration in('P1Y','P365D','1y') then '1y'
--            when a.subscription.subscription_duration in('P1M','P30D','1m') then '1m'
--            when a.subscription.subscription_duration = 'l' then 'otp'
--            else 'other'
--        end  = billings_by_date_sample.subscription_duration
--     and case
--            when a.payments.month0_late_or_renewal = 'month0' or a.source_table = 'ios_assets_transactions_state'
--            then 'month0'
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) = date_trunc(date(a.purchase_date), year)
--            then concat('year0 - ', a.payments.month0_late_or_renewal)
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) <> date_trunc(date(a.purchase_date), year)
--            then concat('backlog - ', a.payments.month0_late_or_renewal)
--            else 'bug'
--        end = billings_by_date_sample.month0_year0_backlog
--     and case
--            when a.subscription.product_category = 'asset' then 'asset'
--            when a.griffin.griffin_app_name like '%.cn' then 'CN'
--            when a.source_table = 'payments' then 'web_payments'
--            else 'app_store'
--        end = billings_by_date_sample.payment_source
--     and case
--            when a.transaction.is_trial_period = true then true
--            else false
--        end  = billings_by_date_sample.is_trial
--     and case
--            when a.subscription.trial_length in('7d','1w') then '1w'
--            when a.subscription.is_trial_product = false then 'non_trial_product'
--            else a.subscription.trial_length
--        end = billings_by_date_sample.trial_length
--     and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year)  = date_trunc(billings_by_date_sample.attribution_year, year)
--     and (a.payments.gross_proceeds > 0 ) = billings_by_date_sample.is_paid
--     and date(a.purchase_date) = date(billings_by_date_sample.purchase_date)
--    left join had_trial_transactions
--         on had_trial_transactions.lt_subscription_id = a.lt_subscription_id
--    left join `ltx-dwh-prod-processed.subscriptions.unified_subscriptions` as subs
--         on subs.lt_subscription_id = a.lt_subscription_id
--   where coalesce(subs.subscription.is_sol, false) = billings_by_date_sample.is_sol
--     and case
--            when had_trial_transactions.is_trial_period = true then true
--            else false
--        end = billings_by_date_sample.had_trial
--   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
--)
--select sum( round(actual_proceeds_sum, 2)-round(actual_proceeds, 2)+round(gross_proceeds_sum, 2)-round(gross_proceeds, 2)+transactions_cnt-transactions)
--  from unified_transactions_records;
--
--
---- Check the ytd_rev of billings_by_date agg table, the result shoulld be 0, other result is a bug
--with had_trial_transactions as(
--    select
--           lt_subscription_id,
--           transaction.is_trial_period is_trial_period
--      from `ltx-dwh-prod-processed.subscriptions.unified_transactions`
--     where transaction.is_trial_period = true
--     group by 1,2
--)
--,billings_by_date_sample as
--(
--   select * from `ltx-dwh-prod-processed.agg.billings_by_date` tablesample system (0.000001 PERCENT)
--    where coalesce(ytd_rev,0) > 0
--)
--,unified_transactions_records as
--(
--   select
--        date_trunc(a.purchase_date, year) as purchase_year,
--        case
--            when a.license_applications in('facetune2, facetune2_android','facetune2_android','facetune2','facetune2_android_china','ftvideo','facetune')
--            then 'retouch/facetune'
--            when a.license_applications in('videoleap, videoleap_android', 'videoleap_android','videoleap')
--            then 'video'
--            when a.license_applications in('editor','photoleap, editor')
--            then 'photo'
--            when a.license_applications in('antares_web, antares, antares_android', 'quickart','lightwave',
--                                           'antares_android','antares_web','antares','venus','quickshot_android','quickshot','pixaloop_android','phoenix','vega','bambi')
--            then 'legacy'
--            else 'bug'
--        end as vertical,
--        case
--            when coalesce(a.license_app_real_world_name,a.license_applications) = 'facetune' then 'Facetune2 Web' -- we can't identify what kind of facetune app is it from web payments
--            when a.license_app_real_world_name is null and a.license_applications = 'photoleap, editor' and a.payments.purchase_platform = 'web' then 'Photoleap Web'
--            else coalesce(a.license_app_real_world_name,'bug')
--        end as license_app_real_world_name,
--        coalesce(a.license_applications,'bug') as license_applications,
--        coalesce(a.license_platforms,'bug') as license_platforms,
--        coalesce(a.subscription.current_subscription_store_country,'bug') as app_store_country,
--        case
--            when a.subscription.subscription_duration in('P1Y','P365D','1y') then '1y'
--            when a.subscription.subscription_duration in('P1M','P30D','1m') then '1m'
--            when a.subscription.subscription_duration = 'l' then 'otp'
--            else 'other'
--        end as subscription_duration,
--        case
--            when a.payments.month0_late_or_renewal = 'month0' or a.source_table = 'ios_assets_transactions_state'
--            then 'month0'
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) = date_trunc(date(a.purchase_date), year)
--            then concat('year0 - ', a.payments.month0_late_or_renewal)
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) <> date_trunc(date(a.purchase_date), year)
--            then concat('backlog - ', a.payments.month0_late_or_renewal)
--            else 'bug'
--        end as month0_year0_backlog,
--        case
--            when a.subscription.product_category = 'asset' then 'asset'
--            when a.griffin.griffin_app_name like '%.cn' then 'CN'
--            when a.source_table = 'payments' then 'web_payments'
--            else 'app_store'
--        end as payment_source,
--        case
--            when a.transaction.is_trial_period = true then true
--            else false
--        end as is_trial,
--        case
--            when had_trial_transactions.is_trial_period = true then true
--            else false
--        end as had_trial, -- if the subscriber had a subscription (by global_subscription_id) it will be true for all dates)
--        case
--            when a.subscription.trial_length in('7d','1w') then '1w'
--            when a.subscription.is_trial_product = false then 'non_trial_product'
--            else a.subscription.trial_length
--        end as trial_length,
--        date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) as attribution_year,
--        case when a.payments.gross_proceeds > 0 then true else false end as is_paid,
--        coalesce(subs.subscription.is_sol, false) as is_sol,
--        ytd_rev,
--        sum(a.payments.actual_proceeds) as ytd_rev_calc
--     from billings_by_date_sample
--     join `ltx-dwh-prod-processed.subscriptions.unified_transactions` as a
--       on case
--            when a.license_applications in('facetune2, facetune2_android','facetune2_android','facetune2','facetune2_android_china','ftvideo','facetune')
--            then 'retouch/facetune'
--            when a.license_applications in('videoleap, videoleap_android', 'videoleap_android','videoleap')
--            then 'video'
--            when a.license_applications in('editor','photoleap, editor')
--            then 'photo'
--            when a.license_applications in('antares_web, antares, antares_android', 'quickart','lightwave','antares_android',
--                                           'antares_web','antares','venus','quickshot_android','quickshot','pixaloop_android','phoenix','vega','bambi')
--            then 'legacy'
--            else 'bug'
--        end = billings_by_date_sample.vertical
--     and case
--            when coalesce(a.license_app_real_world_name,a.license_applications) = 'facetune' then 'Facetune2 Web' -- we can't identify what kind of facetune app is it from web payments
--            when a.license_app_real_world_name is null and a.license_applications = 'photoleap, editor' and a.payments.purchase_platform = 'web' then 'Photoleap Web'
--            else coalesce(a.license_app_real_world_name,'bug')
--        end = billings_by_date_sample.license_app_real_world_name
--        and a.license_applications = billings_by_date_sample.license_applications
--        and a.license_platforms = billings_by_date_sample.license_platforms
--        and a.subscription.current_subscription_store_country = billings_by_date_sample.current_subscription_store_country
--     and case
--            when a.subscription.subscription_duration in('P1Y','P365D','1y') then '1y'
--            when a.subscription.subscription_duration in('P1M','P30D','1m') then '1m'
--            when a.subscription.subscription_duration = 'l' then 'otp'
--            else 'other'
--        end  = billings_by_date_sample.subscription_duration
--     and case
--            when a.payments.month0_late_or_renewal = 'month0' or a.source_table = 'ios_assets_transactions_state'
--            then 'month0'
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) = date_trunc(date(a.purchase_date), year)
--            then concat('year0 - ', a.payments.month0_late_or_renewal)
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) <> date_trunc(date(a.purchase_date), year)
--            then concat('backlog - ', a.payments.month0_late_or_renewal)
--            else 'bug'
--        end = billings_by_date_sample.month0_year0_backlog
--     and case
--            when a.subscription.product_category = 'asset' then 'asset'
--            when a.griffin.griffin_app_name like '%.cn' then 'CN'
--            when a.source_table = 'payments' then 'web_payments'
--            else 'app_store'
--        end = billings_by_date_sample.payment_source
--     and  case
--            when a.transaction.is_trial_period = true then true
--            else false
--        end  = billings_by_date_sample.is_trial
--     and case
--            when a.subscription.trial_length in('7d','1w') then '1w'
--            when a.subscription.is_trial_product = false then 'non_trial_product'
--            else a.subscription.trial_length
--        end = billings_by_date_sample.trial_length
--     and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year)  = date_trunc(billings_by_date_sample.attribution_year, year)
--     and (a.payments.gross_proceeds > 0 ) = billings_by_date_sample.is_paid
--     and date_trunc(date(a.purchase_date), year) = date_trunc(date(billings_by_date_sample.purchase_date), year)
--     and date(a.purchase_date)<= date(billings_by_date_sample.purchase_date)
--    left join had_trial_transactions
--          on had_trial_transactions.lt_subscription_id = a.lt_subscription_id
--    left join `ltx-dwh-prod-processed.subscriptions.unified_subscriptions` as subs
--        on subs.lt_subscription_id = a.lt_subscription_id
--   where coalesce(subs.subscription.is_sol, false) = billings_by_date_sample.is_sol
--     and case
--            when had_trial_transactions.is_trial_period = true then true
--            else false
--        end = billings_by_date_sample.had_trial
--   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
--)
--select sum(round(ytd_rev_calc,2)-round(ytd_rev,2))
--  from unified_transactions_records;
--
--
---- Check the mtd_rev of billings_by_date agg table, the result shoulld be 0, other result is a bug
--with had_trial_transactions as(
--    select
--           lt_subscription_id,
--           transaction.is_trial_period is_trial_period
--      from `ltx-dwh-prod-processed.subscriptions.unified_transactions`
--     where transaction.is_trial_period = true
--     group by 1,2
--)
--,billings_by_date_sample as
--(
--   select * from `ltx-dwh-prod-processed.agg.billings_by_date` tablesample system (0.000001 PERCENT)
--    where coalesce(mtd_rev,0) > 0
--)
--,unified_transactions_records as
--(
--   select
--        date_trunc(a.purchase_date, month) as purchase_month,
--        case
--            when a.license_applications in('facetune2, facetune2_android','facetune2_android','facetune2','facetune2_android_china','ftvideo','facetune')
--            then 'retouch/facetune'
--            when a.license_applications in('videoleap, videoleap_android', 'videoleap_android','videoleap')
--            then 'video'
--            when a.license_applications in('editor','photoleap, editor')
--            then 'photo'
--            when a.license_applications in('antares_web, antares, antares_android', 'quickart','lightwave','antares_android',
--                                           'antares_web','antares','venus','quickshot_android','quickshot','pixaloop_android','phoenix','vega','bambi')
--            then 'legacy'
--            else 'bug'
--        end as vertical,
--        case
--            when coalesce(a.license_app_real_world_name,a.license_applications) = 'facetune' then 'Facetune2 Web' -- we can't identify what kind of facetune app is it from web payments
--            when a.license_app_real_world_name is null and a.license_applications = 'photoleap, editor' and a.payments.purchase_platform = 'web' then 'Photoleap Web'
--            else coalesce(a.license_app_real_world_name,'bug')
--        end as license_app_real_world_name,
--        coalesce(a.license_applications,'bug') as license_applications,
--        coalesce(a.license_platforms,'bug') as license_platforms,
--        coalesce(a.subscription.current_subscription_store_country,'bug') as app_store_country,
--        case
--            when a.subscription.subscription_duration in('P1Y','P365D','1y') then '1y'
--            when a.subscription.subscription_duration in('P1M','P30D','1m') then '1m'
--            when a.subscription.subscription_duration = 'l' then 'otp'
--            else 'other'
--        end as subscription_duration,
--        case
--            when a.payments.month0_late_or_renewal = 'month0' or a.source_table = 'ios_assets_transactions_state'
--            then 'month0'
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) = date_trunc(date(a.purchase_date), year)
--            then concat('year0 - ', a.payments.month0_late_or_renewal)
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) <> date_trunc(date(a.purchase_date), year)
--            then concat('backlog - ', a.payments.month0_late_or_renewal)
--            else 'bug'
--        end as month0_year0_backlog,
--        case
--            when a.subscription.product_category = 'asset' then 'asset'
--            when a.griffin.griffin_app_name like '%.cn' then 'CN'
--            when a.source_table = 'payments' then 'web_payments'
--            else 'app_store'
--        end as payment_source,
--        case
--            when a.transaction.is_trial_period = true then true
--            else false
--        end as is_trial,
--        case
--            when had_trial_transactions.is_trial_period = true then true
--            else false
--        end as had_trial, -- if the subscriber had a subscription (by global_subscription_id) it will be true for all dates)
--        case
--            when a.subscription.trial_length in('7d','1w') then '1w'
--            when a.subscription.is_trial_product = false then 'non_trial_product'
--            else a.subscription.trial_length
--        end as trial_length,
--        date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) as attribution_year,
--        case when a.payments.gross_proceeds > 0 then true else false end as is_paid,
--        coalesce(subs.subscription.is_sol, false) as is_sol,
--        mtd_rev,
--        sum(a.payments.actual_proceeds) as mtd_rev_calc
--   from billings_by_date_sample
--   join `ltx-dwh-prod-processed.subscriptions.unified_transactions` as a
--     on case
--            when a.license_applications in('facetune2, facetune2_android','facetune2_android','facetune2','facetune2_android_china','ftvideo','facetune')
--            then 'retouch/facetune'
--            when a.license_applications in('videoleap, videoleap_android', 'videoleap_android','videoleap')
--            then 'video'
--            when a.license_applications in('editor','photoleap, editor')
--            then 'photo'
--            when a.license_applications in('antares_web, antares, antares_android', 'quickart','lightwave','antares_android',
--                                           'antares_web','antares','venus','quickshot_android','quickshot','pixaloop_android','phoenix','vega','bambi')
--            then 'legacy'
--            else 'bug'
--        end = billings_by_date_sample.vertical
--    and case
--            when coalesce(a.license_app_real_world_name,a.license_applications) = 'facetune' then 'Facetune2 Web' -- we can't identify what kind of facetune app is it from web payments
--            when a.license_app_real_world_name is null and a.license_applications = 'photoleap, editor' and a.payments.purchase_platform = 'web' then 'Photoleap Web'
--            else coalesce(a.license_app_real_world_name,'bug')
--        end = billings_by_date_sample.license_app_real_world_name
--    and a.license_applications = billings_by_date_sample.license_applications
--    and a.license_platforms = billings_by_date_sample.license_platforms
--    and a.subscription.current_subscription_store_country = billings_by_date_sample.current_subscription_store_country
--    and case
--            when a.subscription.subscription_duration in('P1Y','P365D','1y') then '1y'
--            when a.subscription.subscription_duration in('P1M','P30D','1m') then '1m'
--            when a.subscription.subscription_duration = 'l' then 'otp'
--            else 'other'
--        end  = billings_by_date_sample.subscription_duration
--    and case
--            when a.payments.month0_late_or_renewal = 'month0' or a.source_table = 'ios_assets_transactions_state'
--            then 'month0'
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) = date_trunc(date(a.purchase_date), year)
--            then concat('year0 - ', a.payments.month0_late_or_renewal)
--            when a.payments.month0_late_or_renewal in ('late','renewal') and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year) <> date_trunc(date(a.purchase_date), year)
--            then concat('backlog - ', a.payments.month0_late_or_renewal)
--            else 'bug'
--        end = billings_by_date_sample.month0_year0_backlog
--    and case
--            when a.subscription.product_category = 'asset' then 'asset'
--            when a.griffin.griffin_app_name like '%.cn' then 'CN'
--            when a.source_table = 'payments' then 'web_payments'
--            else 'app_store'
--        end = billings_by_date_sample.payment_source
--    and case
--            when a.transaction.is_trial_period = true then true
--            else false
--        end  = billings_by_date_sample.is_trial
--    and case
--            when a.subscription.trial_length in('7d','1w') then '1w'
--            when a.subscription.is_trial_product = false then 'non_trial_product'
--            else a.subscription.trial_length
--        end = billings_by_date_sample.trial_length
--    and date_trunc(date(coalesce(a.attribution.attribution_timestamp, a.device.install_time, a.original_purchase_date)),year)  = date_trunc(billings_by_date_sample.attribution_year, year)
--    and (a.payments.gross_proceeds > 0 ) = billings_by_date_sample.is_paid
--    and date_trunc(date(a.purchase_date), month) = date_trunc(date(billings_by_date_sample.purchase_date), month)
--    and date(a.purchase_date)<= date(billings_by_date_sample.purchase_date)
--   left join had_trial_transactions
--     on had_trial_transactions.lt_subscription_id = a.lt_subscription_id
--   left join `ltx-dwh-prod-processed.subscriptions.unified_subscriptions` as subs
--     on subs.lt_subscription_id = a.lt_subscription_id
--  where coalesce(subs.subscription.is_sol, false) = billings_by_date_sample.is_sol
--    and case
--            when had_trial_transactions.is_trial_period = true then true
--            else false
--        end = billings_by_date_sample.had_trial
--  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
--)
--select sum(round(mtd_rev_calc,2)-round(mtd_rev,2))
--  from unified_transactions_records;


  """


# print(extract_active_sources_refs(clean_sql, remove_commented=True))
print(clean_sql)