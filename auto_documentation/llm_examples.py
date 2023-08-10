EXAMPLES = [
    {'input':
        {
            'table': 'unified_subscriptions_revenue_allocation',
            'sql_code':
                (""" sql

select

      -- METADATA --
       s.source_table,
        -- The coalesce is for quickshot_android_china that isn't in ulei table
       coalesce(lap_u.vertical, lap_s.vertical) as vertical,
       coalesce(lap_u.application_array, lap_s.application_array) as application,
       coalesce(lap_u.app_real_world_name_array, lap_s.app_real_world_name_array) as app_real_world_name,
       coalesce(lap_u.license_platforms, lap_s.license_platforms) as platform,
       case when lap_s.is_basic_application = true then false else true end as is_multi_app_license,
       case when (lap_s.is_basic_application = false) and
                 -- For MAS without usage there is no first usage record and in ulei table will be one default record
                 -- that will be the winning
                 (lap_u.application_array = imfu.winning_app_name or imfu.winning_app_name is null) then true -- Winning app for MAS
            when (lap_s.is_basic_application = true) then true  -- Basic app will be the winning
            when (lap_s.is_basic_application = false) and (lap_u.application_array != imfu.winning_app_name) then false -- Not winning app for MAS
       end as is_winning_app,
       case when (lap_s.is_basic_application = false) then s.license_applications end as multi_app_license,
       s.lt_subscription_id,            -- replace global_subscription_id
       s.original_purchase_date,

       -- SUBSCRIPTION --
       s.subscription,

       -- PAYMENTS --
       case when (lap_s.is_basic_application = true) -- basic app
              or (lap_s.is_basic_application = false) and (lap_u.application_array = imfu.winning_app_name or imfu.winning_app_name is null) then -- first usage app for mas
             STRUCT(
                     -- Common columns for first usage and non first usage records
                      s.payments.purchase_platform,
                      s.payments.revenue_so_far as revenue_so_far_alloc,
                      s.payments.refund_date,
                      s.payments.last_dispute_open_date,
                      s.payments.next_expected_proceeds_usd,
                      s.payments.next_potential_proceeds_usd)
            else -- not first usage app for mas
            STRUCT(
                      -- Common columns for first usage and non first usage records
                      s.payments.purchase_platform,
                      s.payments.vendor,
                      s.payments.payment_origin,
                      s.payments.month0_late,
                      s.payments.current_is_in_billing_retry_period,
                      s.payments.had_intro_price)
            end as payments,

       -- GRIFFIN --
       s.griffin,

        -- LOGIN --
       s.login,

       -- ULEI_ATTRIBUTION --
       STRUCT(u.attribution_user_id,
              u.attribution_user_group_number,
              coalesce(u.attribution_timestamp, s.original_purchase_date) as attribution_timestamp,
              u.next_attribution_user_group_timestamp
               )  as attribution,




       -- PROJECTED --
       case when (lap_s.is_basic_application = true) -- basic app
              or (lap_s.is_basic_application = false) and (lap_u.application_array = imfu.winning_app_name or imfu.winning_app_name is null) then -- first usage app for mas
                    STRUCT(
--                           s.projected.sum_net_proceeds_incl_projected_trials as sum_net_proceeds_incl_projected_trials, -- TODO: remove when everyone will change to revenue_so_far_alloc
                          s.projected.adjusted_revenue_so_far as adjusted_revenue_so_far_alloc,
                          s.projected.max_adjusted_pct_to_pay
                     )
               else -- not first usage app for mas
                   STRUCT(
--                           0 as sum_net_proceeds_incl_projected_trials, -- TODO: remove when everyone will change to revenue_so_far_alloc
                          0 as adjusted_revenue_so_far_alloc,
                          s.projected.max_adjusted_pct_to_pay
                     )
               end as projected,

       -- CROSS SALE --
       STRUCT(s.cross_sale.cross_sale_lt_subscription_id,
              s.cross_sale.griffin_cross_sale_vendor,
              s.cross_sale.griffin_cross_sale_payment_source_id) as cross_sale

from
   {{ref('unified_subscriptions')}} s
 --`ltx-dwh-prod-processed.subscriptions.unified_subscriptions` s
left join
    {{ref('license_applications_properties')}} lap_s
    --`ltx-dwh-prod-processed.dimensions.license_applications_properties` ap
    on s.license_applications = lap_s.application_array
left join
    {{ref('ulei_att_user_groups_and_subs')}} u
    --`ltx-dwh-prod-processed.ulei.ulei_att_user_groups_and_subs` u
        on s.lt_subscription_id = u.lt_subscription_id
left join
          {{ ref('inter_mas_first_usage') }} imfu
          -- `ltx-dwh-prod-processed.subscriptions.inter_mas_first_usage` imfu
        on s.lt_subscription_id = imfu.lt_subscription_id
left join {{ref('license_applications_properties')}} lap_u
--left join `ltx-dwh-prod-processed.dimensions.license_applications_properties` lap
  on u.application = lap_u.application_array
"""),
            "dependencies": [
                {"source": "unified_subscriptions",
                 # "documentation": """ """
                 },
                {"source": "license_applications_properties",
                 # "documentation": """ """
                 },
                {"source": "ulei_att_user_groups_and_subs",
                 # "documentation": """ """
                 },
                {"source": "inter_mas_first_usage",
                 # "documentation": """ """
                },
            ]
        },
        "output":
            (
"""
- name: unified_subscriptions_revenue_allocation
  description: 'subscriptions related data from all sources: iOS/Android/Griffin.The
    table granularity is by lt_subscription_id and application.The table source is
    `unified_subscriptions`  + plus splitting web-payments subscriptions into each
    of the licensed applications to create a row for each lt_subscription_id and licensed
    application.In addition to creating a row for each licensed app, the revenue generated
    from the subscription is allocated for each licensed application according to
    the agreed business logic.The current business logic is to allocate the revenue
    to the first used application.In this table the cross sell manipulation is not
    applied.For all regular product analysis, and all KPIs except the performance
    marketing KPIs (ROAS,CAC, CPI, etc.)Use this table.For revenue and ARPU calculations
    you should also consider creating the calculation from`Unified_subscriptions_revenue_allocation_cross_sell`
    Corresponding old table/s:Unified_subscriptions_stateUnified_subscriptions_state_with_griffinagg.subscriptions'
  columns:
  - name: source_table
    description: 'The name of the table that the data came from.Potential values:
      "payment_sources"/"android_subscriptions_state"/"ios_subscriptions_state"'
  - name: vertical
    description: The application groups by activity e.g. retouch/facetune:(facetune2_android,
      facetune2, facetune2_android_china, ftvideo, facetune, facetune2 china), video
      :(videoleap_android, videoleap), photo :(editor, photoleap, editor), legacy
      :(quickart, lightwave, antares_android, Boosted Web, antares, venus, quickshot_android,
      quickshot, pixaloop_android, phoenix, vega, bambi, Seen).
  - name: application
    description: The internal Lightricks name for the event producer e.g. facetune2,
      facetune2_android.
  - name: app_real_world_name
    description: The external/real world Lightricks name for the app e.g. Facetune2
      iOS, Photoleap iOS, Lightleap iOS.
  - name: platform
    description: The type of platform the event producer or app is running on e.g.
      ios, android.
  - name: is_multi_app_license
    description: Indicate of this license is allowed for multiple applications or
      multiple platforms
  - name: is_winning_app
    description: 'The application that was used first in this license '
  - name: multi_app_license
    description: The licenses of the subscription (for multi app licenses only).
  - name: lt_subscription_id
    description: A unique key per subscription, being generated with farm_fingerprint(application
      || original_transaction_id/ purchase_token/payment_source_id)
  - name: original_purchase_date
    description: The date the subscription started
  - name: subscription.payment_source_id
    description: The identifier of the subscription as we get it from griffin
  - name: payments.purchase_platform
    description: The type of platform that the purchase was made from. e.g. ios, android,
      web.
  - name: payments.vendor
    description: The vendor that this payment source belongs to. This can consist
      of one of the following values adyencc, appstore, stripe, paypal, wechat.
  - name: payments.payment_origin
    description: ''
  - name: payments.month0_late
    description: Subscriptions segmentation of the payment based on the attribution
      date. month0 - the attribution month and the original purchase month are equal.
      year0 - late - the attribution month was before the original purchase month
      but in the same year.backlog - late - the attribution year was before the original
      purchase year.
  - name: payments.current_is_in_billing_retry_period
    description: "If an attempt for billing was made and failed the subscription in\
      \ in retry period, this indicate if it\u2019s during that period or not"
  - name: payments.had_intro_price
    description: Indicate if this subscription received intro price or not
  - name: payments.revenue_so_far
    description: The revenue that we received from this subscription after the vendors
      cut (similar to actual_proceeds in unified_transaction)
  - name: payments.revenue_so_far_alloc
    description: ''
  - name: payments.refund_date
    description: The date of the refund. In case of multiple refunds, it will include
      the first refund date
  - name: payments.last_dispute_open_date
    description: The date of the last dispute
  - name: payments.next_expected_proceeds_usd
    description: The value we expect to get from this subscription in the next transaction.
      If the auto renew off the value will be 0.
  - name: payments.next_potential_proceeds_usd
    description: The value we expect to get from this subscription in the next transaction
      regardless the auto renew status
  - name: attribution.attribution_user_id
    description: A user id concatinated with the group number according to ulei model
      (user is identified for each application, and is based on lt_id or icloud, and
      should represent a real person)
  - name: attribution.attribution_user_group_number
    description: The number of install that is accountant as new install (no usage
      30 days prior to the install and no active subscription)
  - name: attribution.attribution_timestamp
    description: The time of the install/ subscription creation of the attribution
      user
  - name: attribution.next_attribution_user_group_timestamp
    description: The start time of the next group of the attribution user group. Indicated
      when this group is not active anymore
  - name: projected.sum_net_proceeds_incl_projected_trials
    description: ''
  - name: projected.adjusted_revenue_so_far_alloc
    description: ''
  - name: projected.max_adjusted_pct_to_pay
    description: This field indicated the percentage to convert from trial to paid
      subscription, during trial. After the trial, this field is either 0 (not coverted)
      or 1 (converted)
  - name: cross_sale.cross_sale_lt_subscription_id
    description: The lt_subscription_id of the existing subscription that caused this
      additional subscription. Currently has values only for griffin.
  - name: cross_sale.griffin_cross_sale_vendor
    description: The vendor of the existing subscription that caused this additional
      subscription. Currently has values only for griffin.
  - name: cross_sale.griffin_cross_sale_payment_source_id
    description: The payment_source_id of the existing subscription that caused this
      additional subscription. Currently has values only for griffin.
      """)
    }
]
