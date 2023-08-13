import re
import streamlit as st

from .auto_documentation.doc_utils import extract_active_sources_refs

sql = """
{% set partitions_to_replace =
    [
      'date(current_date)',
      'date(date_sub(current_date, interval 1 day))',
      'date(date_sub(current_date, interval 2 day))'
    ]
%}
{% set event_time_interval_filter = 'interval -401 day' %} -- one time backfill to Jul 1st 2022, later it's incremental
{{
    config(
            materialized='incremental',
            incremental_strategy = 'insert_overwrite',
            partitions = partitions_to_replace,
            schema='app',
            partition_by={  "field": "editing_session_id_date","data_type": "date"},
            cluster_by=['id_for_vendor', 'editing_session_id', 'feature_usage_id'],
            labels = {'ft2': 'usage_funnel'}
           )
 }}
/*
dbt run --target stg --models ft2_uf_feature_started
dbt compile --target stg --models ft2_uf_feature_started
dbt run --full-refresh --target stg --models ft2_uf_feature_started
dbt compile --full-refresh --target stg --models ft2_uf_feature_started
and date(fs.device_timestamp) in (date(current_date),date(date_sub(current_date, interval 1 day)),date(date_sub(current_date, interval 2 day)))
*/
WITH devices as (
    select
        id_for_vendor,
        date(first_device_timestamp) as device_first_launch,
    from
        {{source('devices','unified_devices'   )   }}
    where true
    and application = 'facetune2'
    and date(first_device_timestamp) < current_date()
)
, unioned_features as (
     select
        fs.id_for_vendor,
        fs.editing_session_id,
        fs.usage_id as feature_usage_id,
        'feature' as feature_type,
        fs.feature_name,
        fs.device_timestamp as feature_started_ts
      from
        {{ source( 'analytics', 'ios_facetune2_feature_started' ) }} as fs
      where true
        and date(fs._PARTITIONTIME) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and date(fs.meta_received_at) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and date(fs.device_timestamp) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and fs.device_timestamp < CURRENT_TIMESTAMP()
    {% if is_incremental() %}
       and date(device_timestamp) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
union all
      select
        ud.id_for_vendor,
        ud.editing_session_id,
        ud.feature_usage_id,
        'undo' as feature_type,
        'undo' as feature_name,
        ud.device_timestamp as feature_started_ts
      from
        {{ source( 'analytics', 'ios_facetune2_undo_pressed' ) }} as ud
      where true
        and date(ud._PARTITIONTIME) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and date(ud.meta_received_at) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and date(ud.device_timestamp) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and ud.device_timestamp < CURRENT_TIMESTAMP()
        and ud.feature_usage_id is null -- distinguish "undo feature" from "undo sub feature"
    {% if is_incremental() %}
       and date(device_timestamp) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
union all
      select
        rd.id_for_vendor,
        rd.editing_session_id,
        rd.feature_usage_id,
        'redo' as feature_type,
        'redo' as feature_name,
        rd.device_timestamp as feature_started_ts
      from
        {{ source('analytics', 'ios_facetune2_redo_pressed' )}} as rd
      where true
        and date(rd._PARTITIONTIME) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and date(rd.meta_received_at) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and date(rd.device_timestamp) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and rd.device_timestamp < CURRENT_TIMESTAMP()
        and rd.feature_usage_id is null -- distinguish "redo feature" from "redo sub feature"
    {% if is_incremental() %}
       and date(device_timestamp) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
union all
      select
        e.id_for_vendor,
        e.editing_session_id,
        cast(null as string) as feature_usage_id,
        'export' as feature_type,
        'export' as feature_name,
        e.device_timestamp as feature_started_ts
      from
        {{ source( 'analytics', 'ios_facetune2_image_exported' ) }} e
      where true
        and date(e._PARTITIONTIME) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and date(e.meta_received_at) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and date(e.device_timestamp) >= DATE_ADD(CURRENT_DATE(), {{ event_time_interval_filter }})
        and e.device_timestamp < CURRENT_TIMESTAMP()
    {% if is_incremental() %}
       and date(device_timestamp) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    )
, feature_started_with_devices as (
    select
    farm_fingerprint(
        concat(
            coalesce(cast(fs.id_for_vendor as string), ''),
            coalesce(cast(fs.editing_session_id as string), ''),
            coalesce(cast(fs.feature_usage_id as string), ''),
            coalesce(cast(fs.feature_type as string), ''),
            coalesce(cast(fs.feature_name as string), ''),
            coalesce(cast(fs.feature_started_ts as string), '')
            )
        ) as feature_unique_key,
      fs.editing_session_id,
      fs.feature_usage_id,
      fs.feature_type,
      fs.feature_name,
      fs.feature_started_ts,
      d.device_first_launch,
      d.id_for_vendor,
      s.id_for_vendor is not null as is_subscriber
    from
      unioned_features as fs
    join
      devices as d
    on
      fs.id_for_vendor = d.id_for_vendor
      and DATETIME(fs.feature_started_ts) >= d.device_first_launch
    left join
        {{ ref( 'purchase_to_verified_devices' ) }} as s
    on
        fs.id_for_vendor = s.id_for_vendor
        and fs.feature_started_ts >= s.original_purchase_date
        and fs.feature_started_ts < coalesce(s.last_expiration_date_or_renewal_date, current_timestamp())
        and s.application = 'facetune2'
    where true
      and d.id_for_vendor is not null
      and d.device_first_launch is not null
)
select *,
       date(editing_session_id_ts) as editing_session_id_date,
       lead(feature_started_ts)
            over (partition by id_for_vendor,editing_session_id order by feature_started_ts, feature_name) as next_feature_started_ts
from (
    select
        feature_unique_key,
        device_first_launch,
        id_for_vendor,
        min(feature_started_ts)
            over (partition by id_for_vendor,editing_session_id) as editing_session_id_ts, -- get the first feature-time for the session
        editing_session_id,
        feature_started_ts,
        feature_usage_id,
        feature_type,
        feature_name,
        is_subscriber,
    from feature_started_with_devices
    where true
    qualify
        row_number() over (partition by feature_unique_key order by is_subscriber desc) = 1
) t
  """

clean_sql = extract_active_sources_refs(sql, remove_commented=True)
# print(extract_active_sources_refs(clean_sql, remove_commented=True))
print(clean_sql)