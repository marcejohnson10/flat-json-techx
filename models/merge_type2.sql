		{{
			config(
				materialized ='incremental',
				unique_key =['O_ORDERKEY','O_ORDERDATE'],
				on_schema_change='sync_all_columns',
				incremental_strategy='merge',
				additional_matching_columns = ['O_CUSTKEY','O_ORDERSTATUS'],
                merge_exclude_columns = ['V_INGESTION_TIMESTAMP','D_START_DATE', 'D_END_DATE', 'F_CURRENT_FLAG'],
				condition = ['src.O_CUSTKEY = tgt.O_CUSTKEY','src.O_CUSTKEY = 123',"tgt.O_ORDERSTATUS not in ('F','O')"],
                scd_type = '2'
			)
		}}

--- select query to create target tables
select {{ dbt_utils.star ( from=ref('orders_raw') ) }},  --- add name of all column in the source
        current_timestamp() as D_START_DATE, 
        null as D_END_DATE, 
        'Y' as F_CURRENT_FLAG,
       current_timestamp() as Z_LOAD,
       current_timestamp() as Z_LST_UPDT
from {{ ref('orders_raw') }} --source name 

---include a column present in both source and target and which can identify new records
{%- if is_incremental() -%}

  -- this filter will only be applied on an incremental run
  where 1=1 --V_INGESTION_TIMESTAMP > (select coalesce (max(V_INGESTION_TIMESTAMP), '0001-01-01') from {{ this }})

{%- endif -%}
