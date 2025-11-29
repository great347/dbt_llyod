{{- config(alias='sqlserver_customer_bronze'
, materialized ='incremental'
,unique_key = 'customer_id')
-}}

SELECT 
   customer_id,
   customer_fullname,
   customer_postcode,
   address_city,
   address_region,
   last_update_date,
   current_timestamp() AS inserted_at
FROM 
    {{ source('raw_database_source', 'sqlserver_customer_raw') }}

{% if is_incremental() %}
   where load_datetime > (
     select coalesce(max(load_datetime), '2001-01-01') from {{ this }}
   )
{% endif %}