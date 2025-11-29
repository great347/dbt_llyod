{{- config(alias='api_transaction_bronze'
, materialized ='incremental'
, unique_key='transaction_id')
-}}

SELECT 
    transaction_id,
    consumer_id,
    transaction_created_at,
    transaction_update_date,
    transaction_type,
    transaction_payment_value,
    current_timestamp() AS inserted_at
FROM 
    {{ source('raw_database_source', 'api_transaction_raw') }}

{% if is_incremental() %}
   where transaction_update_date > (
     select coalesce(max(transaction_update_date), '2001-01-01') from {{ this }}
   )
{% endif %}