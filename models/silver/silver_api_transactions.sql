{{- config(alias='silver_api_transaction'
, materialized ='table'
, unique_key='customer_id')
-}}


select 
    transaction_id,
    consumer_id,
    transaction_type,
    DATE(transaction_created_at) as transaction_date,
    DATE(transaction_update_date) as transaction_update_date,
    transaction_payment_value as transaction_payment,
    inserted_at
from {{ ref('bronze_api_transactions') }}