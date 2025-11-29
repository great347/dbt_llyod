{{- config(alias='fact_transaction'
, materialized ='table'
, unique_key='transaction_skey')
-}}



select
    transaction_skey,
    transaction_id,
    ta.consumer_id,
    transaction_type,
    transaction_date,
    transaction_update_date,
    transaction_payment,
    inserted_at

from
    {{ ref('silver_api_transactions')}} ta
left join {{ ref('silver_sqlserver_customers')}} sc
        ta.customer_id = sc.customer_id