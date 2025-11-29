{{- config(alias='dim_customer'
, materialized ='table'
, unique_key='customer_skey')
-}}



select
    customer_skey,
    customer_id,
    customer_firstname,
    customer_lastname,
    customer_postcode,
    address_city,
    address_region,
    last_update_date,
    inserted_at

from
    {{ ref('silver_sqlserver_customers')}}