{{- config(alias='silver_sqlserver_customer'
, materialized ='table'
, unique_key='customer_id')
-}}


with customer_bronze_source as (
    select
        customer_id,
        customer_fullname,
        customer_postcode,
        address_city,
        address_region,
        last_update_date,
        inserted_at
    from 
        {{ ref('bronze_sqlserver_customers') }}
)
,

cleaned_customer_names as (
    select
        *,
        -- removes titles in the customer_fullname
        ( {{clean_fullname('customer_fullname')}} ) as cleaned_fullname_derived
    from
        customer_bronze_source
)
,
split_customer_irst_and_last_names as (
    select
        *,
        REGEXP_EXTRACT(cleaned_fullname_derived, r'^(\S+)') as first_name,
        REGEXP_EXTRACT(cleaned_fullname_derived, r'.* (\S+)$') as last_name
    from
        cleaned_customer_names
)
,

customer_silver as (
    select
    
        customer_id,
        customer_fullname as original_fullname,
        ( {{mask_postcode('customer_postcode')}} ) as customer_postcode,
        address_city,
        address_region,
        last_update_date,

        -- Select derived names
        first_name,
        last_name,

        -- Apply the hashing macro to the new first_name and last_name columns
         ( {{hash_column_name('first_name')}} ) as hashed_first_name,
         ( {{hash_column_name('last_name')}} ) as hashed_last_name,
        inserted_at  -- Added parentheses around macro call
    

    from split_customer_irst_and_last_names
    
)

select
    -- Select existing columns
    customer_id,
    hashed_first_name as customer_firstname,
    hashed_last_name as customer_lastname,
    customer_postcode,
    address_city,
    address_region,
    DATE(last_update_date) as last_update_date,
    inserted_at

from
    customer_silver