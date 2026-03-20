{{
    config(
        materialized='table'
    )
}}

/*
  dim_customer reads from the dbt snapshot (SCD Type 2).
  The snapshot table is snap_dim_customer in the final schema.
  We expose dbt_scd_id as customer_key and rename the audit columns.
*/

select
    row_number() over (order by customer_id, dbt_updated_at) as customer_key,
    customer_id,
    customer_first_name,
    customer_family_name,
    customer_gender,
    customer_birth_date,
    customer_country,
    customer_phone_number,
    dbt_valid_from   as effective_date,
    dbt_valid_to     as expiry_date,
    case
        when dbt_valid_to is null then true
        else false
    end              as is_current
from {{ ref('snap_dim_customer') }}
