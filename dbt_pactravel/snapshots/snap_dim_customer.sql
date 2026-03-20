{% snapshot snap_dim_customer %}

{{
    config(
        target_schema='final',
        unique_key='customer_id',
        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=False
    )
}}

select
    customer_id,
    customer_first_name,
    customer_family_name,
    customer_gender,
    customer_birth_date,
    customer_country,
    customer_phone_number
from {{ source('pactravel', 'customers') }}

{% endsnapshot %}
