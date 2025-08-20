select
  customer_id,
  upper(first_name) as first_name_caps,
  last_name
from {{ ref('customers') }}