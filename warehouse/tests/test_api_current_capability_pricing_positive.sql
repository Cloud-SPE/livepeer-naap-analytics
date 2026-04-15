select *
from {{ ref('api_current_capability_pricing') }}
where price_per_unit <= 0
   or pixels_per_unit <= 0
