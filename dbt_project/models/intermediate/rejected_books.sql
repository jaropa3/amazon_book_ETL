{{ config(materialized='table') }}

-- rekordy z bronze odrzucone w staging (brak asin)
select
    null as asin,
    title,
    author,
    price,
    scraped_at::timestamp as scraped_at,
    'brak asin' as rejection_reason
from {{ source('bronze', 'books') }}
where asin is null

union all

-- duplikaty w ramach tej samej sesji odrzucone w int_books
select
    asin,
    title,
    author,
    price::text,
    scraped_at,
    'duplikat w sesji (asin+scraped_at)' as rejection_reason
from (
    select
        *,
        row_number() over (
            partition by asin, scraped_at
            order by (price is null) asc, (rating is null) asc
        ) as rn
    from {{ ref('stg_books') }}
) ranked
where rn > 1
