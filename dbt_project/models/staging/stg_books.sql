with source as (
    select * from {{ source('bronze', 'books') }}
)

select
    asin,
    title,
    author,
    nullif(replace(replace(replace(price, '$', ''), 'PLN', ''), chr(160), ''), '0.00')::numeric as price,
    split_part(rating, ' ', 1)::numeric as rating,
    scraped_at::timestamp as scraped_at
from source
where asin is not null
