with staged as (
    select * from {{ ref('stg_books') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by asin, scraped_at
            order by (price is null) asc, (rating is null) asc
        ) as rn
    from staged
)

select
    asin,
    title,
    author,
    price,
    rating,
    scraped_at
from ranked
where rn = 1
