{{ config(materialized='table') }}

with history as (
    select * from {{ ref('fct_books_history') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by asin
            order by scraped_at desc
        ) as rn
    from history
)

select
    asin,
    title,
    author,
    scraped_at as last_seen_at
from ranked
where rn = 1
