{{ config(
    materialized='incremental',
    unique_key='book_sk'
) }}

select
    md5(asin || '_' || scraped_at::text) as book_sk,
    asin,
    title,
    author,
    price,
    rating,
    scraped_at
from {{ ref('int_books') }}
