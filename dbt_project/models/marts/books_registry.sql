{{ config(materialized='table') }}

select distinct
    asin,
    title,
    author
from {{ ref('fct_books_history') }}
