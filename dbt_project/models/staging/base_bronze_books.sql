with source as (
        select * from {{ source('bronze', 'books') }}
  ),
  renamed as (
      select
          {{ adapter.quote("asin") }},
        {{ adapter.quote("title") }},
        {{ adapter.quote("author") }},
        {{ adapter.quote("price") }},
        {{ adapter.quote("rating") }},
        {{ adapter.quote("scraped_at") }}

      from source
  )
  select * from renamed
    