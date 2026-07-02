select asin, scraped_at, count(*)
from {{ ref('int_books') }}
group by asin, scraped_at
having count(*) > 1
