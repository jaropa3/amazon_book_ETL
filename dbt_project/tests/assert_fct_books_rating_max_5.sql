-- Zwraca wiersze naruszające test (rating musi być między 0 a 5 gdy nie jest null)
select *
from {{ ref('fct_books_history') }}
where rating is not null and (rating < 0 or rating > 5)
