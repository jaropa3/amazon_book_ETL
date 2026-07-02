-- Zwraca wiersze naruszające test (price musi być > 0 gdy nie jest null)
select *
from {{ ref('fct_books_history') }}
where price is not null and price <= 0
