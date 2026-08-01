-- Singular test: harga tidak boleh negatif di Gold layer manapun

select *
from {{ ref('gold_daily_price_summary') }}
where price_usd < 0