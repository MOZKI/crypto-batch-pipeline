-- Singular test: price_volatility_30d secara matematis tidak mungkin negatif.
-- Kalau ada baris yang lolos query ini, berarti ada bug di perhitungan upstream.

select *
from {{ ref('gold_volatility_metric') }}
where price_volatility_30d < 0