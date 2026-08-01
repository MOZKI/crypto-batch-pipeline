-- Gold: volatilitas harga 30 hari per coin (stddev sebagai proxy risiko)
-- Menjawab business question: coin mana yang volatilitasnya tinggi?

select
    coin_id,
    coin_name,
    ingestion_date,
    stddev(price_usd) over (
        partition by coin_id
        order by ingestion_date
        rows between 29 preceding and current row
    ) as price_volatility_30d,
    count(*) over (
        partition by coin_id
        order by ingestion_date
        rows between 29 preceding and current row
    ) as days_included_in_window
from {{ ref('silver_coin_market_cleaned') }}