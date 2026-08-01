-- Gold: moving average harga 7 hari per coin
-- Menjawab business question: bagaimana tren harga jangka pendek-menengah?

select
    coin_id,
    coin_name,
    ingestion_date,
    price_usd,
    avg(price_usd) over (
        partition by coin_id
        order by ingestion_date
        rows between 6 preceding and current row
    ) as ma_7d,
    count(*) over (
        partition by coin_id
        order by ingestion_date
        rows between 6 preceding and current row
    ) as days_included_in_window
from {{ ref('silver_coin_market_cleaned') }}