-- Gold: ranking top gainers/losers harian di antara 15 coin yang di-track
-- Menjawab business question: coin mana yang paling naik/turun signifikan hari ini?

select
    coin_id,
    coin_name,
    ingestion_date,
    price_change_pct_24h,
    rank() over (
        partition by ingestion_date
        order by price_change_pct_24h desc
    ) as gainer_rank,
    rank() over (
        partition by ingestion_date
        order by price_change_pct_24h asc
    ) as loser_rank
from {{ ref('silver_coin_market_cleaned') }}