-- Gold: snapshot harian per coin, siap dikonsumsi Metabase

select
    coin_id,
    coin_name,
    symbol,
    ingestion_date,
    price_usd,
    market_cap_usd,
    market_cap_rank,
    volume_24h_usd,
    price_change_pct_24h
from {{ ref('silver_coin_market_cleaned') }}