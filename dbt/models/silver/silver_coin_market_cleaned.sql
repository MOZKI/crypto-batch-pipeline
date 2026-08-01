-- Silver layer: cleaning, dedup, rename, type casting dari Bronze
-- Dedup jaga-jaga kalau ada retry/backfill yang overlap di hari yang sama

with source as (
    select * from {{ source('bronze', 'coingecko_market_raw') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by id, ingestion_date
            order by extracted_at desc
        ) as rn
    from source
),

cleaned as (
    select
        id                              as coin_id,
        symbol                          as symbol,
        name                             as coin_name,
        current_price::numeric          as price_usd,
        market_cap::numeric             as market_cap_usd,
        market_cap_rank::integer        as market_cap_rank,
        total_volume::numeric           as volume_24h_usd,
        high_24h::numeric               as high_24h_usd,
        low_24h::numeric                as low_24h_usd,
        price_change_percentage_24h::numeric as price_change_pct_24h,
        circulating_supply::numeric     as circulating_supply,
        total_supply::numeric           as total_supply,
        max_supply::numeric             as max_supply,
        (max_supply is null)            as is_supply_unlimited,
        ath::numeric                    as all_time_high_usd,
        ath_date::timestamp             as all_time_high_date,
        last_updated::timestamp         as source_last_updated,
        extracted_at::timestamp         as extracted_at,
        ingestion_date::date            as ingestion_date,
        id || '_' || ingestion_date::text as unique_key
    from deduped
    where rn = 1
      and current_price is not null
      and current_price >= 0
)

select * from cleaned