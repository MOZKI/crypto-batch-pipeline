"""
> Config Source & MinIO < 
Notes:
List coin yang diambil dari CoinGecko, dan setting MinIO untuk menyimpan raw data.
"""

# Top 15 coin by market cap 
COIN_IDS = [
    "bitcoin",
    "ethereum",
    "tether",
    "binancecoin",
    "solana",
    "ripple",
    "usd-coin",
    "cardano",
    "dogecoin",
    "tron",
    "avalanche-2",
    "chainlink",
    "polkadot",
    "matic-network",
    "litecoin",
]

MINIO_CONN_ID = "minio_s3"
MINIO_BUCKET = "coingecko-raw"

BACKFILL_PREFIX = "raw/coingecko/backfill"
DAILY_PREFIX = "raw/coingecko/daily"

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
BACKFILL_DAYS = 30