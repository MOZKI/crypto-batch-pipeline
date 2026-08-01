"""
Config terpusat untuk pipeline crypto batch.
Diimport oleh DAG backfill maupun DAG extract harian, biar coin list
dan setting MinIO gak duplikat di banyak file.
"""

# Top 15 coin by market cap (CoinGecko id, bukan symbol)
# Cara ambil id yang benar: GET /coins/markets, field "id" per coin
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

# Prefix path di dalam bucket, dipisah biar backfill (one-time)
# dan daily extract (recurring) gak nyampur
BACKFILL_PREFIX = "raw/coingecko/backfill"
DAILY_PREFIX = "raw/coingecko/daily"

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
BACKFILL_DAYS = 30