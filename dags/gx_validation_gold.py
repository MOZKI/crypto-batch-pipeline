"""
Modul validasi Great Expectations untuk Gold layer.
Beda dari gx_validation.py (raw data): di sini kita baca data
LANGSUNG dari Postgres (bukan dari JSON MinIO), karena Gold layer
sudah berupa tabel hasil transformasi DBT.
"""

import great_expectations as gx
from airflow.providers.postgres.hooks.postgres import PostgresHook


class GoldValidationError(Exception):
    """Raised kalau data Gold gagal validasi GX."""


def _validate_df(df, expectations, label):
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name=f"{label}_source")
    data_asset = data_source.add_dataframe_asset(name=f"{label}_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"{label}_batch_def"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    failures = []
    for expectation in expectations:
        result = batch.validate(expectation)
        if not result.success:
            failures.append(
                {
                    "expectation": expectation.__class__.__name__,
                    "column": getattr(expectation, "column", None),
                    "result": result.result,
                }
            )
    return failures


def validate_gold_layer(postgres_conn_id: str = "postgres_dwh") -> None:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    all_failures = {}

    # 1. gold_daily_price_summary: harga tidak boleh negatif/null
    df_summary = hook.get_pandas_df(
        "SELECT * FROM gold.gold_daily_price_summary"
    )
    failures = _validate_df(
        df_summary,
        [
            gx.expectations.ExpectColumnValuesToNotBeNull(column="price_usd"),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="price_usd", min_value=0, max_value=None
            ),
        ],
        label="gold_daily_price_summary",
    )
    if failures:
        all_failures["gold_daily_price_summary"] = failures

    # 2. gold_volatility_metric: volatility tidak boleh negatif
    df_vol = hook.get_pandas_df(
        "SELECT * FROM gold.gold_volatility_metric WHERE price_volatility_30d IS NOT NULL"
    )
    if not df_vol.empty:
        failures = _validate_df(
            df_vol,
            [
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="price_volatility_30d", min_value=0, max_value=None
                ),
            ],
            label="gold_volatility_metric",
        )
        if failures:
            all_failures["gold_volatility_metric"] = failures

    # 3. gold_top_gainers_losers: gainer_rank harus positif integer, tidak null
    df_rank = hook.get_pandas_df(
        "SELECT * FROM gold.gold_top_gainers_losers"
    )
    failures = _validate_df(
        df_rank,
        [
            gx.expectations.ExpectColumnValuesToNotBeNull(column="gainer_rank"),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="gainer_rank", min_value=1, max_value=None
            ),
        ],
        label="gold_top_gainers_losers",
    )
    if failures:
        all_failures["gold_top_gainers_losers"] = failures

    if all_failures:
        raise GoldValidationError(
            f"Validasi GX Gold layer gagal: {all_failures}"
        )

    print("Validasi GX Gold layer berhasil untuk semua tabel.")