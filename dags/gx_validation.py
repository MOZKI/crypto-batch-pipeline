"""
> GX Validation Raw Data <
Notes:
Validasi raw snapshot dari CoinGecko (hasil extract snapshot.json) 
menggunakan Great Expectations. Digunakan oleh DAG daily_pipeline.
"""

import great_expectations as gx
import pandas as pd
from great_expectations.exceptions import GreatExpectationsError


class RawDataValidationError(Exception):
    """Raised kalau raw data gagal validasi GX."""


def validate_raw_snapshot(records: list[dict]) -> None:
    """
    Validasi list of dict (hasil parse raw_data dari snapshot.json)
    menggunakan beberapa expectation dasar.

    Raise RawDataValidationError kalau ada expectation yang gagal.
    """
    df = pd.DataFrame(records)

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name="coingecko_raw_source")
    data_asset = data_source.add_dataframe_asset(name="coingecko_raw_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "coingecko_raw_batch_def"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="current_price"),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="current_price", min_value=0, max_value=None
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="market_cap"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="id"),
    ]

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

    if failures:
        raise RawDataValidationError(
            f"Validasi GX gagal untuk {len(failures)} expectation: {failures}"
        )

    print(f"Validasi GX berhasil untuk {len(records)} baris data.")