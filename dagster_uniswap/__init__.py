from dagster import Definitions, load_assets_from_modules

from .assets import data, decode, metrics
from .assets.metrics import tvl, volume_metrics, fees, yield_metric, price
from .assets.decode import run_cryo_data_decoded, swap_decoded, burn_decoded, mint_decoded, flash_decoded, collect_decoded
from .resources import duckdb_resource
from .jobs import (
    raw_blocks_job,
    raw_logs_job,
    cryo_data_decode_job,
    pool_constants_job,
    tick_job,
    decoded_to_db_job,
    metrics_job,
)

data_assets = load_assets_from_modules([data])

decode_assets = [run_cryo_data_decoded, swap_decoded, burn_decoded, mint_decoded, flash_decoded, collect_decoded]
metrics_assets = [tvl, volume_metrics, fees, yield_metric, price]

defs = Definitions(
    assets=[*data_assets, *decode_assets, *metrics_assets],
    resources={
        'duckdb': duckdb_resource,
    },
    jobs=[
        raw_blocks_job,
        raw_logs_job,
        cryo_data_decode_job,
        pool_constants_job,
        tick_job,
        decoded_to_db_job,
        metrics_job,
    ],
)
