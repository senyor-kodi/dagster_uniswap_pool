from ..assets.data import get_blocks_op, get_logs_op
from ..assets.decode import cryo_data_decode_op
from dagster import define_asset_job, AssetSelection, job
import asyncio
import nest_asyncio

nest_asyncio.apply()

@job
def raw_blocks_job():
    get_blocks_op()

@job
def raw_logs_job():
    get_logs_op()

@job
def cryo_data_decode_job():
    cryo_data_decode_op()

pool_assets = AssetSelection.keys('pool_constants', 'pool_table')
tick_assets = AssetSelection.keys('tick_map', 'tick_table')
decoded_assets = AssetSelection.keys(
    'swap_decoded',
    'burn_decoded',
    'mint_decoded',
    'collect_decoded',
    'flash_decoded',
)
metrics_assets = AssetSelection.keys(
    'tvl', 'volume_metrics', 'fees', 'yield_metric', 'price'
)

pool_constants_job = define_asset_job(
    name='pool_constants_job',
    selection=pool_assets,
)
tick_job = define_asset_job(
    name='tick_job',
    selection=tick_assets,
)
decoded_to_db_job = define_asset_job(
    name='decoded_to_db_job',
    selection=decoded_assets,
)
metrics_job = define_asset_job(
    name='metrics_job',
    selection=metrics_assets,
)
