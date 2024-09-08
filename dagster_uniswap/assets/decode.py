import os
import polars as pl
import ctc
import polars_evm
import asyncio
from decimal import Decimal

from dagster import (
    asset,
    op,
    graph_asset,
    AssetExecutionContext,
)
from ..constants import (
    DIRECTORY_PATH,
    WETH_SPX_ADDRESS,
    WETH_USDT_ADDRESS,
    START_BLOCK,
    END_BLOCK,
)
from dagster_duckdb import DuckDBResource
from dagster import file_relative_path, get_dagster_logger

from .data import run_get_logs

import nest_asyncio

nest_asyncio.apply()

# ----------------------------------------
#   Cryo data decoded
# ----------------------------------------

@op()
async def cryo_data_decode_op(run_get_logs):

    # Read the blocks data
    blocks_df = pl.read_parquet(
        file_relative_path(__file__, '../data/raw/blocks/*.parquet')
    )

    for path, event_name in paths:

        df = pl.read_parquet(path)
        
        #df = df.filter(df['address'].evm.binary_to_hex() == WETH_SPX_ADDRESS)
        df = df.filter(df['address'].evm.binary_to_hex() == WETH_USDT_ADDRESS)
        
        # Check if df is empty
        if df.shape[0] == 0:
            continue
        
        # Join with blocks_df to get the timestamp
        df = df.join(blocks_df, on='block_number', how='left')

        # Save block numbers and timestamps
        block_numbers = df['block_number']
        timestamps = df['timestamp'].cast(pl.Int64) * 1000

        abi = await ctc.async_get_event_abi(
            contract_address=WETH_USDT_ADDRESS, event_name=event_name
        )
        decoded = await ctc.async_decode_events_dataframe(
            events=df.evm.binary_to_hex().rename(
                {
                    'topic0': 'event_hash',
                    'data': 'unindexed',
                    'address': 'contract_address',
                }
            ),
            event_abis=[abi],
            context=None,
        )
        decoded_data = {
            column: list(map(str, decoded.get_column(column).to_list()))
            for column in decoded.columns
        }
        decoded_df = pl.from_dict(
            decoded_data, schema=column_types[event_name]
        )

        # Add the block numbers and timestamps as new columns
        decoded_df = decoded_df.with_columns(
            pl.lit(block_numbers).alias('block_number'),
            pl.lit(timestamps).alias('timestamp'),
        )

        # Add the datetime column
        decoded_df = decoded_df.with_columns(
            pl.col('timestamp')
            .cast(pl.Datetime)
            .dt.with_time_unit('ms')
            .alias('datetime')
        )
        
        # Apply final column transformations
        decoded_df = decoded_df.select(final_columns[event_name])

        decoded_df.write_parquet(
            file_relative_path(
                __file__, f'../data/decoded/{event_name.lower()}.parquet'
            )
        )

@graph_asset(group_name='prepared_data')
def run_cryo_data_decoded(run_get_logs):
    return cryo_data_decode_op(run_get_logs)


# Define the paths and event names
paths = [
    (file_relative_path(__file__, '../data/raw/swap/*.parquet'), 'Swap'),
    (file_relative_path(__file__, '../data/raw/burn/*.parquet'), 'Burn'),
    (file_relative_path(__file__, '../data/raw/mint/*.parquet'), 'Mint'),
    (file_relative_path(__file__, '../data/raw/collect/*.parquet'), 'Collect'),
    (file_relative_path(__file__, '../data/raw/flash/*.parquet'), 'Flash'),
]

# Define the column types for each event
column_types = {
    'Swap': {
        'arg__sender': str,
        'arg__recipient': str,
        'arg__amount0': str,
        'arg__amount1': str,
        'arg__sqrtPriceX96': str,
        'arg__liquidity': str,
        'arg__tick': str,
    },
    'Burn': {
        'arg__owner': str,
        'arg__tickLower': str,
        'arg__tickUpper': str,
        'arg__amount': str,
        'arg__amount0': str,
        'arg__amount1': str,
    },
    'Mint': {
        'arg__owner': str,
        'arg__tickLower': str,
        'arg__tickUpper': str,
        'arg__sender': str,
        'arg__amount': str,
        'arg__amount0': str,
        'arg__amount1': str,
    },
    'Collect': {
        'arg__owner': str,
        'arg__tickLower': str,
        'arg__tickUpper': str,
        'arg__recipient': str,
        'arg__amount0': str,
        'arg__amount1': str,
    },
    'Flash': {
        'arg__sender': str,
        'arg__recipient': str,
        'arg__amount0': str,
        'arg__amount1': str,
        'arg__paid0': str,
        'arg__paid1': str,
    },
}

# Define the final column selections for each event
final_columns = {
    'Swap': [
        pl.col('arg__sender').alias('sender'),
        pl.col('arg__recipient').alias('recipient'),
        pl.col('arg__amount0').cast(pl.Float64).alias('amount0'),
        pl.col('arg__amount1').cast(pl.Float64).alias('amount1'),
        pl.col('arg__sqrtPriceX96').cast(pl.Float64).alias('sqrtPriceX96'),
        pl.col('arg__liquidity').cast(pl.Float64).alias('liquidity'),
        pl.col('arg__tick').cast(pl.Float64).alias('tick'),
        pl.col('block_number'),
        pl.col('timestamp'),
        pl.col('datetime'),
    ],
    'Mint': [
        pl.col('arg__owner').alias('owner'),
        pl.col('arg__tickLower').cast(pl.Float64).alias('tickLower'),
        pl.col('arg__tickUpper').cast(pl.Float64).alias('tickUpper'),
        pl.col('arg__amount').cast(pl.Float64).alias('amount'),
        pl.col('arg__amount0').cast(pl.Float64).alias('amount0'),
        pl.col('arg__amount1').cast(pl.Float64).alias('amount1'),
        pl.col('block_number'),
        pl.col('timestamp'),
        pl.col('datetime'),
    ],
    'Burn': [
        pl.col('arg__owner').alias('owner'),
        pl.col('arg__tickLower').cast(pl.Float64).alias('tickLower'),
        pl.col('arg__tickUpper').cast(pl.Float64).alias('tickUpper'),
        pl.col('arg__amount').cast(pl.Float64).alias('amount'),
        pl.col('arg__amount0').cast(pl.Float64).alias('amount0'),
        pl.col('arg__amount1').cast(pl.Float64).alias('amount1'),
        pl.col('block_number'),
        pl.col('timestamp'),
        pl.col('datetime'),
    ],
    'Collect': [
        pl.col('arg__owner').alias('owner'),
        pl.col('arg__tickLower').cast(pl.Float64).alias('tickLower'),
        pl.col('arg__tickUpper').cast(pl.Float64).alias('tickUpper'),
        pl.col('arg__recipient').alias('recipient'),
        pl.col('arg__amount0').cast(pl.Float64).alias('amount0'),
        pl.col('arg__amount1').cast(pl.Float64).alias('amount1'),
        pl.col('block_number'),
        pl.col('timestamp'),
        pl.col('datetime'),
    ],
    'Flash': [
        pl.col('arg__sender').alias('owner'),
        pl.col('arg__recipient').alias('recipient'),
        pl.col('arg__amount0').cast(pl.Float64).alias('amount0'),
        pl.col('arg__amount1').cast(pl.Float64).alias('amount1'),
        pl.col('arg__paid0').cast(pl.Float64).alias('paid0'),
        pl.col('arg__paid1').cast(pl.Float64).alias('paid1'),
        pl.col('block_number'),
        pl.col('timestamp'),
        pl.col('datetime'),
    ],
}


@asset(compute_kind='duckdb', group_name='prepared_data')
def swap_decoded(context: AssetExecutionContext, duckdb: DuckDBResource, run_cryo_data_decoded):
    
    file_path = file_relative_path(__file__, '../data/decoded/swap.parquet')

    # Check if the file exists
    if not os.path.exists(file_path):
        context.log.info(f'File {file_path} not found. Exiting.')
        return
    
    df = pl.read_parquet(
        file_path
    )

    # Write the data to a DuckDB table
    with duckdb.get_connection() as con:
        con.register('tvl_df_view', df)
        con.execute('CREATE TABLE IF NOT EXISTS swap AS SELECT * FROM df')

        nrows = con.execute('SELECT COUNT(*) FROM swap').fetchone()[0]  # type: ignore
        metadata = con.execute(
            "select * from duckdb_tables() where table_name = 'swap'"
        ).pl()

    context.add_output_metadata(
        metadata={
            'num_rows': nrows,
            'table_name': metadata['table_name'][0],
            'datbase_name': metadata['database_name'][0],
            'schema_name': metadata['schema_name'][0],
            'column_count': metadata['column_count'][0],
            'estimated_size': metadata['estimated_size'][0],
        }
    )

    context.log.info('Created swap table')

@asset(compute_kind='duckdb', group_name='prepared_data')
def burn_decoded(context: AssetExecutionContext, duckdb: DuckDBResource, run_cryo_data_decoded):
    
    file_path = file_relative_path(__file__, '../data/decoded/burn.parquet')

    # Check if the file exists
    if not os.path.exists(file_path):
        context.log.info(f'File {file_path} not found. Exiting.')
        return
    
    df = pl.read_parquet(
        file_path
    )

    with duckdb.get_connection() as con:
        con.register('tvl_df_view', df)
        con.execute('CREATE TABLE IF NOT EXISTS burn AS SELECT * FROM df')

        nrows = con.execute('SELECT COUNT(*) FROM burn').fetchone()[0]
        metadata = con.execute(
            "select * from duckdb_tables() where table_name = 'burn'"
        ).pl()

    context.add_output_metadata(
        metadata={
            'num_rows': nrows,
            'table_name': metadata['table_name'][0],
            'database_name': metadata['database_name'][0],
            'schema_name': metadata['schema_name'][0],
            'column_count': metadata['column_count'][0],
            'estimated_size': metadata['estimated_size'][0],
        }
    )

    context.log.info('Created burn table')


@asset(compute_kind='duckdb', group_name='prepared_data')
def mint_decoded(context: AssetExecutionContext, duckdb: DuckDBResource, run_cryo_data_decoded):
    df = pl.read_parquet(
        file_relative_path(__file__, '../data/decoded/mint.parquet')
    )

    with duckdb.get_connection() as con:
        con.register('tvl_df_view', df)
        con.execute('CREATE TABLE IF NOT EXISTS mint AS SELECT * FROM df')

        nrows = con.execute('SELECT COUNT(*) FROM mint').fetchone()[0]
        metadata = con.execute(
            "select * from duckdb_tables() where table_name = 'mint'"
        ).pl()

    context.add_output_metadata(
        metadata={
            'num_rows': nrows,
            'table_name': metadata['table_name'][0],
            'database_name': metadata['database_name'][0],
            'schema_name': metadata['schema_name'][0],
            'column_count': metadata['column_count'][0],
            'estimated_size': metadata['estimated_size'][0],
        }
    )

    context.log.info('Created mint table')


@asset(compute_kind='duckdb', group_name='prepared_data')
def collect_decoded(context: AssetExecutionContext, duckdb: DuckDBResource, run_cryo_data_decoded):
    df = pl.read_parquet(
        file_relative_path(__file__, '../data/decoded/collect.parquet')
    )

    with duckdb.get_connection() as con:
        con.register('tvl_df_view', df)
        con.execute('CREATE TABLE IF NOT EXISTS collect AS SELECT * FROM df')

        nrows = con.execute('SELECT COUNT(*) FROM collect').fetchone()[0]
        metadata = con.execute(
            "select * from duckdb_tables() where table_name = 'collect'"
        ).pl()

    context.add_output_metadata(
        metadata={
            'num_rows': nrows,
            'table_name': metadata['table_name'][0],
            'database_name': metadata['database_name'][0],
            'schema_name': metadata['schema_name'][0],
            'column_count': metadata['column_count'][0],
            'estimated_size': metadata['estimated_size'][0],
        }
    )

    context.log.info('Created collect table')


@asset(compute_kind='duckdb', group_name='prepared_data')
def flash_decoded(context: AssetExecutionContext, duckdb: DuckDBResource, run_cryo_data_decoded):
    
    file_path = file_relative_path(__file__, '../data/decoded/flash.parquet')

    # Check if the file exists
    if not os.path.exists(file_path):
        context.log.info(f'File {file_path} not found. Exiting.')
        return
    
    df = pl.read_parquet(
        file_path
    )

    with duckdb.get_connection() as con:
        con.register('tvl_df_view', df)
        con.execute('CREATE TABLE IF NOT EXISTS flash AS SELECT * FROM df')

        nrows = con.execute('SELECT COUNT(*) FROM flash').fetchone()[0]
        metadata = con.execute(
            "select * from duckdb_tables() where table_name = 'flash'"
        ).pl()

    context.add_output_metadata(
        metadata={
            'num_rows': nrows,
            'table_name': metadata['table_name'][0],
            'database_name': metadata['database_name'][0],
            'schema_name': metadata['schema_name'][0],
            'column_count': metadata['column_count'][0],
            'estimated_size': metadata['estimated_size'][0],
        }
    )

    context.log.info('Created flash table')

