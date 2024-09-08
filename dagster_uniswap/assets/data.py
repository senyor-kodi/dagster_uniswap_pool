import os
import pandas as pd
import polars as pl
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
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
    UNISWAP_ENDPOINT,
    TOPIC_BURN,
    TOPIC_COLLECT,
    TOPIC_FLASH,
    TOPIC_MINT,
    TOPIC_SWAP,
    DIRECTORY_PATH,
    WETH_SPX_ADDRESS,
    WETH_USDT_ADDRESS,
    START_BLOCK,
    END_BLOCK,
)
from dagster_duckdb import DuckDBResource
from dagster_shell import execute_shell_command

from .utils import (
    fee_tier_to_tick_spacing,
    get_historical_liquidity,
    tick_to_price_adjusted,
)

import nest_asyncio

nest_asyncio.apply()

client = Client(
    transport=RequestsHTTPTransport(
        url=UNISWAP_ENDPOINT,
        verify=True,
        retries=5,
    )
)

topics = {
    'swap': TOPIC_SWAP,
    'burn': TOPIC_BURN,
    'mint': TOPIC_MINT,
    'collect': TOPIC_COLLECT,
    'flash': TOPIC_FLASH,
}

# ----------------------------------------
#   On-chain data via cryo
# ----------------------------------------

@op
def get_blocks_op(context):
    relative_path = 'dagster_uniswap/data/raw'
    execute_shell_command(
        f'cd {DIRECTORY_PATH}/{relative_path}/blocks && rm -rf * && cryo blocks --blocks {START_BLOCK}:{END_BLOCK} --include-columns all -l 50',
        "NONE", context.log
    )
    return
    
@graph_asset(group_name='raw_data')
def run_get_blocks():
    return get_blocks_op()
    

@op
def get_logs_op(context, run_get_blocks):
    relative_path = 'dagster_uniswap/data/raw'
    for key, topic in topics.items():
        execute_shell_command(
            f'cd {DIRECTORY_PATH}/{relative_path}/{key} && rm -rf * && cryo logs --contract {WETH_SPX_ADDRESS} --blocks {START_BLOCK}:{END_BLOCK} --topic0 {topic} -l 50', 
            "NONE", context.log
        )
    
@graph_asset(group_name='raw_data')
def run_get_logs(run_get_blocks):
    return get_logs_op(run_get_blocks)


# ----------------------------------------
#   the Graph uniswap endpoint data
# ----------------------------------------


@asset(compute_kind='python', group_name='raw_data')
def pool_constants(context: AssetExecutionContext):

    pool_query = """query get_pools($pool_id: ID!) {
        pools(where: {id: $pool_id}) {
            tick
            sqrtPrice
            liquidity
            feeTier
            token0 {
            symbol
            decimals
            }
            token1 {
            symbol
            decimals
            }
        }
    }"""

    variables = {
        #'pool_id': WETH_SPX_ADDRESS
        'pool_id': WETH_USDT_ADDRESS
    }   # replace by contract address of pool
    response = client.execute(gql(pool_query), variable_values=variables)

    pool = response['pools'][0]
    current_tick = int(pool['tick'])
    fee_tier = int(pool['feeTier'])
    fee_rate = fee_tier / 1e6
    tick_spacing = fee_tier_to_tick_spacing(fee_tier)

    token0 = pool['token0']['symbol']
    token1 = pool['token1']['symbol']
    decimals0 = int(pool['token0']['decimals'])
    decimals1 = int(pool['token1']['decimals'])

    return {
        'pool_address': pool,
        'current_tick': current_tick,
        'fee_tier': fee_tier,
        'fee_rate': fee_rate,
        'tick_spacing': tick_spacing,
        'token0': token0,
        'token1': token1,
        'decimals0': decimals0,
        'decimals1': decimals1,
    }


# @asset(compute_kind='duckdb', deps=pool_constants)
@asset(compute_kind='duckdb', group_name='prepared_data')
def pool_table(pool_constants, duckdb: DuckDBResource):

    # check if there's more efficient method
    with duckdb.get_connection() as conn:
        #conn = duckdb.get_connection()
        conn.execute(
            'CREATE TABLE IF NOT EXISTS pool_data (fee_tier INT, fee_rate INT, tick_spacing INT, current_tick INT, token0 VARCHAR(255), token1 VARCHAR(255), decimals0 INT, decimals1 INT)'
        )
        conn.execute(
            'INSERT INTO pool_data (fee_tier, fee_rate, tick_spacing, current_tick, token0, token1, decimals0, decimals1) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (
                pool_constants['fee_tier'],
                pool_constants['fee_rate'],
                pool_constants['tick_spacing'],
                pool_constants['current_tick'],
                pool_constants['token0'],
                pool_constants['token1'],
                pool_constants['decimals0'],
                pool_constants['decimals1'],
            ),
        )

        nrows = conn.execute('SELECT COUNT(*) FROM pool_data').fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'pool_data'"
        ).pl()

    """
    context.log.info("Created pool_data table")
    
    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )
    """


@asset(compute_kind='python', group_name='raw_data')
def tick_map(pool_constants):

    (
        tick_data,
        current_adjusted_price,
        total_amount0,
        total_amount1,
        block_number,
    ) = get_historical_liquidity(
        #pool_id=WETH_SPX_ADDRESS, block_number=END_BLOCK, client=client
        pool_id=WETH_USDT_ADDRESS, block_number=END_BLOCK, client=client
    )

    tick_df = (
        pd.DataFrame.from_dict(tick_data, orient='index')
        .reset_index()
        .rename(columns={'index': 'tick'})
    )
    tick_df['price'] = 1 / (
        tick_df['tick'].apply(lambda x: 
            tick_to_price_adjusted(
                tick=x,
                decimals0=pool_constants['decimals0'],
                decimals1=pool_constants['decimals1'],
            )
        )
        / (10 ** (pool_constants['decimals1'] - pool_constants['decimals0']))
    )

    return [
        tick_df,
        current_adjusted_price,
        total_amount0,
        total_amount1,
        block_number,
    ]


@asset(compute_kind='duckdb', group_name='prepared_data')
def tick_table(tick_map, duckdb: DuckDBResource):
    (
        tick_df,
        current_adjusted_price,
        total_amount0,
        total_amount1,
        block_number,
    ) = tick_map
    
    # Drop the liquidity column
    tick_df = tick_df.drop('liquidity', axis=1)

    print(f'tick_df columns: {tick_df.columns}')
    print(f'tick_df dtypes: {tick_df.dtypes}')
    
    with duckdb.get_connection() as con:
        tick_df.to_sql('tick', con, if_exists='replace', index=False)
        con.execute(
            'CREATE TABLE IF NOT EXISTS tick_info (current_adjusted_price DOUBLE, total_amount0 DOUBLE, total_amount1 DOUBLE, block_number BIGINT)'
        )
        con.execute(
            'INSERT INTO tick_info (current_adjusted_price, total_amount0, total_amount1, block_number) VALUES (?, ?, ?, ?)',
            (
                current_adjusted_price,
                total_amount0,
                total_amount1,
                block_number,
            ),
        )

