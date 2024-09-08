import polars as pl
import pandas as pd

from dagster import AssetIn, asset
from dagster import file_relative_path, get_dagster_logger
from dagster_duckdb import DuckDBResource

from .utils import tick_to_price_adjusted

from .data import pool_constants
from .decode import swap_decoded, burn_decoded, mint_decoded, flash_decoded, collect_decoded


@asset(compute_kind='polars', 
       group_name='metrics', 
       deps=[swap_decoded, burn_decoded,mint_decoded, flash_decoded, collect_decoded])
def tvl(pool_constants, duckdb: DuckDBResource):
    mint_df = pl.read_parquet(
        file_relative_path(__file__, '../data/decoded/mint.parquet')
    ).select(
        pl.col('datetime'),
        (-pl.col('amount0') / 10 ** pool_constants['decimals0']).alias(
            'amount0'
        ),
        (-pl.col('amount1') / 10 ** pool_constants['decimals1']).alias(
            'amount1'
        ),
    )
    collect_df = pl.read_parquet(
        file_relative_path(__file__, '../data/decoded/collect.parquet')
    ).select(
        pl.col('datetime'),
        (-pl.col('amount0') / 10 ** pool_constants['decimals0']).alias(
            'amount0'
        ),
        (-pl.col('amount1') / 10 ** pool_constants['decimals1']).alias(
            'amount1'
        ),
    )
    swap_df = pl.read_parquet(
        file_relative_path(__file__, '../data/decoded/swap.parquet')
    ).select(
        pl.col('datetime'),
        (-pl.col('amount0') / 10 ** pool_constants['decimals0']).alias(
            'amount0'
        ),
        (-pl.col('amount1') / 10 ** pool_constants['decimals1']).alias(
            'amount1'
        ),
    )

    # TODO add flash
    """
    flash_df = pl.read_parquet(
        file_relative_path(__file__, '../data/decoded/flash.parquet')
    )
    """

    tvl_df = pl.concat([swap_df, mint_df, collect_df])
    tvl_df = tvl_df.sort('datetime')

    tvl_df = tvl_df.groupby_dynamic('datetime', every='1h').agg(
        [pl.col(f'amount0').sum(), pl.col(f'amount1').sum()]
    )

    tvl_df = tvl_df.with_columns(
        pl.col(f'amount0').cumsum().alias(f'amount0_tvl'),
        pl.col(f'amount1').cumsum().alias(f'amount1_tvl'),
    ).select('datetime', 'amount0_tvl', 'amount1_tvl')

    with duckdb.get_connection() as con:
        
        # Register the Polars DataFrame as a virtual table
        con.register('tvl_df_view', tvl_df)
        # Create a new table from the virtual table
        con.execute('CREATE TABLE IF NOT EXISTS tvl AS SELECT * FROM tvl_df_view')


@asset(compute_kind='polars', group_name='metrics', 
       deps=[swap_decoded, burn_decoded,mint_decoded, flash_decoded, collect_decoded])
def volume_metrics(pool_constants, duckdb: DuckDBResource):
    swap_df = pl.read_parquet(
        file_relative_path(__file__, '../data/decoded/swap.parquet')
    )

    volume_df = swap_df.with_columns(
        pl.col('amount0') / 10 ** pool_constants['decimals0'],
        pl.col('amount1') / 10 ** pool_constants['decimals1'],
    )

    volume_df = volume_df.with_columns(
        pl.when(volume_df['amount0'] < 0)
        .then(0)
        .otherwise(volume_df['amount0'])
        .alias(f'amount0_in'),
        pl.when(volume_df['amount0'] < 0)
        .then(-volume_df['amount0'])
        .otherwise(0)
        .alias(f'amount0_out'),
        pl.when(volume_df['amount1'] < 0)
        .then(0)
        .otherwise(volume_df['amount1'])
        .alias(f'amount1_in'),
        pl.when(volume_df['amount1'] < 0)
        .then(-volume_df['amount1'])
        .otherwise(0)
        .alias(f'amount1_out'),
    ).sort('datetime')

    # Group by daily windows and aggregate volumes
    volume_df = volume_df.groupby_dynamic('datetime', every='1h').agg(
        [
            pl.col(f'amount0_in').sum().alias(f'amount0_volume_in'),
            pl.col(f'amount0_out').sum().alias(f'amount0_volume_out'),
            pl.col(f'amount1_in').sum().alias(f'amount1_volume_in'),
            pl.col(f'amount1_out').sum().alias(f'amount1_volume_out'),
        ]
    )

    delta_df = volume_df.with_columns(
        (pl.col(f'amount0_volume_out') - pl.col(f'amount0_volume_in')).alias(
            f'delta_amount0_out'
        ),
        (pl.col(f'amount1_volume_out') - pl.col(f'amount1_volume_in')).alias(
            f'delta_amount1_out'
        ),
    ).select('datetime', 'delta_amount0_out', 'delta_amount1_out')

    # Create a cumulative sum of the delta volumes (in and out) of token0 and token1
    cvd_df = delta_df.with_columns(
        pl.col(f'delta_amount0_out')
        .cumsum()
        .alias(f'cumulative_delta_amount0_out'),
        pl.col(f'delta_amount1_out')
        .cumsum()
        .alias(f'cumulative_delta_amount1_out'),
    ).select(
        'datetime',
        'cumulative_delta_amount0_out',
        'cumulative_delta_amount1_out',
    )

    with duckdb.get_connection() as con:

        # volume table
        con.register('volume_df_view', volume_df)
        con.execute('CREATE TABLE IF NOT EXISTS volume AS SELECT * FROM volume_df_view')

        # delta volume table
        con.register('delta_df_view', delta_df)
        con.execute('CREATE TABLE IF NOT EXISTS delta_volume AS SELECT * FROM delta_df_view')

        # cumulative volume delta (cvd) table
        con.register('cvd_df_view', cvd_df)
        con.execute('CREATE TABLE IF NOT EXISTS cvd AS SELECT * FROM cvd_df_view')


@asset(
    deps=['tvl', 'volume_metrics'], compute_kind='polars', group_name='metrics'
)
def fees(duckdb: DuckDBResource):

    with duckdb.get_connection() as con:

        fee_rate_result = con.execute('SELECT fee_rate FROM pool_data')
        fee_rate = fee_rate_result.fetchone()[0]

        volume_df = con.execute('SELECT * FROM volume').pl()

        # Calculate fees
        fees_df = volume_df.with_columns(
            (pl.col('amount0_volume_in') * fee_rate).alias(f'fee_amount0'),
            (pl.col('amount1_volume_in') * fee_rate).alias(f'fee_amount1'),
        ).select('datetime', 'fee_amount0', 'fee_amount1')

        con.register('fees_df_view', fees_df)
        con.execute('CREATE TABLE IF NOT EXISTS fees AS SELECT * FROM fees_df_view')

    return fees_df


@asset(compute_kind='polars', group_name='metrics')
def yield_metric(fees, duckdb: DuckDBResource):

    with duckdb.get_connection() as con:

        tvl_df = con.execute('SELECT * from tvl').pl()

        # join fees and tvl dfs
        yield_df = fees.join(tvl_df, on='datetime')

        # calculate yield
        yield_df = yield_df.with_columns(
            (pl.col('fee_amount0') / pl.col('amount0_tvl')).alias(
                'yield_amount0'
            ),
            (pl.col('fee_amount1') / pl.col('amount1_tvl')).alias(
                'yield_amount1'
            ),
        ).select('datetime', 'yield_amount0', 'yield_amount1')

        con.register('yield_df_view', yield_df)
        con.execute('CREATE TABLE IF NOT EXISTS yield AS SELECT * FROM yield_df_view')


@asset(compute_kind='polars', group_name='metrics', 
       deps=[swap_decoded, burn_decoded,mint_decoded, flash_decoded, collect_decoded])
def price(pool_constants, duckdb: DuckDBResource):

    swap_df = pl.read_parquet(
        file_relative_path(__file__, '../data/decoded/swap.parquet')
    )

    price_df = swap_df.select('datetime', 'tick')
    price_df = price_df.with_columns(
        pl.col('tick')
        .map(
            lambda x: tick_to_price_adjusted(
                tick=x,
                decimals0=pool_constants['decimals0'],
                decimals1=pool_constants['decimals0'],
            )
        )
        .alias('price'),
    )

    with duckdb.get_connection() as con:
        con.register('price_df_view', price_df)
        con.execute('CREATE TABLE IF NOT EXISTS price AS SELECT * FROM price_df_view')
