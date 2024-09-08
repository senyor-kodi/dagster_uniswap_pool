from ..constants import TICK_BASE
from collections import defaultdict
import math

import pandas as pd

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

pool_query_historical = """query get_pool_historical($pool_id: ID!, $block_number: Int!) {
  pools(where: {id: $pool_id}, block: {number: $block_number}) {
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
}
"""

tick_query_historical = """
query get_ticks($num_skip: Int, $pool_id: ID!, $block_number: Int!) {
  ticks(where: {pool: $pool_id}, block: {number: $block_number}, skip: $num_skip) {
    tickIdx
    liquidityNet
  }
}
"""

def tick_to_price(tick):
    return TICK_BASE ** tick

def tick_to_price_adjusted(tick, decimals0, decimals1):
    return (10 ** (decimals1 - decimals0)) / (TICK_BASE**tick)


def fee_tier_to_tick_spacing(fee_tier):
    return {500: 10, 3000: 60, 10000: 200}.get(fee_tier, 60)


def get_historical_liquidity(client, pool_id: str, block_number: int):

    variables = {'pool_id': pool_id, 'block_number': block_number}
    response = client.execute(
        gql(pool_query_historical), variable_values=variables
    )

    pool = response['pools'][0]
    current_tick = int(pool['tick'])
    ft = int(pool['feeTier'])
    tick_spacing = fee_tier_to_tick_spacing(ft)

    token0 = pool['token0']['symbol']
    token1 = pool['token1']['symbol']
    decimals0 = int(pool['token0']['decimals'])
    decimals1 = int(pool['token1']['decimals'])

    tick_mapping = {}
    num_skip = 0
    while True:
        variables = {
            'num_skip': num_skip,
            'pool_id': pool_id,
            'block_number': block_number,
        }
        response = client.execute(
            gql(tick_query_historical), variable_values=variables
        )
        if len(response['ticks']) == 0:
            break
        num_skip += len(response['ticks'])
        for item in response['ticks']:
            tick_mapping[int(item['tickIdx'])] = int(item['liquidityNet'])

    # Start from zero; if we were iterating from the current tick, would start from the pool's total liquidity
    liquidity = 0

    # Find the boundaries of the price range
    min_tick = min(tick_mapping.keys())
    max_tick = max(tick_mapping.keys())

    # Compute the tick range
    current_range_bottom_tick = (
        math.floor(current_tick / tick_spacing) * tick_spacing
    )
    current_range_top_tick = current_range_bottom_tick + tick_spacing

    current_price = tick_to_price(tick=current_tick) # make sure decimals are correct
    adjusted_current_price = (
        1 / current_price / (10 ** (decimals1 - decimals0))
    )

    # Sum up all tokens in the pool
    total_amount0 = 0
    total_amount1 = 0

    # Guess the preferred way to display the price;
    # try to print most assets in terms of USD;
    # if that fails, try to use the price value that's above 1.0 when adjusted for decimals.
    stablecoins = [
        'USDC',
        'DAI',
        'USDT',
        'TUSD',
        'LUSD',
        'BUSD',
        'GUSD',
        'UST',
    ]
    if token0 in stablecoins and token1 not in stablecoins:
        invert_price = True
    elif adjusted_current_price < 1.0:
        invert_price = True
    else:
        invert_price = False

    # Iterate over the tick map starting from the bottom
    tick = min_tick
    tick_data = defaultdict(
        lambda: {'amount0': 0, 'amount1': 0, 'liquidity': 0}
    )

    while tick <= max_tick:
        liquidity_delta = tick_mapping.get(tick, 0)
        liquidity += liquidity_delta

        price = tick_to_price(tick)
        adjusted_price = price / (10 ** (decimals1 - decimals0))
        if invert_price:
            adjusted_price = 1 / adjusted_price
            tokens = '{} for {}'.format(token0, token1)
        else:
            tokens = '{} for {}'.format(token1, token0)

        should_print_tick = liquidity != 0

        # Compute square roots of prices corresponding to the bottom and top ticks
        bottom_tick = tick
        top_tick = bottom_tick + tick_spacing
        sa = tick_to_price(bottom_tick // 2)
        sb = tick_to_price(top_tick // 2)

        if tick <= current_range_bottom_tick:
            # Compute the amounts of tokens potentially in the range
            amount1 = liquidity * (sb - sa)   # eq(9) in technical note
            amount0 = amount1 / (sb * sa)   # eq(4) and eq(9) in technical note

            # Only token1 locked
            total_amount1 += amount1

            if should_print_tick:
                adjusted_amount0 = amount0 / (10**decimals0)
                adjusted_amount1 = amount1 / (10**decimals1)

                tick_data[tick]['amount1'] += adjusted_amount1
                tick_data[tick]['liquidity'] += liquidity

        else:
            # Compute the amounts of tokens potentially in the range
            amount1 = liquidity * (sb - sa)
            amount0 = amount1 / (sb * sa)

            # Only token0 locked
            total_amount0 += amount0

            if should_print_tick:
                adjusted_amount0 = amount0 / (10**decimals0)
                adjusted_amount1 = amount1 / (10**decimals1)
                tick_data[tick]['amount0'] += adjusted_amount0
                tick_data[tick]['liquidity'] += liquidity

        tick += tick_spacing

    current_adjusted_price = 1 / (
        tick_to_price(current_tick) / (10 ** (decimals1 - decimals0))
    )

    return (
        tick_data,
        current_adjusted_price,
        total_amount0,
        total_amount1,
        block_number,
    )

