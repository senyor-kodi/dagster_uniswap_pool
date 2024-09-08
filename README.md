# dagster_uniswap

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

This project is designed to analyze a Uniswap pool, from extracting on-chain data, decoding it, storing it, to creating metrics. 

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `dagster_uniswap/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Tools and Resources

The following tools and resources are utilized in this project:

- [Cryo](https://github.com/paradigmxyz/cryo): A tool by Paradigm written in Rust that enables fast extraction of data.
- [Check-the-chain](https://github.com/checkthechain/checkthechain): Used for data decoding.
- [Quicknode](https://www.quicknode.com/): Provides access to RPC endpoint.
- [DuckDB](https://duckdb.org/): Serves as the database for data storage.
- [Polars](https://www.pola.rs/): go-to Python library for data manipulation and creating metrics.
- [Dagster](https://dagster.io/): Used for orchestration.
- [The Graph](https://thegraph.com/): indexing protocol for blockchain data. Specifically, the Uniswap [endpoint](https://thegraph.com/hosted-service/subgraph/uniswap/uniswap-v3).  

## Code Explanation and Modification

The code extracts all blocks and event logs for events swap, burn, collect, mint, and flash from Uniswap v3 events for a defined address. Currently, the address is set to `0x11b815efB8f581194ae79006d24E0d814B7697F6` for the WETH/USDT 0.05% pool. 

![dagster_uniswap_assets](https://github.com/monsieur-calcifer/dagster_uniswap_pool/assets/149976181/c714ce3a-57d1-40b8-a0f7-a6e7c8e99dc4)

Assets are separated into three groups: raw_data, prepared_data, metrics. 

- raw_data: extract all blocks and event logs events swap, burn, collect, mint, and flash from Uniswap v3 events via cryo; download pool info and tick data from the Graph Uniswap endpoint.
- prepared_data: decode raw event logs into readable format with check-the-chain; loads decoded data into DuckDB database.
- metrics: compute metrics and insights from prepared data. 

The start block and end block should be defined in the `constants.py` file. Some metrics are cumulative, so if you want to study a pool, you'd need to specify the `START_BLOCK` for when the pool was created. 

To modify the code for your own use, simply change the address, start block, and end block in the `constants.py` file to match the pool you wish to analyze.

## Future Enhancements

Plans for future enhancements include:

- Introduction of a BI tool or similar to display metrics data.
- Addition of more metrics, particularly related to the analysis of liquidity and liquidity providers on pools.
- Addition of capabilities to run a job to update data on a regular basis.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `dagster_uniswap_tests` directory and you can run tests using `pytest`:

```bash
pytest dagster_uniswap_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.
