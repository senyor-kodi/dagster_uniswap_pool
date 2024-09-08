import os
import re
import sys

from dagster import file_relative_path, get_dagster_logger
from dagster_duckdb import DuckDBResource

duckdb_resource = DuckDBResource(
    database=file_relative_path(__file__, '../data/db/pools.db')
)
logger = get_dagster_logger()
