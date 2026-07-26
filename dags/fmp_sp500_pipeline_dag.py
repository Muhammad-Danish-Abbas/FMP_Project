"""
Airflow Dag FMP Data
---------------------
Task 1 -> get_sp500_symbols   : S&P 500 list Wikipedia se nikalta hai
Task 2 -> hit_fmp_api          : un symbols ke liye FMP API call karta hai
Task 3 -> load_to_snowflake    : FMP se aya data Snowflake me load karta hai

Schedule: testing ke liye 5 min, production me "@daily" use karo.
Owner: Danish | Retries: 2 | Retry delay: 2 min
"""

from __future__ import annotations

import datetime
import json

from io import StringIO

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ---- Config (jitna ho sake, in sab ko Airflow Variables/Connections me rakho) ----
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_TABLE = "SP500_STOCK_DATA"

# testing ke liye sirf 2 symbols; production me is limit ko hata dena
TEST_SYMBOL_LIMIT = 2

default_args = {
    "owner": "Danish",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=2),
}


@dag(
    dag_id="fmp_sp500_pipeline",
    description="Airflow Dag FMP Data",
    schedule="*/5 * * * *",  # testing; production me "@daily" kar dena
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fmp", "sp500", "snowflake"],
)
def fmp_sp500_pipeline():

    @task
    def get_sp500_symbols() -> list[str]:
        """Task 1: Wikipedia se S&P 500 symbols nikalna."""
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {"User-Agent": "Mozilla/5.0 (compatible; AirflowFMPProject/1.0)"}
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text))
        symbols_df = tables[0]
        symbols = symbols_df["Symbol"].tolist()
        return symbols[:TEST_SYMBOL_LIMIT]

    @task
    def hit_fmp_api(symbols: list[str]) -> list[dict]:
        """Task 2: har symbol ke liye FMP profile API hit karna."""
        api_key = Variable.get("fmp_api_key")  # Airflow Variable me set karo, code me kabhi mat likho
        all_profiles: list[dict] = []
        for symbol in symbols:
            url = "https://financialmodelingprep.com/stable/profile"
            resp = requests.get(
                url, params={"symbol": symbol, "apikey": api_key}, timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                all_profiles.extend(data)
            else:
                all_profiles.append(data)
        return all_profiles

    @task
    def load_to_snowflake(profiles: list[dict]):
        """Task 3: FMP se aya profile data Snowflake table me insert karna."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            for record in profiles:
                cur.execute(
                    f"""
                    INSERT INTO {SNOWFLAKE_TABLE}
                        (symbol, company_name, price, raw_json, loaded_at)
                    SELECT %s, %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP()
                    """,
                    (
                        record.get("symbol"),
                        record.get("companyName"),
                        record.get("price"),
                        json.dumps(record),
                    ),
                )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    symbols = get_sp500_symbols()
    profiles = hit_fmp_api(symbols)
    load_to_snowflake(profiles)


fmp_sp500_pipeline()
