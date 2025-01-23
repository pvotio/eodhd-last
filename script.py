import os
import sys
import struct
import logging
import requests
import pyodbc
import pandas as pd
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_pyodbc_attrs(access_token: str) -> dict:
    """
    Format the Azure AD access token for pyodbc's SQL_COPT_SS_ACCESS_TOKEN.
    """
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    enc_token = access_token.encode('utf-16-le')
    token_struct = struct.pack('=i', len(enc_token)) + enc_token
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}

def get_tickers(engine, ticker_sql):
    """
    Fetch a list of tickers from the database using the specified SQL query.
    Assumes the first column in the result set is the ticker.
    """
    if not ticker_sql:
        logging.error("TICKER_SQL environment variable not set. Exiting.")
        sys.exit(1)

    try:
        df = pd.read_sql(ticker_sql, engine)
        tickers = df.iloc[:, 0].dropna().tolist()
        logging.info(f"Fetched {len(tickers)} tickers from the database.")
        return tickers
    except Exception as e:
        logging.error(f"Error fetching tickers: {e}")
        sys.exit(1)

def fetch_realtime_data(ticker, api_token):
    """
    Fetch real-time quote data from EODHD for the given ticker.
    Endpoint: https://eodhd.com/api/real-time/{Ticker}?api_token={api_token}&fmt=json
    Expected to return a JSON with at least the fields: open, high, low, close, volume, etc.
    """
    url = f"https://eodhd.com/api/real-time/{ticker}"
    params = {
        "api_token": api_token,
        "fmt": "json"
    }
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        # data should be a dictionary like:
        # {
        #   "code": "AAPL",
        #   "timestamp": 1677097200,
        #   "gmtoffset": -18000,
        #   "open": 154.65,
        #   "close": 155.00,
        #   "high": 156.1,
        #   "low": 153.8,
        #   "volume": 12345678,
        #   "currency": "USD",
        #   ...
        # }
        return data
    except Exception as e:
        logging.error(f"Error fetching real-time data for ticker '{ticker}': {e}")
        return None

def main():
    # 1) Load environment variables (make sure to set these in your environment)
    db_server    = os.getenv("DB_SERVER")
    db_name      = os.getenv("DB_NAME")
    api_token    = os.getenv("EODHD_API_TOKEN")
    target_table = os.getenv("TARGET_TABLE")
    ticker_sql   = os.getenv("TICKER_SQL")

    if not (db_server and db_name and api_token and ticker_sql):
        logging.error("Missing required environment variables. Check DB_SERVER, DB_NAME, EODHD_API_TOKEN, TICKER_SQL.")
        sys.exit(1)

    # 2) Obtain Azure AD token using DefaultAzureCredential
    logging.info("Obtaining Azure AD token via DefaultAzureCredential()...")
    try:
        credential = DefaultAzureCredential()
        token = credential.get_token("https://database.windows.net/.default")
        access_token = token.token
        logging.info("Successfully obtained access token for SQL Database.")
    except Exception as e:
        logging.error(f"Failed to obtain access token for SQL Database: {e}")
        sys.exit(1)

    # 3) Build SQLAlchemy engine for reading Tickers
    attrs = get_pyodbc_attrs(access_token)
    odbc_connection_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db_server};"
        f"DATABASE={db_name};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connection_str)}",
        connect_args={'attrs_before': attrs}
    )

    # 4) Fetch the tickers from the DB
    tickers = get_tickers(engine, ticker_sql)
    if not tickers:
        logging.info("No tickers found. Exiting.")
        return

    # 5) Delete existing rows from the target table
    logging.info(f"Deleting existing rows from {target_table}...")
    delete_sql = f"DELETE FROM {target_table};"
    try:
        with pyodbc.connect(odbc_connection_str, attrs_before=attrs) as conn:
            cursor = conn.cursor()
            cursor.execute(delete_sql)
            conn.commit()
        logging.info(f"All rows deleted from {target_table}.")
    except Exception as ex:
        logging.error(f"Failed to delete rows from {target_table}: {ex}")
        sys.exit(1)

    # 6) Prepare INSERT statement
    # Matches your table schema:
    # ext2_ticker, open, high, low, close, volume, currency,
    # timestamp_created_utc, timestamp_read_utc
    insert_sql = f"""
    INSERT INTO {target_table} (
        ext2_ticker,
        [open],
        high,
        low,
        [close],
        volume,
        currency,
        timestamp_created_utc,
        timestamp_read_utc
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def fetch_and_insert_one(ticker):
        """
        Fetch real-time data for a single ticker and insert one row into the database.
        """
        data = fetch_realtime_data(ticker, api_token)
        if not data:
            return 0  # no data or error

        now_utc = datetime.now(timezone.utc)

        # If there's a timestamp in the JSON, convert it; otherwise default to now.
        # Often "timestamp" is an epoch in seconds. Adjust if needed.
        # If the JSON doesn't contain 'timestamp', fall back to now_utc.
        epoch_ts = data.get("timestamp")
        if epoch_ts:
            read_time = datetime.utcfromtimestamp(epoch_ts)
        else:
            read_time = now_utc

        row_tuple = (
            ticker,
            data.get("open"),
            data.get("high"),
            data.get("low"),
            data.get("close"),
            data.get("volume"),
            data.get("currency", ""),  # default to empty if not present
            now_utc,
            read_time
        )

        # Insert into DB
        try:
            with pyodbc.connect(odbc_connection_str, attrs_before=attrs) as conn:
                cursor = conn.cursor()
                cursor.fast_executemany = True
                cursor.execute(insert_sql, row_tuple)
                conn.commit()
            return 1
        except Exception as e:
            logging.error(f"Error inserting data for ticker '{ticker}': {e}")
            return 0

    # 7) Parallel fetch + insert
    logging.info(f"Fetching & inserting real-time data for {len(tickers)} tickers...")
    total_inserted = 0
    max_workers = 25  # adjust as needed

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(fetch_and_insert_one, t): t for t in tickers}
        for future in as_completed(future_map):
            ticker = future_map[future]
            try:
                inserted = future.result()
                total_inserted += inserted
                logging.info(f"{ticker}: Inserted {inserted} row(s). Running total: {total_inserted}")
            except Exception as exc:
                logging.error(f"{ticker} failed: {exc}")

    logging.info(f"All tickers completed. Total rows inserted: {total_inserted}")
    logging.info("Script completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Script terminated with an error: {e}")
        sys.exit(1)
