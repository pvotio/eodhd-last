#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Real‑time quotes → Azure SQL
 - one persistent DB connection per worker thread
 - global token‑bucket rate limiter
 
Change REQUESTS_PER_SEC below to suit your EODHD plan.

This version hardens timestamp handling so that the job no longer crashes
when the upstream API returns the value as a string (or an empty/invalid
value). It also swaps the deprecated ``datetime.utcfromtimestamp`` for the
timezone‑aware ``datetime.fromtimestamp(..., timezone.utc)``.
"""

from __future__ import annotations

import logging
import os
import struct
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote_plus

import pandas as pd
import pyodbc
import requests
from azure.identity import DefaultAzureCredential
from sqlalchemy import create_engine

# --------------------------------------------------------------------------- #
#               ──  EDIT THIS VALUE TO THROTTLE THE API  ──
REQUESTS_PER_SEC: int = 50  # allowed outbound requests per second
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)

# --------------------------------------------------------------------------- #
# Token‑bucket rate limiter
# --------------------------------------------------------------------------- #

def start_rate_limiter(rps: int) -> threading.Semaphore:
    """Return a semaphore that allows at most *rps* acquisitions per second."""
    bucket = threading.BoundedSemaphore(rps)

    def refill() -> None:
        while True:
            time.sleep(1)
            # keep releasing until the bucket is full
            while bucket._value < rps:  # pylint: disable=protected-access
                try:
                    bucket.release()
                except ValueError:
                    break

    threading.Thread(target=refill, daemon=True).start()
    return bucket


# --------------------------------------------------------------------------- #
# Thread‑local SQL connection helper
# --------------------------------------------------------------------------- #

thread_local = threading.local()


def get_thread_conn(conn_str: str, attrs: dict[str, Any]) -> pyodbc.Connection:  # type: ignore[name‑defined]
    """Give each worker thread its own persistent pyodbc connection."""
    if not hasattr(thread_local, "conn"):
        logging.debug("Opening SQL connection in %s", threading.current_thread().name)
        thread_local.conn = pyodbc.connect(  # type: ignore[attr‑defined]
            conn_str,
            attrs_before=attrs,
            autocommit=False,
            timeout=5,
        )
    return thread_local.conn  # type: ignore[return‑value]


# --------------------------------------------------------------------------- #
# Azure‑AD token decoration for pyodbc
# --------------------------------------------------------------------------- #

def make_attrs(access_token: str) -> dict[int, bytes]:
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # pyodbc constant (undocumented)
    enc = access_token.encode("utf-16-le")
    token_struct = struct.pack("=i", len(enc)) + enc
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}


# --------------------------------------------------------------------------- #
# Safe timestamp parser
# --------------------------------------------------------------------------- #

def parse_epoch_to_utc(ts_raw: Any) -> Optional[datetime]:
    """Convert anything resembling an epoch timestamp to UTC ``datetime``.

    Returns *None* if the value is missing or malformed.
    """
    try:
        if ts_raw is None:
            return None
        ts_int = int(ts_raw)
        return datetime.fromtimestamp(ts_int, timezone.utc)
    except (ValueError, TypeError, OSError):
        # ValueError → cannot cast / wrong base‑10 input
        # TypeError  → non‑castable type (dict, list, etc.)
        # OSError    → out‑of‑range on some platforms
        return None


# --------------------------------------------------------------------------- #
# HTTP helper (rate‑limited)
# --------------------------------------------------------------------------- #

def fetch_realtime_data(
    ticker: str,
    api_token: str,
    rate_sem: threading.Semaphore,
    max_retries: int = 3,
    session: Optional[requests.Session] = None,
) -> Optional[dict[str, Any]]:
    url = f"https://eodhd.com/api/real-time/{ticker}"
    params = {"api_token": api_token, "fmt": "json"}

    session = session or requests

    for attempt in range(1, max_retries + 1):
        rate_sem.acquire()
        try:
            resp = session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.HTTPError as he:  # type: ignore[attr‑defined]
            if he.response is not None and he.response.status_code == 429 and attempt < max_retries:
                retry_after_hdr = he.response.headers.get("Retry-After")
                try:
                    wait = max(1, int(retry_after_hdr))
                except (TypeError, ValueError):
                    wait = 5
                logging.warning("%s → 429, waiting %ds (retry %d/%d)", ticker, wait, attempt, max_retries)
                time.sleep(wait)
                continue
            status = he.response.status_code if he.response is not None else "N/A"
            logging.error("%s → HTTP %s", ticker, status)
            return None

        except Exception as e:  # pylint: disable=broad‑except
            logging.error("%s → network error: %s", ticker, e)
            return None

    return None


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> None:
    # ---------- required env vars ----------
    db_server = os.getenv("DB_SERVER")
    db_name = os.getenv("DB_NAME")
    api_token = os.getenv("EODHD_API_TOKEN")
    target_table = os.getenv("TARGET_TABLE")
    ticker_sql = os.getenv("TICKER_SQL")

    if not all((db_server, db_name, api_token, target_table, ticker_sql)):
        logging.critical("Missing one or more required environment variables.")
        sys.exit(1)

    # ---------- global rate limiter ----------
    rate_sem = start_rate_limiter(REQUESTS_PER_SEC)
    logging.info("Global rate limit set to %d request(s) per second.", REQUESTS_PER_SEC)

    # ---------- Azure AD access token ----------
    cred = DefaultAzureCredential()
    token = cred.get_token("https://database.windows.net/.default")
    attrs = make_attrs(token.token)

    # ---------- connection strings ----------
    odbc_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db_server};"
        f"DATABASE={db_name};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}",
        connect_args={"attrs_before": attrs},
    )

    # ---------- fetch tickers ----------
    df = pd.read_sql(ticker_sql, engine)
    tickers = df.iloc[:, 0].dropna().tolist()
    if not tickers:
        logging.warning("No tickers to process — exiting.")
        return
    logging.info("Fetched %d tickers.", len(tickers))

    # ---------- clear target table ----------
    logging.info("Clearing %s ...", target_table)
    with pyodbc.connect(odbc_str, attrs_before=attrs) as conn:  # type: ignore[arg‑type]
        conn.execute(f"DELETE FROM {target_table};")
        conn.commit()

    # ---------- prepared insert ----------
    insert_sql = f"""
    INSERT INTO {target_table} (
        ext2_ticker,
        [open], high, low, [close], volume,
        currency,
        timestamp_created_utc,
        timestamp_read_utc
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    session = requests.Session()

    # ---------- worker function ----------
    def worker(ticker: str) -> int:
        data = fetch_realtime_data(ticker, api_token, rate_sem, session=session)
        if not data:
            return 0

        now = datetime.now(timezone.utc)
        read = parse_epoch_to_utc(data.get("timestamp")) or now

        row = (
            ticker,
            data.get("open"),
            data.get("high"),
            data.get("low"),
            data.get("close"),
            data.get("volume"),
            None,  # currency → NULL
            now,
            read,
        )

        try:
            conn = get_thread_conn(odbc_str, attrs)
            cur = conn.cursor()
            cur.execute(insert_sql, row)
            conn.commit()
            return 1
        except Exception as e:  # pylint: disable=broad‑except
            logging.error("Insert failed for %s: %s", ticker, e)
            return 0

    # ---------- parallel execution ----------
    max_workers = 25
    total = 0
    logging.info("Processing %d tickers with %d workers ...", len(tickers), max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futs = {exe.submit(worker, t): t for t in tickers}
        for fut in as_completed(futs):
            total += fut.result()

    logging.info("Done. %d rows inserted into %s.", total, target_table)


# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    try:
        main()
    except Exception as err:  # pylint: disable=broad‑except
        logging.critical("Fatal: %s", err)
        sys.exit(1)
