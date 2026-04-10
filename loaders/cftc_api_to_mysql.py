# -*- coding: utf-8 -*-
"""
Created on Sat Apr  4 21:29:59 2026

@author: 박승욱
"""

# cftc_api_to_mysql.py

import os
import sys
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loading_common import (
    get_source_id,
    build_series_payload,
    save_series_payload,
    check_result,
)

# --------------------------------------------------
# 환경설정
# --------------------------------------------------
BASE_DIR = PROJECT_ROOT
load_dotenv(BASE_DIR / ".env")

CFTC_APP_TOKEN = os.getenv("CFTC_APP_TOKEN", "").strip()

SOURCE_CODE = "CFTC"
BASE_URL = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"

REQUEST_TIMEOUT_SECONDS = 30
REQUEST_LIMIT = 50000

# --------------------------------------------------
# stitch 대상 구간
# 사용자가 직접 검증한 market_name / market_code 순서
# --------------------------------------------------
SEGMENTS = [
    {
        "segment_name": "NYCE_098661",
        "market_and_exchange_names": "U.S. DOLLAR INDEX - NEW YORK COTTON EXCHANGE",
        "cftc_contract_market_code": "098661",
        "expected_min_date": "1986-01-15",
        "expected_max_date": "1992-05-15",
    },
    {
        "segment_name": "NYCE_098662",
        "market_and_exchange_names": "U.S. DOLLAR INDEX - NEW YORK COTTON EXCHANGE",
        "cftc_contract_market_code": "098662",
        "expected_min_date": "1992-05-29",
        "expected_max_date": "2004-12-28",
    },
    {
        "segment_name": "NYBOT_098662",
        "market_and_exchange_names": "U.S. DOLLAR INDEX - NEW YORK BOARD OF TRADE",
        "cftc_contract_market_code": "098662",
        "expected_min_date": "2005-01-04",
        "expected_max_date": "2007-08-28",
    },
    {
        "segment_name": "ICE_US_098662_OLD_LABEL",
        "market_and_exchange_names": "U.S. DOLLAR INDEX - ICE FUTURES U.S.",
        "cftc_contract_market_code": "098662",
        "expected_min_date": "2007-09-04",
        "expected_max_date": "2022-02-01",
    },
    {
        "segment_name": "ICE_US_098662_NEW_LABEL",
        "market_and_exchange_names": "USD INDEX - ICE FUTURES U.S.",
        "cftc_contract_market_code": "098662",
        "expected_min_date": "2022-02-08",
        "expected_max_date": None,
    },
]

SERIES_INFO = {
    "series_code": "CFTC_USD_INDEX_NONCOMM_NET_PCT",
    "series_name": "CFTC 달러화 투기 순포지션 거래 비중",
    "category_name": "포지션",
    "frequency": "weekly",
    "unit": "%",
    "chart_type": "line",
    "default_axis": "right",
    "default_color": None,
    "is_recession_series": False,
    "notes": (
        "CFTC Current Legacy Report API stitch 적재 | "
        "dataset=6dca-aqww | "
        "segments=NYCE(098661) -> NYCE(098662) -> NYBOT(098662) -> "
        "ICE U.S. old label(098662) -> ICE U.S. new label(098662) | "
        "Net% = Pct_of_OI_NonComm_Long_All - Pct_of_OI_NonComm_Short_All"
    ),
    "source_series_code": (
        "6dca-aqww:"
        "NYCE_098661+NYCE_098662+NYBOT_098662+ICE_US_098662_OLD+ICE_US_098662_NEW"
    ),
    "source_series_name": "U.S. Dollar Index | Non-Commercial Net Position % | Stitched",
    "transform_code": "cftc_api_stitch",
    "transform_name": "RAW",
    "is_transformed": False,
    "source_unit": "%",
}


# --------------------------------------------------
# 공통 유틸
# --------------------------------------------------
def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_date(value):
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()


def build_headers():
    if not CFTC_APP_TOKEN:
        raise ValueError(".env 파일에 CFTC_APP_TOKEN이 없습니다.")

    return {
        "X-App-Token": CFTC_APP_TOKEN,
    }


# --------------------------------------------------
# API 호출
# --------------------------------------------------
def build_segment_params(segment):
    market_name = segment["market_and_exchange_names"]
    market_code = segment["cftc_contract_market_code"]

    return {
        "$select": (
            "report_date_as_yyyy_mm_dd as date,"
            "market_and_exchange_names,"
            "cftc_contract_market_code,"
            "pct_of_oi_noncomm_long_all as noncomm_long_pct,"
            "pct_of_oi_noncomm_short_all as noncomm_short_pct,"
            "(pct_of_oi_noncomm_long_all - pct_of_oi_noncomm_short_all) as noncomm_net_pct"
        ),
        "$where": (
            f"market_and_exchange_names='{market_name}' "
            f"AND cftc_contract_market_code='{market_code}'"
        ),
        "$order": "report_date_as_yyyy_mm_dd ASC",
        "$limit": str(REQUEST_LIMIT),
    }


def fetch_segment_rows(segment):
    response = requests.get(
        BASE_URL,
        params=build_segment_params(segment),
        headers=build_headers(),
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise ValueError(
            f"예상과 다른 JSON 구조입니다. "
            f"segment={segment['segment_name']} | type={type(data)}"
        )

    return data


# --------------------------------------------------
# 데이터 변환 / 검증
# --------------------------------------------------
def build_segment_df(segment, rows):
    result_rows = []

    for row in rows:
        date_value = parse_date(row.get("date"))
        value_num = pd.to_numeric(row.get("noncomm_net_pct"), errors="coerce")

        if date_value is None:
            continue
        if pd.isna(value_num):
            continue

        result_rows.append(
            {
                "date": date_value,
                "value": float(value_num),
                "market_and_exchange_names": clean_str(row.get("market_and_exchange_names")),
                "cftc_contract_market_code": clean_str(row.get("cftc_contract_market_code")),
                "segment_name": segment["segment_name"],
            }
        )

    df = pd.DataFrame(result_rows)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "value",
                "market_and_exchange_names",
                "cftc_contract_market_code",
                "segment_name",
            ]
        )

    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)

    return df


def validate_segment_range(segment, df):
    if df.empty:
        raise ValueError(f"segment 데이터 없음: {segment['segment_name']}")

    actual_min = str(df["date"].min())
    actual_max = str(df["date"].max())

    expected_min = clean_str(segment.get("expected_min_date"))
    expected_max = clean_str(segment.get("expected_max_date"))

    if expected_min and actual_min != expected_min:
        raise ValueError(
            f"segment 최소날짜 불일치 | {segment['segment_name']} | "
            f"expected_min={expected_min} | actual_min={actual_min}"
        )

    if expected_max and actual_max != expected_max:
        raise ValueError(
            f"segment 최대날짜 불일치 | {segment['segment_name']} | "
            f"expected_max={expected_max} | actual_max={actual_max}"
        )

    print(
        f"[검증완료] {segment['segment_name']} | "
        f"min={actual_min} | max={actual_max} | rows={len(df)}"
    )


def stitch_all_segments(segment_dfs):
    if not segment_dfs:
        return pd.DataFrame(columns=["date", "value"])

    merged = pd.concat(segment_dfs, ignore_index=True)

    if merged.empty:
        return pd.DataFrame(columns=["date", "value"])

    merged["date"] = pd.to_datetime(merged["date"]).dt.date
    merged["value"] = pd.to_numeric(merged["value"], errors="coerce")

    merged = merged.dropna(subset=["date", "value"])
    merged = merged.drop_duplicates(subset=["date"], keep="last")
    merged = merged.sort_values("date").reset_index(drop=True)

    return merged[["date", "value"]]


def validate_stitched_df(stitched_df):
    if stitched_df.empty:
        raise ValueError("stitch 결과가 비어 있습니다.")

    stitched_min = str(stitched_df["date"].min())
    stitched_max = str(stitched_df["date"].max())

    print(
        f"[stitch완료] 전체 기간={stitched_min} ~ {stitched_max} | "
        f"rows={len(stitched_df)}"
    )


# --------------------------------------------------
# 실행
# --------------------------------------------------
def main():
    source_id = get_source_id(SOURCE_CODE)

    segment_dfs = []

    for segment in SEGMENTS:
        rows = fetch_segment_rows(segment)
        df = build_segment_df(segment, rows)
        validate_segment_range(segment, df)
        segment_dfs.append(df)

    stitched_df = stitch_all_segments(segment_dfs)
    validate_stitched_df(stitched_df)

    payload = build_series_payload(
        series_info=SERIES_INFO,
        source_id=source_id,
        data_df=stitched_df,
    )
    save_series_payload(payload)

    print(
        f"[완료] {payload['series_code']} | "
        f"source={SOURCE_CODE} | "
        f"저장 건수={len(stitched_df)} | "
        f"기간={stitched_df['date'].min()} ~ {stitched_df['date'].max()}"
    )

    check_result(payload["series_code"], limit=10)


if __name__ == "__main__":
    main()