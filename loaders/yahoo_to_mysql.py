# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 18:27:55 2026

@author: 박승욱
"""

# yahoo_to_mysql.py
# Yahoo Finance (yfinance) -> MySQL
#
# 목적
# 1) KOSPI / WTI / USDKRW:
#    - 기존 CSV 적재분 + Yahoo 최신분을 같은 series_code로 연결 저장
# 2) DXY:
#    - Yahoo 단일 source로 전체 기간 적재
# 3) S&P500:
#    - Yahoo 단일 source로 전체 기간 적재
#
# 핵심 원칙
# - DB 이후 구조(loading_common)는 그대로 재사용
# - series_code는 프로젝트에서 일관되게 유지
# - save 전에 기존 DB 히스토리를 다시 읽어와 Yahoo 데이터와 합친 뒤 저장
#   -> series_meta.start_date / end_date가 과거 구간을 잃지 않도록 보호

from pathlib import Path
import sys

import pandas as pd
import yfinance as yf
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db import engine
from loading_common import (
    get_source_id,
    build_series_payload,
    save_series_payload,
    check_result,
)

BASE_DIR = PROJECT_ROOT
SOURCE_CODE = "YAHOO"


# --------------------------------------------------
# 처리 대상 설정
# --------------------------------------------------
SERIES_CONFIG = [
    {
        "ticker": "^KS11",
        "start_date": "1996-12-11",
        "series_info": {
            "series_code": "KOSPI",
            "series_name": "KOSPI Index",
            "category_name": "국내주식",
            "frequency": "daily",
            "unit": "index",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "Yahoo Finance Close 적재 (CSV 과거구간 + Yahoo 최신구간 결합)",
            "source_series_code": "^KS11",
            "source_series_name": "KOSPI Index",
            "transform_code": "yahoo_close",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "index",
        },
    },
    {
        "ticker": "^GSPC",
        "start_date": "1950-01-03",
        "series_info": {
            "series_code": "SP500_YF",
            "series_name": "S&P 500",
            "category_name": "미국주식",
            "frequency": "daily",
            "unit": "index",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "Yahoo Finance Close 적재 (Yahoo 단일 source)",
            "source_series_code": "^GSPC",
            "source_series_name": "S&P 500",
            "transform_code": "yahoo_close",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "index",
        },
    },
    {
        "ticker": "CL=F",
        "start_date": "2000-08-23",
        "series_info": {
            "series_code": "WTI",
            "series_name": "Crude Oil WTI",
            "category_name": "원자재",
            "frequency": "daily",
            "unit": "usd_per_bbl",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "Yahoo Finance Close 적재 (CSV 과거구간 + Yahoo 최신구간 결합)",
            "source_series_code": "CL=F",
            "source_series_name": "Crude Oil WTI Futures",
            "transform_code": "yahoo_close",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "USD per barrel",
        },
    },
    {
        "ticker": "KRW=X",
        "start_date": "2003-12-01",
        "series_info": {
            "series_code": "USDKRW",
            "series_name": "USD/KRW",
            "category_name": "환율",
            "frequency": "daily",
            "unit": "krw_per_usd",
            "chart_type": "line",
            "default_axis": "right",
            "default_color": None,
            "is_recession_series": False,
            "notes": "Yahoo Finance Close 적재 (CSV 과거구간 + Yahoo 최신구간 결합)",
            "source_series_code": "KRW=X",
            "source_series_name": "USD/KRW",
            "transform_code": "yahoo_close",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "KRW per USD",
        },
    },
    {
        "ticker": "DX-Y.NYB",
        "start_date": "1971-01-04",
        "series_info": {
            "series_code": "DXY",
            "series_name": "US Dollar Index",
            "category_name": "환율",
            "frequency": "daily",
            "unit": "index",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "Yahoo Finance Close 적재 (Yahoo 단일 source)",
            "source_series_code": "DX-Y.NYB",
            "source_series_name": "US Dollar Index",
            "transform_code": "yahoo_close",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "index",
        },
    },
]


# --------------------------------------------------
# Yahoo 다운로드
# --------------------------------------------------
def normalize_yahoo_columns(df):
    """
    yfinance 결과 컬럼이 일반 Index일 수도 있고 MultiIndex일 수도 있으므로
    둘 다 처리 가능하게 정리
    """
    if df is None or df.empty:
        return pd.DataFrame()

    result = df.copy()

    if isinstance(result.columns, pd.MultiIndex):
        # 예: ('Close', '^KS11') 형태
        if "Close" in result.columns.get_level_values(0):
            result = result["Close"]
            if isinstance(result, pd.DataFrame):
                # 단일 ticker라도 DataFrame일 수 있으니 첫 컬럼 사용
                result = result.iloc[:, 0]
            result = result.to_frame(name="Close")
        else:
            raise ValueError("Yahoo 데이터에 Close 컬럼이 없습니다.")
    else:
        if "Close" not in result.columns:
            raise ValueError("Yahoo 데이터에 Close 컬럼이 없습니다.")
        result = result[["Close"]].copy()

    return result


def download_yahoo_close_df(ticker, start_date):
    raw_df = yf.download(
        tickers=ticker,
        start=start_date,
        interval="1d",
        auto_adjust=False,
        progress=False,
    )

    raw_df = normalize_yahoo_columns(raw_df)

    if raw_df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = raw_df.reset_index().rename(columns={"Date": "date", "Close": "value"})

    if "date" not in df.columns or "value" not in df.columns:
        raise ValueError(f"Yahoo 응답 컬럼 확인 필요: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["date", "value"])
    df = df.drop_duplicates(subset=["date"])
    df = df.sort_values("date").reset_index(drop=True)

    return df[["date", "value"]]


# --------------------------------------------------
# 기존 DB 히스토리 조회, 결합
# --------------------------------------------------
def load_existing_series_history(series_code):
    query = text("""
        SELECT
            d.date_value AS date,
            d.value_num AS value
        FROM series_data d
        JOIN series_meta m
          ON d.series_id = m.series_id
        WHERE m.series_code = :series_code
        ORDER BY d.date_value
    """)

    df = pd.read_sql(query, engine, params={"series_code": series_code})

    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"])
    df = df.drop_duplicates(subset=["date"])
    df = df.sort_values("date").reset_index(drop=True)

    return df[["date", "value"]]


def merge_existing_and_yahoo(existing_df, yahoo_df):
    if existing_df is None or existing_df.empty:
        merged = yahoo_df.copy()
    else:
        merged = pd.concat([existing_df, yahoo_df], ignore_index=True)

    if merged.empty:
        return pd.DataFrame(columns=["date", "value"])

    merged["date"] = pd.to_datetime(merged["date"]).dt.date
    merged["value"] = pd.to_numeric(merged["value"], errors="coerce")

    merged = merged.dropna(subset=["date", "value"])
    merged = merged.drop_duplicates(subset=["date"], keep="last")
    merged = merged.sort_values("date").reset_index(drop=True)

    return merged[["date", "value"]]


# --------------------------------------------------
# 1개 시리즈 적재
# --------------------------------------------------
def load_one_series(source_id, config):
    ticker = config["ticker"]
    start_date = config["start_date"]
    series_info = config["series_info"]
    series_code = series_info["series_code"]

    yahoo_df = download_yahoo_close_df(
        ticker=ticker,
        start_date=start_date,
    )

    if yahoo_df.empty:
        print(f"[건너뜀] {series_code} | Yahoo 데이터 없음")
        return

    existing_df = load_existing_series_history(series_code)
    final_df = merge_existing_and_yahoo(existing_df, yahoo_df)

    payload = build_series_payload(
        series_info=series_info,
        source_id=source_id,
        data_df=final_df,
    )
    save_series_payload(payload)

    print(
        f"[완료] {series_code} | "
        f"ticker={ticker} | "
        f"저장 건수={len(final_df)} | "
        f"기간={final_df['date'].min()} ~ {final_df['date'].max()}"
    )

    check_result(series_code, limit=10)


# --------------------------------------------------
# 실행
# --------------------------------------------------
def main():
    source_id = get_source_id(SOURCE_CODE)

    for config in SERIES_CONFIG:
        try:
            print(
                f"[시작] series_code={config['series_info']['series_code']} | "
                f"ticker={config['ticker']}"
            )
            load_one_series(source_id, config)
        except Exception as e:
            print(
                f"[실패] series_code={config['series_info']['series_code']} | "
                f"ticker={config['ticker']} | 오류={e}"
            )


if __name__ == "__main__":
    main()