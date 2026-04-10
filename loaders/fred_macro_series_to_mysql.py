# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 17:52:45 2026

@author: 박승욱
"""

# 미국 macro tracker용 FRED 시계열 적재
# 목적:
# 1) 미국 경제지표 RAW 시계열을 FRED에서 가져온다
# 2) 기존 공통 저장 구조(loading_common.py)를 그대로 재사용한다
# 3) series_meta에 macro tracker용 추가 컬럼도 함께 채운다
# 4) 기존 '국내주식 Monitor' 구조에는 영향 없이 series_meta / series_data에 적재한다


import os
import sys
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
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

load_dotenv(PROJECT_ROOT / ".env")

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
SOURCE_CODE = "FRED"
REQUEST_TIMEOUT_SECONDS = 30


# --------------------------------------------------
# 1) 적재 대상 설정
# - 1차 버전은 RAW 중심
# - 이후 필요하면 series 추가
# --------------------------------------------------
SERIES_CONFIG = [
    {
        "series_id": "PAYEMS",
        "series_info": {
            "series_code": "PAYEMS",
            "series_name": "All Employees, Total Nonfarm",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "고용",
            "indicator_code": "US_PAYEMS",
            "indicator_name_ko": "비농업고용",
            "indicator_name_en": "All Employees, Total Nonfarm",
            "is_macro_tracker": True,
            "display_order": 10,
            "is_active": True,
            "frequency": "monthly",
            "unit": "thousand_persons",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 고용 지표",
            "source_series_code": "PAYEMS",
            "source_series_name": "All Employees, Total Nonfarm",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Thousands of Persons",
        },
    },
    {
        "series_id": "UNRATE",
        "series_info": {
            "series_code": "UNRATE",
            "series_name": "Unemployment Rate",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "고용",
            "indicator_code": "US_UNRATE",
            "indicator_name_ko": "실업률",
            "indicator_name_en": "Unemployment Rate",
            "is_macro_tracker": True,
            "display_order": 20,
            "is_active": True,
            "frequency": "monthly",
            "unit": "%",
            "chart_type": "line",
            "default_axis": "right",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 고용 지표",
            "source_series_code": "UNRATE",
            "source_series_name": "Unemployment Rate",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Percent",
        },
    },
    {
        "series_id": "CIVPART",
        "series_info": {
            "series_code": "CIVPART",
            "series_name": "Labor Force Participation Rate",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "고용",
            "indicator_code": "US_CIVPART",
            "indicator_name_ko": "경제활동참가율",
            "indicator_name_en": "Labor Force Participation Rate",
            "is_macro_tracker": True,
            "display_order": 30,
            "is_active": True,
            "frequency": "monthly",
            "unit": "%",
            "chart_type": "line",
            "default_axis": "right",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 고용 지표",
            "source_series_code": "CIVPART",
            "source_series_name": "Labor Force Participation Rate",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Percent",
        },
    },
    {
        "series_id": "U6RATE",
        "series_info": {
            "series_code": "U6RATE",
            "series_name": "U-6 Unemployment Rate",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "고용",
            "indicator_code": "US_U6RATE",
            "indicator_name_ko": "광의실업률(U6)",
            "indicator_name_en": "U-6 Unemployment Rate",
            "is_macro_tracker": True,
            "display_order": 40,
            "is_active": True,
            "frequency": "monthly",
            "unit": "%",
            "chart_type": "line",
            "default_axis": "right",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 고용 지표",
            "source_series_code": "U6RATE",
            "source_series_name": "U-6 Unemployment Rate",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Percent",
        },
    },
    {
        "series_id": "ICSA",
        "series_info": {
            "series_code": "ICSA",
            "series_name": "Initial Claims",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "고용",
            "indicator_code": "US_ICSA",
            "indicator_name_ko": "신규 실업수당청구건수",
            "indicator_name_en": "Initial Claims",
            "is_macro_tracker": True,
            "display_order": 50,
            "is_active": True,
            "frequency": "weekly",
            "unit": "persons",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 고용 지표",
            "source_series_code": "ICSA",
            "source_series_name": "Initial Claims",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Number",
        },
    },
    {
        "series_id": "PI",
        "series_info": {
            "series_code": "PI",
            "series_name": "Personal Income",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "소득과 지출",
            "indicator_code": "US_PI",
            "indicator_name_ko": "개인소득",
            "indicator_name_en": "Personal Income",
            "is_macro_tracker": True,
            "display_order": 110,
            "is_active": True,
            "frequency": "monthly",
            "unit": "usd_billions_saar",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 소득과 지출 지표",
            "source_series_code": "PI",
            "source_series_name": "Personal Income",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Billions of Dollars",
        },
    },
    {
        "series_id": "PCE",
        "series_info": {
            "series_code": "PCE",
            "series_name": "Personal Consumption Expenditures",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "소득과 지출",
            "indicator_code": "US_PCE",
            "indicator_name_ko": "개인소비지출",
            "indicator_name_en": "Personal Consumption Expenditures",
            "is_macro_tracker": True,
            "display_order": 120,
            "is_active": True,
            "frequency": "monthly",
            "unit": "usd_billions_saar",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 소득과 지출 지표",
            "source_series_code": "PCE",
            "source_series_name": "Personal Consumption Expenditures",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Billions of Dollars",
        },
    },
    {
        "series_id": "RSAFS",
        "series_info": {
            "series_code": "RSAFS",
            "series_name": "Advance Retail Sales: Retail Trade and Food Services,total",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "소득과 지출",
            "indicator_code": "US_RSAFS",
            "indicator_name_ko": "소매판매",
            "indicator_name_en": "Advance Retail Sales: Retail Trade and Food Services,total",
            "is_macro_tracker": True,
            "display_order": 130,
            "is_active": True,
            "frequency": "monthly",
            "unit": "usd_millions",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 소득과 지출 지표",
            "source_series_code": "RSAFS",
            "source_series_name": "Advance Retail Sales: Retail Trade and Food Services,total",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Millions of Dollars",
        },
    },
    {
        "series_id": "INDPRO",
        "series_info": {
            "series_code": "INDPRO",
            "series_name": "Industrial Production Index",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "산업",
            "indicator_code": "US_INDPRO",
            "indicator_name_ko": "산업생산",
            "indicator_name_en": "Industrial Production Index",
            "is_macro_tracker": True,
            "display_order": 210,
            "is_active": True,
            "frequency": "monthly",
            "unit": "index",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 산업 지표",
            "source_series_code": "INDPRO",
            "source_series_name": "Industrial Production Index",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Index",
        },
    },
    {
        "series_id": "TCU",
        "series_info": {
            "series_code": "TCU",
            "series_name": "Capacity Utilization: Total Industry",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "산업",
            "indicator_code": "US_TCU",
            "indicator_name_ko": "설비가동률",
            "indicator_name_en": "Capacity Utilization: Total Industry",
            "is_macro_tracker": True,
            "display_order": 220,
            "is_active": True,
            "frequency": "monthly",
            "unit": "%",
            "chart_type": "line",
            "default_axis": "right",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 산업 지표",
            "source_series_code": "TCU",
            "source_series_name": "Capacity Utilization: Total Industry",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Percent of Capacity",
        },
    },
    {
        "series_id": "DGORDER",
        "series_info": {
            "series_code": "DGORDER",
            "series_name": "Manufacturers' New Orders: Durable Goods",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "산업",
            "indicator_code": "US_DGORDER",
            "indicator_name_ko": "내구재주문",
            "indicator_name_en": "Manufacturers' New Orders: Durable Goods",
            "is_macro_tracker": True,
            "display_order": 230,
            "is_active": True,
            "frequency": "monthly",
            "unit": "usd_millions",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 산업 지표",
            "source_series_code": "DGORDER",
            "source_series_name": "Manufacturers' New Orders: Durable Goods",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Millions of Dollars",
        },
    },
    {
        "series_id": "NEWORDER",
        "series_info": {
            "series_code": "NEWORDER",
            "series_name": "Manufacturers' New Orders: Nondefense Capital Goods Excluding Aircraft",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "산업",
            "indicator_code": "US_NEWORDER",
            "indicator_name_ko": "핵심 자본재 수주",
            "indicator_name_en": "Manufacturers' New Orders: Nondefense Capital Goods Excluding Aircraft",
            "is_macro_tracker": True,
            "display_order": 240,
            "is_active": True,
            "frequency": "monthly",
            "unit": "usd_millions",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 산업 지표",
            "source_series_code": "NEWORDER",
            "source_series_name": "Manufacturers' New Orders: Nondefense Capital Goods Excluding Aircraft",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Millions of Dollars",
        },
    },
    {
        "series_id": "CPIAUCSL",
        "series_info": {
            "series_code": "CPIAUCSL",
            "series_name": "Consumer Price Index for All Urban Consumers: All Items",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "물가",
            "indicator_code": "US_CPI",
            "indicator_name_ko": "CPI",
            "indicator_name_en": "Consumer Price Index for All Urban Consumers: All Items",
            "is_macro_tracker": True,
            "display_order": 410,
            "is_active": True,
            "frequency": "monthly",
            "unit": "index",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 물가 지표",
            "source_series_code": "CPIAUCSL",
            "source_series_name": "Consumer Price Index for All Urban Consumers: All Items",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Index 1982-1984=100",
        },
    },
    {
        "series_id": "PPIACO",
        "series_info": {
            "series_code": "PPIACO",
            "series_name": "Producer Price Index by Commodity: All Commodities",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "물가",
            "indicator_code": "US_PPI",
            "indicator_name_ko": "PPI",
            "indicator_name_en": "Producer Price Index by Commodity: All Commodities",
            "is_macro_tracker": True,
            "display_order": 420,
            "is_active": True,
            "frequency": "monthly",
            "unit": "index",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 물가 지표",
            "source_series_code": "PPIACO",
            "source_series_name": "Producer Price Index by Commodity: All Commodities",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Index 1982=100",
        },
    },
    {
        "series_id": "PCEPI",
        "series_info": {
            "series_code": "PCEPI",
            "series_name": "Personal Consumption Expenditures: Chain-type Price Index",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "물가",
            "indicator_code": "US_PCEPI",
            "indicator_name_ko": "PCE 물가지수",
            "indicator_name_en": "Personal Consumption Expenditures: Chain-type Price Index",
            "is_macro_tracker": True,
            "display_order": 430,
            "is_active": True,
            "frequency": "monthly",
            "unit": "index",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 물가 지표",
            "source_series_code": "PCEPI",
            "source_series_name": "Personal Consumption Expenditures: Chain-type Price Index",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Index 2017=100",
        },
    },
    {
        "series_id": "T10YIE",
        "series_info": {
            "series_code": "T10YIE",
            "series_name": "10-Year Breakeven Inflation Rate",
            "category_name": "미국 매크로",
            "country_code": "US",
            "macro_category": "물가",
            "indicator_code": "US_T10YIE",
            "indicator_name_ko": "10년 기대인플레이션",
            "indicator_name_en": "10-Year Breakeven Inflation Rate",
            "is_macro_tracker": True,
            "display_order": 440,
            "is_active": True,
            "frequency": "daily",
            "unit": "%",
            "chart_type": "line",
            "default_axis": "right",
            "default_color": None,
            "is_recession_series": False,
            "notes": "FRED API 적재 | macro tracker 미국 물가 관련 지표",
            "source_series_code": "T10YIE",
            "source_series_name": "10-Year Breakeven Inflation Rate",
            "transform_code": "fred_api",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "Percent",
        },
    },
]


# --------------------------------------------------
# 2) 공통 유틸
# --------------------------------------------------
def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_value(value):
    if value is None:
        return None

    text_value = str(value).strip()

    if text_value in ("", ".", "nan", "None", "null"):
        return None

    try:
        return float(text_value)
    except ValueError:
        return None


def normalize_frequency(value):
    raw = clean_str(value).lower()

    if raw in ("d", "day", "daily"):
        return "daily"
    if raw in ("w", "week", "weekly"):
        return "weekly"
    if raw in ("m", "month", "monthly"):
        return "monthly"
    if raw in ("q", "quarter", "quarterly"):
        return "quarterly"
    if raw in ("a", "y", "year", "annual", "yearly"):
        return "yearly"

    return raw


# --------------------------------------------------
# 3) FRED API 호출
# --------------------------------------------------
def fetch_fred_series_meta(series_id):
    url = "https://api.stlouisfed.org/fred/series"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
    }

    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    payload = response.json()
    seriess = payload.get("seriess", [])

    if not seriess:
        raise ValueError(f"FRED meta 조회 실패: series_id={series_id}")

    return seriess[0]


def fetch_fred_observations(series_id):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "asc",
    }

    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    payload = response.json()
    observations = payload.get("observations", [])

    if observations is None:
        raise ValueError(f"FRED observations 조회 실패: series_id={series_id}")

    return observations


# --------------------------------------------------
# 4) observations -> DataFrame 변환
# --------------------------------------------------
def build_data_df(observations):
    rows = []

    for obs in observations:
        date_raw = obs.get("date")
        value_raw = obs.get("value")

        if not date_raw:
            continue

        value_num = parse_value(value_raw)
        if value_num is None:
            continue

        rows.append(
            {
                "date": pd.to_datetime(date_raw).date(),
                "value": value_num,
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)

    return df


# --------------------------------------------------
# 5) FRED meta로 series_info 보정
# - frequency, notes, source_unit 등 일부를 실제 FRED 응답 기준으로 보강
# --------------------------------------------------
def merge_series_info_with_fred_meta(base_series_info, fred_meta):
    series_info = dict(base_series_info)

    fred_title = clean_str(fred_meta.get("title"))
    fred_frequency = clean_str(fred_meta.get("frequency_short") or fred_meta.get("frequency"))
    fred_units = clean_str(fred_meta.get("units"))
    fred_notes = clean_str(fred_meta.get("notes"))
    fred_series_id = clean_str(fred_meta.get("id"))

    if fred_title:
        series_info["source_series_name"] = fred_title

    if fred_frequency:
        series_info["frequency"] = normalize_frequency(fred_frequency)

    if fred_units:
        series_info["source_unit"] = fred_units

    notes_parts = [clean_str(series_info.get("notes"))]

    if fred_notes:
        notes_parts.append("FRED notes 포함")

    series_info["notes"] = " | ".join([x for x in notes_parts if x])

    if fred_series_id:
        series_info["source_series_code"] = fred_series_id

    return series_info


# --------------------------------------------------
# 6) 기존 loading_common 저장 후,
#    Step 4에서 추가한 series_meta 확장 컬럼 업데이트
# --------------------------------------------------
def update_macro_tracker_meta_fields(series_code, series_info):
    query = text("""
        UPDATE series_meta
        SET
            country_code = :country_code,
            macro_category = :macro_category,
            indicator_code = :indicator_code,
            indicator_name_ko = :indicator_name_ko,
            indicator_name_en = :indicator_name_en,
            is_macro_tracker = :is_macro_tracker,
            display_order = :display_order,
            is_active = :is_active
        WHERE series_code = :series_code
    """)

    params = {
        "series_code": series_code,
        "country_code": series_info.get("country_code"),
        "macro_category": series_info.get("macro_category"),
        "indicator_code": series_info.get("indicator_code"),
        "indicator_name_ko": series_info.get("indicator_name_ko"),
        "indicator_name_en": series_info.get("indicator_name_en"),
        "is_macro_tracker": bool(series_info.get("is_macro_tracker", False)),
        "display_order": series_info.get("display_order"),
        "is_active": bool(series_info.get("is_active", True)),
    }

    with engine.begin() as conn:
        conn.execute(query, params)


# --------------------------------------------------
# 7) 1개 시리즈 적재
# --------------------------------------------------
def load_one_series(source_id, config):
    fred_series_id = config["series_id"]
    base_series_info = config["series_info"]

    fred_meta = fetch_fred_series_meta(fred_series_id)
    observations = fetch_fred_observations(fred_series_id)
    data_df = build_data_df(observations)

    if data_df.empty:
        print(f"[건너뜀] {fred_series_id} | 유효 데이터 없음")
        return

    series_info = merge_series_info_with_fred_meta(
        base_series_info=base_series_info,
        fred_meta=fred_meta,
    )

    payload = build_series_payload(
        series_info=series_info,
        source_id=source_id,
        data_df=data_df,
    )

    save_series_payload(payload)
    update_macro_tracker_meta_fields(
        series_code=series_info["series_code"],
        series_info=series_info,
    )

    print(
        f"[완료] {series_info['series_code']} | "
        f"fred_series_id={fred_series_id} | "
        f"rows={len(data_df)} | "
        f"date_range={data_df['date'].min()} ~ {data_df['date'].max()}"
    )

    check_result(series_info["series_code"], limit=5)


# --------------------------------------------------
# 8) main
# --------------------------------------------------
def main():
    if not FRED_API_KEY:
        raise ValueError(".env 파일에 FRED_API_KEY가 없습니다.")

    source_id = get_source_id(SOURCE_CODE)

    for config in SERIES_CONFIG:
        series_code = config["series_info"]["series_code"]

        try:
            load_one_series(source_id=source_id, config=config)
        except Exception as e:
            print(f"[실패] {series_code} | {e}")


if __name__ == "__main__":
    main()