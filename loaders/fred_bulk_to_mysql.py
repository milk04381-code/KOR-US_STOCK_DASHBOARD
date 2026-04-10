# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 15:56:28 2026

@author: 박승욱
"""

# 1. FRED에서 메타 조회
# 2. FRED에서 observations 조회
# 3. 필요하면 transform 결정
# - RAW / YOY / MOM / QOQ 
# 4. series_meta 저장
# 5. series_data 저장

import os
import math
import sys
from pathlib import Path

import requests
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db import engine

load_dotenv(PROJECT_ROOT / ".env")

FRED_API_KEY = os.getenv("FRED_API_KEY", "")

# --------------------------------------------------
# 적재 대상
# - transforms 미지정 시 frequency 기준 자동
# - monthly : RAW + YOY + MOM
# - quarterly : RAW + YOY + QOQ
# - USREC    : RAW only
# --------------------------------------------------
SERIES_CONFIG = [
    {"series_id": "UNRATE"},
    {"series_id": "USREC", "transforms": ["RAW"]},
    {"series_id": "T10Y2Y"},
    {"series_id": "T10Y3M"},
    {"series_id": "BAMLH0A0HYM2"},
]

TRANSFORM_SPECS = {
    "RAW": {
        "fred_units": "lin",
        "suffix": "",
        "display_suffix": "",
        "target_unit": None,
    },
    "YOY": {
        "fred_units": "pc1",
        "suffix": "_YOY",
        "display_suffix": " (YoY)",
        "target_unit": "% YoY",
    },
    "MOM": {
        "fred_units": "pch",
        "suffix": "_MOM",
        "display_suffix": " (MoM)",
        "target_unit": "% MoM",
    },
    "QOQ": {
        "fred_units": "pch",
        "suffix": "_QOQ",
        "display_suffix": " (QoQ)",
        "target_unit": "% QoQ",
    },
}

SOURCE_CODE = "FRED"


# -----------------------------
# 공통 기초 함수
# -----------------------------
def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_value(value):
    if value is None:
        return None

    value = str(value).strip()

    if value in ("", ".", "nan", "None", "null"):
        return None

    try:
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        return number
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


def prepare_observations_df(observations):
    if not observations:
        return pd.DataFrame(columns=["date", "value"])

    rows = []
    for obs in observations:
        date_value = obs.get("date")
        value_num = parse_value(obs.get("value"))

        if not date_value:
            continue
        if value_num is None:
            continue

        rows.append(
            {
                "date": pd.to_datetime(date_value).date(),
                "value": value_num,
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = df.sort_values("date").reset_index(drop=True)
    return df


# -----------------------------
# FRED 전용 호출
# -----------------------------
def fetch_fred_series_meta(api_key, series_code):
    url = "https://api.stlouisfed.org/fred/series"
    params = {
        "series_id": series_code,
        "api_key": api_key,
        "file_type": "json",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "seriess" not in data or len(data["seriess"]) == 0:
        raise ValueError(f"FRED 메타데이터 조회 실패: {series_code}")

    return data["seriess"][0]


def fetch_fred_observations(api_key, series_code, fred_units="lin"):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_code,
        "units": fred_units,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "asc",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "observations" not in data:
        raise ValueError(f"FRED 시계열 조회 실패: {series_code} / units={fred_units}")

    return data["observations"]


# -----------------------------
# transform 결정
# -----------------------------
def decide_default_transforms(meta):
    series_code = clean_str(meta.get("id")).upper()
    frequency_short = clean_str(meta.get("frequency_short")).upper()

    if series_code == "USREC":
        return ["RAW"]

    if frequency_short == "M":
        return ["RAW", "YOY", "MOM"]

    if frequency_short == "Q":
        return ["RAW", "YOY", "QOQ"]

    return ["RAW"]


def resolve_transform_names(config_item, meta):
    transform_names = config_item.get("transforms")

    if not transform_names:
        return decide_default_transforms(meta)

    result = []
    for name in transform_names:
        upper_name = str(name).upper()

        if upper_name not in TRANSFORM_SPECS:
            raise ValueError(f"지원하지 않는 transform: {name}")

        result.append(upper_name)

    return result


# -----------------------------
# 저장 규칙
# -----------------------------
def normalize_unit(raw_unit, series_code=None, series_name=None, transform_name="RAW"):
    raw_unit_str = clean_str(raw_unit)
    raw_lower = raw_unit_str.lower()
    series_code = clean_str(series_code).upper()
    series_name_lower = clean_str(series_name).lower()

    if transform_name == "YOY":
        return "% YoY"
    if transform_name == "MOM":
        return "% MoM"
    if transform_name == "QOQ":
        return "% QoQ"

    if series_code == "USREC":
        return "indicator"

    if "recession" in series_name_lower and "indicator" in series_name_lower:
        return "indicator"

    if (
        "%" in raw_lower
        or "percent" in raw_lower
        or "percentage" in raw_lower
        or "percent change" in raw_lower
    ):
        return "%"

    if "index" in raw_lower:
        return "index"

    return raw_unit_str


def infer_category_name(base_series_code):
    if base_series_code == "USREC":
        return "경기침체"
    return "미국 매크로"


def build_storage_series_code(base_series_code, transform_name):
    spec = TRANSFORM_SPECS[transform_name]
    suffix = spec["suffix"]
    return f"{base_series_code}{suffix}" if suffix else base_series_code


def build_storage_series_name(base_title, transform_name):
    spec = TRANSFORM_SPECS[transform_name]
    display_suffix = spec["display_suffix"]
    return f"{base_title}{display_suffix}" if display_suffix else base_title


def build_meta_params(meta, source_id, transform_name):
    base_series_code = clean_str(meta["id"])
    base_title = clean_str(meta.get("title"))
    source_units_raw = clean_str(meta.get("units"))

    storage_series_code = build_storage_series_code(base_series_code, transform_name)
    storage_series_name = build_storage_series_name(base_title, transform_name)
    storage_unit = normalize_unit(
        raw_unit=source_units_raw,
        series_code=base_series_code,
        series_name=base_title,
        transform_name=transform_name,
    )

    frequency_value = normalize_frequency(
        meta.get("frequency_short") or meta.get("frequency")
    )

    is_recession = (base_series_code == "USREC" and transform_name == "RAW")

    return {
        "series_code": storage_series_code,
        "series_name": storage_series_name,
        "source_id": source_id,
        "category_name": infer_category_name(base_series_code),
        "frequency": frequency_value,
        "unit": storage_unit,
        "chart_type": "area" if is_recession else "line",
        "default_axis": "left",
        "default_color": "gray" if is_recession else None,
        "is_recession_series": is_recession,
        "start_date": meta.get("observation_start"),
        "end_date": meta.get("observation_end"),
        "notes": meta.get("notes"),
        "source_series_code": base_series_code,
        "source_series_name": base_title,
        "transform_code": TRANSFORM_SPECS[transform_name]["fred_units"],
        "transform_name": transform_name,
        "is_transformed": (transform_name != "RAW"),
        "source_unit": source_units_raw,
    }


def build_series_payload(meta, source_id, transform_name, df):
    params = build_meta_params(meta, source_id=source_id, transform_name=transform_name)

    return {
        "series_code": params["series_code"],
        "meta_params": params,
        "data_df": df,
    }


# -----------------------------
# DB 공통 저장
# -----------------------------
def get_source_id(source_code):
    query = text("""
        SELECT source_id
        FROM data_source
        WHERE source_code = :source_code
        LIMIT 1
    """)

    with engine.begin() as conn:
        source_id = conn.execute(query, {"source_code": source_code}).scalar()

    if source_id is None:
        raise ValueError(f"data_source 테이블에 source_code='{source_code}'가 없습니다.")

    return source_id


def upsert_series_meta_by_params(params):
    check_query = text("""
        SELECT series_id
        FROM series_meta
        WHERE series_code = :series_code
        LIMIT 1
    """)

    insert_query = text("""
        INSERT INTO series_meta
        (
            series_code,
            series_name,
            source_id,
            category_name,
            frequency,
            unit,
            chart_type,
            default_axis,
            default_color,
            is_recession_series,
            start_date,
            end_date,
            last_updated,
            notes,
            source_series_code,
            source_series_name,
            transform_code,
            transform_name,
            is_transformed,
            source_unit
        )
        VALUES
        (
            :series_code,
            :series_name,
            :source_id,
            :category_name,
            :frequency,
            :unit,
            :chart_type,
            :default_axis,
            :default_color,
            :is_recession_series,
            :start_date,
            :end_date,
            NOW(),
            :notes,
            :source_series_code,
            :source_series_name,
            :transform_code,
            :transform_name,
            :is_transformed,
            :source_unit
        )
    """)

    update_query = text("""
        UPDATE series_meta
        SET
            series_name = :series_name,
            source_id = :source_id,
            category_name = :category_name,
            frequency = :frequency,
            unit = :unit,
            chart_type = :chart_type,
            default_axis = :default_axis,
            default_color = :default_color,
            is_recession_series = :is_recession_series,
            start_date = :start_date,
            end_date = :end_date,
            last_updated = NOW(),
            notes = :notes,
            source_series_code = :source_series_code,
            source_series_name = :source_series_name,
            transform_code = :transform_code,
            transform_name = :transform_name,
            is_transformed = :is_transformed,
            source_unit = :source_unit
        WHERE series_code = :series_code
    """)

    with engine.begin() as conn:
        existing_series_id = conn.execute(
            check_query,
            {"series_code": params["series_code"]},
        ).scalar()

        if existing_series_id is None:
            conn.execute(insert_query, params)
        else:
            conn.execute(update_query, params)


def insert_series_data_by_code(series_code, df):
    series_id_query = text("""
        SELECT series_id
        FROM series_meta
        WHERE series_code = :series_code
        LIMIT 1
    """)

    insert_query = text("""
        INSERT INTO series_data (series_id, date_value, value_num)
        VALUES (:series_id, :date_value, :value_num)
        ON DUPLICATE KEY UPDATE
            value_num = VALUES(value_num)
    """)

    with engine.begin() as conn:
        series_id = conn.execute(
            series_id_query,
            {"series_code": series_code},
        ).scalar()

        if series_id is None:
            raise ValueError(f"series_meta에 해당 series_code가 없습니다: {series_code}")

        rows = [
            {
                "series_id": series_id,
                "date_value": row["date"],
                "value_num": float(row["value"]),
            }
            for _, row in df.iterrows()
        ]

        if rows:
            conn.execute(insert_query, rows)


def save_series_payload(payload):
    series_code = payload["series_code"]
    meta_params = payload["meta_params"]
    data_df = payload["data_df"]

    upsert_series_meta_by_params(meta_params)
    insert_series_data_by_code(series_code, data_df)


# -----------------------------
# 확인용
# -----------------------------
def check_result(series_code):
    meta_query = text("""
        SELECT
            series_id,
            series_code,
            series_name,
            frequency,
            unit,
            source_series_code,
            transform_name,
            is_transformed,
            is_recession_series,
            last_updated
        FROM series_meta
        WHERE series_code = :series_code
    """)

    data_query = text("""
        SELECT
            d.date_value,
            d.value_num
        FROM series_data d
        JOIN series_meta m
          ON d.series_id = m.series_id
        WHERE m.series_code = :series_code
        ORDER BY d.date_value DESC
        LIMIT 5
    """)

    meta_df = pd.read_sql(meta_query, engine, params={"series_code": series_code})
    data_df = pd.read_sql(data_query, engine, params={"series_code": series_code})

    print(f"\n[{series_code}] series_meta 확인")
    print(meta_df)

    print(f"\n[{series_code}] series_data 최근 5건 확인")
    print(data_df)


# -----------------------------
# 1개 시리즈 적재
# -----------------------------
def load_one_series(config_item):
    base_series_code = config_item["series_id"]
    source_id = get_source_id(SOURCE_CODE)

    meta = fetch_fred_series_meta(FRED_API_KEY, base_series_code)
    transform_names = resolve_transform_names(config_item, meta)

    for transform_name in transform_names:
        fred_units = TRANSFORM_SPECS[transform_name]["fred_units"]

        observations = fetch_fred_observations(
            FRED_API_KEY,
            base_series_code,
            fred_units=fred_units,
        )
        df = prepare_observations_df(observations)

        payload = build_series_payload(
            meta=meta,
            source_id=source_id,
            transform_name=transform_name,
            df=df,
        )

        save_series_payload(payload)

        print(
            f"[완료] {payload['series_code']} "
            f"| transform={transform_name} "
            f"| fred_units={fred_units} "
            f"| 저장 건수: {len(df)}"
        )

        check_result(payload["series_code"])


def main():
    if not FRED_API_KEY:
        raise ValueError(".env 파일에 FRED_API_KEY가 없습니다.")

    for config_item in SERIES_CONFIG:
        try:
            load_one_series(config_item)
        except Exception as e:
            print(f"[실패] {config_item.get('series_id')} | {e}")


if __name__ == "__main__":
    main()