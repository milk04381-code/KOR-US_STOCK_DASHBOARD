# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 21:33:26 2026

@author: 박승욱
"""
# loading_common.py
# 여러 loader에서 공통으로 사용하는 DB 저장 유틸
# 역할:
# 1. source_id 조회
# 2. meta payload 생성
# 3. series_meta upsert
# 4. series_data upsert
# 5. 저장 결과 확인

import math
import pandas as pd
from sqlalchemy import text

from db import engine


# -----------------------------
# 공통 기초 함수
# -----------------------------
def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_value(value):
    """
    예:
    "1,138.30" -> 1138.30
    "1,253,192" -> 1253192.0
    "" / "." / nan -> None
    """
    if value is None:
        return None

    value = str(value).strip().replace(",", "")

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


# -----------------------------
# payload 생성
# -----------------------------
def build_meta_params(series_info, source_id, data_df):
    if data_df.empty:
        start_date = None
        end_date = None
    else:
        start_date = data_df["date"].min()
        end_date = data_df["date"].max()

    return {
        "series_code": series_info["series_code"],
        "series_name": series_info["series_name"],
        "source_id": source_id,
        "category_name": series_info.get("category_name"),
        "frequency": normalize_frequency(series_info.get("frequency")),
        "unit": series_info.get("unit"),
        "chart_type": series_info.get("chart_type", "line"),
        "default_axis": series_info.get("default_axis", "left"),
        "default_color": series_info.get("default_color"),
        "is_recession_series": series_info.get("is_recession_series", False),
        "start_date": start_date,
        "end_date": end_date,
        "notes": series_info.get("notes"),
        "source_series_code": series_info.get("source_series_code"),
        "source_series_name": series_info.get("source_series_name"),
        "transform_code": series_info.get("transform_code", "raw"),
        "transform_name": series_info.get("transform_name", "RAW"),
        "is_transformed": series_info.get("is_transformed", False),
        "source_unit": series_info.get("source_unit"),
    }


def build_series_payload(series_info, source_id, data_df):
    meta_params = build_meta_params(
        series_info=series_info,
        source_id=source_id,
        data_df=data_df,
    )

    return {
        "series_code": meta_params["series_code"],
        "meta_params": meta_params,
        "data_df": data_df,
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
def check_result(series_code, limit=10):
    meta_query = text("""
        SELECT
            series_id,
            series_code,
            series_name,
            category_name,
            frequency,
            unit,
            source_series_code,
            transform_name,
            is_transformed,
            is_recession_series,
            start_date,
            end_date,
            last_updated
        FROM series_meta
        WHERE series_code = :series_code
    """)

    data_query = text(f"""
        SELECT
            d.date_value,
            d.value_num
        FROM series_data d
        JOIN series_meta m
          ON d.series_id = m.series_id
        WHERE m.series_code = :series_code
        ORDER BY d.date_value DESC
        LIMIT {int(limit)}
    """)

    meta_df = pd.read_sql(meta_query, engine, params={"series_code": series_code})
    data_df = pd.read_sql(data_query, engine, params={"series_code": series_code})

    print(f"\n[{series_code}] series_meta 확인")
    print(meta_df)

    print(f"\n[{series_code}] series_data 최근 {limit}건 확인")
    print(data_df)