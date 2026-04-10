# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 19:50:08 2026

@author: 박승욱
"""

# services/data_service.py
# Day9 수정
# 역할:
# 1. 선택된 series code 정리
# 2. series_meta 조회
# 3. chart_rule 조회
# 4. unit / frequency 정규화
# 5. chart_rule exact match / fallback match 적용
# 6. values + meta + rule 결과를 합쳐 최종 dataset 생성
# 7. 사용자 axis override 적용

import os
import re

import pandas as pd
from sqlalchemy import text, bindparam

from db import engine


DEBUG_CHART_RULE = os.getenv("CHART_RULE_DEBUG", "0") == "1"


def debug_print(*args):
    if DEBUG_CHART_RULE:
        print("[chart_rule_debug]", *args)


def normalize_selected_codes(selected_codes):
    if selected_codes is None:
        return []
    if isinstance(selected_codes, str):
        return [selected_codes]
    return list(selected_codes)


def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


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


def normalize_unit(value, transform_name=""):
    raw = clean_str(value)
    raw_lower = raw.lower()

    tf = clean_str(transform_name).upper()

    if tf == "YOY":
        return "% YoY"
    if tf == "MOM":
        return "% MoM"
    if tf == "QOQ":
        return "% QoQ"

    compact = re.sub(r"\s+", "", raw_lower)

    if "yoy" in compact:
        return "% YoY"
    if "mom" in compact:
        return "% MoM"
    if "qoq" in compact:
        return "% QoQ"

    if raw_lower in ("indicator", "recession indicator"):
        return "indicator"

    if "index" in raw_lower:
        return "index"

    if (
        "%" in raw_lower
        or "percent" in raw_lower
        or "percentage" in raw_lower
    ):
        return "%"

    return raw


def load_series_meta_all(include_recession=False):
    where_clause = ""
    if not include_recession:
        where_clause = "WHERE is_recession_series = FALSE"

    query = text(f"""
        SELECT
            series_id,
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
            source_series_code,
            transform_name,
            is_transformed
        FROM series_meta
        {where_clause}
        ORDER BY
            CASE WHEN category_name IS NULL OR category_name = '' THEN 1 ELSE 0 END,
            category_name,
            series_name
    """)
    df = pd.read_sql(query, engine)

    if not df.empty:
        df["frequency"] = df["frequency"].apply(normalize_frequency)
        df["unit"] = df.apply(
            lambda row: normalize_unit(
                row.get("unit"),
                row.get("transform_name", ""),
            ),
            axis=1,
        )

    return df


def load_series_dropdown_options():
    df = load_series_meta_all(include_recession=False)

    if df.empty:
        return []

    df["category_name"] = df["category_name"].fillna("기타")
    options = []

    for category, group in df.groupby("category_name", sort=True):
        options.append(
            {
                "label": f"──────── {category} ────────",
                "value": f"__header__{category}",
                "disabled": True,
            }
        )

        for _, row in group.sort_values("series_name").iterrows():
            label = f"  {row['series_name']} ({row['series_code']})"
            options.append(
                {
                    "label": label,
                    "value": row["series_code"],
                    "search": (
                        f"{row['series_name']} "
                        f"{row['series_code']} "
                        f"{row.get('source_series_code', '')} "
                        f"{row.get('transform_name', '')} "
                        f"{category}"
                    ),
                }
            )

    return options


def get_default_series_selection(default_codes=None, fallback_count=2):
    default_codes = default_codes or []

    df = load_series_meta_all(include_recession=False)
    if df.empty:
        return []

    available_codes = set(df["series_code"].tolist())
    selected = [code for code in default_codes if code in available_codes]

    if selected:
        return selected

    return df["series_code"].head(fallback_count).tolist()


def load_series_meta_for_codes(selected_codes):
    selected_codes = normalize_selected_codes(selected_codes)

    if not selected_codes:
        return pd.DataFrame()

    query = text("""
        SELECT
            series_id,
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
            source_series_code,
            source_series_name,
            transform_code,
            transform_name,
            is_transformed,
            source_unit
        FROM series_meta
        WHERE series_code IN :selected_codes
    """).bindparams(bindparam("selected_codes", expanding=True))

    df = pd.read_sql(query, engine, params={"selected_codes": selected_codes})

    if not df.empty:
        df["frequency_raw"] = df["frequency"]
        df["unit_raw"] = df["unit"]

        df["frequency"] = df["frequency"].apply(normalize_frequency)
        df["unit"] = df.apply(
            lambda row: normalize_unit(
                row.get("unit"),
                row.get("transform_name", ""),
            ),
            axis=1,
        )

        df["default_axis"] = df["default_axis"].fillna("left").astype(str).str.strip().str.lower()
        df["chart_type"] = df["chart_type"].fillna("line").astype(str).str.strip().str.lower()

    return df


def load_chart_rules():
    query = text("""
        SELECT
            rule_id,
            unit,
            frequency,
            recommended_axis,
            recommended_chart_type,
            use_secondary_axis,
            rule_description
        FROM chart_rule
        ORDER BY rule_id
    """)
    df = pd.read_sql(query, engine)

    if df.empty:
        return df

    df["unit_raw"] = df["unit"]
    df["frequency_raw"] = df["frequency"]

    df["unit"] = df["unit"].apply(normalize_unit)
    df["frequency"] = df["frequency"].apply(normalize_frequency)
    df["recommended_axis"] = (
        df["recommended_axis"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )
    df["recommended_chart_type"] = (
        df["recommended_chart_type"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )
    df["use_secondary_axis"] = df["use_secondary_axis"].fillna(False).astype(bool)

    return df


def resolve_chart_rule_for_row(meta_row, rules_df):
    unit = clean_str(meta_row.get("unit"))
    frequency = clean_str(meta_row.get("frequency"))

    effective_axis = clean_str(meta_row.get("default_axis")).lower() or "left"
    effective_chart_type = clean_str(meta_row.get("chart_type")).lower() or "line"

    if rules_df.empty:
        return {
            "effective_axis": effective_axis,
            "effective_chart_type": effective_chart_type,
            "matched_rule_id": None,
            "rule_match_type": "default",
        }

    exact_match = rules_df[
        (rules_df["unit"] == unit) &
        (rules_df["frequency"] == frequency)
    ]

    unit_only_match = rules_df[
        (rules_df["unit"] == unit) &
        (rules_df["frequency"].isin(["", None]))
    ]

    chosen_rule = None
    rule_match_type = "default"

    if not exact_match.empty:
        chosen_rule = exact_match.iloc[0]
        rule_match_type = "exact"
    elif not unit_only_match.empty:
        chosen_rule = unit_only_match.iloc[0]
        rule_match_type = "unit_only"

    matched_rule_id = None

    if chosen_rule is not None:
        matched_rule_id = chosen_rule.get("rule_id")

        recommended_axis = clean_str(chosen_rule.get("recommended_axis")).lower()
        recommended_chart_type = clean_str(chosen_rule.get("recommended_chart_type")).lower()
        use_secondary_axis = bool(chosen_rule.get("use_secondary_axis"))

        if recommended_axis in ("left", "right"):
            effective_axis = recommended_axis
        elif use_secondary_axis:
            effective_axis = "right"

        if recommended_chart_type in ("line", "area", "bar"):
            effective_chart_type = recommended_chart_type

    debug_print(
        f"series_code={meta_row.get('series_code')} | "
        f"unit={unit} | frequency={frequency} | "
        f"effective_axis={effective_axis} | "
        f"effective_chart_type={effective_chart_type} | "
        f"match={rule_match_type} | rule_id={matched_rule_id}"
    )

    return {
        "effective_axis": effective_axis,
        "effective_chart_type": effective_chart_type,
        "matched_rule_id": matched_rule_id,
        "rule_match_type": rule_match_type,
    }


def apply_chart_rules(meta_df, rules_df):
    if meta_df.empty:
        return meta_df

    meta_df = meta_df.copy()

    resolved_list = []
    for _, row in meta_df.iterrows():
        resolved = resolve_chart_rule_for_row(row, rules_df)
        resolved_list.append(resolved)

    resolved_df = pd.DataFrame(resolved_list)

    meta_df["effective_axis"] = resolved_df["effective_axis"]
    meta_df["effective_chart_type"] = resolved_df["effective_chart_type"]
    meta_df["matched_rule_id"] = resolved_df["matched_rule_id"]
    meta_df["rule_match_type"] = resolved_df["rule_match_type"]

    return meta_df


def load_series_values(selected_codes, start_date, end_date):
    selected_codes = normalize_selected_codes(selected_codes)

    if not selected_codes:
        return pd.DataFrame()

    query = text("""
        SELECT
            m.series_code,
            d.date_value,
            d.value_num
        FROM series_data d
        JOIN series_meta m
          ON d.series_id = m.series_id
        WHERE m.series_code IN :selected_codes
          AND d.date_value BETWEEN :start_date AND :end_date
        ORDER BY m.series_code, d.date_value
    """).bindparams(bindparam("selected_codes", expanding=True))

    df = pd.read_sql(
        query,
        engine,
        params={
            "selected_codes": selected_codes,
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    if not df.empty:
        df["date_value"] = pd.to_datetime(df["date_value"])
        df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce")
        df = df.dropna(subset=["value_num"]).sort_values(["series_code", "date_value"])

    return df


def load_recession_data(start_date, end_date):
    query = text("""
        SELECT
            d.date_value,
            d.value_num
        FROM series_data d
        JOIN series_meta m
          ON d.series_id = m.series_id
        WHERE m.series_code = 'USREC'
          AND d.date_value BETWEEN :start_date AND :end_date
        ORDER BY d.date_value
    """)

    df = pd.read_sql(
        query,
        engine,
        params={
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    if not df.empty:
        df["date_value"] = pd.to_datetime(df["date_value"])
        df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce").fillna(0).astype(int)

    return df


def load_chart_dataset(selected_codes, start_date, end_date, axis_override_map=None):
    selected_codes = normalize_selected_codes(selected_codes)

    if not selected_codes:
        return pd.DataFrame()

    meta_df = load_series_meta_for_codes(selected_codes)
    if meta_df.empty:
        return pd.DataFrame()

    rules_df = load_chart_rules()
    meta_df = apply_chart_rules(meta_df, rules_df)

    values_df = load_series_values(selected_codes, start_date, end_date)
    if values_df.empty:
        return pd.DataFrame()

    merged = values_df.merge(
        meta_df[
            [
                "series_code",
                "series_name",
                "category_name",
                "frequency",
                "unit",
                "unit_raw",
                "frequency_raw",
                "default_color",
                "default_axis",
                "chart_type",
                "effective_axis",
                "effective_chart_type",
                "transform_name",
                "source_series_code",
                "matched_rule_id",
                "rule_match_type",
            ]
        ],
        on="series_code",
        how="left",
    )

    if axis_override_map:
        merged["effective_axis"] = merged.apply(
            lambda row: axis_override_map.get(row["series_code"], row["effective_axis"]),
            axis=1,
        )

    merged["series_name"] = merged["series_name"].fillna(merged["series_code"])
    merged["unit"] = merged["unit"].fillna("")
    merged["default_color"] = merged["default_color"].fillna("")
    merged["default_axis"] = merged["default_axis"].fillna("left")
    merged["chart_type"] = merged["chart_type"].fillna("line")
    merged["effective_axis"] = merged["effective_axis"].fillna("left")
    merged["effective_chart_type"] = merged["effective_chart_type"].fillna("line")
    merged["rule_match_type"] = merged["rule_match_type"].fillna("default")

    debug_cols = [
        "series_code",
        "series_name",
        "unit_raw",
        "unit",
        "frequency_raw",
        "frequency",
        "default_axis",
        "chart_type",
        "effective_axis",
        "effective_chart_type",
        "matched_rule_id",
        "rule_match_type",
    ]

    debug_df = merged[debug_cols].drop_duplicates().sort_values(["series_code"])
    for _, row in debug_df.iterrows():
        debug_print(
            f"{row['series_code']} | "
            f"unit_raw={row['unit_raw']} -> unit={row['unit']} | "
            f"freq_raw={row['frequency_raw']} -> freq={row['frequency']} | "
            f"default_axis={row['default_axis']} | "
            f"chart_type={row['chart_type']} | "
            f"effective_axis={row['effective_axis']} | "
            f"effective_chart_type={row['effective_chart_type']} | "
            f"rule_id={row['matched_rule_id']} | "
            f"match={row['rule_match_type']}"
        )

    return merged.sort_values(["series_code", "date_value"]).reset_index(drop=True)