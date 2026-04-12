# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 18:13:41 2026

@author: 박승욱
"""
# services/macro_tracker_service.py 
# 수정본

from copy import deepcopy

import pandas as pd
from sqlalchemy import text

from db import engine


# -------------------------
# RULE MAP
# -------------------------
RULE_MAP = {
    "NFP": {
        "freq_type": "M",
        "trend_unit": "개월",
        "change_calc_type": "pct_change",
        "speed_calc_type": "pct_change",
    },
    "실업률": {
        "freq_type": "M",
        "trend_unit": "개월",
        "change_calc_type": "pp_diff",
        "speed_calc_type": "pp_diff",
    },
    "참여율": {
        "freq_type": "M",
        "trend_unit": "개월",
        "change_calc_type": "pp_diff",
        "speed_calc_type": "pp_diff",
    },
    "소매판매": {
        "freq_type": "M",
        "trend_unit": "개월",
        "change_calc_type": "pct_change",
        "speed_calc_type": "pct_change",
    },
    "광공업생산": {
        "freq_type": "M",
        "trend_unit": "개월",
        "change_calc_type": "pct_change",
        "speed_calc_type": "pct_change",
    },
    "CPI": {
        "freq_type": "M",
        "trend_unit": "개월",
        "change_calc_type": "pp_diff",
        "speed_calc_type": "pp_diff",
    },
    "주택착공": {
        "freq_type": "M",
        "trend_unit": "개월",
        "change_calc_type": "pct_change",
        "speed_calc_type": "pct_change",
    },
    "취업자수": {
        "freq_type": "M",
        "trend_unit": "개월",
        "change_calc_type": "diff",
        "speed_calc_type": "diff",
    },
    "한국 실업률": {
        "freq_type": "M",
        "trend_unit": "개월",
        "change_calc_type": "pp_diff",
        "speed_calc_type": "pp_diff",
    },
}

DISPLAY_NAME_MAP = {
    "비농업고용": "NFP",
    "실업률": "실업률",
    "경제활동참가율": "참여율",
    "소매판매": "소매판매",
    "산업생산": "광공업생산",
    "CPI": "CPI",
    "주택착공": "주택착공",
    "취업자수": "취업자수",
    "한국 실업률": "한국 실업률",
}


# -------------------------
# 기본 유틸
# -------------------------
def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_display_value(value):
    if value is None:
        return None

    text_value = str(value).strip()

    if text_value == "":
        return None

    text_value = text_value.replace(",", "")

    if text_value.endswith("%p"):
        return float(text_value[:-2])

    if text_value.endswith("bp"):
        return float(text_value[:-2])

    if text_value.endswith("k"):
        return float(text_value[:-1])

    if text_value.endswith("%"):
        return float(text_value[:-1])

    return float(text_value)


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


def format_month_key(ts):
    dt = pd.to_datetime(ts)
    return dt.strftime("%y%m")


def format_chart_date(ts):
    dt = pd.to_datetime(ts)
    return dt.strftime("%Y-%m-%d")


def choose_display_name(meta_row):
    name_ko = clean_str(meta_row.get("indicator_name_ko"))
    series_name = clean_str(meta_row.get("series_name"))

    if name_ko:
        return DISPLAY_NAME_MAP.get(name_ko, name_ko)

    return series_name


def format_value_for_table(value, indicator, unit, frequency):
    if value is None or pd.isna(value):
        return ""

    indicator = clean_str(indicator)
    unit = clean_str(unit).lower()
    frequency = normalize_frequency(frequency)

    if indicator == "NFP":
        return f"{value:,.0f}k"

    if indicator in ("실업률", "참여율", "CPI", "한국 실업률"):
        return f"{value:.1f}%"

    if indicator == "주택착공":
        return f"{value:.2f}"

    if "%" in unit or "percent" in unit or "percentage" in unit:
        return f"{value:.1f}%"

    if "index" in unit:
        return f"{value:.1f}"

    if frequency == "monthly":
        return f"{value:,.1f}"

    if frequency == "weekly":
        return f"{value:,.1f}"

    if frequency == "daily":
        return f"{value:,.2f}"

    return f"{value:,.1f}"


def _ordered_numeric_values(value_map, months):
    values = []

    for month in months:
        value = value_map.get(month, "")
        numeric = parse_display_value(value)
        if numeric is not None:
            values.append(numeric)

    return values


# -------------------------
# 계산 함수
# -------------------------
def _calc_change_value(current_value, previous_value, change_calc_type):
    if previous_value is None or current_value is None:
        return None

    if change_calc_type == "pct_change":
        if previous_value == 0:
            return None
        return ((current_value - previous_value) / abs(previous_value)) * 100.0

    if change_calc_type == "diff":
        return current_value - previous_value

    if change_calc_type == "pp_diff":
        return current_value - previous_value

    if change_calc_type == "bps_diff":
        return (current_value - previous_value) * 100.0

    return None


def _format_change_value(change_value, change_calc_type):
    if change_value is None:
        return ""

    if change_calc_type == "pct_change":
        return f"{change_value:+.1f}%"

    if change_calc_type == "diff":
        return f"{change_value:+.1f}"

    if change_calc_type == "pp_diff":
        return f"{change_value:+.1f}%p"

    if change_calc_type == "bps_diff":
        return f"{change_value:+.0f}bp"

    return ""


def _calc_speed_from_series(values, speed_calc_type):
    if len(values) < 3:
        return ""

    delta_prev = _calc_change_value(values[-2], values[-3], speed_calc_type)
    delta_curr = _calc_change_value(values[-1], values[-2], speed_calc_type)

    if delta_prev is None or delta_curr is None:
        return ""

    if abs(delta_curr - delta_prev) < 1e-12:
        return "유지"

    if delta_prev < delta_curr:
        return "가속"

    return "둔화"


def _build_speed_history(values, speed_calc_type):
    history = []

    for idx in range(2, len(values)):
        delta_prev = _calc_change_value(values[idx - 1], values[idx - 2], speed_calc_type)
        delta_curr = _calc_change_value(values[idx], values[idx - 1], speed_calc_type)

        if delta_prev is None or delta_curr is None:
            history.append("")
            continue

        if abs(delta_curr - delta_prev) < 1e-12:
            history.append("유지")
        elif delta_prev < delta_curr:
            history.append("가속")
        else:
            history.append("둔화")

    return history


def _calc_trend_from_series(values, speed_calc_type, trend_unit):
    history = _build_speed_history(values, speed_calc_type)

    if not history:
        return ""

    current_speed = history[-1]
    if current_speed == "":
        return ""

    count = 1
    for speed in reversed(history[:-1]):
        if speed == current_speed:
            count += 1
        else:
            break

    return f"{count}{trend_unit}"


def _enrich_indicator(item, months):
    enriched = deepcopy(item)

    indicator = item["indicator"]
    if indicator not in RULE_MAP:
        enriched["change_display"] = ""
        enriched["speed"] = ""
        enriched["trend"] = ""
        return enriched

    rule = RULE_MAP[indicator]
    actual_values = _ordered_numeric_values(item["actual"], months)

    if len(actual_values) >= 2:
        change_value = _calc_change_value(
            current_value=actual_values[-1],
            previous_value=actual_values[-2],
            change_calc_type=rule["change_calc_type"],
        )
    else:
        change_value = None

    enriched["change_display"] = _format_change_value(change_value, rule["change_calc_type"])
    enriched["speed"] = _calc_speed_from_series(actual_values, rule["speed_calc_type"])
    enriched["trend"] = _calc_trend_from_series(
        actual_values,
        rule["speed_calc_type"],
        rule["trend_unit"],
    )

    return enriched


# -------------------------
# DB 조회
# -------------------------
def load_macro_meta(country="US", category="ALL"):
    where_parts = [
        "is_macro_tracker = TRUE",
        "is_active = TRUE",
        "country_code = :country",
    ]
    params = {"country": country}

    if category != "ALL":
        where_parts.append("macro_category = :category")
        params["category"] = category

    query = text(f"""
        SELECT
            series_id,
            series_code,
            series_name,
            country_code,
            macro_category,
            indicator_code,
            indicator_name_ko,
            indicator_name_en,
            frequency,
            unit,
            display_order
        FROM series_meta
        WHERE {' AND '.join(where_parts)}
        ORDER BY display_order, series_id
    """)

    df = pd.read_sql(query, engine, params=params)

    if df.empty:
        return df

    df["frequency"] = df["frequency"].apply(normalize_frequency)
    return df


def load_macro_series_values(series_codes):
    if not series_codes:
        return pd.DataFrame(columns=["series_code", "date_value", "value_num"])

    placeholders = ", ".join([f":code_{idx}" for idx in range(len(series_codes))])
    params = {f"code_{idx}": code for idx, code in enumerate(series_codes)}

    query = text(f"""
        SELECT
            m.series_code,
            d.date_value,
            d.value_num
        FROM series_data d
        JOIN series_meta m
          ON d.series_id = m.series_id
        WHERE m.series_code IN ({placeholders})
        ORDER BY m.series_code, d.date_value
    """)

    df = pd.read_sql(query, engine, params=params)

    if df.empty:
        return df

    df["date_value"] = pd.to_datetime(df["date_value"])
    df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce")
    df = df.dropna(subset=["value_num"]).sort_values(["series_code", "date_value"])

    return df


def build_indicator_item(meta_row, full_months, series_df):
    display_name = choose_display_name(meta_row)
    frequency = meta_row.get("frequency")
    unit = meta_row.get("unit")

    actual_map = {month: "" for month in full_months}
    series_rows = []

    if not series_df.empty:
        for _, row in series_df.iterrows():
            month_key = format_month_key(row["date_value"])
            value_num = float(row["value_num"])

            actual_map[month_key] = format_value_for_table(
                value=value_num,
                indicator=display_name,
                unit=unit,
                frequency=frequency,
            )

            series_rows.append(
                {
                    "date": format_chart_date(row["date_value"]),
                    "value": value_num,
                }
            )

        release_date = format_chart_date(series_df["date_value"].max())
    else:
        release_date = ""

    item = {
        "indicator": display_name,
        "category": clean_str(meta_row.get("macro_category")),
        "release_date": release_date,
        "asset_moves": {"stock": "", "bond": "", "fx": ""},
        "actual": actual_map,
        "expected": {},
        "previous": {},
        "series": series_rows,
        "frequency": frequency,
        "unit": unit,
        "series_code": clean_str(meta_row.get("series_code")),
    }

    return _enrich_indicator(item, full_months)


# -------------------------
# 외부 제공 함수
# -------------------------
def get_macro_tracker_payload(country="US", category="ALL"):
    meta_df = load_macro_meta(country=country, category=category)

    if meta_df.empty:
        return {
            "country": country,
            "months": [],
            "policy_row": {
                "label": "통화정책 국면",
                "values": {},
            },
            "indicators": [],
        }

    series_codes = meta_df["series_code"].dropna().astype(str).tolist()
    data_df = load_macro_series_values(series_codes)

    if data_df.empty:
        return {
            "country": country,
            "months": [],
            "policy_row": {
                "label": "통화정책 국면",
                "values": {},
            },
            "indicators": [],
        }

    full_months = (
        data_df["date_value"]
        .drop_duplicates()
        .sort_values()
        .apply(format_month_key)
        .tolist()
    )

    indicators = []
    for _, meta_row in meta_df.iterrows():
        series_code = clean_str(meta_row["series_code"])
        series_df = data_df[data_df["series_code"] == series_code].copy()
        item = build_indicator_item(meta_row, full_months, series_df)
        indicators.append(item)

    policy_label = "연준 통화정책 국면" if country == "US" else "통화정책 국면"

    return {
        "country": country,
        "months": full_months,
        "policy_row": {
            "label": policy_label,
            "values": {month: "" for month in full_months},
        },
        "indicators": indicators,
    }