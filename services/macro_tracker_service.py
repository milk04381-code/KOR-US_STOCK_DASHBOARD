# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 18:13:41 2026

@author: 박승욱
"""

# services/macro_tracker_service.py

from copy import deepcopy


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


# -------------------------
# MOCK DATA
# -------------------------
MOCK_DB = {
    "US": {
        "policy_label": "연준 통화정책 국면",
        "months": ["2401", "2402", "2405"],
        "policy_rate_series": {
            "2401": 5.25,
            "2402": 5.50,
            "2405": 5.25,
        },
        "indicators": [
            {
                "indicator": "NFP",
                "category": "고용",
                "release_date": "240607",
                "asset_moves": {"stock": "-0.8%", "bond": "+6bp", "fx": "+0.4%"},
                "actual": {"2401": "170k", "2402": "130k", "2405": "65k"},
                "expected": {"2401": "160k", "2402": "120k", "2405": "70k"},
                "previous": {"2401": "150k", "2402": "140k", "2405": "60k"},
            },
            {
                "indicator": "실업률",
                "category": "고용",
                "release_date": "240607",
                "asset_moves": {"stock": "-0.8%", "bond": "+6bp", "fx": "+0.4%"},
                "actual": {"2401": "4.1%", "2402": "4.2%", "2405": "4.5%"},
                "expected": {"2401": "4.0%", "2402": "4.1%", "2405": "4.4%"},
                "previous": {"2401": "4.0%", "2402": "4.1%", "2405": "4.4%"},
            },
            {
                "indicator": "참여율",
                "category": "고용",
                "release_date": "240607",
                "asset_moves": {"stock": "-0.8%", "bond": "+6bp", "fx": "+0.4%"},
                "actual": {"2401": "62.5%", "2402": "62.6%", "2405": "62.8%"},
                "expected": {"2401": "62.4%", "2402": "62.5%", "2405": "62.7%"},
                "previous": {"2401": "62.3%", "2402": "62.4%", "2405": "62.6%"},
            },
            {
                "indicator": "소매판매",
                "category": "소득과 지출",
                "release_date": "240614",
                "asset_moves": {"stock": "+0.4%", "bond": "-3bp", "fx": "-0.2%"},
                "actual": {"2401": "0.8%", "2402": "0.6%", "2405": "0.2%"},
                "expected": {"2401": "0.7%", "2402": "0.5%", "2405": "0.3%"},
                "previous": {"2401": "0.6%", "2402": "0.5%", "2405": "0.1%"},
            },
            {
                "indicator": "광공업생산",
                "category": "산업",
                "release_date": "240614",
                "asset_moves": {"stock": "+0.2%", "bond": "-1bp", "fx": "-0.1%"},
                "actual": {"2401": "1.3%", "2402": "1.0%", "2405": "0.4%"},
                "expected": {"2401": "1.1%", "2402": "0.9%", "2405": "0.5%"},
                "previous": {"2401": "1.0%", "2402": "0.8%", "2405": "0.3%"},
            },
            {
                "indicator": "CPI",
                "category": "물가",
                "release_date": "240612",
                "asset_moves": {"stock": "-1.0%", "bond": "+9bp", "fx": "+0.5%"},
                "actual": {"2401": "3.1%", "2402": "3.3%", "2405": "3.5%"},
                "expected": {"2401": "3.0%", "2402": "3.2%", "2405": "3.4%"},
                "previous": {"2401": "2.9%", "2402": "3.1%", "2405": "3.4%"},
            },
            {
                "indicator": "주택착공",
                "category": "주택",
                "release_date": "240618",
                "asset_moves": {"stock": "+0.1%", "bond": "-2bp", "fx": "-0.1%"},
                "actual": {"2401": "1.45", "2402": "1.41", "2405": "1.32"},
                "expected": {"2401": "1.43", "2402": "1.40", "2405": "1.31"},
                "previous": {"2401": "1.42", "2402": "1.39", "2405": "1.30"},
            },
        ],
    },
    "KR": {
        "policy_label": "한국은행 통화정책 국면",
        "months": ["2401", "2402", "2405"],
        "policy_rate_series": {
            "2401": 3.50,
            "2402": 3.50,
            "2405": 3.50,
        },
        "indicators": [
            {
                "indicator": "취업자수",
                "category": "고용",
                "release_date": "240612",
                "asset_moves": {"stock": "+0.3%", "bond": "-2bp", "fx": "-0.2%"},
                "actual": {"2401": "280", "2402": "300", "2405": "180"},
                "expected": {"2401": "270", "2402": "290", "2405": "170"},
                "previous": {"2401": "260", "2402": "280", "2405": "160"},
            },
            {
                "indicator": "한국 실업률",
                "category": "고용",
                "release_date": "240612",
                "asset_moves": {"stock": "+0.3%", "bond": "-2bp", "fx": "-0.2%"},
                "actual": {"2401": "3.0%", "2402": "2.9%", "2405": "2.9%"},
                "expected": {"2401": "3.1%", "2402": "2.9%", "2405": "2.8%"},
                "previous": {"2401": "3.1%", "2402": "3.0%", "2405": "2.8%"},
            },
        ],
    },
}


# -------------------------
# 기본 유틸
# -------------------------
def parse_display_value(value):
    if value is None:
        return None

    text = str(value).strip()

    if text == "":
        return None

    text = text.replace(",", "")

    if text.endswith("%p"):
        return float(text[:-2])

    if text.endswith("bp"):
        return float(text[:-2])

    if text.endswith("k"):
        return float(text[:-1])

    if text.endswith("%"):
        return float(text[:-1])

    return float(text)


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


def _compute_policy_phase_row(policy_rate_series, months):
    phases = {}
    ordered_rates = [policy_rate_series.get(month) for month in months]

    for idx, month in enumerate(months):
        current = ordered_rates[idx]

        if idx == 0:
            phases[month] = "유지"
            continue

        previous = ordered_rates[idx - 1]

        if current == previous:
            phases[month] = "유지"
            continue

        if current > previous:
            phases[month] = "긴축"
        else:
            phases[month] = "완화"

    return phases


def _enrich_indicator(item, months):
    enriched = deepcopy(item)

    indicator = item["indicator"]
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
# 외부 제공 함수
# -------------------------
def get_macro_tracker_payload(country="US", category="ALL"):
    base = MOCK_DB[country]
    months = base["months"]

    if category == "ALL":
        filtered = base["indicators"]
    else:
        filtered = [item for item in base["indicators"] if item["category"] == category]

    enriched_indicators = [_enrich_indicator(item, months) for item in filtered]

    policy_values = _compute_policy_phase_row(base["policy_rate_series"], months)

    return {
        "country": country,
        "months": months,
        "policy_row": {
            "label": base["policy_label"],
            "values": policy_values,
        },
        "indicators": enriched_indicators,
    }