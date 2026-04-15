# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 18:13:41 2026

@author: 박승욱
"""

# services/macro_tracker_service.py
# 수정본
# 반영 범위:
# 1) 전기 대비 변동 / 속도 / 추세 로직 수정
# - NFP change_calc_type: diff
# - 계산 입력: 표시 문자열이 아닌 raw numeric 기준
# - 속도: 상승 가속 / 상승 둔화 / 하락 가속 / 하락 둔화 / 유지 / 상승 전환 / 하락 전환
# - 추세: 방향 지속기간 (상승 n개월 / 하락 n개월 / 유지 n개월)
#
# 2) 정책 국면 칼럼 구현
# - 비교 대상: 기준금리 목표범위 상단
#   * 2008-12-15 이전: DFEDTAR
#   * 2008-12-16 이후: DFEDTARU
# - 판정 기준:
#   * 이번 기준시기 금리 > 직전 기준시기 금리 -> 긴축
#   * 이번 기준시기 금리 < 직전 기준시기 금리 -> 완화
#   * 같으면 유지
# - 출력 위치를 위한 section-level payload 제공:
#   * policy_phase_by_period = {period_key: "긴축/완화/유지"}
#
# 3) 성능 개선
# - cached_macro_payload() 추가
# - include_policy_phase 옵션 추가
#   * 표 렌더 시 True
#   * 차트 등 경량 호출 시 False 가능

from copy import deepcopy
from functools import lru_cache

import pandas as pd
from sqlalchemy import text

from db import engine


RULE_MAP = {
    "NFP": {
        "trend_unit": "개월",
        "change_calc_type": "diff",
        "speed_calc_type": "diff",
    },
    "실업률": {
        "trend_unit": "개월",
        "change_calc_type": "pp_diff",
        "speed_calc_type": "pp_diff",
    },
    "참여율": {
        "trend_unit": "개월",
        "change_calc_type": "pp_diff",
        "speed_calc_type": "pp_diff",
    },
    "소매판매": {
        "trend_unit": "개월",
        "change_calc_type": "pct_change",
        "speed_calc_type": "pct_change",
    },
    "광공업생산": {
        "trend_unit": "개월",
        "change_calc_type": "pct_change",
        "speed_calc_type": "pct_change",
    },
    "CPI": {
        "trend_unit": "개월",
        "change_calc_type": "pct_change",
        "speed_calc_type": "pct_change",
    },
    "PPI": {
        "trend_unit": "개월",
        "change_calc_type": "pct_change",
        "speed_calc_type": "pct_change",
    },
    "PCE 물가지수": {
        "trend_unit": "개월",
        "change_calc_type": "pct_change",
        "speed_calc_type": "pct_change",
    },
    "주택착공": {
        "trend_unit": "개월",
        "change_calc_type": "pct_change",
        "speed_calc_type": "pct_change",
    },
    "취업자수": {
        "trend_unit": "개월",
        "change_calc_type": "diff",
        "speed_calc_type": "diff",
    },
    "한국 실업률": {
        "trend_unit": "개월",
        "change_calc_type": "pp_diff",
        "speed_calc_type": "pp_diff",
    },
    "설비가동률": {
        "trend_unit": "개월",
        "change_calc_type": "pp_diff",
        "speed_calc_type": "pp_diff",
    },
    "10년 기대인플레이션": {
        "trend_unit": "일",
        "change_calc_type": "pp_diff",
        "speed_calc_type": "pp_diff",
    },
}

DISPLAY_NAME_MAP = {
    "비농업고용": "NFP",
    "실업률": "실업률",
    "경제활동참가율": "참여율",
    "광의실업률(U6)": "광의실업률(U6)",
    "신규 실업수당청구건수": "신규 실업수당청구건수",
    "개인소득": "개인소득",
    "개인소비지출": "개인소비지출",
    "소매판매": "소매판매",
    "산업생산": "광공업생산",
    "설비가동률": "설비가동률",
    "내구재주문": "내구재주문",
    "핵심 자본재 수주": "핵심 자본재 수주",
    "CPI": "CPI",
    "PPI": "PPI",
    "PCE 물가지수": "PCE 물가지수",
    "10년 기대인플레이션": "10년 기대인플레이션",
    "주택착공": "주택착공",
    "취업자수": "취업자수",
    "한국 실업률": "한국 실업률",
}

FREQUENCY_LABEL_MAP = {
    "monthly": "월간",
    "weekly": "주간",
    "daily": "일간",
    "quarterly": "분기",
    "yearly": "연간",
}

POLICY_CUTOFF_DATE = pd.Timestamp("2008-12-16")
EPS = 1e-12


# -------------------------
# 기본 유틸
# -------------------------
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


def choose_display_name(meta_row):
    name_ko = clean_str(meta_row.get("indicator_name_ko"))
    series_name = clean_str(meta_row.get("series_name"))

    if name_ko:
        return DISPLAY_NAME_MAP.get(name_ko, name_ko)

    return series_name


def format_period_key(ts, frequency):
    dt = pd.to_datetime(ts)
    freq = normalize_frequency(frequency)

    if freq == "monthly":
        return dt.strftime("%Y%m")
    if freq == "weekly":
        return dt.strftime("%Y%m%d")
    if freq == "daily":
        return dt.strftime("%Y%m%d")
    if freq == "quarterly":
        quarter = ((dt.month - 1) // 3) + 1
        return f"{dt.strftime('%Y')}Q{quarter}"
    if freq == "yearly":
        return dt.strftime("%Y")

    return dt.strftime("%Y-%m-%d")


def format_period_label(ts, frequency):
    dt = pd.to_datetime(ts)
    freq = normalize_frequency(frequency)

    if freq == "monthly":
        return dt.strftime("%Y%m")
    if freq == "weekly":
        return dt.strftime("%y%m%d")
    if freq == "daily":
        return dt.strftime("%y%m%d")
    if freq == "quarterly":
        quarter = ((dt.month - 1) // 3) + 1
        return f"{dt.strftime('%y')}Q{quarter}"
    if freq == "yearly":
        return dt.strftime("%Y")

    return dt.strftime("%Y-%m-%d")


def format_chart_date(ts):
    return pd.to_datetime(ts).strftime("%Y-%m-%d")


def format_value_for_table(value, series_code, unit, frequency):
    if value is None or pd.isna(value):
        return ""

    series_code = clean_str(series_code).upper()
    unit = clean_str(unit).lower()
    frequency = normalize_frequency(frequency)

    if series_code == "PAYEMS":
        return f"{value:,.0f}k"

    if series_code == "T10YIE":
        return f"{value:.2f}%"

    if "%" in unit or "percent" in unit or "percentage" in unit:
        return f"{value:.1f}%"

    if "index" in unit:
        return f"{value:.1f}"

    if "persons" in unit or "number" in unit:
        return f"{value:,.1f}"

    if frequency == "daily":
        return f"{value:,.2f}"

    return f"{value:,.1f}"


def _ordered_numeric_values_from_map(value_map, period_keys):
    values = []

    for key in period_keys:
        value = value_map.get(key)
        if value is None or pd.isna(value):
            continue
        values.append(float(value))

    return values


# -------------------------
# 계산 함수
# -------------------------
def _calc_change_value(current_value, previous_value, change_calc_type):
    if previous_value is None or current_value is None:
        return None

    if change_calc_type == "pct_change":
        if abs(previous_value) < EPS:
            return None
        return ((current_value - previous_value) / abs(previous_value)) * 100.0

    if change_calc_type == "diff":
        return current_value - previous_value

    if change_calc_type == "pp_diff":
        return current_value - previous_value

    if change_calc_type == "bps_diff":
        return (current_value - previous_value) * 100.0

    return None


def _format_change_value(change_value, change_calc_type, series_code="", unit=""):
    if change_value is None:
        return ""

    series_code = clean_str(series_code).upper()
    unit = clean_str(unit).lower()

    if change_calc_type == "pct_change":
        return f"{change_value:+.1f}%"

    if change_calc_type == "diff":
        if series_code == "PAYEMS":
            return f"{change_value:+,.1f}k"

        if "persons" in unit or "number" in unit:
            return f"{change_value:+,.1f}"

        if "index" in unit:
            return f"{change_value:+.1f}"

        return f"{change_value:+.1f}"

    if change_calc_type == "pp_diff":
        return f"{change_value:+.1f}%p"

    if change_calc_type == "bps_diff":
        return f"{change_value:+.0f}bp"

    return ""


def _classify_speed(delta_prev, delta_curr):
    if delta_prev is None or delta_curr is None:
        return ""

    if abs(delta_curr - delta_prev) < EPS:
        return "유지"

    if delta_prev < -EPS and delta_curr > EPS:
        return "상승 전환"
    if delta_prev > EPS and delta_curr < -EPS:
        return "하락 전환"
    if abs(delta_prev) <= EPS and delta_curr > EPS:
        return "상승 전환"
    if abs(delta_prev) <= EPS and delta_curr < -EPS:
        return "하락 전환"

    if abs(delta_curr) <= EPS:
        if delta_prev > EPS:
            return "상승 둔화"
        if delta_prev < -EPS:
            return "하락 둔화"
        return "유지"

    direction = "상승" if delta_curr > 0 else "하락"

    if abs(delta_curr) > abs(delta_prev):
        return f"{direction} 가속"

    return f"{direction} 둔화"


def _calc_speed_from_series(values, speed_calc_type):
    if len(values) < 3:
        return ""

    delta_prev = _calc_change_value(values[-2], values[-3], speed_calc_type)
    delta_curr = _calc_change_value(values[-1], values[-2], speed_calc_type)

    return _classify_speed(delta_prev, delta_curr)


def _classify_direction(change_value):
    if change_value is None:
        return ""

    if abs(change_value) < EPS:
        return "유지"
    if change_value > 0:
        return "상승"
    return "하락"


def _build_direction_history(values, change_calc_type):
    history = []

    for idx in range(1, len(values)):
        change_value = _calc_change_value(
            values[idx],
            values[idx - 1],
            change_calc_type,
        )
        history.append(_classify_direction(change_value))

    return history


def _calc_trend_from_series(values, change_calc_type, trend_unit):
    history = _build_direction_history(values, change_calc_type)

    if not history:
        return ""

    current_direction = history[-1]
    if current_direction == "":
        return ""

    count = 1
    for direction in reversed(history[:-1]):
        if direction == current_direction:
            count += 1
        else:
            break

    return f"{current_direction} {count}{trend_unit}"


# -------------------------
# 정책 국면 함수
# -------------------------
def load_policy_target_upper_series(end_date=None):
    if end_date is None:
        end_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    query = text("""
        SELECT
            m.series_code,
            d.date_value,
            d.value_num
        FROM series_data d
        JOIN series_meta m
          ON d.series_id = m.series_id
        WHERE m.series_code IN ('DFEDTAR', 'DFEDTARU')
          AND d.date_value <= :end_date
        ORDER BY d.date_value, m.series_code
    """)

    df = pd.read_sql(query, engine, params={"end_date": end_date})

    if df.empty:
        return pd.DataFrame(columns=["date_value", "target_upper"])

    df["date_value"] = pd.to_datetime(df["date_value"])
    df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce")
    df = df.dropna(subset=["value_num"]).sort_values(["date_value", "series_code"])

    old_df = df[
        (df["series_code"] == "DFEDTAR") &
        (df["date_value"] < POLICY_CUTOFF_DATE)
    ][["date_value", "value_num"]].copy()

    new_df = df[
        (df["series_code"] == "DFEDTARU") &
        (df["date_value"] >= POLICY_CUTOFF_DATE)
    ][["date_value", "value_num"]].copy()

    merged = pd.concat([old_df, new_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["date_value"], keep="last")
    merged = merged.sort_values("date_value").reset_index(drop=True)
    merged = merged.rename(columns={"value_num": "target_upper"})

    return merged


def _get_policy_target_asof(policy_df, ref_date):
    if policy_df is None or policy_df.empty or ref_date is None:
        return None

    ref_ts = pd.to_datetime(ref_date)
    temp = policy_df.loc[policy_df["date_value"] <= ref_ts]

    if temp.empty:
        return None

    return float(temp["target_upper"].iloc[-1])


def _classify_policy_phase(previous_rate, current_rate):
    if previous_rate is None or current_rate is None:
        return ""

    if current_rate > previous_rate:
        return "긴축"
    if current_rate < previous_rate:
        return "완화"
    return "유지"


def build_policy_phase_by_period(period_dates, frequency, policy_df, country_code):
    if clean_str(country_code).upper() != "US":
        return {}

    if policy_df is None or policy_df.empty:
        return {}

    if not period_dates:
        return {}

    freq = normalize_frequency(frequency)
    policy_map = {}
    ordered_dates = pd.to_datetime(pd.Series(period_dates)).sort_values().tolist()

    for idx, curr_ref in enumerate(ordered_dates):
        curr_key = format_period_key(curr_ref, freq)

        if idx == 0:
            policy_map[curr_key] = ""
            continue

        prev_ref = ordered_dates[idx - 1]
        previous_rate = _get_policy_target_asof(policy_df, prev_ref)
        current_rate = _get_policy_target_asof(policy_df, curr_ref)

        policy_map[curr_key] = _classify_policy_phase(previous_rate, current_rate)

    return policy_map


def _enrich_indicator(item, period_keys):
    enriched = deepcopy(item)

    indicator = item["indicator"]
    rule = RULE_MAP.get(indicator)

    if rule is None:
        enriched["change_display"] = ""
        enriched["speed"] = ""
        enriched["trend"] = ""
        return enriched

    actual_values = _ordered_numeric_values_from_map(
        item.get("actual_num", {}),
        period_keys,
    )

    if len(actual_values) >= 2:
        change_value = _calc_change_value(
            current_value=actual_values[-1],
            previous_value=actual_values[-2],
            change_calc_type=rule["change_calc_type"],
        )
    else:
        change_value = None

    enriched["change_display"] = _format_change_value(
        change_value,
        rule["change_calc_type"],
        series_code=item.get("series_code", ""),
        unit=item.get("unit", ""),
    )
    enriched["speed"] = _calc_speed_from_series(
        actual_values,
        rule["speed_calc_type"],
    )
    enriched["trend"] = _calc_trend_from_series(
        actual_values,
        rule["change_calc_type"],
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


def load_macro_series_values(series_codes, start_date=None, end_date=None):
    if not series_codes:
        return pd.DataFrame(columns=["series_code", "date_value", "value_num"])

    if start_date is None:
        start_date = "1970-01-01"
    if end_date is None:
        end_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    placeholders = ", ".join([f":code_{idx}" for idx in range(len(series_codes))])
    params = {f"code_{idx}": code for idx, code in enumerate(series_codes)}
    params["start_date"] = start_date
    params["end_date"] = end_date

    query = text(f"""
        SELECT
            m.series_code,
            d.date_value,
            d.value_num
        FROM series_data d
        JOIN series_meta m
          ON d.series_id = m.series_id
        WHERE m.series_code IN ({placeholders})
          AND d.date_value BETWEEN :start_date AND :end_date
        ORDER BY m.series_code, d.date_value
    """)

    df = pd.read_sql(query, engine, params=params)

    if df.empty:
        return df

    df["date_value"] = pd.to_datetime(df["date_value"])
    df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce")
    df = df.dropna(subset=["value_num"]).sort_values(["series_code", "date_value"])

    return df


def build_indicator_item(meta_row, section_keys, series_df):
    display_name = choose_display_name(meta_row)
    frequency = normalize_frequency(meta_row.get("frequency"))
    unit = clean_str(meta_row.get("unit"))
    series_code = clean_str(meta_row.get("series_code"))

    actual_map = {key: "" for key in section_keys}
    actual_num_map = {key: None for key in section_keys}
    series_rows = []

    if not series_df.empty:
        for _, row in series_df.iterrows():
            key = format_period_key(row["date_value"], frequency)
            value_num = float(row["value_num"])

            actual_num_map[key] = value_num
            actual_map[key] = format_value_for_table(
                value=value_num,
                series_code=series_code,
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
        "series_code": series_code,
        "indicator": display_name,
        "category": clean_str(meta_row.get("macro_category")),
        "release_date": release_date,
        "asset_moves": {"stock": "", "bond": "", "fx": ""},
        "actual": actual_map,
        "actual_num": actual_num_map,
        "series": series_rows,
        "frequency": frequency,
        "unit": unit,
    }

    return _enrich_indicator(item, section_keys)


def build_section_payload(
    meta_df,
    data_df,
    frequency,
    policy_df=None,
    include_policy_phase=True,
):
    section_meta = meta_df[meta_df["frequency"] == frequency].copy()

    if section_meta.empty:
        return None

    section_codes = section_meta["series_code"].dropna().astype(str).tolist()
    section_data = data_df[data_df["series_code"].isin(section_codes)].copy()

    if section_data.empty:
        period_dates = []
        period_keys = []
        period_labels = []
    else:
        period_dates = (
            section_data["date_value"]
            .dropna()
            .drop_duplicates()
            .sort_values()
            .tolist()
        )

        period_keys = [format_period_key(x, frequency) for x in period_dates]
        period_labels = [format_period_label(x, frequency) for x in period_dates]

    indicators = []
    for _, meta_row in section_meta.iterrows():
        series_code = clean_str(meta_row["series_code"])
        series_df = section_data[section_data["series_code"] == series_code].copy()
        item = build_indicator_item(meta_row, period_keys, series_df)
        indicators.append(item)

    country_code = ""
    if not section_meta.empty:
        country_code = clean_str(section_meta.iloc[0].get("country_code"))

    if include_policy_phase:
        policy_phase_by_period = build_policy_phase_by_period(
            period_dates=period_dates,
            frequency=frequency,
            policy_df=policy_df,
            country_code=country_code,
        )
    else:
        policy_phase_by_period = {}

    return {
        "frequency": frequency,
        "frequency_label": FREQUENCY_LABEL_MAP.get(frequency, frequency),
        "period_keys": period_keys,
        "period_labels": period_labels,
        "policy_phase_by_period": policy_phase_by_period,
        "indicators": indicators,
    }


# -------------------------
# 외부 제공 함수
# -------------------------
@lru_cache(maxsize=32)
def cached_macro_payload(country, category, start_date, end_date):
    return get_macro_tracker_payload(
        country=country,
        category=category,
        start_date=start_date,
        end_date=end_date,
        include_policy_phase=True,
    )


def get_macro_tracker_payload(
    country="US",
    category="ALL",
    start_date="1970-01-01",
    end_date=None,
    include_policy_phase=True,
):
    if end_date is None:
        end_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    meta_df = load_macro_meta(country=country, category=category)

    if meta_df.empty:
        return {
            "country": country,
            "sections": [],
        }

    series_codes = meta_df["series_code"].dropna().astype(str).tolist()
    data_df = load_macro_series_values(
        series_codes,
        start_date=start_date,
        end_date=end_date,
    )

    if data_df.empty:
        return {
            "country": country,
            "sections": [],
        }

    policy_df = pd.DataFrame(columns=["date_value", "target_upper"])
    if include_policy_phase and clean_str(country).upper() == "US":
        policy_df = load_policy_target_upper_series(end_date=end_date)

    ordered_frequencies = ["monthly", "weekly", "daily", "quarterly", "yearly"]
    sections = []

    for frequency in ordered_frequencies:
        section = build_section_payload(
            meta_df=meta_df,
            data_df=data_df,
            frequency=frequency,
            policy_df=policy_df,
            include_policy_phase=include_policy_phase,
        )
        if section is not None and section["indicators"]:
            sections.append(section)

    return {
        "country": country,
        "sections": sections,
    }