# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 18:33:28 2026

@author: 박승욱
"""

# Macro Regime 계산 서비스

from datetime import date
from functools import lru_cache

import numpy as np
import pandas as pd

from services.data_service import load_chart_dataset


COUNTRY_SERIES_MAP = {
    "US": {
        "price": "SP500_YF",
        "cli": "OECD_CLI_USA",
        "cpi": "CPIAUCSL",
    },
    "KR": {
        "price": "KOSPI",
        "cli": "OECD_CLI_KOR",
        "cpi": "KOR_CPI",
    }
}

REGIME_ORDER = ["경기 과열", "골디락스", "경기 둔화", "스태그플레이션"]
ASSET_GROUP_ORDER = ["주식", "채권", "통화", "원자재", "대체자산"]


def month_diff_inclusive(start_ts, end_ts):
    start_ts = pd.to_datetime(start_ts)
    end_ts = pd.to_datetime(end_ts)
    return (end_ts.year - start_ts.year) * 12 + (end_ts.month - start_ts.month) + 1


def format_ym(ts):
    return pd.to_datetime(ts).strftime("%Y.%m")


def compute_macro_regime(cli_df, cpi_df):
    cli = cli_df[["date_value", "value_num"]].rename(columns={"value_num": "cli"}).copy()
    cpi = cpi_df[["date_value", "value_num"]].rename(columns={"value_num": "cpi"}).copy()

    cli["date_value"] = pd.to_datetime(cli["date_value"])
    cpi["date_value"] = pd.to_datetime(cpi["date_value"])

    df = pd.merge(cli, cpi, on="date_value", how="inner")
    df = df.sort_values("date_value").reset_index(drop=True)

    # -------------------------
    # Growth: OECD CLI 방향성
    # -------------------------
    df["growth"] = df["cli"].diff()

    # -------------------------
    # Inflation: CPI YoY의 3MMA - 36MMA
    # -------------------------
    df["cpi_yoy"] = df["cpi"].pct_change(periods=12) * 100.0
    df["cpi_yoy_3mma"] = df["cpi_yoy"].rolling(3).mean()
    df["cpi_yoy_36mma"] = df["cpi_yoy"].rolling(36).mean()
    df["inflation"] = df["cpi_yoy_3mma"] - df["cpi_yoy_36mma"]

    df = df.dropna(subset=["growth", "inflation"]).copy()

    df["growth_sign"] = np.where(df["growth"] >= 0, 1, -1)
    df["infl_sign"] = np.where(df["inflation"] >= 0, 1, -1)

    def classify(row):
        if row["growth_sign"] > 0 and row["infl_sign"] > 0:
            return "경기 과열"
        elif row["growth_sign"] > 0 and row["infl_sign"] < 0:
            return "골디락스"
        elif row["growth_sign"] < 0 and row["infl_sign"] < 0:
            return "경기 둔화"
        else:
            return "스태그플레이션"

    df["regime"] = df.apply(classify, axis=1)

    return df


def build_regime_intervals(regime_df):
    """
    차트 shading용 interval.
    각 국면의 끝점을 '다음 국면 시작월'로 둬서 배경이 빈틈 없이 채워지도록 한다.
    """
    if regime_df.empty:
        return []

    df = regime_df.sort_values("date_value").reset_index(drop=True)

    intervals = []
    start_date = df.loc[0, "date_value"]
    current_regime = df.loc[0, "regime"]

    for i in range(1, len(df)):
        row = df.loc[i]

        if row["regime"] != current_regime:
            intervals.append((start_date, row["date_value"], current_regime))
            start_date = row["date_value"]
            current_regime = row["regime"]

    intervals.append((start_date, df["date_value"].iloc[-1], current_regime))
    return intervals


def build_regime_spells(regime_df):
    """
    통계(spell)용 연속 구간.
    전환월은 새 국면의 시작월이므로, 이전 국면의 종료일은 직전 월로 둔다.
    """
    if regime_df.empty:
        return []

    df = regime_df.sort_values("date_value").reset_index(drop=True)

    spells = []
    start_date = df.loc[0, "date_value"]
    current_regime = df.loc[0, "regime"]

    for i in range(1, len(df)):
        row = df.loc[i]

        if row["regime"] != current_regime:
            spells.append((start_date, df.loc[i - 1, "date_value"], current_regime))
            start_date = row["date_value"]
            current_regime = row["regime"]

    spells.append((start_date, df["date_value"].iloc[-1], current_regime))
    return spells


def build_summary_table_df(regime_df, spells):
    """
    - 개월 수 / 비중(%): regime_df 실제 월 row 기준
    - 시작횟수 / Avg / Median / Max: spell 기준
    """
    # -------------------------
    # 1) 월 개수 / 비중: regime_df 실제 row 기준
    # -------------------------
    if regime_df is None or regime_df.empty:
        actual_month_count_map = {regime_name: 0 for regime_name in REGIME_ORDER}
        total_months = 0
    else:
        temp_regime_df = regime_df.copy()
        temp_regime_df["date_value"] = pd.to_datetime(temp_regime_df["date_value"])
        temp_regime_df = temp_regime_df.sort_values("date_value").reset_index(drop=True)

        actual_month_count_map = (
            temp_regime_df.groupby("regime")
            .size()
            .reindex(REGIME_ORDER, fill_value=0)
            .to_dict()
        )
        total_months = int(len(temp_regime_df))

    # -------------------------
    # 2) spell 통계
    # -------------------------
    spell_rows = []
    for start_date, end_date, regime_name in spells:
        spell_rows.append(
            {
                "regime": regime_name,
                "start_date": pd.to_datetime(start_date),
                "end_date": pd.to_datetime(end_date),
                "months": month_diff_inclusive(start_date, end_date),
            }
        )

    spell_df = pd.DataFrame(spell_rows)

    result_rows = []

    for regime_name in REGIME_ORDER:
        actual_months = int(actual_month_count_map.get(regime_name, 0))

        if spell_df.empty:
            occurrence_count = 0
            avg_months = 0.0
            median_months = 0.0
            max_months = 0
        else:
            temp = spell_df[spell_df["regime"] == regime_name].copy()
            occurrence_count = int(len(temp))
            avg_months = round(float(temp["months"].mean()), 1) if occurrence_count > 0 else 0.0
            median_months = round(float(temp["months"].median()), 1) if occurrence_count > 0 else 0.0
            max_months = int(temp["months"].max()) if occurrence_count > 0 else 0

        weight_pct = round((actual_months / total_months) * 100, 1) if total_months > 0 else 0.0

        result_rows.append(
            {
                "국면": regime_name,
                "개월 수": actual_months,
                "비중(%)": weight_pct,
                "시작횟수(회)": occurrence_count,
                "평균 유지 개월(Avg)": avg_months,
                "중앙값(Median)": median_months,
                "최댓값(Max)": max_months,
            }
        )

    total_occurrence_count = int(len(spell_df)) if not spell_df.empty else 0

    if spell_df.empty:
        total_avg_months = 0.0
        total_median_months = 0.0
        total_max_months = 0
    else:
        total_avg_months = round(float(spell_df["months"].mean()), 1)
        total_median_months = round(float(spell_df["months"].median()), 1)
        total_max_months = int(spell_df["months"].max())

    result_rows.append(
        {
            "국면": "총계",
            "개월 수": total_months,
            "비중(%)": 100.0 if total_months > 0 else 0.0,
            "시작횟수(회)": total_occurrence_count,
            "평균 유지 개월(Avg)": total_avg_months,
            "중앙값(Median)": total_median_months,
            "최댓값(Max)": total_max_months,
        }
    )

    return pd.DataFrame(result_rows)


def build_transition_matrix_df(regime_df):
    rows = []

    if regime_df is None or regime_df.empty or len(regime_df) < 2:
        for from_regime in REGIME_ORDER:
            for to_regime in REGIME_ORDER:
                rows.append(
                    {
                        "from_regime": from_regime,
                        "to_regime": to_regime,
                        "months": 0,
                        "ratio": 0.0,
                    }
                )
        return pd.DataFrame(rows)

    df = regime_df.sort_values("date_value").reset_index(drop=True)

    # 마지막 관측월은 제외:
    # t월이 from 국면이고, t+1월이 실제로 존재하는 경우만 집계
    for i in range(len(df) - 1):
        from_regime = df.loc[i, "regime"]
        to_regime = df.loc[i + 1, "regime"]
        rows.append({"from_regime": from_regime, "to_regime": to_regime})

    raw_df = pd.DataFrame(rows)

    if raw_df.empty:
        full_rows = []
        for from_regime in REGIME_ORDER:
            for to_regime in REGIME_ORDER:
                full_rows.append(
                    {
                        "from_regime": from_regime,
                        "to_regime": to_regime,
                        "months": 0,
                        "ratio": 0.0,
                    }
                )
        return pd.DataFrame(full_rows)

    months_df = (
        raw_df.groupby(["from_regime", "to_regime"])
        .size()
        .reset_index(name="months")
    )

    # 비중 분모 = 다음 달이 존재하는 from 국면 개월 수
    row_total_df = (
        months_df.groupby("from_regime")["months"]
        .sum()
        .reset_index(name="row_total_months")
    )

    months_df = months_df.merge(row_total_df, on="from_regime", how="left")
    months_df["ratio"] = np.where(
        months_df["row_total_months"] > 0,
        (months_df["months"] / months_df["row_total_months"] * 100).round(1),
        0.0,
    )

    full_rows = []
    for from_regime in REGIME_ORDER:
        for to_regime in REGIME_ORDER:
            full_rows.append({"from_regime": from_regime, "to_regime": to_regime})

    full_df = pd.DataFrame(full_rows)

    result = full_df.merge(
        months_df[["from_regime", "to_regime", "months", "ratio"]],
        on=["from_regime", "to_regime"],
        how="left",
    ).fillna({"months": 0, "ratio": 0.0})

    result["months"] = result["months"].astype(int)
    result["ratio"] = result["ratio"].astype(float)

    return result


def build_current_regime_card(spells):
    if not spells:
        return {
            "current_regime": "-",
            "start_text": "-",
            "duration_months": 0,
            "latest_text": "-",
        }

    start_date, end_date, regime_name = spells[-1]

    return {
        "current_regime": regime_name,
        "start_text": format_ym(start_date),
        "duration_months": month_diff_inclusive(start_date, end_date),
        "latest_text": format_ym(end_date),
    }


def build_asset_return_placeholder(start_date, end_date):
    label = f"{format_ym(start_date)} ~ {format_ym(end_date)}"

    rows = []
    for asset_group in ASSET_GROUP_ORDER:
        row = {"자산군": asset_group}
        for regime_name in REGIME_ORDER:
            row[regime_name] = "-"
        rows.append(row)

    return label, pd.DataFrame(rows)


def build_transition_table_placeholder(current_regime):
    scenario_columns = []

    for from_regime in REGIME_ORDER:
        for to_regime in REGIME_ORDER:
            scenario_columns.append(f"{from_regime}->{to_regime}")

    rows = []
    for asset_group in ASSET_GROUP_ORDER:
        row = {"자산군": asset_group}
        for scenario in scenario_columns:
            row[scenario] = 0.0
        rows.append(row)

    return pd.DataFrame(rows), scenario_columns, current_regime


@lru_cache(maxsize=32)
def cached_macro_regime_payload(country, start_date, end_date):
    mapping = COUNTRY_SERIES_MAP[country]

    # 차트 표시용: price + cli만
    chart_codes = [
        mapping["price"],
        mapping["cli"],
    ]

    chart_dataset = load_chart_dataset(
        selected_codes=chart_codes,
        start_date=start_date,
        end_date=end_date,
        axis_override_map={
            mapping["price"]: "left",
            mapping["cli"]: "right",
        },
    )

    # 계산용: cli + cpi
    calc_dataset = load_chart_dataset(
        selected_codes=[mapping["cli"], mapping["cpi"]],
        start_date=start_date,
        end_date=end_date,
        axis_override_map={
            mapping["cli"]: "right",
        },
    )

    cli_df = calc_dataset[calc_dataset["series_code"] == mapping["cli"]].copy()
    cpi_df = calc_dataset[calc_dataset["series_code"] == mapping["cpi"]].copy()

    regime_df = compute_macro_regime(cli_df, cpi_df)

    # 차트용 / 통계용 분리
    chart_intervals = build_regime_intervals(regime_df)
    spells = build_regime_spells(regime_df)

    if regime_df.empty:
        effective_start = pd.to_datetime(start_date)
        effective_end = pd.to_datetime(end_date)
    else:
        effective_start = regime_df["date_value"].min()
        effective_end = regime_df["date_value"].max()

    summary_df = build_summary_table_df(regime_df, spells)
    transition_df = build_transition_matrix_df(regime_df)
    current_card = build_current_regime_card(spells)
    asset_return_label, asset_return_df = build_asset_return_placeholder(effective_start, effective_end)
    transition_table_df, transition_columns, highlighted_from_regime = build_transition_table_placeholder(
        current_card["current_regime"]
    )

    return {
        "chart_dataset": chart_dataset,
        "intervals": chart_intervals,   # 차트 shading용
        "spells": spells,               # 통계용 spell
        "regime_df": regime_df,
        "summary_df": summary_df,
        "transition_df": transition_df,
        "current_card": current_card,
        "asset_return_label": asset_return_label,
        "asset_return_df": asset_return_df,
        "transition_table_df": transition_table_df,
        "transition_columns": transition_columns,
        "highlighted_from_regime": highlighted_from_regime,
        "effective_start": effective_start.strftime("%Y-%m-%d"),
        "effective_end": effective_end.strftime("%Y-%m-%d"),
    }


def get_macro_regime_payload(country, start_date, end_date):
    if end_date is None:
        end_date = date.today().isoformat()

    return cached_macro_regime_payload(country, start_date, end_date)