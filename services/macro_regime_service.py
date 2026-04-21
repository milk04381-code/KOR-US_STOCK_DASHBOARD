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

ASSET_PROXY_LIST = [
    {"자산군": "주식", "구분": "글로벌", "ETF": "ACWI"},
    {"자산군": "주식", "구분": "선진국", "ETF": "URTH"},
    {"자산군": "주식", "구분": "신흥국", "ETF": "EEM"},
    {"자산군": "주식", "구분": "미국", "ETF": "SPY"},

    {"자산군": "채권", "구분": "장기국채", "ETF": "TLT"},
    {"자산군": "채권", "구분": "IG", "ETF": "LQD"},
    {"자산군": "채권", "구분": "HY", "ETF": "HYG"},
    {"자산군": "채권", "구분": "물가연동국채", "ETF": "TIP"},

    {"자산군": "통화", "구분": "달러", "ETF": "UUP"},
    {"자산군": "통화", "구분": "EM FX", "ETF": "CEW"},

    {"자산군": "원자재", "구분": "원유", "ETF": "DBO"},
    {"자산군": "원자재", "구분": "금", "ETF": "GLD"},
    {"자산군": "원자재", "구분": "구리", "ETF": "CPER"},
    {"자산군": "원자재", "구분": "농산물", "ETF": "DBA"},

    {"자산군": "대체자산", "구분": "리츠", "ETF": "REET"},
    {"자산군": "대체자산", "구분": "인프라", "ETF": "IGF"},

    {"자산군": "현금", "구분": "초단기국채", "ETF": "SGOV"},
]


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

    df["growth"] = df["cli"]

    df["cpi_yoy"] = df["cpi"].pct_change(periods=12) * 100.0
    df["cpi_yoy_3mma"] = df["cpi_yoy"].rolling(3).mean()
    df["inflation"] = df["cpi_yoy_3mma"]

    df = df.dropna(subset=["growth", "inflation"]).copy()

    df["growth_sign"] = np.where(df["growth"] >= 100.0, 1, -1)
    df["infl_sign"] = np.where(df["inflation"] > 3.0, 1, -1)

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


def load_asset_price_dataset(start_date, end_date):
    selected_codes = [item["ETF"] for item in ASSET_PROXY_LIST]

    return load_chart_dataset(
        selected_codes=selected_codes,
        start_date=start_date,
        end_date=end_date,
    )


def build_asset_monthly_return_df(asset_price_df):
    if asset_price_df is None or asset_price_df.empty:
        return pd.DataFrame(columns=["month", "ETF", "monthly_price", "monthly_return"])

    df = asset_price_df[["series_code", "date_value", "value_num"]].copy()

    df["date_value"] = pd.to_datetime(df["date_value"], errors="coerce")
    df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce", downcast="float")
    df = df.dropna(subset=["date_value", "value_num"])

    if df.empty:
        return pd.DataFrame(columns=["month", "ETF", "monthly_price", "monthly_return"])

    df["series_code"] = df["series_code"].astype("category")
    df = df.sort_values(["series_code", "date_value"])

    result_frames = []

    for etf, group in df.groupby("series_code", observed=True, sort=False):
        s = group.set_index("date_value")["value_num"].sort_index()
        monthly_price = s.resample("ME").last()
        monthly_return = monthly_price.pct_change()

        temp = pd.DataFrame({
            "month": monthly_price.index,
            "ETF": etf,
            "monthly_price": monthly_price.values,
            "monthly_return": monthly_return.values,
        })
        result_frames.append(temp)

    if not result_frames:
        return pd.DataFrame(columns=["month", "ETF", "monthly_price", "monthly_return"])

    result = pd.concat(result_frames, ignore_index=True)
    result["month"] = pd.to_datetime(result["month"]).dt.to_period("M").dt.to_timestamp("M")
    result["ETF"] = result["ETF"].astype("category")
    result["monthly_price"] = pd.to_numeric(result["monthly_price"], downcast="float")
    result["monthly_return"] = pd.to_numeric(result["monthly_return"], downcast="float")

    return result


def build_asset_forward_3m_df(asset_monthly_df):
    if asset_monthly_df is None or asset_monthly_df.empty:
        return pd.DataFrame(columns=["month", "ETF", "forward_3m_return"])

    df = asset_monthly_df[["month", "ETF", "monthly_price"]].copy()
    df = df.sort_values(["ETF", "month"]).reset_index(drop=True)

    df["forward_3m_return"] = (
        df.groupby("ETF", observed=True)["monthly_price"].shift(-3) / df["monthly_price"] - 1.0
    )
    df["forward_3m_return"] = pd.to_numeric(df["forward_3m_return"], downcast="float")

    return df[["month", "ETF", "forward_3m_return"]].copy()


def build_asset_return_placeholder(start_date, end_date):
    label = f"{format_ym(start_date)} ~ {format_ym(end_date)}"

    rows = []
    for item in ASSET_PROXY_LIST:
        row = {
            "자산군": item["자산군"],
            "구분": item["구분"],
            "ETF": item["ETF"],
        }
        for regime_name in REGIME_ORDER:
            row[regime_name] = "-"
        rows.append(row)

    return label, pd.DataFrame(rows)


def build_asset_return_table_df(start_date, end_date, regime_df, asset_monthly_df=None):
    label = f"{format_ym(start_date)} ~ {format_ym(end_date)}"

    if asset_monthly_df is None:
        asset_price_df = load_asset_price_dataset(start_date, end_date)
        asset_monthly_df = build_asset_monthly_return_df(asset_price_df)

    if regime_df is None or regime_df.empty or asset_monthly_df.empty:
        return build_asset_return_placeholder(start_date, end_date)

    regime_month_df = regime_df[["date_value", "regime"]].copy()
    regime_month_df["month"] = pd.to_datetime(regime_month_df["date_value"]).dt.to_period("M").dt.to_timestamp("M")
    regime_month_df = regime_month_df[["month", "regime"]].drop_duplicates(subset=["month"])
    regime_month_df["regime"] = regime_month_df["regime"].astype("category")

    merged_df = pd.merge(asset_monthly_df, regime_month_df, on="month", how="inner")
    merged_df = merged_df.dropna(subset=["monthly_return"])

    rows = []
    for item in ASSET_PROXY_LIST:
        etf = item["ETF"]
        temp = merged_df[merged_df["ETF"] == etf]

        row = {
            "자산군": item["자산군"],
            "구분": item["구분"],
            "ETF": etf,
        }

        for regime_name in REGIME_ORDER:
            regime_temp = temp[temp["regime"] == regime_name]
            if regime_temp.empty:
                row[regime_name] = "-"
            else:
                row[regime_name] = f"{regime_temp['monthly_return'].mean() * 100:.1f}"

        rows.append(row)

    return label, pd.DataFrame(rows)


def build_transition_table_placeholder(current_regime):
    scenario_columns = []

    for from_regime in REGIME_ORDER:
        for to_regime in REGIME_ORDER:
            scenario_columns.append(f"{from_regime}->{to_regime}")

    rows = []
    for item in ASSET_PROXY_LIST:
        row = {
            "자산군": item["자산군"],
            "구분": item["구분"],
            "ETF": item["ETF"],
        }
        for scenario in scenario_columns:
            row[scenario] = 0.0
        rows.append(row)

    return pd.DataFrame(rows), scenario_columns, current_regime


def build_transition_return_table_df(start_date, end_date, regime_df, current_regime, asset_monthly_df=None):
    if asset_monthly_df is None:
        asset_price_df = load_asset_price_dataset(start_date, end_date)
        asset_monthly_df = build_asset_monthly_return_df(asset_price_df)

    asset_forward_df = build_asset_forward_3m_df(asset_monthly_df)

    if regime_df is None or regime_df.empty or len(regime_df) < 2 or asset_forward_df.empty:
        return build_transition_table_placeholder(current_regime)

    regime_month_df = regime_df[["date_value", "regime"]].copy()
    regime_month_df["month"] = pd.to_datetime(regime_month_df["date_value"]).dt.to_period("M").dt.to_timestamp("M")
    regime_month_df = (
        regime_month_df[["month", "regime"]]
        .drop_duplicates(subset=["month"])
        .sort_values("month")
        .reset_index(drop=True)
    )

    regime_month_df["from_regime"] = regime_month_df["regime"].shift(1)
    regime_month_df["to_regime"] = regime_month_df["regime"]
    regime_month_df = regime_month_df.dropna(subset=["from_regime", "to_regime"]).copy()
    regime_month_df["scenario"] = regime_month_df["from_regime"] + "->" + regime_month_df["to_regime"]

    merged_df = pd.merge(
        asset_forward_df,
        regime_month_df[["month", "from_regime", "to_regime", "scenario"]],
        on="month",
        how="inner",
    )
    merged_df = merged_df.dropna(subset=["forward_3m_return"])

    scenario_columns = []
    for from_regime in REGIME_ORDER:
        for to_regime in REGIME_ORDER:
            scenario_columns.append(f"{from_regime}->{to_regime}")

    rows = []
    for item in ASSET_PROXY_LIST:
        etf = item["ETF"]
        temp = merged_df[merged_df["ETF"] == etf]

        row = {
            "자산군": item["자산군"],
            "구분": item["구분"],
            "ETF": etf,
        }

        for scenario in scenario_columns:
            scenario_temp = temp[temp["scenario"] == scenario]
            if scenario_temp.empty:
                row[scenario] = 0.0
            else:
                row[scenario] = round(float(scenario_temp["forward_3m_return"].mean() * 100.0), 1)

        rows.append(row)

    return pd.DataFrame(rows), scenario_columns, current_regime


@lru_cache(maxsize=32)
def cached_macro_regime_payload(country, start_date, end_date):
    mapping = COUNTRY_SERIES_MAP[country]

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

    asset_price_df = load_asset_price_dataset(effective_start, effective_end)
    asset_monthly_df = build_asset_monthly_return_df(asset_price_df)

    asset_return_label, asset_return_df = build_asset_return_table_df(
        effective_start,
        effective_end,
        regime_df,
        asset_monthly_df=asset_monthly_df,
    )

    transition_table_df, transition_columns, highlighted_from_regime = build_transition_return_table_df(
        effective_start,
        effective_end,
        regime_df,
        current_card["current_regime"],
        asset_monthly_df=asset_monthly_df,
    )

    return {
        "chart_dataset": chart_dataset,
        "intervals": chart_intervals,
        "spells": spells,
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