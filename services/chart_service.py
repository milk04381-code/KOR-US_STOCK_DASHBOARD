# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 19:52:04 2026

@author: 박승욱
"""

# services/chart_service.py
# Day9 수정
# 역할:
# - data_service.py가 만든 dataset을 Plotly figure로 변환
# - recession shading을 yref='paper' 방식으로 안정화
#
# 핵심:
# - 좌측 축이 비어 있어도 recession shading이 사라지지 않음
# - effective_axis 값만 보고 좌/우축 결정
# - 축 제목은 실제 effective_axis 기준으로 생성

import os

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from services.data_service import load_recession_data


DEBUG_CHART_RULE = os.getenv("CHART_RULE_DEBUG", "0") == "1"


def debug_print(*args):
    if DEBUG_CHART_RULE:
        print("[chart_service_debug]", *args)


# ---------------------------
# 색상 팔레트 (순서 기반)
# ---------------------------
COLOR_PALETTE = [
    "#d62728",  # red
    "#1f77b4",  # blue
    "#2ca02c",  # green
    "#ff7f0e",  # orange
    "#9467bd",  # purple
    "#8c564b",  # brown
    "#e377c2",  # pink
    "#7f7f7f",  # gray
    "#bcbd22",  # olive
    "#17becf",  # cyan
]


def get_color_by_order(index, series_code):
    if series_code == "USREC":
        return "#7f7f7f"
    return COLOR_PALETTE[index % len(COLOR_PALETTE)]


def infer_next_period(last_date, date_series):
    dates = pd.to_datetime(date_series).sort_values()

    if len(dates) >= 2:
        delta = dates.iloc[-1] - dates.iloc[-2]

        if 27 <= delta.days <= 31:
            return last_date + pd.offsets.MonthBegin(1)

        if 80 <= delta.days <= 100:
            return last_date + pd.offsets.QuarterBegin(startingMonth=1)

        return last_date + delta

    return last_date + pd.offsets.MonthBegin(1)


def build_recession_intervals(rec_df):
    if rec_df.empty:
        return []

    df = rec_df.copy().sort_values("date_value").reset_index(drop=True)

    intervals = []
    in_recession = False
    rec_start = None

    for _, row in df.iterrows():
        current_date = row["date_value"]
        current_value = int(row["value_num"])

        if current_value == 1 and not in_recession:
            in_recession = True
            rec_start = current_date

        elif current_value == 0 and in_recession:
            intervals.append((rec_start, current_date))
            in_recession = False
            rec_start = None

    if in_recession and rec_start is not None:
        last_date = df["date_value"].iloc[-1]
        next_date = infer_next_period(last_date, df["date_value"])
        intervals.append((rec_start, next_date))

    return intervals


def add_recession_shading(fig, start_date, end_date):
    rec_df = load_recession_data(start_date, end_date)

    if rec_df.empty:
        return fig

    intervals = build_recession_intervals(rec_df)

    for x0, x1 in intervals:
        fig.add_shape(
            type="rect",
            xref="x",
            yref="paper",
            x0=x0,
            x1=x1,
            y0=0,
            y1=1,
            fillcolor="gray",
            opacity=0.20,
            layer="below",
            line_width=0,
        )

    return fig


def is_percent_unit(unit):
    if not unit:
        return False

    unit_lower = str(unit).strip().lower()

    return (
        "%" in unit_lower
        or "percent" in unit_lower
        or "percentage" in unit_lower
    )


def format_value_by_unit(unit):
    if not unit:
        return "%{y:,.2f}"

    if is_percent_unit(unit):
        return "%{y:,.2f}"

    if str(unit).strip().lower() == "indicator":
        return "%{y:,.0f}"

    if str(unit).strip().lower() == "index":
        return "%{y:,.2f}"

    return "%{y:,.2f}"


def make_hovertemplate(unit):
    unit_text = unit if unit else "-"
    value_format = format_value_by_unit(unit)

    return (
        "<b>%{fullData.name}</b><br>"
        "Date: %{x|%Y-%m-%d}<br>"
        f"Value: {value_format} {unit_text}"
        "<extra></extra>"
    )


def add_one_series_trace(fig, temp, index):
    series_code = temp["series_code"].iloc[0]
    series_name = temp["series_name"].iloc[0]
    effective_axis = str(temp["effective_axis"].iloc[0]).strip().lower()
    chart_type = str(temp["effective_chart_type"].iloc[0]).strip().lower()
    unit = temp["unit"].iloc[0]
    
    axis_label = "R" if effective_axis == "right" else "L"
    legend_name = f"{series_name} ({axis_label})"

    color = get_color_by_order(index, series_code)
    use_secondary = (effective_axis == "right")
    hovertemplate = make_hovertemplate(unit)

    debug_print(
        f"add_trace | series_code={series_code} | "
        f"series_name={series_name} | "
        f"effective_axis={effective_axis} | "
        f"secondary_y={use_secondary} | "
        f"chart_type={chart_type} | unit={unit}"
    )

    if chart_type == "bar":
        trace = go.Bar(
            x=temp["date_value"],
            y=temp["value_num"],
            name=legend_name,
            marker={"color": color},
            opacity=0.85,
            hovertemplate=hovertemplate,
        )

    elif chart_type == "area":
        trace = go.Scatter(
            x=temp["date_value"],
            y=temp["value_num"],
            mode="lines",
            name=legend_name,
            line={"width": 2, "color": color},
            fill="tozeroy",
            hovertemplate=hovertemplate,
        )

    else:
        trace = go.Scatter(
            x=temp["date_value"],
            y=temp["value_num"],
            mode="lines",
            name=legend_name,
            line={"width": 2, "color": color},
            hovertemplate=hovertemplate,
        )

    fig.add_trace(trace, secondary_y=use_secondary)
    return fig


def build_axis_title(dataset, axis_name):
    units = (
        dataset.loc[dataset["effective_axis"] == axis_name, "unit"]
        .dropna()
        .astype(str)
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )

    if not units:
        return "값"

    return ", ".join(units)


def build_empty_figure(message):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_layout(
        title=message,
        template="plotly_white",
        margin=dict(l=60, r=60, t=70, b=50),
    )
    return fig


def build_main_figure(dataset, start_date, end_date, show_recession=True):
    if dataset is None or dataset.empty:
        return build_empty_figure("데이터가 없습니다.")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    unique_codes = dataset["series_code"].dropna().unique().tolist()

    debug_print("build_main_figure | unique_codes =", unique_codes)

    for idx, series_code in enumerate(unique_codes):
        temp = dataset[dataset["series_code"] == series_code].copy().sort_values("date_value")
        fig = add_one_series_trace(fig, temp, index=idx)

    if show_recession:
        fig = add_recession_shading(fig, start_date, end_date)

    left_title = build_axis_title(dataset, "left")
    right_title = build_axis_title(dataset, "right")

    debug_print(
        f"axis_titles | left={left_title} | right={right_title}"
    )

    fig.update_yaxes(title_text=left_title, secondary_y=False, showgrid=True)
    fig.update_yaxes(title_text=right_title, secondary_y=True, showgrid=False)

    fig.update_xaxes(
        title_text="Date",
        showgrid=True,
        rangeslider_visible=False,
    )

    fig.update_layout(
        title="국내 주식 Monitor",
        template="plotly_white",
        hovermode="x unified",
        barmode="group",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            title=None,
        ),
        margin=dict(l=60, r=60, t=70, b=50),
    )

    return fig