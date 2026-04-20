# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 19:52:04 2026

@author: 박승욱
"""

# services/chart_service.py

import os

import pandas as pd
import plotly.graph_objects as go

from services.data_service import load_recession_data


DEBUG_CHART_RULE = os.getenv("CHART_RULE_DEBUG", "0") == "1"


def debug_print(*args):
    if DEBUG_CHART_RULE:
        print("[chart_service_debug]", *args)


COLOR_PALETTE = [
    "#d62728",
    "#1f77b4",
    "#2ca02c",
    "#ff7f0e",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
]

REGIME_COLORS = {
    "경기 과열": "#c58a2b",
    "골디락스": "#e9d8a6",
    "경기 둔화": "#a9a9a9",
    "스태그플레이션": "#b7c9df",
}


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


def add_recession_outline_boxes(fig, start_date, end_date):
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
            fillcolor="rgba(0,0,0,0)",
            line=dict(
                color="red",
                width=1.5,
                dash="dot",
            ),
            layer="above",
        )

    return fig


def add_regime_shading(fig, intervals):
    if not intervals:
        return fig

    for x0, x1, regime_name in intervals:
        fig.add_shape(
            type="rect",
            xref="x",
            yref="paper",
            x0=x0,
            x1=x1,
            y0=0,
            y1=1,
            fillcolor=REGIME_COLORS.get(regime_name, "#cccccc"),
            opacity=0.40,
            layer="below",
            line_width=0,
        )

    return fig


def add_regime_legend_traces(fig):
    for regime_name, color in REGIME_COLORS.items():
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="lines",
                line=dict(color=color, width=10),
                name=regime_name,
                showlegend=True,
                hoverinfo="skip",
                yaxis="y",
            )
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


def format_value_text(value, unit):
    if value is None or pd.isna(value):
        return "-"

    try:
        numeric_value = float(value)
    except Exception:
        return str(value)

    unit_text = str(unit).strip() if unit else ""

    if is_percent_unit(unit_text):
        value_text = f"{numeric_value:,.2f}"
    elif unit_text.lower() == "indicator":
        value_text = f"{numeric_value:,.0f}"
    elif unit_text.lower() == "index":
        value_text = f"{numeric_value:,.2f}"
    else:
        value_text = f"{numeric_value:,.2f}"

    if unit_text:
        return f"{value_text} {unit_text}"
    return value_text


def make_hovertemplate(unit):
    unit_text = unit if unit else "-"
    value_format = format_value_by_unit(unit)

    return (
        "<b>%{fullData.name}</b><br>"
        "Date: %{x|%Y-%m-%d}<br>"
        f"Value: {value_format} {unit_text}"
        "<extra></extra>"
    )


def downsample_series(df, max_points=1500):
    if df is None or df.empty:
        return df

    if len(df) <= max_points:
        return df

    step = max(1, len(df) // max_points)
    sampled = df.iloc[::step].copy()

    last_idx = df.index[-1]
    if sampled.index[-1] != last_idx:
        sampled = pd.concat([sampled, df.iloc[[-1]]], axis=0)

    sampled = sampled[~sampled.index.duplicated(keep="last")]
    return sampled


def apply_shift_months(temp, shift_months):
    if not shift_months:
        return temp

    shifted = temp.copy()
    shifted["date_value"] = pd.to_datetime(shifted["date_value"]) + pd.DateOffset(months=int(shift_months))
    return shifted


def get_axis_usage_flags(dataset, reverse_axis_map=None):
    reverse_axis_map = reverse_axis_map or {}

    left_normal = False
    left_reverse = False
    right_normal = False
    right_reverse = False

    if dataset is None or dataset.empty:
        return {
            "left_normal": False,
            "left_reverse": False,
            "right_normal": False,
            "right_reverse": False,
            "use_left_split_axis": False,
            "use_right_split_axis": False,
        }

    temp_df = dataset[["series_code", "effective_axis"]].drop_duplicates().copy()

    for _, row in temp_df.iterrows():
        code = str(row["series_code"]).strip()
        axis_side = str(row["effective_axis"]).strip().lower()
        is_reversed = bool(reverse_axis_map.get(code, False))

        if axis_side == "right":
            if is_reversed:
                right_reverse = True
            else:
                right_normal = True
        else:
            if is_reversed:
                left_reverse = True
            else:
                left_normal = True

    return {
        "left_normal": left_normal,
        "left_reverse": left_reverse,
        "right_normal": right_normal,
        "right_reverse": right_reverse,
        "use_left_split_axis": left_normal and left_reverse,
        "use_right_split_axis": right_normal and right_reverse,
    }


def get_axis_key(effective_axis, is_reversed, axis_usage_flags):
    axis_side = str(effective_axis).strip().lower()
    if axis_side not in ("left", "right"):
        axis_side = "left"

    if axis_side == "left":
        if axis_usage_flags.get("use_left_split_axis", False):
            return "y3" if is_reversed else "y"
        return "y"

    if axis_usage_flags.get("use_right_split_axis", False):
        return "y4" if is_reversed else "y2"
    return "y2"


def build_axis_title(dataset, axis_name, reverse_axis_map=None, axis_usage_flags=None):
    reverse_axis_map = reverse_axis_map or {}
    axis_usage_flags = axis_usage_flags or {}

    if axis_name == "left":
        target_codes = dataset.loc[dataset["effective_axis"] == "left", "series_code"].dropna().astype(str).tolist()
        if axis_usage_flags.get("use_left_split_axis", False):
            code_set = {code for code in target_codes if not reverse_axis_map.get(code, False)}
        else:
            code_set = set(target_codes)
    elif axis_name == "right":
        target_codes = dataset.loc[dataset["effective_axis"] == "right", "series_code"].dropna().astype(str).tolist()
        if axis_usage_flags.get("use_right_split_axis", False):
            code_set = {code for code in target_codes if not reverse_axis_map.get(code, False)}
        else:
            code_set = set(target_codes)
    elif axis_name == "left_reverse":
        if not axis_usage_flags.get("use_left_split_axis", False):
            return ""
        target_codes = dataset.loc[dataset["effective_axis"] == "left", "series_code"].dropna().astype(str).tolist()
        code_set = {code for code in target_codes if reverse_axis_map.get(code, False)}
    elif axis_name == "right_reverse":
        if not axis_usage_flags.get("use_right_split_axis", False):
            return ""
        target_codes = dataset.loc[dataset["effective_axis"] == "right", "series_code"].dropna().astype(str).tolist()
        code_set = {code for code in target_codes if reverse_axis_map.get(code, False)}
    else:
        code_set = set()

    if not code_set:
        return ""

    units = (
        dataset.loc[dataset["series_code"].isin(list(code_set)), "unit"]
        .dropna()
        .astype(str)
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )

    if not units:
        title = "값"
    else:
        title = ", ".join(units)

    if axis_name in ("left_reverse", "right_reverse"):
        title += " (역축)"
    elif axis_name == "left" and axis_usage_flags.get("left_reverse", False) and not axis_usage_flags.get("use_left_split_axis", False):
        title += " (역축)"
    elif axis_name == "right" and axis_usage_flags.get("right_reverse", False) and not axis_usage_flags.get("use_right_split_axis", False):
        title += " (역축)"

    return title


def build_empty_figure(message):
    fig = go.Figure()
    fig.update_layout(
        title=message,
        template="plotly_white",
        margin=dict(l=60, r=140, t=70, b=50),
    )
    return fig


def add_latest_value_annotation(fig, temp, unit, color, axis_key):
    if temp is None or temp.empty:
        return fig

    last_row = temp.sort_values("date_value").iloc[-1]
    latest_y = last_row["value_num"]

    annotation_text = format_value_text(latest_y, unit)

    fig.add_annotation(
        x=1.01,
        y=latest_y,
        xref="paper",
        yref=axis_key,
        text=annotation_text,
        showarrow=False,
        xanchor="left",
        yanchor="middle",
        font=dict(size=11, color=color),
        bgcolor="rgba(255,255,255,0.85)",
        bordercolor=color,
        borderwidth=1,
    )

    return fig


def add_one_series_trace(
    fig,
    temp,
    index,
    axis_usage_flags,
    shift_month_map=None,
    reverse_axis_map=None,
    show_latest_value_labels=False,
):
    shift_month_map = shift_month_map or {}
    reverse_axis_map = reverse_axis_map or {}

    series_code = temp["series_code"].iloc[0]
    series_name = temp["series_name"].iloc[0]
    effective_axis = str(temp["effective_axis"].iloc[0]).strip().lower()
    chart_type = str(temp["effective_chart_type"].iloc[0]).strip().lower()
    unit = temp["unit"].iloc[0]

    shift_months = int(shift_month_map.get(series_code, 0) or 0)
    is_reversed = bool(reverse_axis_map.get(series_code, False))

    axis_label = "R" if effective_axis == "right" else "L"
    reverse_label = ", 역축" if is_reversed else ""
    shift_label = f", {shift_months:+d}M" if shift_months != 0 else ""
    legend_name = f"{series_name} ({axis_label}{reverse_label}{shift_label})"

    color = get_color_by_order(index, series_code)
    hovertemplate = make_hovertemplate(unit)
    yaxis_key = get_axis_key(effective_axis, is_reversed, axis_usage_flags)

    temp = apply_shift_months(temp, shift_months)

    debug_print(
        f"add_trace | series_code={series_code} | "
        f"series_name={series_name} | "
        f"effective_axis={effective_axis} | "
        f"yaxis_key={yaxis_key} | "
        f"chart_type={chart_type} | unit={unit} | "
        f"shift_months={shift_months} | reversed={is_reversed}"
    )

    common_kwargs = {
        "x": temp["date_value"],
        "y": temp["value_num"],
        "name": legend_name,
        "hovertemplate": hovertemplate,
        "showlegend": True,
        "yaxis": yaxis_key,
    }

    if chart_type == "bar":
        trace = go.Bar(
            **common_kwargs,
            marker={"color": color},
            opacity=0.85,
        )
    elif chart_type == "area":
        trace = go.Scatter(
            **common_kwargs,
            mode="lines",
            line={"width": 2, "color": color},
            fill="tozeroy",
        )
    else:
        trace = go.Scatter(
            **common_kwargs,
            mode="lines",
            line={"width": 2, "color": color},
        )

    fig.add_trace(trace)

    if show_latest_value_labels:
        fig = add_latest_value_annotation(
            fig=fig,
            temp=temp,
            unit=unit,
            color=color,
            axis_key=yaxis_key,
        )

    return fig


def build_main_figure(
    dataset,
    start_date,
    end_date,
    show_recession=True,
    recession_style="shading",
    background_intervals=None,
    chart_title=None,
    shift_month_map=None,
    reverse_axis_map=None,
    show_latest_value_labels=False,
):
    shift_month_map = shift_month_map or {}
    reverse_axis_map = reverse_axis_map or {}

    if dataset is None or dataset.empty:
        return build_empty_figure("데이터가 없습니다.")

    fig = go.Figure()
    unique_codes = dataset["series_code"].dropna().unique().tolist()

    debug_print("build_main_figure | unique_codes =", unique_codes)

    axis_usage_flags = get_axis_usage_flags(dataset, reverse_axis_map=reverse_axis_map)

    use_left_split_axis = axis_usage_flags["use_left_split_axis"]
    use_right_split_axis = axis_usage_flags["use_right_split_axis"]

    # split axis가 있으면 좌우에 축 공간 확보
    x_domain_start = 0.08 if use_left_split_axis else 0.00
    x_domain_end = 0.92 if use_right_split_axis else 1.00

    left_outer_pos = 0.00
    right_outer_pos = 1.00

    if background_intervals:
        fig = add_regime_shading(fig, background_intervals)

    for idx, series_code in enumerate(unique_codes):
        temp = dataset[dataset["series_code"] == series_code].copy().sort_values("date_value")
        temp = downsample_series(temp, max_points=1500)
        fig = add_one_series_trace(
            fig=fig,
            temp=temp,
            index=idx,
            axis_usage_flags=axis_usage_flags,
            shift_month_map=shift_month_map,
            reverse_axis_map=reverse_axis_map,
            show_latest_value_labels=show_latest_value_labels,
        )

    if background_intervals:
        fig = add_regime_legend_traces(fig)

    if show_recession:
        if recession_style == "shading":
            fig = add_recession_shading(fig, start_date, end_date)
        elif recession_style == "outline":
            fig = add_recession_outline_boxes(fig, start_date, end_date)

    left_title = build_axis_title(
        dataset,
        "left",
        reverse_axis_map=reverse_axis_map,
        axis_usage_flags=axis_usage_flags,
    )
    right_title = build_axis_title(
        dataset,
        "right",
        reverse_axis_map=reverse_axis_map,
        axis_usage_flags=axis_usage_flags,
    )
    left_reverse_title = build_axis_title(
        dataset,
        "left_reverse",
        reverse_axis_map=reverse_axis_map,
        axis_usage_flags=axis_usage_flags,
    )
    right_reverse_title = build_axis_title(
        dataset,
        "right_reverse",
        reverse_axis_map=reverse_axis_map,
        axis_usage_flags=axis_usage_flags,
    )

    # split이 없을 때만 기본축 자체 반전
    yaxis_autorange = "reversed" if (axis_usage_flags["left_reverse"] and not use_left_split_axis) else True
    yaxis2_autorange = "reversed" if (axis_usage_flags["right_reverse"] and not use_right_split_axis) else True

    layout_kwargs = dict(
        title=chart_title or "국내 주식 Monitor",
        template="plotly_white",
        hovermode="x unified",
        barmode="group",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            title=None,
        ),
        margin=dict(
            l=120 if use_left_split_axis else 70,
            r=180 if use_right_split_axis else 130,
            t=70,
            b=50,
        ),
        xaxis=dict(
            title="Date",
            showgrid=True,
            rangeslider={"visible": False},
            domain=[x_domain_start, x_domain_end],
        ),
        # 기본 왼쪽 축
        yaxis=dict(
            title=left_title or "값",
            side="left",
            showgrid=True,
            zeroline=False,
            autorange=yaxis_autorange,
            anchor="x",
        ),
        # 기본 오른쪽 축
        # split 여부와 상관없이 기본 secondary axis 역할 유지
        yaxis2=dict(
            title=right_title or "",
            side="right",
            overlaying="y",
            showgrid=False,
            zeroline=False,
            autorange=yaxis2_autorange if not use_right_split_axis else True,
            anchor="x",
        ),
    )

    # 왼쪽 일반 + 왼쪽 역축
    # 기본 yaxis는 그대로 두고, 역축만 바깥쪽 보조축 추가
    if use_left_split_axis:
        layout_kwargs["yaxis3"] = dict(
            title=left_reverse_title or "",
            side="left",
            overlaying="y",
            showgrid=False,
            zeroline=False,
            autorange="reversed",
            anchor="free",
            position=left_outer_pos,
        )

    # 오른쪽 일반 + 오른쪽 역축
    # 기본 yaxis2는 유지, 역축만 가장 바깥쪽에 추가
    if use_right_split_axis:
        layout_kwargs["yaxis4"] = dict(
            title=right_reverse_title or "",
            side="right",
            overlaying="y",
            showgrid=False,
            zeroline=False,
            autorange="reversed",
            anchor="free",
            position=right_outer_pos,
        )

    fig.update_layout(**layout_kwargs)

    return fig