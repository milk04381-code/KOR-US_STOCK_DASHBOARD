# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 18:04:11 2026

@author: 박승욱
"""

# tabs/domestic_monitor.py

from datetime import date

from dash import dcc, html, Input, Output, ALL

from services.data_service import (
    normalize_selected_codes,
    load_chart_dataset,
)
from services.chart_service import build_main_figure


def get_layout(series_options, default_value):
    return dcc.Tab(
        label="국내 주식 Monitor",
        children=[
            html.Div(
                [
                    html.Div(
                        [
                            html.Label("지표 선택"),
                            dcc.Dropdown(
                                id="series-dropdown",
                                options=series_options,
                                value=default_value,
                                multi=True,
                                placeholder="지표를 선택하세요",
                                closeOnSelect=False,
                            ),
                        ]
                    ),

                    html.Br(),

                    html.Div(
                        [
                            html.Label("축/선행·후행/역축 설정"),
                            html.Div(
                                id="axis-selector-container",
                                style={
                                    "marginTop": "8px",
                                    "marginBottom": "8px",
                                },
                            ),
                        ]
                    ),

                    html.Br(),

                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("시작일"),
                                    dcc.DatePickerSingle(
                                        id="start-date",
                                        date="1970-01-01",
                                        display_format="YYYY-MM-DD",
                                    ),
                                ],
                                style={"width": "48%", "display": "inline-block"},
                            ),
                            html.Div(
                                [
                                    html.Label("종료일"),
                                    dcc.DatePickerSingle(
                                        id="end-date",
                                        date=None,
                                        display_format="YYYY-MM-DD",
                                        placeholder="종료일 비우면 오늘 날짜 자동",
                                    ),
                                ],
                                style={
                                    "width": "48%",
                                    "display": "inline-block",
                                    "float": "right",
                                },
                            ),
                        ]
                    ),

                    html.Br(),
                    html.Br(),

                    dcc.Checklist(
                        id="recession-check",
                        options=[{"label": "Recession 음영 표시", "value": "show"}],
                        value=["show"],
                    ),

                    html.Div(
                        id="selection-summary",
                        style={
                            "marginTop": "8px",
                            "marginBottom": "8px",
                            "fontSize": "14px",
                            "color": "#444",
                        },
                    ),

                    dcc.Graph(id="main-chart", style={"height": "760px"}),
                ],
                style={"width": "88%", "margin": "auto"},
            )
        ],
    )


def register_callbacks(app):
    @app.callback(
        Output("axis-selector-container", "children"),
        Input("series-dropdown", "value"),
    )
    def update_axis_selector(selected_codes):
        selected_codes = normalize_selected_codes(selected_codes)

        if not selected_codes:
            return html.Div("선택된 지표가 없습니다.", style={"color": "#666"})

        rows = []

        for code in selected_codes:
            rows.append(
                html.Div(
                    [
                        html.Span(
                            code,
                            style={
                                "display": "inline-block",
                                "width": "120px",
                                "fontWeight": "bold",
                            },
                        ),
                        dcc.Dropdown(
                            id={"type": "axis-dropdown", "index": code},
                            options=[
                                {"label": "자동", "value": ""},
                                {"label": "왼쪽 축", "value": "left"},
                                {"label": "오른쪽 축", "value": "right"},
                            ],
                            value="",
                            clearable=False,
                            style={
                                "width": "140px",
                                "display": "inline-block",
                                "verticalAlign": "middle",
                                "marginRight": "12px",
                            },
                        ),
                        html.Span(
                            "시차(개월)",
                            style={
                                "display": "inline-block",
                                "marginRight": "6px",
                                "fontSize": "13px",
                            },
                        ),
                        dcc.Input(
                            id={"type": "shift-input", "index": code},
                            type="number",
                            value=0,
                            step=1,
                            style={
                                "width": "80px",
                                "display": "inline-block",
                                "verticalAlign": "middle",
                                "marginRight": "12px",
                            },
                        ),
                        dcc.Checklist(
                            id={"type": "reverse-check", "index": code},
                            options=[{"label": "역축", "value": "reverse"}],
                            value=[],
                            style={
                                "display": "inline-block",
                                "verticalAlign": "middle",
                            },
                            inline=True,
                        ),
                    ],
                    style={"marginBottom": "8px"},
                )
            )

        return rows

    @app.callback(
        Output("main-chart", "figure"),
        Output("selection-summary", "children"),
        Input("series-dropdown", "value"),
        Input("start-date", "date"),
        Input("end-date", "date"),
        Input("recession-check", "value"),
        Input({"type": "axis-dropdown", "index": ALL}, "value"),
        Input({"type": "axis-dropdown", "index": ALL}, "id"),
        Input({"type": "shift-input", "index": ALL}, "value"),
        Input({"type": "shift-input", "index": ALL}, "id"),
        Input({"type": "reverse-check", "index": ALL}, "value"),
        Input({"type": "reverse-check", "index": ALL}, "id"),
    )
    def update_chart(
        selected_codes,
        start_date,
        end_date,
        recession_value,
        axis_values,
        axis_ids,
        shift_values,
        shift_ids,
        reverse_values,
        reverse_ids,
    ):
        selected_codes = normalize_selected_codes(selected_codes)

        if end_date is None:
            end_date = date.today().isoformat()

        axis_override_map = {}
        for axis_value, axis_id in zip(axis_values, axis_ids):
            if axis_value in ("left", "right"):
                axis_override_map[axis_id["index"]] = axis_value

        shift_month_map = {}
        for shift_value, shift_id in zip(shift_values, shift_ids):
            code = shift_id["index"]
            try:
                shift_month_map[code] = int(shift_value or 0)
            except Exception:
                shift_month_map[code] = 0

        reverse_axis_map = {}
        for reverse_value, reverse_id in zip(reverse_values, reverse_ids):
            reverse_axis_map[reverse_id["index"]] = "reverse" in (reverse_value or [])

        dataset = load_chart_dataset(
            selected_codes=selected_codes,
            start_date=start_date,
            end_date=end_date,
            axis_override_map=axis_override_map,
        )

        fig = build_main_figure(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            show_recession=("show" in (recession_value or [])),
            shift_month_map=shift_month_map,
            reverse_axis_map=reverse_axis_map,
            show_latest_value_labels=True,
        )

        selected_count = len(selected_codes)

        if selected_count > 0:
            settings_parts = []

            if axis_override_map:
                override_text = ", ".join(
                    [f"{code}:{axis}" for code, axis in axis_override_map.items()]
                )
                settings_parts.append(f"축 지정: {override_text}")

            nonzero_shift_items = [
                f"{code}:{months:+d}M"
                for code, months in shift_month_map.items()
                if months != 0
            ]
            if nonzero_shift_items:
                settings_parts.append(f"시차: {', '.join(nonzero_shift_items)}")

            reversed_items = [
                code for code, is_reversed in reverse_axis_map.items()
                if is_reversed
            ]
            if reversed_items:
                settings_parts.append(f"역축: {', '.join(reversed_items)}")

            if settings_parts:
                summary_text = (
                    f"선택 지표: {selected_count}개 | 기간: {start_date} ~ {end_date} | "
                    + " | ".join(settings_parts)
                )
            else:
                summary_text = f"선택 지표: {selected_count}개 | 기간: {start_date} ~ {end_date}"
        else:
            summary_text = "선택된 지표가 없습니다."

        return fig, summary_text