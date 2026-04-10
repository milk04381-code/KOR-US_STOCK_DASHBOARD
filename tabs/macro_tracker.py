# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 13:27:30 2026

@author: 박승욱
"""
# tabs/macro_tracker.py
# 수정 목적:
# 1. 상단 2열 + 하단 1행 레이아웃으로 변경
# 2. 좌상단: DB 기반 요약 표
# 3. 우상단: 기존 dcc.Graph
# 4. 하단: Investing Economic Calendar iframe
# 5. 표에서 '예상', '이전' 행 제거 (실제값 1행만 표시)
# -*- coding: utf-8 -*-

from dash import dcc, html, Input, Output, ALL
import plotly.graph_objs as go

from services.macro_tracker_service import (
    get_macro_tracker_payload,
    parse_display_value,
)

COUNTRY_OPTIONS = [
    {"label": "미국", "value": "US"},
    {"label": "한국", "value": "KR"},
]

CATEGORY_OPTIONS = [
    {"label": "전체", "value": "ALL"},
    {"label": "고용", "value": "고용"},
    {"label": "소득과 지출", "value": "소득과 지출"},
    {"label": "산업", "value": "산업"},
    {"label": "ISM PMI", "value": "ISM PMI"},
    {"label": "물가", "value": "물가"},
    {"label": "주택", "value": "주택"},
]

# Investing support가 요청한 "full HTML embed code"를 그대로 유지
INVESTING_EMBED_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>Investing Economic Calendar</title>
    <style>
        body {
            margin: 0;
            padding: 0;
            background: white;
        }
        .wrap {
            width: 650px;
            margin: 0 auto;
            background: white;
        }
        .poweredBy {
            font-family: Arial, Helvetica, sans-serif;
        }
        .underline_link {
            font-size: 11px;
            color: #06529D;
            font-weight: bold;
        }
    </style>
</head>
<body>
    <div class="wrap">
        <iframe src="https://sslecal2.investing.com?columns=exc_flags,exc_currency,exc_importance,exc_actual,exc_forecast,exc_previous&features=datepicker,timezone,timeselector,filters&countries=11,5&calType=week&timeZone=8&lang=1"
                width="650"
                height="467"
                frameborder="0"
                allowtransparency="true"
                marginwidth="0"
                marginheight="0"></iframe>

        <div class="poweredBy" style="font-family: Arial, Helvetica, sans-serif;">
            <span style="font-size: 11px;color: #333333;text-decoration: none;">
                Real Time Economic Calendar provided by
                <a href="https://www.investing.com/" rel="nofollow" target="_blank"
                   style="font-size: 11px;color: #06529D; font-weight: bold;"
                   class="underline_link">Investing.com</a>.
            </span>
        </div>
    </div>
</body>
</html>
"""

def _checkbox_cell(indicator, selected_indicators):
    checked = [indicator] if indicator in selected_indicators else []
    return html.Td(
        dcc.Checklist(
            id={"type": "macro-checklist", "index": indicator},
            options=[{"label": "", "value": indicator}],
            value=checked,
            style={"display": "flex", "justifyContent": "center"},
            inputStyle={"marginRight": "0px"},
        ),
        style={
            "textAlign": "center",
            "border": "1px solid #333",
            "padding": "6px 8px",
            "verticalAlign": "middle",
            "width": "56px",
            "minWidth": "56px",
        },
    )

def _td(text, row_span=1, col_span=1, align="center", bg=None, bold=False):
    style = {
        "border": "1px solid #333",
        "padding": "6px 8px",
        "textAlign": align,
        "verticalAlign": "middle",
        "whiteSpace": "nowrap",
        "fontSize": "13px",
        "backgroundColor": bg or "white",
    }
    if bold:
        style["fontWeight"] = "bold"

    return html.Td(text, rowSpan=row_span, colSpan=col_span, style=style)

def _th(text, row_span=1, col_span=1, align="center", bg="#f2f2f2"):
    return html.Th(
        text,
        rowSpan=row_span,
        colSpan=col_span,
        style={
            "border": "1px solid #333",
            "padding": "6px 8px",
            "textAlign": align,
            "verticalAlign": "middle",
            "whiteSpace": "nowrap",
            "fontSize": "13px",
            "backgroundColor": bg,
            "fontWeight": "normal",
        },
    )

def _build_indicator_row(item, months, selected_indicators):
    actual_map = item["actual"]

    return html.Tr(
        [
            _checkbox_cell(item["indicator"], selected_indicators),
            _td(item["indicator"], align="left", bold=True),
            _td("실제", align="left"),
            *[_td(actual_map.get(month, "")) for month in months],
            _td(item["change_display"]),
            _td(item["speed"]),
            _td(item["trend"]),
            _td(item["release_date"]),
            _td(item["asset_moves"]["stock"], align="right"),
            _td(item["asset_moves"]["bond"], align="right"),
            _td(item["asset_moves"]["fx"], align="right"),
        ]
    )

def _build_table(payload, selected_indicators):
    months = payload["months"]
    indicators = payload["indicators"]
    policy_label = payload["policy_row"]["label"]
    policy_values = payload["policy_row"]["values"]

    body_rows = []
    for item in indicators:
        body_rows.append(_build_indicator_row(item, months, selected_indicators))

    return html.Table(
        [
            html.Thead(
                [
                    html.Tr(
                        [
                            _th("선택", row_span=2),
                            _th("지표명", row_span=2),
                            _th(policy_label, align="left"),
                            *[_th(policy_values.get(month, "")) for month in months],
                            _th("전기 대비 변동", row_span=2),
                            _th("속도", row_span=2),
                            _th("추세", row_span=2),
                            _th("발표일", row_span=2),
                            _th("자산군별 일중 변동", col_span=3),
                        ]
                    ),
                    html.Tr(
                        [
                            _th("기준시기", align="left"),
                            *[_th(month) for month in months],
                            _th("주식 | S&P 500"),
                            _th("채권 | 미국 2년물 국채 금리"),
                            _th("외환 | 달러 인덱스"),
                        ]
                    ),
                ]
            ),
            html.Tbody(body_rows),
        ],
        style={
            "width": "100%",
            "borderCollapse": "collapse",
            "minWidth": "1320px",
            "backgroundColor": "white",
        },
    )

def _build_investing_iframe_block():
    return html.Div(
        [
            html.H3("Economic Calendar (Investing)", style={"marginBottom": "8px"}),
            html.Div(
                "하단 위젯에서 actual / forecast / previous 값을 확인합니다.",
                style={
                    "fontSize": "14px",
                    "color": "#666",
                    "marginBottom": "12px",
                },
            ),
            html.Iframe(
                srcDoc=INVESTING_EMBED_HTML,
                style={
                    "width": "100%",
                    "height": "560px",
                    "border": "1px solid #ddd",
                    "backgroundColor": "white",
                },
            ),
        ],
        style={
            "marginTop": "20px",
            "paddingTop": "8px",
        },
    )

def get_layout():
    initial_payload = get_macro_tracker_payload(country="US", category="ALL")

    return dcc.Tab(
        label="경제지표 Tracker",
        children=[
            html.Div(
                [
                    dcc.Store(id="macro-selected-indicators", data=[]),

                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H3("경제지표 Tracker", style={"marginBottom": "16px"}),

                                    html.Div(
                                        [
                                            html.Div(
                                                [
                                                    html.Label("국가", style={"fontWeight": "bold", "marginBottom": "6px"}),
                                                    dcc.Dropdown(
                                                        id="macro-country-dropdown",
                                                        options=COUNTRY_OPTIONS,
                                                        value="US",
                                                        clearable=False,
                                                    ),
                                                ],
                                                style={"width": "48%"},
                                            ),
                                            html.Div(
                                                [
                                                    html.Label("카테고리", style={"fontWeight": "bold", "marginBottom": "6px"}),
                                                    dcc.Dropdown(
                                                        id="macro-category-dropdown",
                                                        options=CATEGORY_OPTIONS,
                                                        value="ALL",
                                                        clearable=False,
                                                    ),
                                                ],
                                                style={"width": "48%"},
                                            ),
                                        ],
                                        style={
                                            "display": "flex",
                                            "justifyContent": "space-between",
                                            "gap": "12px",
                                            "marginBottom": "16px",
                                        },
                                    ),

                                    html.Div(
                                        id="macro-table-container",
                                        children=_build_table(initial_payload, selected_indicators=[]),
                                        style={
                                            "overflowX": "auto",
                                            "border": "1px solid #ddd",
                                            "backgroundColor": "white",
                                        },
                                    ),
                                ],
                                style={
                                    "width": "58%",
                                    "display": "inline-block",
                                    "verticalAlign": "top",
                                    "paddingRight": "16px",
                                    "boxSizing": "border-box",
                                },
                            ),

                            html.Div(
                                [
                                    html.H3("Interactive Chart_Macro", style={"marginBottom": "16px"}),
                                    html.Div(
                                        "왼쪽 표에서 선택한 지표를 시각화합니다.",
                                        style={
                                            "fontSize": "14px",
                                            "color": "#666",
                                            "marginBottom": "8px",
                                        },
                                    ),
                                    dcc.Graph(
                                        id="macro-main-chart",
                                        style={"height": "700px"},
                                    ),
                                ],
                                style={
                                    "width": "42%",
                                    "display": "inline-block",
                                    "verticalAlign": "top",
                                    "boxSizing": "border-box",
                                },
                            ),
                        ]
                    ),

                    _build_investing_iframe_block(),
                ],
                style={
                    "width": "88%",
                    "margin": "24px auto",
                    "backgroundColor": "white",
                },
            )
        ],
    )

def register_callbacks(app):
    @app.callback(
        Output("macro-table-container", "children"),
        Input("macro-country-dropdown", "value"),
        Input("macro-category-dropdown", "value"),
        Input("macro-selected-indicators", "data"),
    )
    def render_macro_table(country, category, selected_indicators):
        payload = get_macro_tracker_payload(country=country, category=category)
        selected_indicators = selected_indicators or []

        valid_indicators = {item["indicator"] for item in payload["indicators"]}
        filtered_selected = [x for x in selected_indicators if x in valid_indicators]

        return _build_table(payload, selected_indicators=filtered_selected)

    @app.callback(
        Output("macro-selected-indicators", "data"),
        Input({"type": "macro-checklist", "index": ALL}, "value"),
        Input({"type": "macro-checklist", "index": ALL}, "id"),
    )
    def update_selected_indicators(check_values, check_ids):
        selected = []

        for values, comp_id in zip(check_values, check_ids):
            if values and comp_id["index"] in values:
                selected.append(comp_id["index"])

        return selected

    @app.callback(
        Output("macro-main-chart", "figure"),
        Input("macro-selected-indicators", "data"),
        Input("macro-country-dropdown", "value"),
        Input("macro-category-dropdown", "value"),
    )
    def update_macro_chart(selected_indicators, country, category):
        fig = go.Figure()
        payload = get_macro_tracker_payload(country=country, category=category)
        months = payload["months"]
        selected_indicators = selected_indicators or []

        indicator_map = {item["indicator"]: item for item in payload["indicators"]}
        selected_visible = [x for x in selected_indicators if x in indicator_map]

        if not selected_visible:
            fig.update_layout(
                template="plotly_white",
                title="선택된 지표가 없습니다.",
                margin={"l": 40, "r": 20, "t": 60, "b": 40},
            )
            return fig

        for indicator in selected_visible:
            item = indicator_map[indicator]
            y_values = [
                parse_display_value(item["actual"].get(month, ""))
                for month in months
            ]

            fig.add_trace(
                go.Scatter(
                    x=months,
                    y=y_values,
                    mode="lines+markers",
                    name=indicator,
                )
            )

        fig.update_layout(
            template="plotly_white",
            title="선택 지표 추이",
            margin={"l": 40, "r": 20, "t": 60, "b": 40},
            hovermode="x unified",
            xaxis_title="기준월",
            yaxis_title="값",
            legend={"orientation": "h", "y": 1.02, "x": 0},
        )

        return fig