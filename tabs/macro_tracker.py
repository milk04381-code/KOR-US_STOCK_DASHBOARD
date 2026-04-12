# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 13:27:30 2026

@author: 박승욱
"""

# tabs/macro_tracker.py
# 수정본
# 수정 목적:
# 1. 상단 2열 + 하단 1행 레이아웃 유지
# 2. 좌상단: DB 기반 전체 시계열 표
# 3. 우상단: 선택 지표 전체 시계열 차트
# 4. 표는 좌측 고정 열 + 내부 가로/세로 스크롤
# 5. 표 가시폭은 좌측 패널 안으로 제한하여 우측 차트 유지

from dash import dcc, html, Input, Output, ALL
import plotly.graph_objs as go

from services.macro_tracker_service import (
    get_macro_tracker_payload,
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


# -------------------------
# 스타일 상수
# -------------------------
STICKY_BORDER = "1px solid #333"
MONTH_COL_WIDTH = 92

STICKY_LEFT_1 = {
    "position": "sticky",
    "left": "0px",
    "zIndex": 5,
    "backgroundColor": "white",
}
STICKY_LEFT_2 = {
    "position": "sticky",
    "left": "56px",
    "zIndex": 5,
    "backgroundColor": "white",
}
STICKY_LEFT_3 = {
    "position": "sticky",
    "left": "216px",
    "zIndex": 5,
    "backgroundColor": "white",
}

STICKY_HEAD_1 = {
    "position": "sticky",
    "top": "0px",
    "zIndex": 7,
}
STICKY_HEAD_2 = {
    "position": "sticky",
    "top": "34px",
    "zIndex": 7,
}


# -------------------------
# 테이블 유틸
# -------------------------
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
            "border": STICKY_BORDER,
            "padding": "6px 8px",
            "verticalAlign": "middle",
            "width": "56px",
            "minWidth": "56px",
            **STICKY_LEFT_1,
        },
    )


def _td(
    text,
    row_span=1,
    col_span=1,
    align="center",
    bg=None,
    bold=False,
    width=None,
    min_width=None,
    sticky_style=None,
):
    style = {
        "border": STICKY_BORDER,
        "padding": "6px 8px",
        "textAlign": align,
        "verticalAlign": "middle",
        "whiteSpace": "nowrap",
        "fontSize": "13px",
        "backgroundColor": bg or "white",
    }

    if bold:
        style["fontWeight"] = "bold"

    if width is not None:
        style["width"] = width

    if min_width is not None:
        style["minWidth"] = min_width

    if sticky_style:
        style.update(sticky_style)

    return html.Td(text, rowSpan=row_span, colSpan=col_span, style=style)


def _th(
    text,
    row_span=1,
    col_span=1,
    align="center",
    bg="#f2f2f2",
    width=None,
    min_width=None,
    sticky_style=None,
):
    style = {
        "border": STICKY_BORDER,
        "padding": "6px 8px",
        "textAlign": align,
        "verticalAlign": "middle",
        "whiteSpace": "nowrap",
        "fontSize": "13px",
        "backgroundColor": bg,
        "fontWeight": "normal",
    }

    if width is not None:
        style["width"] = width

    if min_width is not None:
        style["minWidth"] = min_width

    if sticky_style:
        style.update(sticky_style)

    return html.Th(text, rowSpan=row_span, colSpan=col_span, style=style)


def _month_td(text):
    return _td(
        text,
        width=f"{MONTH_COL_WIDTH}px",
        min_width=f"{MONTH_COL_WIDTH}px",
    )


def _month_th(text, sticky_top=None):
    sticky = {}
    if sticky_top:
        sticky.update(sticky_top)

    return _th(
        text,
        width=f"{MONTH_COL_WIDTH}px",
        min_width=f"{MONTH_COL_WIDTH}px",
        sticky_style=sticky,
    )


def _build_indicator_row(item, months, selected_indicators):
    actual_map = item["actual"]

    return html.Tr(
        [
            _checkbox_cell(item["indicator"], selected_indicators),
            _td(
                item["indicator"],
                align="left",
                bold=True,
                width="160px",
                min_width="160px",
                sticky_style=STICKY_LEFT_2,
            ),
            _td(
                "실제",
                align="left",
                width="92px",
                min_width="92px",
                sticky_style=STICKY_LEFT_3,
            ),
            *[_month_td(actual_map.get(month, "")) for month in months],
            _td(item.get("change_display", ""), width="92px", min_width="92px"),
            _td(item.get("speed", ""), width="74px", min_width="74px"),
            _td(item.get("trend", ""), width="74px", min_width="74px"),
            _td(item.get("release_date", ""), width="100px", min_width="100px"),
            _td(item.get("asset_moves", {}).get("stock", ""), align="right", width="96px", min_width="96px"),
            _td(item.get("asset_moves", {}).get("bond", ""), align="right", width="96px", min_width="96px"),
            _td(item.get("asset_moves", {}).get("fx", ""), align="right", width="96px", min_width="96px"),
        ]
    )


def _build_table(payload, selected_indicators):
    months = payload["months"]
    indicators = payload["indicators"]
    policy_label = payload["policy_row"]["label"]
    policy_values = payload["policy_row"]["values"]

    if not indicators:
        return html.Div(
            "조건에 맞는 지표가 없습니다.",
            style={
                "padding": "20px",
                "fontSize": "14px",
                "color": "#666",
            },
        )

    body_rows = []
    for item in indicators:
        body_rows.append(_build_indicator_row(item, months, selected_indicators))

    min_table_width = (
        56 + 160 + 92 + (len(months) * MONTH_COL_WIDTH) + 92 + 74 + 74 + 100 + 96 + 96 + 96
    )

    return html.Table(
        [
            html.Thead(
                [
                    html.Tr(
                        [
                            _th(
                                "선택",
                                row_span=2,
                                width="56px",
                                min_width="56px",
                                sticky_style={**STICKY_LEFT_1, **STICKY_HEAD_1},
                            ),
                            _th(
                                "지표명",
                                row_span=2,
                                width="160px",
                                min_width="160px",
                                sticky_style={**STICKY_LEFT_2, **STICKY_HEAD_1},
                            ),
                            _th(
                                policy_label,
                                align="left",
                                width="92px",
                                min_width="92px",
                                sticky_style={**STICKY_LEFT_3, **STICKY_HEAD_1},
                            ),
                            *[
                                _month_th(policy_values.get(month, ""), sticky_top=STICKY_HEAD_1)
                                for month in months
                            ],
                            _th("전기 대비 변동", row_span=2, width="92px", min_width="92px", sticky_style=STICKY_HEAD_1),
                            _th("속도", row_span=2, width="74px", min_width="74px", sticky_style=STICKY_HEAD_1),
                            _th("추세", row_span=2, width="74px", min_width="74px", sticky_style=STICKY_HEAD_1),
                            _th("발표일", row_span=2, width="100px", min_width="100px", sticky_style=STICKY_HEAD_1),
                            _th("자산군별 일중 변동", col_span=3, sticky_style=STICKY_HEAD_1),
                        ]
                    ),
                    html.Tr(
                        [
                            _th(
                                "기준시기",
                                align="left",
                                width="92px",
                                min_width="92px",
                                sticky_style={**STICKY_LEFT_3, **STICKY_HEAD_2},
                            ),
                            *[
                                _month_th(month, sticky_top=STICKY_HEAD_2)
                                for month in months
                            ],
                            _th("주식 | S&P 500", width="96px", min_width="96px", sticky_style=STICKY_HEAD_2),
                            _th("채권 | 미국 2년물 국채 금리", width="96px", min_width="96px", sticky_style=STICKY_HEAD_2),
                            _th("외환 | 달러 인덱스", width="96px", min_width="96px", sticky_style=STICKY_HEAD_2),
                        ]
                    ),
                ]
            ),
            html.Tbody(body_rows),
        ],
        style={
            "width": "100%",
            "borderCollapse": "separate",
            "borderSpacing": "0",
            "minWidth": f"{min_table_width}px",
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


# -------------------------
# 레이아웃
# -------------------------
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
                                        "좌측 고정 열(선택/지표명/구분)을 제외한 전체 시계열 값은 표 내부에서 좌우 스크롤로 확인합니다.",
                                        style={
                                            "fontSize": "13px",
                                            "color": "#666",
                                            "marginBottom": "10px",
                                        },
                                    ),

                                    html.Div(
                                        id="macro-table-container",
                                        children=_build_table(initial_payload, selected_indicators=[]),
                                        style={
                                            "width": "100%",
                                            "maxWidth": "100%",
                                            "overflowX": "auto",
                                            "overflowY": "auto",
                                            "maxHeight": "720px",
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
                                        "왼쪽 표에서 선택한 지표를 전체 시계열 기준으로 시각화합니다.",
                                        style={
                                            "fontSize": "14px",
                                            "color": "#666",
                                            "marginBottom": "8px",
                                        },
                                    ),
                                    dcc.Graph(
                                        id="macro-main-chart",
                                        style={"height": "720px"},
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


# -------------------------
# 콜백
# -------------------------
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
            series = item.get("series", [])

            x_values = [row["date"] for row in series]
            y_values = [row["value"] for row in series]

            fig.add_trace(
                go.Scatter(
                    x=x_values,
                    y=y_values,
                    mode="lines+markers",
                    name=indicator,
                )
            )

        fig.update_layout(
            template="plotly_white",
            title="선택 지표 전체 시계열",
            margin={"l": 40, "r": 20, "t": 60, "b": 40},
            hovermode="x unified",
            xaxis_title="날짜",
            yaxis_title="값",
            legend={"orientation": "h", "y": 1.02, "x": 0},
        )

        return fig