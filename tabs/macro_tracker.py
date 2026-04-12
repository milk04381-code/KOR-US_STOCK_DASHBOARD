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
# 4. 표는 좌우측 고정 열 + 내부 가로/세로 스크롤
# 5. 표 가시폭은 좌측 패널 안으로 제한하여 우측 차트 유지


from dash import dcc, html, Input, Output, ALL
import plotly.graph_objs as go

from services.macro_tracker_service import get_macro_tracker_payload

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
# 폭 / sticky 상수
# -------------------------
W_SELECT = 56
W_NAME = 190
W_KIND = 90
W_TIME = 92
W_CHANGE = 96
W_SPEED = 72
W_TREND = 72
W_RELEASE = 100
W_ASSET = 96

BORDER = "1px solid #333"


def px(value):
    return f"{value}px"


RIGHT_FX = 0
RIGHT_BOND = W_ASSET
RIGHT_STOCK = W_ASSET * 2
RIGHT_RELEASE = W_ASSET * 3
RIGHT_TREND = W_ASSET * 3 + W_RELEASE
RIGHT_SPEED = W_ASSET * 3 + W_RELEASE + W_TREND
RIGHT_CHANGE = W_ASSET * 3 + W_RELEASE + W_TREND + W_SPEED


# -------------------------
# 공통 셀 스타일
# -------------------------
def _base_cell_style(bg="white", align="center"):
    return {
        "border": BORDER,
        "padding": "6px 8px",
        "textAlign": align,
        "verticalAlign": "middle",
        "whiteSpace": "nowrap",
        "fontSize": "13px",
        "backgroundColor": bg,
        "boxSizing": "border-box",
    }


def _sticky_left(style, left):
    result = dict(style)
    result.update({
        "position": "sticky",
        "left": px(left),
        "zIndex": 6,
        "backgroundColor": style.get("backgroundColor", "white"),
    })
    return result


def _sticky_right(style, right):
    result = dict(style)
    result.update({
        "position": "sticky",
        "right": px(right),
        "zIndex": 6,
        "backgroundColor": style.get("backgroundColor", "white"),
    })
    return result


def _sticky_top(style, top=0, z=7):
    result = dict(style)
    result.update({
        "position": "sticky",
        "top": px(top),
        "zIndex": z,
        "backgroundColor": style.get("backgroundColor", "#f2f2f2"),
    })
    return result


def _th(text, width, align="center", sticky_left=None, sticky_right=None):
    style = _base_cell_style(bg="#f2f2f2", align=align)
    style["width"] = px(width)
    style["minWidth"] = px(width)
    style = _sticky_top(style, top=0, z=8)

    if sticky_left is not None:
        style = _sticky_left(style, sticky_left)
        style["zIndex"] = 9

    if sticky_right is not None:
        style = _sticky_right(style, sticky_right)
        style["zIndex"] = 9

    return html.Th(text, style=style)


def _td(text, width, align="center", bold=False, sticky_left=None, sticky_right=None):
    style = _base_cell_style(bg="white", align=align)
    style["width"] = px(width)
    style["minWidth"] = px(width)

    if bold:
        style["fontWeight"] = "bold"

    if sticky_left is not None:
        style = _sticky_left(style, sticky_left)

    if sticky_right is not None:
        style = _sticky_right(style, sticky_right)

    return html.Td(text, style=style)


def _checkbox_cell(series_code, selected_series_codes):
    checked = [series_code] if series_code in selected_series_codes else []

    return html.Td(
        dcc.Checklist(
            id={"type": "macro-checklist", "index": series_code},
            options=[{"label": "", "value": series_code}],
            value=checked,
            style={"display": "flex", "justifyContent": "center"},
            inputStyle={"marginRight": "0px"},
        ),
        style=_sticky_left(
            {
                **_base_cell_style(bg="white", align="center"),
                "width": px(W_SELECT),
                "minWidth": px(W_SELECT),
            },
            0,
        ),
    )


# -------------------------
# 테이블 구성
# -------------------------
def _build_indicator_row(item, period_keys, selected_series_codes):
    actual_map = item["actual"]

    cells = [
        _checkbox_cell(item["series_code"], selected_series_codes),
        _td(item["indicator"], W_NAME, align="left", bold=True, sticky_left=W_SELECT),
        _td("실제", W_KIND, align="left", sticky_left=W_SELECT + W_NAME),
    ]

    for period_key in period_keys:
        cells.append(_td(actual_map.get(period_key, ""), W_TIME))

    cells.extend(
        [
            _td(item.get("change_display", ""), W_CHANGE, sticky_right=RIGHT_CHANGE),
            _td(item.get("speed", ""), W_SPEED, sticky_right=RIGHT_SPEED),
            _td(item.get("trend", ""), W_TREND, sticky_right=RIGHT_TREND),
            _td(item.get("release_date", ""), W_RELEASE, sticky_right=RIGHT_RELEASE),
            _td(item.get("asset_moves", {}).get("stock", ""), W_ASSET, align="right", sticky_right=RIGHT_STOCK),
            _td(item.get("asset_moves", {}).get("bond", ""), W_ASSET, align="right", sticky_right=RIGHT_BOND),
            _td(item.get("asset_moves", {}).get("fx", ""), W_ASSET, align="right", sticky_right=RIGHT_FX),
        ]
    )

    return html.Tr(cells)


def _build_one_frequency_table(section, selected_series_codes):
    period_keys = section["period_keys"]
    indicators = section["indicators"]

    if not indicators:
        return html.Div(
            "조건에 맞는 지표가 없습니다.",
            style={"padding": "12px", "fontSize": "14px", "color": "#666"},
        )

    body_rows = []
    for item in indicators:
        body_rows.append(_build_indicator_row(item, period_keys, selected_series_codes))

    table_min_width = (
        W_SELECT + W_NAME + W_KIND +
        len(period_keys) * W_TIME +
        W_CHANGE + W_SPEED + W_TREND + W_RELEASE + W_ASSET + W_ASSET + W_ASSET
    )

    table = html.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        _th("선택", W_SELECT, sticky_left=0),
                        _th("지표명", W_NAME, align="left", sticky_left=W_SELECT),
                        _th("구분", W_KIND, align="left", sticky_left=W_SELECT + W_NAME),
                        *[_th(period_key, W_TIME) for period_key in period_keys],
                        _th("전기 대비 변동", W_CHANGE, sticky_right=RIGHT_CHANGE),
                        _th("속도", W_SPEED, sticky_right=RIGHT_SPEED),
                        _th("추세", W_TREND, sticky_right=RIGHT_TREND),
                        _th("발표일", W_RELEASE, sticky_right=RIGHT_RELEASE),
                        _th("주식", W_ASSET, sticky_right=RIGHT_STOCK),
                        _th("채권", W_ASSET, sticky_right=RIGHT_BOND),
                        _th("외환", W_ASSET, sticky_right=RIGHT_FX),
                    ]
                )
            ),
            html.Tbody(body_rows),
        ],
        style={
            "width": "100%",
            "minWidth": px(table_min_width),
            "borderCollapse": "separate",
            "borderSpacing": "0",
            "backgroundColor": "white",
        },
    )

    return html.Div(
        [
            html.H4(
                f"{section['frequency_label']} 지표",
                style={"marginTop": "0", "marginBottom": "10px"},
            ),
            html.Div(
                table,
                style={
                    "width": "100%",
                    "maxWidth": "100%",
                    "overflowX": "auto",
                    "overflowY": "auto",
                    "maxHeight": "420px",
                    "border": "1px solid #ddd",
                    "backgroundColor": "white",
                },
            ),
        ],
        style={"marginBottom": "18px"},
    )


def _build_tables(payload, selected_series_codes):
    sections = payload.get("sections", [])

    if not sections:
        return html.Div(
            "조건에 맞는 시계열이 없습니다.",
            style={"padding": "20px", "fontSize": "14px", "color": "#666"},
        )

    blocks = []
    for section in sections:
        blocks.append(_build_one_frequency_table(section, selected_series_codes))

    return html.Div(blocks)


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
                    dcc.Store(id="macro-selected-series-codes", data=[]),

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
                                        "좌측 3열과 우측 요약열은 고정됩니다. 가운데 시계열 부분만 좌우 스크롤로 확인합니다. "
                                        "또한 주기(monthly / weekly / daily)가 다른 데이터는 표를 분리해 표시합니다.",
                                        style={
                                            "fontSize": "13px",
                                            "color": "#666",
                                            "marginBottom": "10px",
                                        },
                                    ),

                                    html.Div(
                                        id="macro-table-container",
                                        children=_build_tables(initial_payload, selected_series_codes=[]),
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
                                        style={"height": "760px"},
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
        Input("macro-selected-series-codes", "data"),
    )
    def render_macro_table(country, category, selected_series_codes):
        payload = get_macro_tracker_payload(country=country, category=category)
        selected_series_codes = selected_series_codes or []

        valid_codes = set()
        for section in payload.get("sections", []):
            for item in section.get("indicators", []):
                valid_codes.add(item["series_code"])

        filtered_selected = [x for x in selected_series_codes if x in valid_codes]

        return _build_tables(payload, selected_series_codes=filtered_selected)

    @app.callback(
        Output("macro-selected-series-codes", "data"),
        Input({"type": "macro-checklist", "index": ALL}, "value"),
        Input({"type": "macro-checklist", "index": ALL}, "id"),
    )
    def update_selected_series_codes(check_values, check_ids):
        selected = []

        for values, comp_id in zip(check_values, check_ids):
            if values and comp_id["index"] in values:
                selected.append(comp_id["index"])

        return selected

    @app.callback(
        Output("macro-main-chart", "figure"),
        Input("macro-selected-series-codes", "data"),
        Input("macro-country-dropdown", "value"),
        Input("macro-category-dropdown", "value"),
    )
    def update_macro_chart(selected_series_codes, country, category):
        fig = go.Figure()
        payload = get_macro_tracker_payload(country=country, category=category)
        selected_series_codes = selected_series_codes or []

        item_map = {}
        for section in payload.get("sections", []):
            for item in section.get("indicators", []):
                item_map[item["series_code"]] = item

        selected_visible = [x for x in selected_series_codes if x in item_map]

        if not selected_visible:
            fig.update_layout(
                template="plotly_white",
                title="선택된 지표가 없습니다.",
                margin={"l": 40, "r": 20, "t": 60, "b": 40},
            )
            return fig

        for series_code in selected_visible:
            item = item_map[series_code]
            series = item.get("series", [])

            if not series:
                continue

            fig.add_trace(
                go.Scatter(
                    x=[row["date"] for row in series],
                    y=[row["value"] for row in series],
                    mode="lines+markers",
                    name=item["indicator"],
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