# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 13:27:30 2026

@author: 박승욱
"""

# tabs/macro_tracker.py
# 수정본
# 반영 사항:
# 1) 가운데 시계열 헤더 1행 왼쪽에 '연준의 통화정책 국면' 라벨 복구
# 2) 가운데 시계열 헤더 2행 왼쪽에 '기준시기' 라벨 복구
# 3) 스크롤 기본 위치를 오른쪽 끝으로 다시 보정
# 4) 선택/지표명 헤더를 1행 구조(rowSpan=2)로 복구
# 5) 전기 대비 변동 헤더 줄바꿈 허용
# 6) 체크박스 선택 시 우측 차트 즉시 반응하도록 보강
# 7) 기존 레이아웃 구조는 최대한 유지

from datetime import date
import time
import os

from dash import dcc, html, Input, Output, State, ALL
import plotly.graph_objs as go

from services.macro_tracker_service import get_macro_tracker_payload, cached_macro_payload
from services.data_service import load_chart_dataset
from services.chart_service import build_main_figure

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
        body { margin: 0; padding: 0; background: white; }
        .wrap { width: 100%; margin: 0 auto; background: white; }
        .poweredBy { font-family: Arial, Helvetica, sans-serif; }
        .underline_link { font-size: 11px; color: #06529D; font-weight: bold; }
    </style>
</head>
<body>
    <div class="wrap">
        <iframe src="https://sslecal2.investing.com?columns=exc_flags,exc_currency,exc_importance,exc_actual,exc_forecast,exc_previous&features=datepicker,timezone,timeselector,filters&countries=11,5&calType=week&timeZone=8&lang=1"
                width="100%"
                height="467"
                frameborder="0"
                allowtransparency="true"
                marginwidth="0"
                marginheight="0"></iframe>
        <div class="poweredBy">
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

TECHNICAL_CHART_EMBED_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>Investing Technical Chart</title>
    <style>
        body { margin: 0; padding: 0; background: white; }
        .wrap { width: 100%; margin: 0 auto; background: white; }
        iframe { width: 100%; border: 0; }
    </style>
</head>
<body>
    <div class="wrap">
        <iframe height="480"
                src="https://ssltvc.investing.com/?pair_ID=166&height=480&width=650&interval=86400&plotStyle=candles&domain_ID=1&lang_ID=1&timezone_ID=7"></iframe>
    </div>
</body>
</html>
"""

# -------------------------
# 폭 설정
# -------------------------
W_SELECT = 44
W_NAME = 120

W_TIME_LABEL = 92
W_TIME = 72

W_CHANGE = 78
W_SPEED = 74
W_TREND = 86
W_RELEASE = 82
W_ASSET = 66

ROW_HEIGHT = 38
HEAD_HEIGHT = 34

BORDER = "1px solid #333"

DEBUG_MACRO = os.getenv("DEBUG_MACRO", "1") == "1"

def macro_debug(*args):
    if DEBUG_MACRO:
        print("[macro_debug]", *args, flush=True)


# -------------------------
# 공통 스타일
# -------------------------
def _cell_style(width, bg="white", align="center", bold=False):
    style = {
        "border": BORDER,
        "padding": "4px 6px",
        "textAlign": align,
        "verticalAlign": "middle",
        "whiteSpace": "nowrap",
        "fontSize": "12px",
        "backgroundColor": bg,
        "boxSizing": "border-box",
        "width": f"{width}px",
        "minWidth": f"{width}px",
        "maxWidth": f"{width}px",
        "height": f"{ROW_HEIGHT}px",
        "lineHeight": "1.2",
        "overflow": "hidden",
        "textOverflow": "ellipsis",
    }
    if bold:
        style["fontWeight"] = "bold"
    return style


def _head_style(
    width=None,
    bg="#f2f2f2",
    align="center",
    height=HEAD_HEIGHT,
    nowrap=True,
    overflow_hidden=True,
):
    style = {
        "border": BORDER,
        "padding": "4px 6px",
        "textAlign": align,
        "verticalAlign": "middle",
        "whiteSpace": "nowrap" if nowrap else "normal",
        "fontSize": "12px",
        "backgroundColor": bg,
        "boxSizing": "border-box",
        "height": f"{height}px",
        "lineHeight": "1.2",
        "overflow": "hidden" if overflow_hidden else "visible",
        "textOverflow": "ellipsis" if overflow_hidden else "clip",
        "wordBreak": "keep-all",
    }
    if width is not None:
        style["width"] = f"{width}px"
        style["minWidth"] = f"{width}px"
        style["maxWidth"] = f"{width}px"
    return style


def _merged_head_style(width=None, bg="#f2f2f2", align="center", nowrap=True, overflow_hidden=True):
    return _head_style(
        width=width,
        bg=bg,
        align=align,
        height=HEAD_HEIGHT * 2,
        nowrap=nowrap,
        overflow_hidden=overflow_hidden,
    )


def _table_base_style():
    return {
        "borderCollapse": "collapse",
        "tableLayout": "fixed",
        "backgroundColor": "white",
    }


def _checkbox_control(series_code, selected_series_codes):
    checked = [series_code] if series_code in selected_series_codes else []
    return dcc.Checklist(
        id={"type": "macro-checklist", "index": series_code},
        options=[{"label": "", "value": series_code}],
        value=checked,
        style={"display": "flex", "justifyContent": "center"},
        inputStyle={"marginRight": "0px"},
    )


# -------------------------
# 좌/중/우 패널 빌더
# -------------------------
def _build_left_table(indicators, selected_series_codes):
    total_width = W_SELECT + W_NAME

    table = html.Table(
        [
            html.Thead(
                [
                    html.Tr(
                        [
                            html.Th("선택", rowSpan=2, style=_merged_head_style(W_SELECT)),
                            html.Th("지표명", rowSpan=2, style=_merged_head_style(W_NAME, align="center")),
                        ]
                    ),
                    html.Tr([]),
                ]
            ),
            html.Tbody(
                [
                    html.Tr(
                        [
                            html.Td(
                                _checkbox_control(item["series_code"], selected_series_codes),
                                style=_cell_style(W_SELECT),
                            ),
                            html.Td(
                                item["indicator"],
                                style=_cell_style(W_NAME, align="left", bold=True),
                            ),
                        ]
                    )
                    for item in indicators
                ]
            ),
        ],
        style={
            **_table_base_style(),
            "width": f"{total_width}px",
            "minWidth": f"{total_width}px",
        },
    )
    return table


def _build_middle_table(period_keys, period_labels, indicators, frequency, policy_phase_by_period=None):
    policy_phase_by_period = policy_phase_by_period or {}
    total_width = W_TIME_LABEL + (len(period_keys) * W_TIME)

    header_row_1 = html.Tr(
        [
            html.Th("연준의 통화정책 국면", style=_head_style(W_TIME_LABEL, align="center")),
            *[
                html.Th(
                    policy_phase_by_period.get(key, ""),
                    style=_head_style(W_TIME),
                    title=policy_phase_by_period.get(key, ""),
                )
                for key in period_keys
            ],
        ]
    )

    header_row_2 = html.Tr(
        [
            html.Th("기준시기", style=_head_style(W_TIME_LABEL, align="center")),
            *[
                html.Th(label, style=_head_style(W_TIME))
                for label in period_labels
            ],
        ]
    )

    body_rows = []
    for item in indicators:
        actual_map = item["actual"]
        body_rows.append(
            html.Tr(
                [
                    html.Td("", style=_cell_style(W_TIME_LABEL)),
                    *[
                        html.Td(actual_map.get(key, ""), style=_cell_style(W_TIME))
                        for key in period_keys
                    ],
                ]
            )
        )

    table = html.Table(
        [
            html.Thead([header_row_1, header_row_2]),
            html.Tbody(body_rows),
        ],
        style={
            **_table_base_style(),
            "width": f"{total_width}px",
            "minWidth": f"{total_width}px",
        },
    )

    return html.Div(
        table,
        id={"type": "macro-middle-scroll", "index": frequency},
        className="macro-middle-scroll",
        style={
            "overflowX": "auto",
            "overflowY": "hidden",
            "width": "100%",
            "backgroundColor": "white",
            "scrollBehavior": "auto",
        },
    )


def _build_right_table(indicators):
    total_width = W_CHANGE + W_SPEED + W_TREND + W_RELEASE + (W_ASSET * 3)

    table = html.Table(
        [
            html.Thead(
                [
                    html.Tr(
                        [
                            html.Th(
                                "전기 대비 변동",
                                rowSpan=2,
                                style=_merged_head_style(
                                    W_CHANGE,
                                    nowrap=False,
                                    overflow_hidden=False,
                                ),
                            ),
                            html.Th("속도", rowSpan=2, style=_merged_head_style(W_SPEED)),
                            html.Th("추세", rowSpan=2, style=_merged_head_style(W_TREND)),
                            html.Th("발표일", rowSpan=2, style=_merged_head_style(W_RELEASE)),
                            html.Th("자산군별 일중 변동", colSpan=3, style=_head_style(W_ASSET * 3)),
                        ]
                    ),
                    html.Tr(
                        [
                            html.Th("주식", style=_head_style(W_ASSET)),
                            html.Th("채권", style=_head_style(W_ASSET)),
                            html.Th("외환", style=_head_style(W_ASSET)),
                        ]
                    ),
                ]
            ),
            html.Tbody(
                [
                    html.Tr(
                        [
                            html.Td(item.get("change_display", ""), style=_cell_style(W_CHANGE)),
                            html.Td(item.get("speed", ""), style=_cell_style(W_SPEED)),
                            html.Td(item.get("trend", ""), style=_cell_style(W_TREND)),
                            html.Td(item.get("release_date", ""), style=_cell_style(W_RELEASE)),
                            html.Td(item.get("asset_moves", {}).get("stock", ""), style=_cell_style(W_ASSET)),
                            html.Td(item.get("asset_moves", {}).get("bond", ""), style=_cell_style(W_ASSET)),
                            html.Td(item.get("asset_moves", {}).get("fx", ""), style=_cell_style(W_ASSET)),
                        ]
                    )
                    for item in indicators
                ]
            ),
        ],
        style={
            **_table_base_style(),
            "width": f"{total_width}px",
            "minWidth": f"{total_width}px",
        },
    )
    return table


# -------------------------
# 주기별 표 하나
# -------------------------
def _build_one_frequency_table(section, selected_series_codes):
    frequency = section["frequency"]
    period_keys = section["period_keys"]
    period_labels = section.get("period_labels", period_keys)
    indicators = section["indicators"]
    policy_phase_by_period = section.get("policy_phase_by_period", {})

    if not indicators:
        return html.Div(
            "데이터 없음",
            style={"padding": "12px", "fontSize": "13px", "color": "#666"},
        )

    left_table = _build_left_table(indicators, selected_series_codes)
    middle_table = _build_middle_table(
        period_keys,
        period_labels,
        indicators,
        frequency,
        policy_phase_by_period=policy_phase_by_period,
    )
    right_table = _build_right_table(indicators)

    left_width = W_SELECT + W_NAME
    right_width = W_CHANGE + W_SPEED + W_TREND + W_RELEASE + (W_ASSET * 3)

    return html.Div(
        [
            html.H4(
                f"{section['frequency_label']} 지표",
                style={"marginTop": "0", "marginBottom": "8px"},
            ),
            html.Div(
                [
                    html.Div(
                        left_table,
                        style={
                            "width": f"{left_width}px",
                            "minWidth": f"{left_width}px",
                            "flex": "0 0 auto",
                        },
                    ),
                    html.Div(
                        middle_table,
                        style={
                            "flex": "1 1 auto",
                            "minWidth": "0",
                        },
                    ),
                    html.Div(
                        right_table,
                        style={
                            "width": f"{right_width}px",
                            "minWidth": f"{right_width}px",
                            "flex": "0 0 auto",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "alignItems": "flex-start",
                    "gap": "0px",
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

    return html.Div(
        [
            _build_one_frequency_table(section, selected_series_codes)
            for section in sections
        ]
    )


def _build_economic_calendar_block():
    return html.Div(
        [
            html.H3("Economic Calendar (Investing)", style={"marginBottom": "8px"}),
            html.Div(
                "하단 위젯에서 actual / forecast / previous 값을 확인합니다.",
                style={"fontSize": "14px", "color": "#666", "marginBottom": "12px"},
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
        style={"flex": "1 1 0", "minWidth": "0", "paddingTop": "8px"},
    )


def _build_technical_chart_block():
    return html.Div(
        [
            html.H3("Technical Charts (Investing)", style={"marginBottom": "8px"}),
            html.Div(
                "미국 주식 대표 지수 기술적 차트를 하단 위젯에서 바로 확인합니다.",
                style={"fontSize": "14px", "color": "#666", "marginBottom": "12px"},
            ),
            html.Iframe(
                srcDoc=TECHNICAL_CHART_EMBED_HTML,
                style={
                    "width": "100%",
                    "height": "560px",
                    "border": "1px solid #ddd",
                    "backgroundColor": "white",
                },
            ),
        ],
        style={"flex": "1 1 0", "minWidth": "0", "paddingTop": "8px"},
    )


def _build_bottom_widget_row():
    return html.Div(
        [
            _build_economic_calendar_block(),
            _build_technical_chart_block(),
        ],
        style={
            "display": "flex",
            "gap": "16px",
            "marginTop": "20px",
            "alignItems": "flex-start",
        },
    )


def get_layout():
    initial_payload = get_macro_tracker_payload(
        country="US",
        category="ALL",
        start_date="1970-01-01",
        end_date=None,
    )

    return dcc.Tab(
        label="경제지표 Tracker",
        children=[
            dcc.Store(id="macro-scroll-trigger"),
            dcc.Store(id="macro-scroll-done"),
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
                                        [
                                            html.Div(
                                                [
                                                    html.Label("시작일", style={"fontWeight": "bold", "marginBottom": "6px"}),
                                                    dcc.DatePickerSingle(
                                                        id="macro-start-date",
                                                        date="1970-01-01",
                                                        display_format="YYYY-MM-DD",
                                                    ),
                                                ],
                                                style={"width": "48%"},
                                            ),
                                            html.Div(
                                                [
                                                    html.Label("종료일", style={"fontWeight": "bold", "marginBottom": "6px"}),
                                                    dcc.DatePickerSingle(
                                                        id="macro-end-date",
                                                        date=None,
                                                        display_format="YYYY-MM-DD",
                                                        placeholder="종료일 비우면 오늘 날짜 자동",
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
                                        "좌측/우측 고정 영역은 스크롤되지 않고, 가운데 시계열만 좌우 스크롤됩니다.",
                                        style={"fontSize": "13px", "color": "#666", "marginBottom": "10px"},
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
                                        style={"fontSize": "14px", "color": "#666", "marginBottom": "8px"},
                                    ),
                                    dcc.Checklist(
                                        id="macro-recession-check",
                                        options=[{"label": "Recession 음영 표시", "value": "show"}],
                                        value=["show"],
                                    ),
                                    dcc.Graph(id="macro-main-chart", style={"height": "760px"}),
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
                    _build_bottom_widget_row(),
                ],
                style={"width": "88%", "margin": "24px auto", "backgroundColor": "white"},
            )
        ],
    )


def register_callbacks(app):
    app.clientside_callback(
        """
        function(trigger_value) {
            if (!trigger_value) {
                return "";
            }

            const applyScrollRight = function() {
                const nodes = document.querySelectorAll('.macro-middle-scroll');
                nodes.forEach(function(node) {
                    node.scrollLeft = node.scrollWidth;
                });
            };

            setTimeout(applyScrollRight, 0);
            setTimeout(applyScrollRight, 120);
            setTimeout(applyScrollRight, 300);

            return "done";
        }
        """,
        Output("macro-scroll-done", "data"),
        Input("macro-scroll-trigger", "data"),
        prevent_initial_call=True,
    )

    @app.callback(
        Output("macro-table-container", "children"),
        Output("macro-scroll-trigger", "data"),
        Input("macro-country-dropdown", "value"),
        Input("macro-category-dropdown", "value"),
        Input("macro-start-date", "date"),
        Input("macro-end-date", "date"),
        State("macro-selected-series-codes", "data"),
    )
    def render_macro_table(country, category, start_date, end_date, selected_series_codes):
        if start_date is None:
            start_date = "1970-01-01"
        if end_date is None:
            end_date = date.today().isoformat()

        payload = cached_macro_payload(
            country,
            category,
            start_date,
            end_date,
        )
        selected_series_codes = selected_series_codes or []

        valid_codes = set()
        for section in payload.get("sections", []):
            for item in section.get("indicators", []):
                valid_codes.add(item["series_code"])

        filtered_selected = [x for x in selected_series_codes if x in valid_codes]

        trigger_value = {
            "country": country,
            "category": category,
            "start_date": start_date,
            "end_date": end_date,
            "ts": time.time(),
        }

        return _build_tables(payload, selected_series_codes=filtered_selected), trigger_value

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
    
        macro_debug(
            "update_selected_series_codes",
            "check_values=", check_values,
            "check_ids=", check_ids,
            "selected=", selected,
        )
    
        return selected
    
    @app.callback(
        Output("macro-main-chart", "figure"),
        Input("macro-selected-series-codes", "data"),
        Input("macro-start-date", "date"),
        Input("macro-end-date", "date"),
        Input("macro-recession-check", "value"),
    )
    def update_macro_chart(
        selected_series_codes,
        start_date,
        end_date,
        recession_value,
    ):
        if start_date is None:
            start_date = "1970-01-01"
        if end_date is None:
            end_date = date.today().isoformat()
    
        selected_series_codes = selected_series_codes or []
    
        if not selected_series_codes:
            fig = go.Figure()
            fig.update_layout(
                template="plotly_white",
                title="선택된 지표가 없습니다.",
                margin={"l": 40, "r": 20, "t": 60, "b": 40},
            )
            return fig
    
        dataset = load_chart_dataset(
            selected_codes=selected_series_codes,
            start_date=start_date,
            end_date=end_date,
            axis_override_map=None,
        )
    
        if dataset is None or dataset.empty:
            fig = go.Figure()
            fig.update_layout(
                template="plotly_white",
                title="데이터가 없습니다.",
                margin={"l": 40, "r": 20, "t": 60, "b": 40},
            )
            return fig
    
        fig = build_main_figure(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            show_recession=("show" in (recession_value or [])),
        )
    
        fig.update_layout(
            title=f"선택 지표 시계열 ({start_date} ~ {end_date})",
        )
        return fig