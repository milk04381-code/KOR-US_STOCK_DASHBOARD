# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 18:06:10 2026

@author: 박승욱
"""

# tabs/macro_regimes_trend.py

from datetime import date

from dash import html, dcc, Input, Output

from services.chart_service import build_main_figure
from services.macro_regime_service import (
    REGIME_ORDER,
    get_macro_regime_payload,
)


def card_style():
    return {
        "border": "1px solid #ddd",
        "borderRadius": "10px",
        "padding": "16px",
        "backgroundColor": "white",
        "boxShadow": "0 1px 4px rgba(0,0,0,0.06)",
    }


def section_title(text):
    return html.H4(
        text,
        style={"marginTop": "0", "marginBottom": "10px"},
    )


def build_current_regime_card(card_data):
    return html.Div(
        [
            section_title("현재 국면 요약"),
            html.Div("현재 국면", style={"fontSize": "13px", "color": "#666"}),
            html.Div(
                card_data["current_regime"],
                style={"fontSize": "26px", "fontWeight": "bold", "marginBottom": "14px"},
            ),
            html.Div(f"시작 시점: {card_data['start_text']}", style={"marginBottom": "8px"}),
            html.Div(f"지속 개월수: {card_data['duration_months']}개월", style={"marginBottom": "8px"}),
            html.Div(f"최신 반영 시점: {card_data['latest_text']}"),
        ],
        style=card_style(),
    )


def build_summary_table(summary_df, current_regime):
    header_style = {
        "border": "1px solid #333",
        "padding": "8px 10px",
        "backgroundColor": "#f3f3f3",
        "fontWeight": "bold",
        "fontSize": "13px",
        "textAlign": "center",
        "whiteSpace": "nowrap",
    }

    body_style = {
        "border": "1px solid #999",
        "padding": "8px 10px",
        "fontSize": "13px",
        "textAlign": "center",
        "whiteSpace": "nowrap",
    }

    rows = []
    for _, row in summary_df.iterrows():
        regime_name = row["국면"]
        is_total = regime_name == "총계"
        is_current = regime_name == current_regime

        rows.append(
            html.Tr(
                [
                    html.Td(
                        regime_name,
                        style={
                            **body_style,
                            "fontWeight": "bold",
                            "borderTop": "3px solid #333" if is_total else ("3px solid red" if is_current else body_style["border"]),
                            "borderBottom": "3px solid red" if is_current else body_style["border"],
                            "borderLeft": "3px solid red" if is_current else body_style["border"],
                        },
                    ),
                    html.Td(
                        f"{int(row['개월 수'])}",
                        style={
                            **body_style,
                            "borderTop": "3px solid #333" if is_total else ("3px solid red" if is_current else body_style["border"]),
                            "borderBottom": "3px solid red" if is_current else body_style["border"],
                        },
                    ),
                    html.Td(
                        f"{row['비중(%)']:.1f}",
                        style={
                            **body_style,
                            "borderTop": "3px solid #333" if is_total else ("3px solid red" if is_current else body_style["border"]),
                            "borderBottom": "3px solid red" if is_current else body_style["border"],
                        },
                    ),
                    html.Td(
                        f"{int(row['시작횟수(회)'])}",
                        style={
                            **body_style,
                            "borderTop": "3px solid #333" if is_total else ("3px solid red" if is_current else body_style["border"]),
                            "borderBottom": "3px solid red" if is_current else body_style["border"],
                        },
                    ),
                    html.Td(
                        f"{row['평균 유지 개월(Avg)']:.1f}",
                        style={
                            **body_style,
                            "borderTop": "3px solid #333" if is_total else ("3px solid red" if is_current else body_style["border"]),
                            "borderBottom": "3px solid red" if is_current else body_style["border"],
                        },
                    ),
                    html.Td(
                        f"{row['중앙값(Median)']:.1f}",
                        style={
                            **body_style,
                            "borderTop": "3px solid #333" if is_total else ("3px solid red" if is_current else body_style["border"]),
                            "borderBottom": "3px solid red" if is_current else body_style["border"],
                        },
                    ),
                    html.Td(
                        f"{int(row['최댓값(Max)'])}",
                        style={
                            **body_style,
                            "borderTop": "3px solid #333" if is_total else ("3px solid red" if is_current else body_style["border"]),
                            "borderBottom": "3px solid red" if is_current else body_style["border"],
                            "borderRight": "3px solid red" if is_current else body_style["border"],
                        },
                    ),
                ],
                style={"backgroundColor": "#fafafa" if is_total else "white"},
            )
        )

    return html.Div(
        [
            section_title("요약 통계"),
            html.Table(
                [
                    html.Thead(
                        html.Tr(
                            [
                                html.Th("국면", style=header_style),
                                html.Th("개월 수", style=header_style),
                                html.Th("비중(%)", style=header_style),
                                html.Th("시작횟수(회)", style=header_style),
                                html.Th("평균 유지 개월(Avg)", style=header_style),
                                html.Th("중앙값(Median)", style=header_style),
                                html.Th("최댓값(Max)", style=header_style),
                            ]
                        )
                    ),
                    html.Tbody(rows),
                ],
                style={
                    "width": "100%",
                    "borderCollapse": "collapse",
                    "backgroundColor": "white",
                },
            ),
        ],
        style=card_style(),
    )


def build_asset_return_table(label_text, asset_return_df, current_regime):
    header_style = {
        "border": "1px solid #333",
        "padding": "8px 10px",
        "backgroundColor": "#f3f3f3",
        "fontWeight": "bold",
        "fontSize": "13px",
        "textAlign": "center",
        "whiteSpace": "nowrap",
    }

    body_style = {
        "border": "1px solid #999",
        "padding": "8px 10px",
        "fontSize": "13px",
        "textAlign": "center",
        "whiteSpace": "nowrap",
    }

    header_cells = [
        html.Th("자산군", style=header_style),
        html.Th("구분", style=header_style),
        html.Th("ETF", style=header_style),
    ]
    for regime_name in REGIME_ORDER:
        style = dict(header_style)
        if regime_name == current_regime:
            style["borderTop"] = "3px solid red"
            style["borderLeft"] = "3px solid red"
            style["borderRight"] = "3px solid red"
        header_cells.append(html.Th(regime_name, style=style))

    rows = []
    first_row_idx = 0 if not asset_return_df.empty else None
    last_row_idx = len(asset_return_df) - 1 if not asset_return_df.empty else None

    prev_asset_group = None

    for row_idx, (_, row) in enumerate(asset_return_df.iterrows()):
        asset_group_text = row["자산군"] if row["자산군"] != prev_asset_group else ""
        prev_asset_group = row["자산군"]

        cells = [
            html.Td(asset_group_text, style={**body_style, "fontWeight": "bold"}),
            html.Td(row["구분"], style=body_style),
            html.Td(row["ETF"], style={**body_style, "fontWeight": "bold"}),
        ]

        for regime_name in REGIME_ORDER:
            style = dict(body_style)
            style["backgroundColor"] = _cell_fill_color(row[regime_name])

            if regime_name == current_regime:
                style["borderLeft"] = "3px solid red"
                style["borderRight"] = "3px solid red"
                if row_idx == first_row_idx:
                    style["borderTop"] = "3px solid red"
                if row_idx == last_row_idx:
                    style["borderBottom"] = "3px solid red"

            cells.append(html.Td(row[regime_name], style=style))

        rows.append(html.Tr(cells))

    return html.Div(
        [
            section_title(f"경기 국면별 자산군별 월평균 수익률 ({label_text})"),
            html.Table(
                [
                    html.Thead(html.Tr(header_cells)),
                    html.Tbody(rows),
                ],
                style={
                    "width": "100%",
                    "borderCollapse": "collapse",
                    "backgroundColor": "white",
                },
            ),
        ],
        style={**card_style(), "marginTop": "16px"},
    )


def build_transition_matrix_table(transition_df, current_regime):
    months_pivot = (
        transition_df
        .pivot(index="from_regime", columns="to_regime", values="months")
        .reindex(index=REGIME_ORDER, columns=REGIME_ORDER)
        .fillna(0)
    )

    ratio_pivot = (
        transition_df
        .pivot(index="from_regime", columns="to_regime", values="ratio")
        .reindex(index=REGIME_ORDER, columns=REGIME_ORDER)
        .fillna(0.0)
    )

    header_style = {
        "border": "1px solid #333",
        "padding": "8px 10px",
        "backgroundColor": "#f3f3f3",
        "fontWeight": "bold",
        "fontSize": "13px",
        "textAlign": "center",
    }

    body_style = {
        "border": "1px solid #999",
        "padding": "8px 10px",
        "fontSize": "13px",
        "textAlign": "center",
        "whiteSpace": "nowrap",
    }

    rows = []
    for regime_name in REGIME_ORDER:
        is_current = regime_name == current_regime
        row_cells = []

        left_style = dict(body_style)
        if is_current:
            left_style["borderLeft"] = "3px solid red"
            left_style["borderTop"] = "3px solid red"
            left_style["borderBottom"] = "3px solid red"

        row_cells.append(
            html.Td(
                regime_name,
                style={**left_style, "fontWeight": "bold"},
            )
        )

        for col_idx, to_regime in enumerate(REGIME_ORDER):
            cell_style = dict(body_style)
            if is_current:
                cell_style["borderTop"] = "3px solid red"
                cell_style["borderBottom"] = "3px solid red"
                if col_idx == len(REGIME_ORDER) - 1:
                    cell_style["borderRight"] = "3px solid red"

            months_value = int(months_pivot.loc[regime_name, to_regime])
            ratio_value = float(ratio_pivot.loc[regime_name, to_regime])

            row_cells.append(
                html.Td(
                    f"{months_value} / {ratio_value:.1f}",
                    style=cell_style,
                )
            )

        rows.append(html.Tr(row_cells))

    return html.Div(
        [
            section_title("유지/전환 matrix"),
            html.Table(
                [
                    html.Thead(
                        html.Tr(
                            [html.Th("From \\ To", style=header_style)] +
                            [html.Th(x, style=header_style) for x in REGIME_ORDER]
                        )
                    ),
                    html.Tbody(rows),
                ],
                style={
                    "width": "100%",
                    "borderCollapse": "collapse",
                    "backgroundColor": "white",
                },
            ),
            html.Div(
                f"현재 국면 row 강조: {current_regime}",
                style={"fontSize": "12px", "color": "#666", "marginTop": "8px"},
            ),
            html.Div(
                "단위: 개월 수 / 비중(%)",
                style={"fontSize": "12px", "color": "#666", "marginTop": "4px"},
            ),
            html.Div(
                "기준: 마지막 관측월은 제외, 비중 분모는 다음 달이 존재하는 from 국면 개월 수",
                style={"fontSize": "12px", "color": "#666", "marginTop": "4px"},
            ),
        ],
        style=card_style(),
    )


def _cell_fill_color(value):
    if value is None:
        return "#ffffff"

    try:
        v = float(value)
    except Exception:
        return "#ffffff"

    if abs(v) < 1e-12:
        return "#ffffff"

    if v > 0:
        if v >= 2:
            return "#7bc67e"
        if v >= 1:
            return "#a6dba0"
        return "#d9f0d3"

    if v <= -2:
        return "#fb6a6a"
    if v <= -1:
        return "#fcaeae"
    return "#fde0dd"


def build_transition_return_table(transition_table_df, transition_columns, current_regime):
    header_style = {
        "border": "1px solid #333",
        "padding": "8px 10px",
        "backgroundColor": "#f3f3f3",
        "fontWeight": "bold",
        "fontSize": "13px",
        "textAlign": "center",
        "whiteSpace": "nowrap",
    }

    body_style = {
        "border": "1px solid #999",
        "padding": "8px 10px",
        "fontSize": "13px",
        "textAlign": "center",
        "whiteSpace": "nowrap",
    }

    sticky_left_1 = 0
    sticky_left_2 = 90
    sticky_left_3 = 180

    sticky_header_base = {
        "position": "sticky",
        "zIndex": 4,
        "backgroundColor": "#f3f3f3",
    }

    sticky_body_base = {
        "position": "sticky",
        "zIndex": 3,
        "backgroundColor": "white",
    }

    highlight_cols = [x for x in transition_columns if x.startswith(f"{current_regime}->")]

    header_cells = [
        html.Th(
            "자산군",
            style={
                **header_style,
                **sticky_header_base,
                "left": f"{sticky_left_1}px",
                "minWidth": "90px",
                "width": "90px",
                "maxWidth": "90px",
            },
        ),
        html.Th(
            "구분",
            style={
                **header_style,
                **sticky_header_base,
                "left": f"{sticky_left_2}px",
                "minWidth": "90px",
                "width": "90px",
                "maxWidth": "90px",
            },
        ),
        html.Th(
            "ETF",
            style={
                **header_style,
                **sticky_header_base,
                "left": f"{sticky_left_3}px",
                "minWidth": "90px",
                "width": "90px",
                "maxWidth": "90px",
            },
        ),
    ]
    for idx, col in enumerate(transition_columns):
        style = dict(header_style)
        if col in highlight_cols:
            style["borderTop"] = "3px solid red"
            if idx == transition_columns.index(highlight_cols[0]):
                style["borderLeft"] = "3px solid red"
            if idx == transition_columns.index(highlight_cols[-1]):
                style["borderRight"] = "3px solid red"

        header_cells.append(
            html.Th(
                col.replace("->", "→"),
                style={**style, "fontSize": "12px"},
            )
        )

    first_row_idx = 0 if not transition_table_df.empty else None
    last_row_idx = len(transition_table_df) - 1 if not transition_table_df.empty else None

    rows = []
    prev_asset_group = None

    for row_idx, (_, row) in enumerate(transition_table_df.iterrows()):
        asset_group_text = row["자산군"] if row["자산군"] != prev_asset_group else ""
        prev_asset_group = row["자산군"]

        row_cells = [
            html.Td(
                asset_group_text,
                style={
                    **body_style,
                    **sticky_body_base,
                    "left": f"{sticky_left_1}px",
                    "fontWeight": "bold",
                    "minWidth": "90px",
                    "width": "90px",
                    "maxWidth": "90px",
                },
            ),
            html.Td(
                row["구분"],
                style={
                    **body_style,
                    **sticky_body_base,
                    "left": f"{sticky_left_2}px",
                    "minWidth": "90px",
                    "width": "90px",
                    "maxWidth": "90px",
                },
            ),
            html.Td(
                row["ETF"],
                style={
                    **body_style,
                    **sticky_body_base,
                    "left": f"{sticky_left_3}px",
                    "fontWeight": "bold",
                    "minWidth": "90px",
                    "width": "90px",
                    "maxWidth": "90px",
                },
            ),
        ]

        for idx, col in enumerate(transition_columns):
            value = row[col]
            style = dict(body_style)
            style["backgroundColor"] = _cell_fill_color(value)

            if col in highlight_cols:
                if idx == transition_columns.index(highlight_cols[0]):
                    style["borderLeft"] = "3px solid red"
                if idx == transition_columns.index(highlight_cols[-1]):
                    style["borderRight"] = "3px solid red"
                if row_idx == first_row_idx:
                    style["borderTop"] = "3px solid red"
                if row_idx == last_row_idx:
                    style["borderBottom"] = "3px solid red"

            row_cells.append(html.Td(f"{value:.1f}", style=style))

        rows.append(html.Tr(row_cells))

    return html.Div(
        [
            section_title("유지/전환 이후 자산군별 3개월 수익률"),
            html.Div(
                html.Table(
                    [
                        html.Thead(html.Tr(header_cells)),
                        html.Tbody(rows),
                    ],
                    style={
                        "borderCollapse": "collapse",
                        "backgroundColor": "white",
                        "minWidth": "1200px",
                    },
                ),
                style={
                    "overflowX": "auto",
                    "position": "relative",
                },
            ),
            html.Div(
                f"현재 국면 column block 강조: {current_regime}→*",
                style={"fontSize": "12px", "color": "#666", "marginTop": "8px"},
            ),
        ],
        style=card_style(),
    )


def get_layout():
    return dcc.Tab(
        label="경기 국면",
        children=[
            html.Div(
                [
                    html.H3("경기 국면"),

                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("국가", style={"fontWeight": "bold", "marginBottom": "6px"}),
                                    dcc.Dropdown(
                                        id="regime-country",
                                        options=[
                                            {"label": "한국", "value": "KR"},
                                            {"label": "미국", "value": "US"},
                                        ],
                                        value="KR",
                                        clearable=False,
                                    ),
                                ],
                                style={"width": "24%"},
                            ),
                            html.Div(
                                [
                                    html.Label("시작일", style={"fontWeight": "bold", "marginBottom": "6px"}),
                                    dcc.DatePickerSingle(
                                        id="regime-start-date",
                                        date="1980-01-01",
                                        display_format="YYYY-MM-DD",
                                    ),
                                ],
                                style={"width": "24%"},
                            ),
                            html.Div(
                                [
                                    html.Label("종료일", style={"fontWeight": "bold", "marginBottom": "6px"}),
                                    dcc.DatePickerSingle(
                                        id="regime-end-date",
                                        date=None,
                                        display_format="YYYY-MM-DD",
                                        placeholder="종료일 비우면 오늘 날짜 자동",
                                    ),
                                ],
                                style={"width": "24%"},
                            ),
                            html.Div(
                                [
                                    html.Label("옵션", style={"fontWeight": "bold", "marginBottom": "6px"}),
                                    dcc.Checklist(
                                        id="regime-options",
                                        options=[
                                            {"label": "Recession 표시", "value": "show_recession"}
                                        ],
                                        value=["show_recession"],
                                        inline=True,
                                    ),
                                ],
                                style={"width": "24%"},
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
                        [dcc.Graph(id="regime-chart", style={"height": "760px"})],
                        style={**card_style(), "marginBottom": "18px"},
                    ),

                    html.Div(
                        [
                            html.Div(id="regime-current-card-wrap", style={"width": "32%"}),
                            html.Div(id="regime-summary-table-wrap", style={"width": "68%"}),
                        ],
                        style={
                            "display": "flex",
                            "gap": "16px",
                            "alignItems": "stretch",
                            "marginBottom": "16px",
                        },
                    ),

                    html.Div(id="regime-asset-return-wrap"),

                    html.Div(
                        [
                            html.Div(id="regime-transition-table-wrap", style={"width": "40%"}),
                            html.Div(id="regime-transition-return-wrap", style={"width": "60%"}),
                        ],
                        style={
                            "display": "flex",
                            "gap": "16px",
                            "alignItems": "stretch",
                            "marginTop": "16px",
                        },
                    ),
                ],
                style={"width": "88%", "margin": "24px auto", "backgroundColor": "white"},
            )
        ],
    )


def register_callbacks(app):
    @app.callback(
        Output("regime-chart", "figure"),
        Output("regime-current-card-wrap", "children"),
        Output("regime-summary-table-wrap", "children"),
        Output("regime-asset-return-wrap", "children"),
        Output("regime-transition-table-wrap", "children"),
        Output("regime-transition-return-wrap", "children"),
        Input("regime-country", "value"),
        Input("regime-start-date", "date"),
        Input("regime-end-date", "date"),
        Input("regime-options", "value"),
    )
    def update_chart(country, start_date, end_date, options):
        if start_date is None:
            start_date = "1980-01-01"
        if end_date is None:
            end_date = date.today().isoformat()

        show_recession = "show_recession" in (options or [])

        payload = get_macro_regime_payload(
            country=country,
            start_date=start_date,
            end_date=end_date,
        )

        current_regime = payload["current_card"]["current_regime"]

        fig = build_main_figure(
            dataset=payload["chart_dataset"],
            start_date=payload["effective_start"],
            end_date=payload["effective_end"],
            show_recession=show_recession,
            recession_style="outline",
            background_intervals=payload["intervals"],
            chart_title="경기 국면 추이",
        )

        current_card = build_current_regime_card(payload["current_card"])
        summary_table = build_summary_table(payload["summary_df"], current_regime)
        asset_return_table = build_asset_return_table(
            payload["asset_return_label"],
            payload["asset_return_df"],
            current_regime,
        )
        transition_table = build_transition_matrix_table(
            payload["transition_df"],
            current_regime,
        )
        transition_return_table = build_transition_return_table(
            payload["transition_table_df"],
            payload["transition_columns"],
            current_regime,
        )

        return (
            fig,
            current_card,
            summary_table,
            asset_return_table,
            transition_table,
            transition_return_table,
        )