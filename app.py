# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 16:03:16 2026

@author: 박승욱
"""


# 임시 배포용 app.py
# 목적:
# 1. Render에서 DB 연결이 실패해도 앱 자체는 뜨게 만들기
# 2. Investing 위젯이 들어간 macro tracker 탭 공개 URL 확보
# 3. 승인 후에는 다시 정상 DB 연결 버전으로 확장 가능하게 유지

from dash import Dash, dcc, html

# ---------------------------------------
# data_service는 DB를 사용하므로,
# 실패할 경우 빈 옵션으로 fallback
# ---------------------------------------
try:
    from services.data_service import (
        load_series_dropdown_options,
        get_default_series_selection,
    )

    series_options = load_series_dropdown_options()
    default_value = get_default_series_selection(
        default_codes=["KOSPI"],
        fallback_count=2,
    )
    data_boot_ok = True
    data_boot_message = ""
except Exception as e:
    series_options = []
    default_value = []
    data_boot_ok = False
    data_boot_message = f"DB 연결 없이 임시 배포 모드로 실행 중입니다. ({e})"


from tabs.domestic_monitor import (
    get_layout as get_domestic_monitor_layout,
    register_callbacks as register_domestic_monitor_callbacks,
)
from tabs.macro_tracker import (
    get_layout as get_macro_tracker_layout,
    register_callbacks as register_macro_tracker_callbacks,
)
from tabs.regime_dashboard import get_layout as get_regime_dashboard_layout
from tabs.interactive_chart_indices import get_layout as get_interactive_chart_indices_layout


app = Dash(__name__)
server = app.server
app.title = "국내/미국 주식 Dashboard"


# ---------------------------------------
# 상단 안내 문구
# - DB가 연결되지 않은 임시 배포 상태임을 명시
# ---------------------------------------
status_banner = None
if not data_boot_ok:
    status_banner = html.Div(
        data_boot_message,
        style={
            "width": "88%",
            "margin": "12px auto 0 auto",
            "padding": "10px 12px",
            "backgroundColor": "#fff3cd",
            "border": "1px solid #ffe69c",
            "color": "#664d03",
            "fontSize": "13px",
            "borderRadius": "4px",
        },
    )


app.layout = html.Div(
    [
        html.H2(
            "국내/미국 주식 Dashboard",
            style={"width": "88%", "margin": "20px auto 8px auto"},
        ),

        status_banner if status_banner is not None else html.Div(),

        dcc.Tabs(
            [
                get_domestic_monitor_layout(series_options, default_value),
                get_macro_tracker_layout(),
                get_regime_dashboard_layout(),
                get_interactive_chart_indices_layout(),
            ],
            style={"width": "88%", "margin": "0 auto"},
        ),
    ]
)


# ---------------------------------------
# 콜백 등록
# - domestic_monitor 콜백은 빈 데이터여도 동작 자체는 가능
# - macro_tracker는 현재 MOCK/위젯 기반이라 그대로 등록
# ---------------------------------------
register_domestic_monitor_callbacks(app)
register_macro_tracker_callbacks(app)


if __name__ == "__main__":
    app.run(debug=True)