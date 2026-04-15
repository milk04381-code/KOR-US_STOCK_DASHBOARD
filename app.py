# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 16:03:16 2026

@author: 박승욱
"""

import os

from dash import Dash, dcc, html

from services.data_service import (
    load_series_dropdown_options,
    get_default_series_selection,
)

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

series_options = load_series_dropdown_options()
default_value = get_default_series_selection(default_codes=["KOSPI"], fallback_count=2)

deploy_branch = os.getenv("RENDER_GIT_BRANCH", "local")
deploy_commit = os.getenv("RENDER_GIT_COMMIT", "local")
deploy_service = os.getenv("RENDER_SERVICE_NAME", "local-service")

app.layout = html.Div(
    [
        html.H2("국내/미국 주식 Dashboard"),
        html.Div(
            f"deploy service: {deploy_service} | branch: {deploy_branch} | commit: {deploy_commit}",
            style={
                "fontSize": "12px",
                "color": "#666",
                "marginBottom": "10px",
            },
        ),
        dcc.Tabs(
            [
                get_domestic_monitor_layout(series_options, default_value),
                get_macro_tracker_layout(),
                get_regime_dashboard_layout(),
                get_interactive_chart_indices_layout(),
            ]
        ),
    ]
)

register_domestic_monitor_callbacks(app)
register_macro_tracker_callbacks(app)

if __name__ == "__main__":
    app.run(debug=True)