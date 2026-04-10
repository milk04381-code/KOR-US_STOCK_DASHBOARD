# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 18:03:25 2026

@author: 박승욱
"""

# tabs/common.py

from dash import dcc, html


def make_placeholder_tab(label: str, message: str):
    return dcc.Tab(
        label=label,
        children=[
            html.Div(
                message,
                style={
                    "width": "88%",
                    "margin": "24px auto",
                    "fontSize": "16px",
                    "color": "#555",
                },
            )
        ],
    )