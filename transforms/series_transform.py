# -*- coding: utf-8 -*-
"""
Created on Sun Apr  5 02:16:53 2026

@author: 박승욱
"""

# 각 시계열 데이터를 변환해주는 파일 
# eg, YoY, MoM, 3MMA, Z-score

import pandas as pd


def build_yoy_df(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    입력:
        data_df: columns = [date, value]

    출력:
        YoY DataFrame (date, value)
    """

    if data_df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = data_df.copy()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # YoY 계산 (12개월 전 대비)
    df["yoy"] = df["value"].pct_change(periods=12) * 100

    yoy_df = df[["date", "yoy"]].rename(columns={"yoy": "value"})
    yoy_df = yoy_df.dropna(subset=["value"])

    yoy_df["date"] = yoy_df["date"].dt.date

    return yoy_df