# -*- coding: utf-8 -*-
"""
Created on Sat Apr  4 16:07:41 2026

@author: 박승욱
"""

# e_nara_api_to_mysql.py

import json
import os
import sys
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loading_common import (
    parse_value,
    get_source_id,
    build_series_payload,
    save_series_payload,
    check_result,
)

# --------------------------------------------------
# 환경설정
# --------------------------------------------------
BASE_DIR = PROJECT_ROOT
load_dotenv(BASE_DIR / ".env")

ENARA_API_KEY = os.getenv("ENARA_API_KEY", "").strip()

SOURCE_CODE = "ENARA"
BASE_URL = "https://www.index.go.kr/unity/openApi/sttsJsonViewer.do"

IX_CODE = "1086"
STATS_CODE = "108601"

TARGET_FILTER = {
    "주기코드": "M",
    "항목코드": "T02",
    "분류1코드": "001",
}

SERIES_INFO = {
    "series_code": "KOSPI_FOREIGN_OWNERSHIP_RATIO",
    "series_name": "코스피 외국인 지분율",
    "category_name": "국내주식",
    "frequency": "monthly",
    "unit": "%",
    "chart_type": "line",
    "default_axis": "right",
    "default_color": None,
    "is_recession_series": False,
    "notes": (
        "e-나라지표 OpenAPI 적재 | "
        "지표코드=1086 | 통계표코드=108601 | "
        "주기코드=M | 항목코드=T02(시가총액대비) | "
        "분류1코드=001(유가증권시장)"
    ),
    "source_series_code": "1086:108601:M:T02:001",
    "source_series_name": "외국인 증권투자 현황 | 유가증권시장 | 시가총액대비",
    "transform_code": "enara_api",
    "transform_name": "RAW",
    "is_transformed": False,
    "source_unit": "%",
}

REQUEST_TIMEOUT_SECONDS = 30


# --------------------------------------------------
# 공통 유틸
# --------------------------------------------------
def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_yyyymm_to_date(value):
    raw = clean_str(value)

    if len(raw) != 6 or not raw.isdigit():
        return None

    ts = pd.to_datetime(raw + "01", format="%Y%m%d", errors="coerce")

    if pd.isna(ts):
        return None

    return ts.date()


# --------------------------------------------------
# API 호출
# --------------------------------------------------
def build_api_url():
    if not ENARA_API_KEY:
        raise ValueError(".env 파일에 ENARA_API_KEY가 없습니다.")

    return (
        f"{BASE_URL}"
        f"?idntfcId={ENARA_API_KEY}"
        f"&ixCode={IX_CODE}"
        f"&statsCode={STATS_CODE}"
    )


def fetch_enara_json():
    api_url = build_api_url()

    response = requests.get(api_url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    text = response.text.strip()
    if not text:
        raise ValueError("e-나라지표 응답이 비어 있습니다.")

    try:
        data = response.json()
    except ValueError:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"응답이 JSON 형식이 아닙니다. 응답 앞부분: {text[:300]}"
            ) from e

    if not isinstance(data, list):
        raise ValueError(f"예상과 다른 JSON 구조입니다. type={type(data)}")

    return data


def filter_target_rows(rows):
    result = []

    for row in rows:
        cycle_code = clean_str(row.get("주기코드"))
        item_code = clean_str(row.get("항목코드"))
        class1_code = clean_str(row.get("분류1코드"))

        if cycle_code != TARGET_FILTER["주기코드"]:
            continue
        if item_code != TARGET_FILTER["항목코드"]:
            continue
        if class1_code != TARGET_FILTER["분류1코드"]:
            continue

        result.append(row)

    return result


def build_data_df(rows):
    result_rows = []

    for row in rows:
        date_value = parse_yyyymm_to_date(row.get("시점"))
        value_num = parse_value(row.get("값"))

        if date_value is None:
            continue
        if value_num is None:
            continue

        result_rows.append(
            {
                "date": date_value,
                "value": value_num,
            }
        )

    df = pd.DataFrame(result_rows)

    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)

    return df


def build_series_info_with_response_meta(base_series_info, rows):
    if not rows:
        return dict(base_series_info)

    first = rows[0]
    series_info = dict(base_series_info)

    indicator_name = clean_str(first.get("지표이름"))
    stat_name = clean_str(first.get("통계표명"))
    item_name = clean_str(first.get("항목이름"))
    class1_name = clean_str(first.get("분류1이름"))
    unit_name = clean_str(first.get("단위"))
    update_date = clean_str(first.get("갱신일"))
    source_text = clean_str(first.get("출처"))

    if indicator_name and stat_name and item_name and class1_name:
        series_info["source_series_name"] = (
            f"{indicator_name} | {stat_name} | {class1_name} | {item_name}"
        )

    if "%" in unit_name:
        series_info["source_unit"] = "%"

    notes_parts = [series_info["notes"]]

    if update_date:
        notes_parts.append(f"갱신일={update_date}")

    if source_text:
        notes_parts.append(f"출처={source_text}")

    series_info["notes"] = " | ".join(notes_parts)

    return series_info


# --------------------------------------------------
# main
# --------------------------------------------------
def main():
    source_id = get_source_id(SOURCE_CODE)

    all_rows = fetch_enara_json()
    target_rows = filter_target_rows(all_rows)

    if not target_rows:
        raise ValueError("조건에 맞는 row가 없습니다.")

    data_df = build_data_df(target_rows)

    if data_df.empty:
        raise ValueError("조건에 맞는 row는 있었지만 저장할 유효 데이터가 없습니다.")

    series_info = build_series_info_with_response_meta(SERIES_INFO, target_rows)

    payload = build_series_payload(
        series_info=series_info,
        source_id=source_id,
        data_df=data_df,
    )
    save_series_payload(payload)

    print(
        f"[완료] {payload['series_code']} | "
        f"source={SOURCE_CODE} | "
        f"저장 건수={len(data_df)} | "
        f"기간={data_df['date'].min()} ~ {data_df['date'].max()}"
    )

    check_result(payload["series_code"], limit=10)


if __name__ == "__main__":
    main()