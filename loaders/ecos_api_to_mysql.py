# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 21:21:26 2026

@author: 박승욱
"""

# ecos_api_to_mysql.py

#목표:
#- 한국은행 ECOS API에서 수입물가지수 데이터를 조회
#- 공통 형태(date, value)로 변환
#- loading_common.py의 공통 저장 구조를 그대로 재사용
#- Open API 과도 접속 우려를 고려해 안전장치 포함

import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loading_common import (
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

ECOS_API_KEY = os.getenv("ECOS_API_KEY", "").strip()

SOURCE_CODE = "ECOS"
BASE_URL = "https://ecos.bok.or.kr/api/StatisticSearch"

# --------------------------------------------------
# 적재 대상: 한국 수입물가지수
# 사용자가 제공한 값 기준
# --------------------------------------------------
SERIES_INFO = {
    "series_code": "KOR_IMPORT_PRICE_INDEX",
    "series_name": "한국 수입물가지수",
    "category_name": "국내 매크로",
    "frequency": "monthly",
    "unit": "index",
    "chart_type": "line",
    "default_axis": "left",
    "default_color": None,
    "is_recession_series": False,
    "notes": (
        "ECOS StatisticSearch 적재 | "
        "STAT_CODE=401Y015 | ITEM_CODE1=*AA(총지수) | ITEM_CODE2=W(원화기준)"
    ),
    "source_series_code": "401Y015:*AA:W",
    "source_series_name": "수입물가지수(기본분류) 총지수 원화기준",
    "transform_code": "ecos_api",
    "transform_name": "RAW",
    "is_transformed": False,
    "source_unit": "2020=100",
}

STAT_CODE = "401Y015"
CYCLE = "M"
START_TIME = "197101"
END_TIME = "202602"
ITEM_CODE1 = "*AA"
ITEM_CODE2 = "W"
ITEM_CODE3 = "?"
ITEM_CODE4 = "?"

# --------------------------------------------------
# 안전장치 설정
# - row 수와 호출횟수 사이 균형을 위해 400 사용
# - 전체 662개월이면 대략 2회 호출
# --------------------------------------------------
ROWS_PER_CALL = 400
REQUEST_SLEEP_SECONDS = 0.4
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 4
BACKOFF_SECONDS = 1.5


# --------------------------------------------------
# 공통 유틸
# --------------------------------------------------
def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_month_to_date(text):
    """
    ECOS TIME 예: 197101 -> 1971-01-01
    """
    raw = clean_str(text)
    if len(raw) != 6 or not raw.isdigit():
        return None

    ts = pd.to_datetime(raw + "01", format="%Y%m%d", errors="coerce")
    if pd.isna(ts):
        return None

    return ts.date()


def build_ecos_url(start_row, end_row):
    """
    ECOS StatisticSearch 경로형 URL 생성
    형식:
    /api/StatisticSearch/{API_KEY}/json/kr/{start_row}/{end_row}/{stat_code}/{cycle}/{start}/{end}/{item1}/{item2}/{item3}/{item4}
    """
    return (
        f"{BASE_URL}/"
        f"{ECOS_API_KEY}/json/kr/"
        f"{start_row}/{end_row}/"
        f"{STAT_CODE}/{CYCLE}/{START_TIME}/{END_TIME}/"
        f"{ITEM_CODE1}/{ITEM_CODE2}/{ITEM_CODE3}/{ITEM_CODE4}"
    )


def request_with_retry(url):
    """
    429 / 5xx / timeout 등에 대해 재시도
    """
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)

            if response.status_code == 429:
                raise requests.HTTPError(
                    f"HTTP 429 Too Many Requests | url={url}",
                    response=response,
                )

            if 500 <= response.status_code < 600:
                raise requests.HTTPError(
                    f"HTTP {response.status_code} Server Error | url={url}",
                    response=response,
                )

            response.raise_for_status()
            return response

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last_error = e

            if attempt == MAX_RETRIES:
                break

            sleep_seconds = BACKOFF_SECONDS * attempt
            print(f"[재시도] attempt={attempt}/{MAX_RETRIES} | sleep={sleep_seconds:.1f}s | {e}")
            time.sleep(sleep_seconds)

    raise last_error


def fetch_one_page(start_row, end_row):
    """
    ECOS 한 페이지 조회
    """
    url = build_ecos_url(start_row, end_row)
    response = request_with_retry(url)

    try:
        data = response.json()
    except ValueError as e:
        raise ValueError(f"ECOS 응답이 JSON이 아닙니다. 응답 앞부분: {response.text[:300]}") from e

    # 정상 응답 예: {"StatisticSearch":{"list_total_count":...,"row":[...]}}
    block = data.get("StatisticSearch")

    if block is None:
        raise ValueError(f"ECOS 응답에 'StatisticSearch' 키가 없습니다. 응답={str(data)[:500]}")

    rows = block.get("row", [])
    if rows is None:
        rows = []

    return rows


def fetch_all_pages():
    """
    안전장치:
    - 한 번에 1000행을 크게 요청하지 않음
    - 400행씩 페이지 조회
    - 각 호출 사이 짧은 sleep
    - 마지막 페이지가 page size보다 작으면 종료
    """
    all_rows = []
    start_row = 1

    while True:
        end_row = start_row + ROWS_PER_CALL - 1
        rows = fetch_one_page(start_row, end_row)

        row_count = len(rows)
        print(f"[조회] rows {start_row}~{end_row} | 수신 {row_count}건")

        if row_count == 0:
            break

        all_rows.extend(rows)

        # 마지막 페이지면 종료
        if row_count < ROWS_PER_CALL:
            break

        start_row = end_row + 1
        time.sleep(REQUEST_SLEEP_SECONDS)

    return all_rows


def normalize_ecos_rows(rows):
    """
    ECOS row -> 공통 컬럼(date, value)
    참고 필드:
    - TIME
    - DATA_VALUE
    """
    if not rows:
        return pd.DataFrame(columns=["date", "value"])

    result_rows = []

    for row in rows:
        time_raw = clean_str(row.get("TIME"))
        value_raw = clean_str(row.get("DATA_VALUE")).replace(",", "")

        date_value = parse_month_to_date(time_raw)
        value_num = pd.to_numeric(value_raw, errors="coerce")

        if date_value is None:
            continue
        if pd.isna(value_num):
            continue

        result_rows.append(
            {
                "date": date_value,
                "value": float(value_num),
            }
        )

    df = pd.DataFrame(result_rows)

    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)
    return df


def extract_response_meta(rows):
    """
    응답 row에서 표시용 메타를 보정
    """
    meta = {
        "stat_name": "",
        "item_name1": "",
        "item_name2": "",
        "unit_name": "",
    }

    if not rows:
        return meta

    first = rows[0]

    meta["stat_name"] = clean_str(first.get("STAT_NAME"))
    meta["item_name1"] = clean_str(first.get("ITEM_NAME1"))
    meta["item_name2"] = clean_str(first.get("ITEM_NAME2"))
    meta["unit_name"] = clean_str(first.get("UNIT_NAME"))

    return meta


def build_series_info_with_response_meta(base_series_info, response_meta):
    """
    응답 메타를 이용해 source_series_name / source_unit 보정
    """
    series_info = dict(base_series_info)

    stat_name = clean_str(response_meta.get("stat_name"))
    item_name1 = clean_str(response_meta.get("item_name1"))
    item_name2 = clean_str(response_meta.get("item_name2"))
    unit_name = clean_str(response_meta.get("unit_name"))

    if stat_name:
        display_name_parts = [stat_name]
        if item_name1:
            display_name_parts.append(item_name1)
        if item_name2:
            display_name_parts.append(item_name2)
        series_info["source_series_name"] = " | ".join(display_name_parts)

    if unit_name:
        series_info["source_unit"] = unit_name

    return series_info


# --------------------------------------------------
# 실행
# --------------------------------------------------
def main():
    if not ECOS_API_KEY:
        raise ValueError(".env 파일에 ECOS_API_KEY가 없습니다.")

    source_id = get_source_id(SOURCE_CODE)

    rows = fetch_all_pages()
    if not rows:
        raise ValueError("ECOS 응답에 유효 row가 없습니다.")

    response_meta = extract_response_meta(rows)
    data_df = normalize_ecos_rows(rows)

    if data_df.empty:
        raise ValueError("ECOS 응답 row는 있었지만 저장할 유효 데이터(date, value)가 없습니다.")

    series_info = build_series_info_with_response_meta(SERIES_INFO, response_meta)

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