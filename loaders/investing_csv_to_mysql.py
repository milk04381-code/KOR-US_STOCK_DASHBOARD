# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 21:33:53 2026

@author: 박승욱
"""

# investing_csv_to_mysql.py
# Investing.com 형식 CSV 여러 개 -> MySQL

from pathlib import Path
import sys
import pandas as pd

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
# 기본 경로 / source
# --------------------------------------------------
BASE_DIR = PROJECT_ROOT
DATA_DIR = BASE_DIR / "data_files" / "investing"

SOURCE_CODE = "INVESTING"

DATE_COL = "Date"
VALUE_COL = "Price"


# --------------------------------------------------
# 처리 대상 설정
# - 같은 Investing 형식 CSV를 여러 개 순차 처리
# - file_name: 실제 파일명
# - series_info: DB series_meta 저장용 메타
# --------------------------------------------------
SERIES_CONFIG = [
    {
        "file_name": "KOSPI Historical Data_1981.05.01-1996.12.10.csv",
        "series_info": {
            "series_code": "KOSPI",
            "series_name": "KOSPI Index",
            "category_name": "국내주식",
            "frequency": "daily",
            "unit": "index",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "Investing.com CSV 적재",
            "source_series_code": "KOSPI",
            "source_series_name": "KOSPI Historical Data",
            "transform_code": "csv_price",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "index",
        },
    },
    {
        "file_name": "USD_KRW Historical Data_1984.06.01-2003.11.30.csv",
        "series_info": {
            "series_code": "USDKRW",
            "series_name": "USD/KRW",
            "category_name": "환율",
            "frequency": "daily",
            "unit": "krw_per_usd",
            "chart_type": "line",
            "default_axis": "right",
            "default_color": None,
            "is_recession_series": False,
            "notes": "Investing.com CSV 적재",
            "source_series_code": "USD/KRW",
            "source_series_name": "USD_KRW Historical Data",
            "transform_code": "csv_price",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "KRW per USD",
        },
    },
    {
        "file_name": "Crude Oil WTI Futures Historical Data_1983.04.04-2000.08.22.csv",
        "series_info": {
            "series_code": "WTI",
            "series_name": "Crude Oil WTI",
            "category_name": "원자재",
            "frequency": "daily",
            "unit": "usd_per_bbl",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "Investing.com CSV 적재",
            "source_series_code": "WTI",
            "source_series_name": "Crude Oil WTI Historical Data",
            "transform_code": "csv_price",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "USD per barrel",
        },
    },
]


# -----------------------------
# Investing 공통 parser
# -----------------------------
def read_investing_csv(csv_file_path):
    """
    Investing.com CSV 기준 필수 컬럼:
    - Date
    - Price

    원본 예:
    Date,Price,Open,High,Low,Vol.,Change %

    최종 반환:
    - columns: date, value
    """
    csv_path = Path(csv_file_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV 파일이 없습니다: {csv_path}")

    raw_df = pd.read_csv(csv_path)

    required_cols = {DATE_COL, VALUE_COL}
    missing_cols = required_cols - set(raw_df.columns)

    if missing_cols:
        raise ValueError(
            f"CSV 필수 컬럼이 없습니다: {sorted(missing_cols)} | 현재 컬럼: {list(raw_df.columns)}"
        )

    rows = []
    for _, row in raw_df.iterrows():
        date_raw = row[DATE_COL]
        value_raw = row[VALUE_COL]

        if pd.isna(date_raw):
            continue

        value_num = parse_value(value_raw)
        if value_num is None:
            continue

        rows.append(
            {
                "date": pd.to_datetime(date_raw).date(),
                "value": value_num,
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = df.drop_duplicates(subset=["date"])
    df = df.sort_values("date").reset_index(drop=True)

    return df


# -----------------------------
# 1개 파일 처리
# -----------------------------
def load_one_series(source_id, config):
    file_name = config["file_name"]
    series_info = config["series_info"]
    file_path = DATA_DIR / file_name

    data_df = read_investing_csv(file_path)

    if data_df.empty:
        print(f"[건너뜀] {series_info['series_code']} | 유효 데이터 없음")
        return

    payload = build_series_payload(
        series_info=series_info,
        source_id=source_id,
        data_df=data_df,
    )

    save_series_payload(payload)

    print(
        f"[완료] {payload['series_code']} | "
        f"source={SOURCE_CODE} | file={file_name} | 저장 건수: {len(data_df)}"
    )

    check_result(payload["series_code"], limit=5)


# -----------------------------
# 메인 실행
# -----------------------------
def main():
    source_id = get_source_id(SOURCE_CODE)

    for config in SERIES_CONFIG:
        series_code = config["series_info"]["series_code"]

        try:
            load_one_series(source_id=source_id, config=config)
        except Exception as e:
            print(f"[실패] {series_code} | {e}")


if __name__ == "__main__":
    main()