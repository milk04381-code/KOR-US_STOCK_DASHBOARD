# -*- coding: utf-8 -*-
"""
Created on Sat Apr  4 14:40:48 2026

@author: 박승욱
"""

# msci_xlsx_to_mysql.py

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
# 기본 설정
# --------------------------------------------------
BASE_DIR = PROJECT_ROOT
DATA_DIR = BASE_DIR / "data_files" / "msci"

XLSX_FILE_PATH = DATA_DIR / "892400 - MSCI ACWI Index  - FULL - 1998-12-31 - 2026-04-01  - Daily.xlsx"
SHEET_NAME = "Performance Data"

SOURCE_CODE = "MSCI"

# 실제 파일 구조 기준 확정
HEADER_ROW = 5
DATE_COL = "Date"
VALUE_COL = "MSCI ACWI Index"

# --------------------------------------------------
# series 설정
# --------------------------------------------------
SERIES_INFO = {
    "series_code": "MSCI_ACWI",
    "series_name": "MSCI AC World Index",
    "category_name": "글로벌 주식",
    "frequency": "daily",
    "unit": "index",
    "chart_type": "line",
    "default_axis": "left",
    "default_color": None,
    "is_recession_series": False,
    "notes": "MSCI ACWI XLSX 적재",
    "source_series_code": "MSCI_ACWI",
    "source_series_name": "MSCI ACWI Index",
    "transform_code": "xlsx_value",
    "transform_name": "RAW",
    "is_transformed": False,
    "source_unit": "index",
}

# --------------------------------------------------
# XLSX 읽기
# --------------------------------------------------
def read_msci_xlsx(xlsx_file_path, sheet_name):
    xlsx_path = Path(xlsx_file_path)

    if not xlsx_path.exists():
        raise FileNotFoundError(f"XLSX 파일이 없습니다: {xlsx_path}")

    # 핵심: header=5
    raw_df = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        header=HEADER_ROW
    )

    required_cols = {DATE_COL, VALUE_COL}
    missing_cols = required_cols - set(raw_df.columns)

    if missing_cols:
        raise ValueError(
            f"필수 컬럼 없음: {missing_cols} | 현재 컬럼: {list(raw_df.columns)}"
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


# --------------------------------------------------
# main
# --------------------------------------------------
def main():
    source_id = get_source_id(SOURCE_CODE)

    data_df = read_msci_xlsx(
        xlsx_file_path=XLSX_FILE_PATH,
        sheet_name=SHEET_NAME,
    )

    if data_df.empty:
        raise ValueError("유효 데이터 없음")

    payload = build_series_payload(
        series_info=SERIES_INFO,
        source_id=source_id,
        data_df=data_df,
    )

    save_series_payload(payload)

    print(
        f"[완료] {payload['series_code']} | "
        f"source={SOURCE_CODE} | 저장 건수: {len(data_df)}"
    )

    check_result(payload["series_code"], limit=10)


if __name__ == "__main__":
    main()