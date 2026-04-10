# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 21:34:19 2026

@author: 박승욱
"""

from pathlib import Path
import sys
import pandas as pd

# --------------------------------------------------
# 프로젝트 루트 경로 추가
# 현재 파일: project_root/loaders/file/finra_xlsx_to_mysql.py
# 프로젝트 루트: parents[2]
# --------------------------------------------------
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
from transforms.series_transform import build_yoy_df


# --------------------------------------------------
# 사용자 설정
# --------------------------------------------------
BASE_DIR = PROJECT_ROOT

DATA_DIR = BASE_DIR / "data_files" / "finra"
XLSX_FILE_PATH = DATA_DIR / "margin-statistics_1997.01-2026.02.xlsx"
SHEET_NAME = "Customer Margin Balances"
SOURCE_CODE = "FINRA"

RAW_SERIES_INFO = {
    "series_code": "FINRA_MARGIN_DEBT",
    "series_name": "Debit Balances in Customers' Securities Margin Accounts",
    "category_name": "신용/레버리지",
    "frequency": "monthly",
    "unit": "usd_mn",
    "chart_type": "line",
    "default_axis": "left",
    "default_color": None,
    "is_recession_series": False,
    "notes": "FINRA XLSX 적재",
    "source_series_code": "Debit Balances in Customers' Securities Margin Accounts",
    "source_series_name": "Debit Balances in Customers' Securities Margin Accounts",
    "transform_code": "xlsx_value",
    "transform_name": "RAW",
    "is_transformed": False,
    "source_unit": "USD million",
}

YOY_SERIES_INFO = {
    "series_code": "FINRA_MARGIN_DEBT_YOY",
    "series_name": "Debit Balances in Customers' Securities Margin Accounts (YoY)",
    "category_name": "신용/레버리지",
    "frequency": "monthly",
    "unit": "% YoY",
    "chart_type": "bar",
    "default_axis": "left",
    "default_color": None,
    "is_recession_series": False,
    "notes": "FINRA XLSX 적재 후 pandas YoY 변환",
    "source_series_code": "FINRA_MARGIN_DEBT",
    "source_series_name": "Debit Balances in Customers' Securities Margin Accounts",
    "transform_code": "yoy",
    "transform_name": "YOY",
    "is_transformed": True,
    "source_unit": "USD million",
}

DATE_COL = "Year-Month"
VALUE_COL = "Debit Balances in Customers' Securities Margin Accounts"


def read_finra_xlsx(xlsx_file_path, sheet_name):
    xlsx_path = Path(xlsx_file_path)

    if not xlsx_path.exists():
        raise FileNotFoundError(f"XLSX 파일이 없습니다: {xlsx_path}")

    raw_df = pd.read_excel(xlsx_path, sheet_name=sheet_name)

    required_cols = {DATE_COL, VALUE_COL}
    missing_cols = required_cols - set(raw_df.columns)

    if missing_cols:
        raise ValueError(
            f"XLSX 필수 컬럼이 없습니다: {sorted(missing_cols)} | 현재 컬럼: {list(raw_df.columns)}"
        )

    rows = []
    for _, row in raw_df.iterrows():
        ym_raw = row[DATE_COL]
        value_raw = row[VALUE_COL]

        if pd.isna(ym_raw):
            continue

        value_num = parse_value(value_raw)
        if value_num is None:
            continue

        rows.append(
            {
                "date": pd.to_datetime(str(ym_raw) + "-01").date(),
                "value": value_num,
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)

    return df


def save_one_series(source_id, series_info, data_df):
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
        f"source={SOURCE_CODE} | 저장 건수: {len(data_df)}"
    )

    check_result(payload["series_code"], limit=5)


def main():
    source_id = get_source_id(SOURCE_CODE)

    data_df = read_finra_xlsx(
        xlsx_file_path=XLSX_FILE_PATH,
        sheet_name=SHEET_NAME,
    )
    if data_df.empty:
        raise ValueError("XLSX에서 저장할 유효 데이터가 없습니다.")

    # -------------------------------
    # RAW 저장
    # -------------------------------
    save_one_series(
        source_id=source_id,
        series_info=RAW_SERIES_INFO,
        data_df=data_df,
    )

    # -------------------------------
    # YoY 생성 및 저장
    # -------------------------------
    yoy_df = build_yoy_df(data_df)

    if yoy_df.empty:
        print("[건너뜀] FINRA_MARGIN_DEBT_YOY | YoY 계산 결과 없음")
    else:
        save_one_series(
            source_id=source_id,
            series_info=YOY_SERIES_INFO,
            data_df=yoy_df,
        )


if __name__ == "__main__":
    main()