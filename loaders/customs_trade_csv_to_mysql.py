# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 02:25:38 2026

@author: 박승욱
"""

#customs_trade_csv_to_mysql.py

# 관세청 수출입 총괄 CSV -> MySQL
#- 기간, 수출 금액, 수입 금액, 무역수지 사용
#- 단위: 천 달러
#- 총계 행 제외
#- 추가: 한국 수출금액 YoY 별도 저장

from pathlib import Path
import sys

import pandas as pd

# --------------------------------------------------
# 프로젝트 루트 경로 추가
# 현재 파일: project_root/loaders/file/customs_trade_csv_to_mysql.py
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
# 기본 경로 / source
# --------------------------------------------------
BASE_DIR = PROJECT_ROOT
DATA_DIR = BASE_DIR / "data_files" / "customs"

CSV_FILE_PATH = DATA_DIR / "수출입 총괄_20260403.csv"

SOURCE_CODE = "CUSTOMS"

# --------------------------------------------------
# CSV 컬럼명
# --------------------------------------------------
DATE_COL = "기간"
EXPORT_COL = "수출 금액"
IMPORT_COL = "수입 금액"
BALANCE_COL = "무역수지"

# --------------------------------------------------
# 저장 대상 시리즈 설정 (RAW)
# --------------------------------------------------
SERIES_CONFIG = [
    {
        "value_col": EXPORT_COL,
        "series_info": {
            "series_code": "KOR_EXPORT_VALUE",
            "series_name": "한국 수출금액",
            "category_name": "수출입 총괄",
            "frequency": "monthly",
            "unit": "usd_thousand",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "관세청 수출입 총괄 CSV 적재",
            "source_series_code": "CUSTOMS_EXPORT_VALUE",
            "source_series_name": "수출 금액",
            "transform_code": "csv_value",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "천 달러",
        },
    },
    {
        "value_col": IMPORT_COL,
        "series_info": {
            "series_code": "KOR_IMPORT_VALUE",
            "series_name": "한국 수입금액",
            "category_name": "수출입 총괄",
            "frequency": "monthly",
            "unit": "usd_thousand",
            "chart_type": "line",
            "default_axis": "left",
            "default_color": None,
            "is_recession_series": False,
            "notes": "관세청 수출입 총괄 CSV 적재",
            "source_series_code": "CUSTOMS_IMPORT_VALUE",
            "source_series_name": "수입 금액",
            "transform_code": "csv_value",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "천 달러",
        },
    },
    {
        "value_col": BALANCE_COL,
        "series_info": {
            "series_code": "KOR_TRADE_BALANCE",
            "series_name": "한국 무역수지",
            "category_name": "수출입 총괄",
            "frequency": "monthly",
            "unit": "usd_thousand",
            "chart_type": "bar",
            "default_axis": "right",
            "default_color": None,
            "is_recession_series": False,
            "notes": "관세청 수출입 총괄 CSV 적재",
            "source_series_code": "CUSTOMS_TRADE_BALANCE",
            "source_series_name": "무역수지",
            "transform_code": "csv_value",
            "transform_name": "RAW",
            "is_transformed": False,
            "source_unit": "천 달러",
        },
    },
]

# --------------------------------------------------
# 추가 저장 대상 시리즈 설정 (YoY)
# --------------------------------------------------
EXPORT_YOY_SERIES_INFO = {
    "series_code": "KOR_EXPORT_VALUE_YOY",
    "series_name": "한국 수출금액 (YoY)",
    "category_name": "수출입 총괄",
    "frequency": "monthly",
    "unit": "% YoY",
    "chart_type": "bar",
    "default_axis": "left",
    "default_color": None,
    "is_recession_series": False,
    "notes": "관세청 수출입 총괄 CSV 적재 후 pandas YoY 변환",
    "source_series_code": "KOR_EXPORT_VALUE",
    "source_series_name": "한국 수출금액",
    "transform_code": "yoy",
    "transform_name": "YOY",
    "is_transformed": True,
    "source_unit": "천 달러",
}


# --------------------------------------------------
# 공통 parser
# --------------------------------------------------
def parse_period_to_date(period_text):
    """
    예:
    2000-01 -> 2000-01-01
    """
    text = str(period_text).strip()

    if text == "" or text == "총계":
        return None

    ts = pd.to_datetime(text + "-01", errors="coerce")

    if pd.isna(ts):
        return None

    return ts.date()


def read_customs_csv(csv_file_path):
    """
    업로드 CSV 전체를 읽고 정리
    반환 컬럼:
    - date
    - 수출 금액
    - 수입 금액
    - 무역수지
    """
    csv_path = Path(csv_file_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV 파일이 없습니다: {csv_path}")

    raw_df = pd.read_csv(csv_path, encoding="utf-8-sig")

    required_cols = {DATE_COL, EXPORT_COL, IMPORT_COL, BALANCE_COL}
    missing_cols = required_cols - set(raw_df.columns)

    if missing_cols:
        raise ValueError(
            f"CSV 필수 컬럼이 없습니다: {sorted(missing_cols)} | 현재 컬럼: {list(raw_df.columns)}"
        )

    rows = []

    for _, row in raw_df.iterrows():
        period_raw = row[DATE_COL]
        date_value = parse_period_to_date(period_raw)

        if date_value is None:
            continue

        export_value = parse_value(row[EXPORT_COL])
        import_value = parse_value(row[IMPORT_COL])
        balance_value = parse_value(row[BALANCE_COL])

        rows.append(
            {
                "date": date_value,
                EXPORT_COL: export_value,
                IMPORT_COL: import_value,
                BALANCE_COL: balance_value,
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=["date", EXPORT_COL, IMPORT_COL, BALANCE_COL])

    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)

    return df


def build_one_series_df(raw_df, value_col):
    """
    wide 형태 -> 공통 저장용(date, value)
    """
    if raw_df.empty:
        return pd.DataFrame(columns=["date", "value"])

    temp = raw_df[["date", value_col]].copy()
    temp = temp.rename(columns={value_col: "value"})

    temp["value"] = pd.to_numeric(temp["value"], errors="coerce")
    temp = temp.dropna(subset=["date", "value"])

    if temp.empty:
        return pd.DataFrame(columns=["date", "value"])

    temp = temp.drop_duplicates(subset=["date"], keep="last")
    temp = temp.sort_values("date").reset_index(drop=True)

    return temp


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
        f"source={SOURCE_CODE} | "
        f"저장 건수: {len(data_df)}"
    )

    check_result(payload["series_code"], limit=5)


# --------------------------------------------------
# main
# --------------------------------------------------
def main():
    source_id = get_source_id(SOURCE_CODE)

    raw_df = read_customs_csv(CSV_FILE_PATH)

    if raw_df.empty:
        raise ValueError("CSV에서 저장할 유효 데이터가 없습니다.")

    # -------------------------------
    # RAW 3개 시리즈 저장
    # -------------------------------
    export_raw_df = pd.DataFrame(columns=["date", "value"])

    for config in SERIES_CONFIG:
        series_code = config["series_info"]["series_code"]

        try:
            data_df = build_one_series_df(raw_df, config["value_col"])

            if series_code == "KOR_EXPORT_VALUE":
                export_raw_df = data_df.copy()

            save_one_series(
                source_id=source_id,
                series_info=config["series_info"],
                data_df=data_df,
            )
        except Exception as e:
            print(f"[실패] {series_code} | {e}")

    # -------------------------------
    # KOR_EXPORT_VALUE YoY 저장
    # -------------------------------
    try:
        export_yoy_df = build_yoy_df(export_raw_df)

        if export_yoy_df.empty:
            print("[건너뜀] KOR_EXPORT_VALUE_YOY | YoY 계산 결과 없음")
        else:
            save_one_series(
                source_id=source_id,
                series_info=EXPORT_YOY_SERIES_INFO,
                data_df=export_yoy_df,
            )
    except Exception as e:
        print(f"[실패] KOR_EXPORT_VALUE_YOY | {e}")


if __name__ == "__main__":
    main()