# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 18:14:16 2026

@author: 박승욱
"""

# oecd_api_to_mysql.py
# Day10: OECD API ingestion

# 구조:
# fetch → transform → meta_params + data_df → save
# (기존 csv_common 구조 100% 재사용)
# OECD 공식 Data query -> CSV 응답 -> pandas DataFrame
# -> 공통 형태(date, value) -> build_series_payload() -> save_series_payload()
#
# 주의:
# 1) OECD 공식 방식 사용:
#    - Data Explorer Developer API에서 복사한 Data query / Structure query 사용
#    - format=csvfilewithlabels 로 CSV 응답 사용
# 2) 기존 공통 저장 구조(loading_common) 그대로 재사용


from io import StringIO
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loading_common import (
    get_source_id,
    build_series_payload,
    save_series_payload,
    check_result,
)

SOURCE_CODE = "OECD"

# --------------------------------------------------
# OECD 공식 query (사용자 제공)
# --------------------------------------------------
CLI_DATA_QUERY = (
    "https://sdmx.oecd.org/public/rest/data/"
    "OECD.SDD.STES,DSD_STES@DF_CLI,4.1/"
    "KOR+G20+AUS+CAN+FRA+DEU+ITA+JPN+MEX+TUR+GBR+USA+BRA+CHN+IND+IDN+ZAF."
    "M.LI...AA...H"
    "?startPeriod=1955-01&dimensionAtObservation=AllDimensions"
)

CLI_STRUCTURE_QUERY = (
    "https://sdmx.oecd.org/public/rest/dataflow/"
    "OECD.SDD.STES/DSD_STES@DF_CLI/4.1?references=all"
)

CPI_DATA_QUERY = (
    "https://sdmx.oecd.org/public/rest/data/"
    "OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,1.0/"
    "OECD+G20.M.N.CPI.._T.N.GY"
    "?startPeriod=1970-01&dimensionAtObservation=AllDimensions"
)

CPI_STRUCTURE_QUERY = (
    "https://sdmx.oecd.org/public/rest/dataflow/"
    "OECD.SDD.TPS/DSD_PRICES@DF_PRICES_ALL/1.0?references=all"
)

# Diffusion 계산용 국가 목록
# 사용자가 지정한 reference area 기준
DIFFUSION_COUNTRIES = [
    "AUS", "CAN", "FRA", "DEU", "ITA", "JPN", "KOR", "MEX",
    "TUR", "GBR", "USA", "BRA", "CHN", "IND", "IDN", "ZAF",
]


# --------------------------------------------------
# 공통 유틸
# --------------------------------------------------
def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def append_csv_format(api_url):
    """
    OECD 공식 문서 기준:
    CSV 응답은 format=csvfile 또는 format=csvfilewithlabels 사용
    여기서는 labels 포함 버전 사용
    """
    parsed = urlparse(api_url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)

    # 기존 format 제거 후 csvfilewithlabels 강제
    query_pairs = [(k, v) for k, v in query_pairs if k.lower() != "format"]
    query_pairs.append(("format", "csvfilewithlabels"))

    new_query = urlencode(query_pairs)
    return urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )


def fetch_oecd_csv(api_url):
    url = append_csv_format(api_url)

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    return pd.read_csv(StringIO(response.text))


def fetch_oecd_structure_text(structure_url):
    """
    Structure query는 주로 확인/디버깅용.
    현재 적재 로직은 data query CSV만으로 동작하지만,
    공식 query를 함께 보관하고 즉시 확인할 수 있게 함.
    """
    response = requests.get(structure_url, timeout=60)
    response.raise_for_status()
    return response.text


def find_first_existing_column(df, candidates, required=True):
    existing = {str(col).strip().lower(): col for col in df.columns}

    for cand in candidates:
        key = cand.strip().lower()
        if key in existing:
            return existing[key]

    if required:
        raise ValueError(
            f"필수 컬럼을 찾지 못했습니다. candidates={candidates} | 현재 컬럼={list(df.columns)}"
        )

    return None


def parse_time_to_date(value):
    """
    OECD TIME_PERIOD 예:
    2024-01, 2024-01-01, 2024
    월간 series는 최종적으로 date 타입으로 저장
    """
    if pd.isna(value):
        return None

    text = clean_str(value)
    if not text:
        return None

    ts = pd.to_datetime(text, errors="coerce")
    if pd.isna(ts):
        return None

    return ts.date()


def standardize_oecd_dataframe(raw_df):
    """
    OECD csvfilewithlabels 응답 -> 공통 컬럼
    결과 컬럼:
    - country_code
    - country_name
    - date
    - value
    """
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=["country_code", "country_name", "date", "value"])

    df = raw_df.copy()

    country_code_col = find_first_existing_column(
        df,
        ["REF_AREA", "LOCATION"],
        required=True,
    )
    country_name_col = find_first_existing_column(
        df,
        ["REF_AREA_LABEL", "LOCATION_LABEL", "Reference area"],
        required=False,
    )
    date_col = find_first_existing_column(
        df,
        ["TIME_PERIOD", "TIME"],
        required=True,
    )
    value_col = find_first_existing_column(
        df,
        ["OBS_VALUE", "Value", "value"],
        required=True,
    )

    result = pd.DataFrame()
    result["country_code"] = df[country_code_col].astype(str).str.strip()

    if country_name_col is None:
        result["country_name"] = result["country_code"]
    else:
        result["country_name"] = df[country_name_col].astype(str).str.strip()

    result["date"] = df[date_col].apply(parse_time_to_date)
    result["value"] = pd.to_numeric(df[value_col], errors="coerce")

    result = result.dropna(subset=["country_code", "date", "value"])
    result = result.drop_duplicates(subset=["country_code", "date"], keep="last")
    result = result.sort_values(["country_code", "date"]).reset_index(drop=True)

    return result


def build_country_series_df(df, country_code):
    temp = df[df["country_code"] == country_code][["date", "value"]].copy()

    if temp.empty:
        return pd.DataFrame(columns=["date", "value"])

    temp = temp.drop_duplicates(subset=["date"], keep="last")
    temp = temp.sort_values("date").reset_index(drop=True)
    return temp


def compute_cli_diffusion_g20(df):
    """
    계산식:
    (A - B) / 전체국가 * 100

    A = 전월 대비 상승 국가 수
    B = 전월 대비 하락 국가 수
    전체국가 = 해당 월의 유효 국가 수

    주의:
    - G20 aggregate 행은 제외
    - 사용자가 준 reference area 국가 목록만 사용
    """
    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    temp = df[df["country_code"].isin(DIFFUSION_COUNTRIES)].copy()
    temp = temp.sort_values(["country_code", "date"]).reset_index(drop=True)

    if temp.empty:
        return pd.DataFrame(columns=["date", "value"])

    temp["prev_value"] = temp.groupby("country_code")["value"].shift(1)
    temp["change"] = temp["value"] - temp["prev_value"]
    temp = temp.dropna(subset=["prev_value"]).copy()

    if temp.empty:
        return pd.DataFrame(columns=["date", "value"])

    rows = []

    for date_value, group in temp.groupby("date"):
        up_count = int((group["change"] > 0).sum())
        down_count = int((group["change"] < 0).sum())
        total_count = int(group["country_code"].nunique())

        if total_count == 0:
            continue

        diffusion_value = (up_count - down_count) / total_count * 100.0

        rows.append(
            {
                "date": date_value,
                "value": diffusion_value,
            }
        )

    result = pd.DataFrame(rows)

    if result.empty:
        return pd.DataFrame(columns=["date", "value"])

    result = result.drop_duplicates(subset=["date"], keep="last")
    result = result.sort_values("date").reset_index(drop=True)
    return result


# --------------------------------------------------
# series_info 생성 / 저장
# --------------------------------------------------
def make_series_info(
    series_code,
    series_name,
    unit,
    source_series_code,
    source_series_name,
    notes,
):
    return {
        "series_code": series_code,
        "series_name": series_name,
        "category_name": "OECD",
        "frequency": "monthly",
        "unit": unit,
        "chart_type": "line",
        "default_axis": "left",
        "default_color": None,
        "is_recession_series": False,
        "notes": notes,
        "source_series_code": source_series_code,
        "source_series_name": source_series_name,
        "transform_code": "oecd_api",
        "transform_name": "RAW",
        "is_transformed": False,
        "source_unit": unit,
    }


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


# --------------------------------------------------
# CLI 적재
# --------------------------------------------------
def load_oecd_cli(source_id):
    # 공식 structure query 확인용
    structure_text = fetch_oecd_structure_text(CLI_STRUCTURE_QUERY)
    print("[CLI 구조 조회 완료] 길이:", len(structure_text))

    raw_df = fetch_oecd_csv(CLI_DATA_QUERY)
    std_df = standardize_oecd_dataframe(raw_df)

    if std_df.empty:
        raise ValueError("CLI data query 응답에 유효 데이터가 없습니다.")

    # 1) G20 aggregate
    cli_g20_df = build_country_series_df(std_df, "G20")
    save_one_series(
        source_id,
        make_series_info(
            series_code="OECD_CLI_G20",
            series_name="OECD CLI G20",
            unit="index",
            source_series_code="G20",
            source_series_name="OECD CLI G20",
            notes="OECD 공식 CLI data query 적재",
        ),
        cli_g20_df,
    )

    # 2) USA
    cli_usa_df = build_country_series_df(std_df, "USA")
    save_one_series(
        source_id,
        make_series_info(
            series_code="OECD_CLI_USA",
            series_name="OECD CLI USA",
            unit="index",
            source_series_code="USA",
            source_series_name="OECD CLI USA",
            notes="OECD 공식 CLI data query 적재",
        ),
        cli_usa_df,
    )

    # 3) KOR
    cli_kor_df = build_country_series_df(std_df, "KOR")
    save_one_series(
        source_id,
        make_series_info(
            series_code="OECD_CLI_KOR",
            series_name="OECD CLI KOR",
            unit="index",
            source_series_code="KOR",
            source_series_name="OECD CLI KOR",
            notes="OECD 공식 CLI data query 적재",
        ),
        cli_kor_df,
    )

    # 4) Diffusion
    diffusion_df = compute_cli_diffusion_g20(std_df)
    save_one_series(
        source_id,
        make_series_info(
            series_code="OECD_CLI_DIFFUSION_G20",
            series_name="OECD CLI Diffusion G20",
            unit="%",
            source_series_code="DIFFUSION_G20",
            source_series_name="OECD CLI Diffusion G20",
            notes="OECD 공식 CLI data query 기반 pandas 직접 계산",
        ),
        diffusion_df,
    )


# --------------------------------------------------
# CPI 적재
# --------------------------------------------------
def load_oecd_cpi(source_id):
    # 공식 structure query 확인용
    structure_text = fetch_oecd_structure_text(CPI_STRUCTURE_QUERY)
    print("[CPI 구조 조회 완료] 길이:", len(structure_text))

    raw_df = fetch_oecd_csv(CPI_DATA_QUERY)
    std_df = standardize_oecd_dataframe(raw_df)

    if std_df.empty:
        raise ValueError("CPI data query 응답에 유효 데이터가 없습니다.")

    cpi_oecd_df = build_country_series_df(std_df, "OECD")
    save_one_series(
        source_id,
        make_series_info(
            series_code="OECD_CPI_OECD",
            series_name="OECD CPI OECD",
            unit="%",
            source_series_code="OECD",
            source_series_name="OECD CPI OECD",
            notes="OECD 공식 CPI data query 적재",
        ),
        cpi_oecd_df,
    )

    cpi_g20_df = build_country_series_df(std_df, "G20")
    save_one_series(
        source_id,
        make_series_info(
            series_code="OECD_CPI_G20",
            series_name="OECD CPI G20",
            unit="%",
            source_series_code="G20",
            source_series_name="OECD CPI G20",
            notes="OECD 공식 CPI data query 적재",
        ),
        cpi_g20_df,
    )


# --------------------------------------------------
# main
# --------------------------------------------------
def main():
    source_id = get_source_id(SOURCE_CODE)

    load_oecd_cli(source_id)
    load_oecd_cpi(source_id)


if __name__ == "__main__":
    main()