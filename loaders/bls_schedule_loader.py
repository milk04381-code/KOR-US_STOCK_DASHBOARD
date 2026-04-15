# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 01:20:20 2026

@author: 박승욱
"""
# -*- coding: utf-8 -*-

# ============================================================
# BLS ICS Schedule Loader (최종 버전)
# ============================================================
#
# [역할]
# - BLS 공식 캘린더(ICS)를 기반으로 발표일(schedule) 데이터 적재
# - indicator_release_event 테이블에 upsert 수행
#
# [왜 이 방식인가]
# - 기존 HTML 파싱 방식은 BLS에서 403 차단 발생
# - ICS는 BLS가 공식 제공하는 캘린더 데이터 (안정적, 무료)
#
# [기존 대비 주요 변경사항]
# 1. HTML scraping 제거 → ICS 파싱으로 전환
# 2. requests + BeautifulSoup 제거 (HTML 구조 의존 제거)
# 3. VEVENT 기반 파싱 (표준 포맷)
# 4. release_code 매핑 로직 추가
# 5. datetime_local / utc 자동 변환
#
# [처리 흐름]
# ICS 다운로드
#   → VEVENT 분리
#       → SUMMARY (지표명 + 기준시기)
#       → DTSTART (발표일/시간)
#           → release_code 매핑
#           → reference_period 생성
#           → DB upsert
#
# ============================================================


import sys
import re
import requests
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo
from sqlalchemy import text


# ------------------------------------------------------------
# 프로젝트 루트 설정 (db.py import용)
# ------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db import engine


# ------------------------------------------------------------
# 설정
# ------------------------------------------------------------

# BLS 공식 캘린더 (핵심 데이터 소스)
ICS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"

# 시간대 설정
LOCAL_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")


# ------------------------------------------------------------
# release 이름 → 내부 코드 매핑
# (ICS SUMMARY 문자열 기반)
# ------------------------------------------------------------
RELEASE_MAPPING = {
    "Employment Situation": "EMPLOYMENT_SITUATION",
    "Consumer Price Index": "CPI",
    "Job Openings and Labor Turnover": "JOLTS",
}


# ------------------------------------------------------------
# 월 이름 → 숫자 변환
# ------------------------------------------------------------
MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}


# ============================================================
# 1. ICS 다운로드
# ============================================================
def fetch_ics():
    # BLS 공식 캘린더 파일 다운로드
    res = requests.get(ICS_URL, timeout=30)
    res.raise_for_status()
    return res.text


# ============================================================
# 2. ICS 이벤트 파싱
# ============================================================
def parse_ics_events(ics_text):
    # VEVENT 단위로 분리
    events = []

    blocks = ics_text.split("BEGIN:VEVENT")

    for block in blocks[1:]:
        summary = None
        dtstart = None

        for line in block.splitlines():
            if line.startswith("SUMMARY:"):
                summary = line.replace("SUMMARY:", "").strip()

            if line.startswith("DTSTART"):
                dtstart = line.split(":")[1].strip()

        if summary and dtstart:
            events.append((summary, dtstart))

    return events


# ============================================================
# 3. SUMMARY → release_code + 기준시기 추출
# ============================================================
def extract_release_info(summary):
    # 예:
    # "Employment Situation for March 2026"

    for name, code in RELEASE_MAPPING.items():
        if name in summary:

            m = re.search(r"for ([A-Za-z]+ \d{4})", summary)
            if not m:
                return None

            ref_text = m.group(1)
            month, year = ref_text.split()

            month_num = MONTH_MAP.get(month)
            year_num = int(year)

            reference_period = date(year_num, month_num, 1)

            return {
                "release_code": code,
                "reference_period": reference_period,
            }

    return None


# ============================================================
# 4. DTSTART → 날짜 / 시간 변환
# ============================================================
def parse_dtstart(dtstart_str):
    # 예:
    # 20260403T083000

    dt = datetime.strptime(dtstart_str[:15], "%Y%m%dT%H%M%S")

    dt_local = dt.replace(tzinfo=LOCAL_TZ)
    dt_utc = dt_local.astimezone(UTC_TZ)

    return dt.date(), dt_local, dt_utc


# ============================================================
# 5. DB 조회
# ============================================================
def get_release_id(code):
    q = text("SELECT release_id FROM release_meta WHERE release_code=:c")

    with engine.begin() as conn:
        return conn.execute(q, {"c": code}).scalar()


# ============================================================
# 6. DB upsert
# ============================================================
def upsert_event(release_id, ref, r_date, dt_local, dt_utc, key):

    # 기존 존재 여부 확인
    check = text("""
        SELECT event_id
        FROM indicator_release_event
        WHERE release_id=:rid
          AND reference_period=:rp
        LIMIT 1
    """)

    # 신규 insert
    insert = text("""
        INSERT INTO indicator_release_event
        (release_id, reference_period, release_date,
         release_datetime_local, release_datetime_utc,
         is_preliminary, is_revision,
         revision_of_event_id, revision_round,
         source_event_key)
        VALUES
        (:rid, :rp, :rd, :dtl, :dtu,
         FALSE, FALSE, NULL, NULL, :key)
    """)

    # 기존 update
    update = text("""
        UPDATE indicator_release_event
        SET release_date=:rd,
            release_datetime_local=:dtl,
            release_datetime_utc=:dtu
        WHERE event_id=:eid
    """)

    params = {
        "rid": release_id,
        "rp": ref,
        "rd": r_date,
        "dtl": dt_local.replace(tzinfo=None),
        "dtu": dt_utc.replace(tzinfo=None),
        "key": key,
    }

    with engine.begin() as conn:
        eid = conn.execute(check, params).scalar()

        if eid is None:
            conn.execute(insert, params)
        else:
            conn.execute(update, {**params, "eid": eid})


# ============================================================
# 7. 메인 실행
# ============================================================
def main():

    print("▶ BLS ICS Loader 시작")

    ics_text = fetch_ics()
    events = parse_ics_events(ics_text)

    total = 0

    for summary, dtstart in events:

        info = extract_release_info(summary)
        if not info:
            continue

        r_date, dt_local, dt_utc = parse_dtstart(dtstart)

        release_id = get_release_id(info["release_code"])
        if not release_id:
            continue

        key = f"BLS_{info['release_code']}_{info['reference_period'].strftime('%Y%m')}"

        upsert_event(
            release_id,
            info["reference_period"],
            r_date,
            dt_local,
            dt_utc,
            key
        )

        total += 1

    print(f"✔ 완료: {total}건")


# ============================================================
# 실행
# ============================================================
if __name__ == "__main__":
    main()