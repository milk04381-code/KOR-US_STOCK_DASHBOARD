CREATE DATABASE IF NOT EXISTS macro_data;
USE macro_data;

/* =========================================================
   CLEANED FINAL SQL
   목적:
   - create_tables.sql의 중복/누적 구문 제거
   - 최종 구조만 반영
   - 빈 MySQL DB에서 1회 실행용
   ========================================================= */

/* ---------------------------------------------------------
   1) 기본 테이블
   --------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS data_source (
    source_id INT AUTO_INCREMENT PRIMARY KEY,
    source_code VARCHAR(50) NOT NULL UNIQUE,
    source_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    base_url VARCHAR(255),
    description TEXT
);

CREATE TABLE IF NOT EXISTS series_meta (
    series_id INT AUTO_INCREMENT PRIMARY KEY,
    series_code VARCHAR(100) NOT NULL UNIQUE,
    series_name VARCHAR(255) NOT NULL,
    source_id INT NOT NULL,

    category_name VARCHAR(100),
    country_code VARCHAR(10) NULL,
    macro_category VARCHAR(100) NULL,
    indicator_code VARCHAR(100) NULL,
    indicator_name_ko VARCHAR(255) NULL,
    indicator_name_en VARCHAR(255) NULL,
    is_macro_tracker BOOLEAN DEFAULT FALSE,
    display_order INT NULL,
    is_active BOOLEAN DEFAULT TRUE,

    frequency VARCHAR(30),
    unit VARCHAR(50),
    unit_group VARCHAR(50),

    chart_type VARCHAR(30),
    default_axis VARCHAR(10),
    default_color VARCHAR(30),

    is_recession_series BOOLEAN DEFAULT FALSE,
    start_date DATE,
    end_date DATE,
    last_updated DATETIME,
    notes TEXT,

    source_series_code VARCHAR(100) NULL,
    source_series_name VARCHAR(255) NULL,
    transform_code VARCHAR(20) NULL,
    transform_name VARCHAR(20) NULL,
    is_transformed BOOLEAN DEFAULT FALSE,
    source_unit VARCHAR(100) NULL,

    FOREIGN KEY (source_id) REFERENCES data_source(source_id)
);

CREATE TABLE IF NOT EXISTS series_data (
    data_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    series_id INT NOT NULL,
    date_value DATE NOT NULL,
    value_num DECIMAL(20,6),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_series_date (series_id, date_value),
    FOREIGN KEY (series_id) REFERENCES series_meta(series_id)
);

CREATE TABLE IF NOT EXISTS chart_rule (
    rule_id INT AUTO_INCREMENT PRIMARY KEY,
    unit VARCHAR(50) NULL,
    frequency VARCHAR(30),
    recommended_axis VARCHAR(10),
    recommended_chart_type VARCHAR(30),
    use_secondary_axis BOOLEAN DEFAULT FALSE,
    rule_description TEXT
);

/* ---------------------------------------------------------
   2) data_source seed
   --------------------------------------------------------- */

INSERT IGNORE INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
VALUES
('MANUAL', 'Manual Input', 'manual', NULL, '테스트용 수동 입력'),
('FRED', 'FRED', 'api', 'https://fred.stlouisfed.org/', 'FRED 데이터'),
('NBER', 'NBER', 'api', 'https://www.nber.org/', '경기침체 기준'),
('INVESTING', 'Investing.com', 'file', 'https://www.investing.com/', 'Investing.com CSV 파일 적재'),
('FINRA', 'FINRA', 'file', 'https://www.finra.org/', 'FINRA XLSX 파일 적재'),
('YAHOO', 'Yahoo Finance', 'api', 'https://finance.yahoo.com/', 'Yahoo Finance 일별 종가 적재'),
('OECD', 'OECD API', 'api', 'https://sdmx.oecd.org/', 'OECD Data Explorer API 적재'),
('KOSIS', 'KOSIS OpenAPI', 'api', 'https://kosis.kr/openapi/', 'KOSIS API 적재'),
('ECOS', 'Bank of Korea ECOS', 'api', 'https://ecos.bok.or.kr/api/', '한국은행 ECOS OpenAPI 적재'),
('CUSTOMS', 'Korea Customs Service', 'file', 'https://tradedata.go.kr/cts/index.do', '관세청 수출입 총괄 CSV 파일 적재'),
('MSCI', 'MSCI', 'file', NULL, 'MSCI ACWI XLSX 적재'),
('ENARA', 'e-나라지표', 'api', 'https://www.index.go.kr/', 'e-나라지표 공유 OpenAPI 적재'),
('CFTC', 'CFTC Socrata API', 'api', 'https://publicreporting.cftc.gov/', 'CFTC Current Legacy Report API 적재'),
('INVESTING_WEB', 'Investing.com Economic Calendar', 'web', 'https://www.investing.com/economic-calendar', 'Expected 값 크롤링용 source');

/* ---------------------------------------------------------
   3) 기본 sample series_meta seed
   --------------------------------------------------------- */

INSERT IGNORE INTO series_meta
(
    series_code,
    series_name,
    source_id,
    category_name,
    frequency,
    unit,
    unit_group,
    chart_type,
    default_axis,
    default_color,
    is_recession_series,
    start_date,
    end_date,
    source_series_code,
    source_series_name,
    transform_code,
    transform_name,
    is_transformed,
    source_unit
)
SELECT
    'KOSPI',
    'KOSPI Index',
    ds.source_id,
    '국내주식',
    'daily',
    'index',
    'index',
    'line',
    'left',
    'blue',
    FALSE,
    '2020-01-01',
    '2026-12-31',
    'KOSPI',
    'KOSPI Index',
    'lin',
    'RAW',
    FALSE,
    'index'
FROM data_source ds
WHERE ds.source_code = 'MANUAL';

INSERT IGNORE INTO series_meta
(
    series_code,
    series_name,
    source_id,
    category_name,
    frequency,
    unit,
    unit_group,
    chart_type,
    default_axis,
    default_color,
    is_recession_series,
    start_date,
    end_date,
    source_series_code,
    source_series_name,
    transform_code,
    transform_name,
    is_transformed,
    source_unit
)
SELECT
    'EXPORT_YOY',
    '한국 수출 증가율',
    ds.source_id,
    '국내 매크로',
    'monthly',
    '%',
    'percent',
    'line',
    'right',
    'red',
    FALSE,
    '2020-01-01',
    '2026-12-31',
    'EXPORT',
    '한국 수출',
    'manual',
    'YOY',
    TRUE,
    '%'
FROM data_source ds
WHERE ds.source_code = 'MANUAL';

INSERT IGNORE INTO series_meta
(
    series_code,
    series_name,
    source_id,
    category_name,
    frequency,
    unit,
    unit_group,
    chart_type,
    default_axis,
    default_color,
    is_recession_series,
    start_date,
    end_date,
    source_series_code,
    source_series_name,
    transform_code,
    transform_name,
    is_transformed,
    source_unit
)
SELECT
    'USREC',
    'NBER Recession Indicator',
    ds.source_id,
    '경기침체',
    'monthly',
    'indicator',
    'indicator',
    'area',
    'left',
    'gray',
    TRUE,
    '1854-12-01',
    '2026-12-31',
    'USREC',
    'NBER Recession Indicator',
    'lin',
    'RAW',
    FALSE,
    'indicator'
FROM data_source ds
WHERE ds.source_code = 'FRED';

/* ---------------------------------------------------------
   4) 기본 sample series_data seed
   --------------------------------------------------------- */

INSERT IGNORE INTO series_data (series_id, date_value, value_num)
SELECT m.series_id, x.date_value, x.value_num
FROM series_meta m
JOIN (
    SELECT 'KOSPI' AS series_code, '2024-01-01' AS date_value, 2600.00 AS value_num
    UNION ALL SELECT 'KOSPI', '2024-02-01', 2650.00
    UNION ALL SELECT 'KOSPI', '2024-03-01', 2700.00
    UNION ALL SELECT 'KOSPI', '2024-04-01', 2750.00
    UNION ALL SELECT 'KOSPI', '2024-05-01', 2720.00

    UNION ALL SELECT 'EXPORT_YOY', '2024-01-01', 5.0
    UNION ALL SELECT 'EXPORT_YOY', '2024-02-01', 7.0
    UNION ALL SELECT 'EXPORT_YOY', '2024-03-01', 3.0
    UNION ALL SELECT 'EXPORT_YOY', '2024-04-01', 4.0
    UNION ALL SELECT 'EXPORT_YOY', '2024-05-01', 6.0

    UNION ALL SELECT 'USREC', '2024-01-01', 0
    UNION ALL SELECT 'USREC', '2024-02-01', 0
    UNION ALL SELECT 'USREC', '2024-03-01', 0
    UNION ALL SELECT 'USREC', '2024-04-01', 0
    UNION ALL SELECT 'USREC', '2024-05-01', 0
) x
  ON m.series_code = x.series_code;

/* ---------------------------------------------------------
   5) chart_rule 최종 seed
   --------------------------------------------------------- */

INSERT IGNORE INTO chart_rule
(
    rule_id,
    unit,
    frequency,
    recommended_axis,
    recommended_chart_type,
    use_secondary_axis,
    rule_description
)
VALUES
(1, 'index',     'daily',      'left',  'line', FALSE, '일간 index는 좌측축 line'),
(2, 'index',     'monthly',    'left',  'line', FALSE, '월간 index는 좌측축 line'),
(3, 'indicator', 'monthly',    'left',  'area', FALSE, 'indicator는 좌측축 area'),
(4, '%',         'daily',      'right', 'line', TRUE,  '원시 % 는 우측축 line'),
(5, '%',         'monthly',    'right', 'line', TRUE,  '원시 % 는 우측축 line'),
(6, '%',         'quarterly',  'right', 'line', TRUE,  '원시 % 는 우측축 line'),
(7, '% YoY',     'monthly',    'left',  'bar',  FALSE, 'YoY 변화율은 좌측축 bar'),
(8, '% MoM',     'monthly',    'left',  'bar',  FALSE, 'MoM 변화율은 좌측축 bar'),
(9, '% QoQ',     'quarterly',  'left',  'bar',  FALSE, 'QoQ 변화율은 좌측축 bar');

/* ---------------------------------------------------------
   6) macro tracker release/meta 구조
   --------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS release_meta (
    release_id INT AUTO_INCREMENT PRIMARY KEY,
    release_code VARCHAR(100) NOT NULL UNIQUE,
    release_name VARCHAR(255) NOT NULL,
    release_name_ko VARCHAR(255) NULL,
    country_code VARCHAR(10) NOT NULL,
    publisher_name VARCHAR(100) NULL,
    release_frequency VARCHAR(30) NULL,
    release_time_local TIME NULL,
    timezone_name VARCHAR(50) NULL,
    source_id INT NULL,
    source_release_key VARCHAR(100) NULL,
    source_url VARCHAR(255) NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_release_meta_source
        FOREIGN KEY (source_id) REFERENCES data_source(source_id)
);

CREATE TABLE IF NOT EXISTS series_release_map (
    map_id INT AUTO_INCREMENT PRIMARY KEY,
    series_id INT NOT NULL,
    release_id INT NOT NULL,
    release_role VARCHAR(30) NOT NULL DEFAULT 'actual',
    is_primary_release BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_series_release_role (series_id, release_id, release_role),
    KEY idx_srm_release_id (release_id),
    CONSTRAINT fk_series_release_map_series
        FOREIGN KEY (series_id) REFERENCES series_meta(series_id),
    CONSTRAINT fk_series_release_map_release
        FOREIGN KEY (release_id) REFERENCES release_meta(release_id)
);

CREATE TABLE IF NOT EXISTS indicator_release_event (
    event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    release_id INT NOT NULL,
    reference_period DATE NOT NULL,
    release_date DATE NOT NULL,
    release_datetime_local DATETIME NULL,
    release_datetime_utc DATETIME NULL,
    is_preliminary BOOLEAN DEFAULT FALSE,
    is_revision BOOLEAN DEFAULT FALSE,
    revision_of_event_id BIGINT NULL,
    revision_round INT NULL,
    source_event_key VARCHAR(100) NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_release_period_datetime (release_id, reference_period, release_datetime_local),
    KEY idx_ire_release_date (release_date),
    KEY idx_ire_release_id (release_id),
    KEY idx_ire_reference_period (reference_period),
    KEY idx_ire_revision_of_event_id (revision_of_event_id),
    CONSTRAINT fk_indicator_release_event_release
        FOREIGN KEY (release_id) REFERENCES release_meta(release_id),
    CONSTRAINT fk_indicator_release_event_revision_of
        FOREIGN KEY (revision_of_event_id) REFERENCES indicator_release_event(event_id)
);

CREATE TABLE IF NOT EXISTS indicator_release_value (
    value_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id BIGINT NOT NULL,
    series_id INT NOT NULL,
    value_type VARCHAR(30) NOT NULL,
    reference_period DATE NULL,
    value_num DECIMAL(20,6) NULL,
    value_text VARCHAR(100) NULL,
    unit_override VARCHAR(50) NULL,
    source_id INT NULL,
    source_value_key VARCHAR(150) NULL,
    captured_at DATETIME NULL,
    source_url VARCHAR(255) NULL,
    is_official BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_event_series_type (event_id, series_id, value_type),
    KEY idx_irv_series_id (series_id),
    KEY idx_irv_value_type (value_type),
    KEY idx_irv_source_id (source_id),
    KEY idx_irv_captured_at (captured_at),
    CONSTRAINT fk_indicator_release_value_event
        FOREIGN KEY (event_id) REFERENCES indicator_release_event(event_id),
    CONSTRAINT fk_indicator_release_value_series
        FOREIGN KEY (series_id) REFERENCES series_meta(series_id),
    CONSTRAINT fk_indicator_release_value_source
        FOREIGN KEY (source_id) REFERENCES data_source(source_id)
);

CREATE TABLE IF NOT EXISTS expected_value_snapshot (
    snapshot_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    release_code VARCHAR(100) NOT NULL,
    indicator_code VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NULL,
    reference_period DATE NULL,
    expected_value_num DECIMAL(20,6) NULL,
    expected_value_text VARCHAR(100) NULL,
    source_id INT NOT NULL,
    captured_at DATETIME NOT NULL,
    source_url VARCHAR(255) NULL,
    raw_payload_json JSON NULL,
    raw_html_hash VARCHAR(100) NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_evs_release_indicator (release_code, indicator_code),
    KEY idx_evs_captured_at (captured_at),
    KEY idx_evs_reference_period (reference_period),
    CONSTRAINT fk_expected_value_snapshot_source
        FOREIGN KEY (source_id) REFERENCES data_source(source_id)
);

CREATE TABLE IF NOT EXISTS indicator_calc_rule (
    calc_rule_id INT AUTO_INCREMENT PRIMARY KEY,
    indicator_code VARCHAR(100) NOT NULL UNIQUE,
    country_code VARCHAR(10) NOT NULL,
    macro_category VARCHAR(100) NULL,
    series_id INT NOT NULL,
    display_name_ko VARCHAR(255) NOT NULL,
    display_order INT NULL,
    calc_basis VARCHAR(30) NOT NULL,
    comparison_lag INT NULL,
    comparison_unit VARCHAR(30) NULL,
    speed_calc_method VARCHAR(50) NULL,
    trend_calc_method VARCHAR(50) NULL,
    policy_phase_method VARCHAR(50) NULL,
    asset_stock_series_code VARCHAR(100) NULL,
    asset_bond_series_code VARCHAR(100) NULL,
    asset_fx_series_code VARCHAR(100) NULL,
    stock_reaction_method VARCHAR(50) NULL,
    bond_reaction_method VARCHAR(50) NULL,
    fx_reaction_method VARCHAR(50) NULL,
    use_expected_value BOOLEAN DEFAULT TRUE,
    use_previous_value BOOLEAN DEFAULT TRUE,
    supports_revision BOOLEAN DEFAULT TRUE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_icr_country_category (country_code, macro_category),
    KEY idx_icr_display_order (display_order),
    CONSTRAINT fk_indicator_calc_rule_series
        FOREIGN KEY (series_id) REFERENCES series_meta(series_id)
);

/* ---------------------------------------------------------
   7) release_meta seed
   --------------------------------------------------------- */

INSERT IGNORE INTO release_meta
(
    release_code,
    release_name,
    release_name_ko,
    country_code,
    publisher_name,
    release_frequency,
    release_time_local,
    timezone_name,
    source_id,
    source_release_key,
    source_url,
    is_active
)
SELECT
    x.release_code,
    x.release_name,
    x.release_name_ko,
    x.country_code,
    x.publisher_name,
    x.release_frequency,
    x.release_time_local,
    x.timezone_name,
    ds.source_id,
    x.source_release_key,
    x.source_url,
    TRUE
FROM (
    SELECT
        'EMPLOYMENT_SITUATION' AS release_code,
        'Employment Situation' AS release_name,
        '고용보고서' AS release_name_ko,
        'US' AS country_code,
        'BLS' AS publisher_name,
        'monthly' AS release_frequency,
        '08:30:00' AS release_time_local,
        'America/New_York' AS timezone_name,
        'FRED_EMPLOYMENT_SITUATION' AS source_release_key,
        NULL AS source_url
    UNION ALL
    SELECT
        'CPI',
        'Consumer Price Index',
        '소비자물가지수',
        'US',
        'BLS',
        'monthly',
        '08:30:00',
        'America/New_York',
        'FRED_CPI',
        NULL
    UNION ALL
    SELECT
        'PPI',
        'Producer Price Index',
        '생산자물가지수',
        'US',
        'BLS',
        'monthly',
        '08:30:00',
        'America/New_York',
        'FRED_PPI',
        NULL
    UNION ALL
    SELECT
        'RETAIL_SALES',
        'Retail Sales',
        '소매판매',
        'US',
        'U.S. Census Bureau',
        'monthly',
        '08:30:00',
        'America/New_York',
        'FRED_RETAIL_SALES',
        NULL
    UNION ALL
    SELECT
        'PERSONAL_INCOME_OUTLAYS',
        'Personal Income and Outlays',
        '개인소득 및 지출',
        'US',
        'BEA',
        'monthly',
        '08:30:00',
        'America/New_York',
        'FRED_PERSONAL_INCOME_OUTLAYS',
        NULL
    UNION ALL
    SELECT
        'INDUSTRIAL_PRODUCTION',
        'Industrial Production and Capacity Utilization',
        '산업생산 및 설비가동률',
        'US',
        'Federal Reserve',
        'monthly',
        '09:15:00',
        'America/New_York',
        'FRED_INDUSTRIAL_PRODUCTION',
        NULL
    UNION ALL
    SELECT
        'JOLTS',
        'Job Openings and Labor Turnover Survey',
        'JOLTS',
        'US',
        'BLS',
        'monthly',
        '10:00:00',
        'America/New_York',
        'FRED_JOLTS',
        NULL
    UNION ALL
    SELECT
        'INITIAL_JOBLESS_CLAIMS',
        'Unemployment Insurance Weekly Claims',
        '신규 실업수당청구',
        'US',
        'U.S. Department of Labor',
        'weekly',
        '08:30:00',
        'America/New_York',
        'FRED_INITIAL_JOBLESS_CLAIMS',
        NULL
) x
JOIN data_source ds
  ON ds.source_code = 'FRED';

/* ---------------------------------------------------------
   8) index 보강
   --------------------------------------------------------- */

CREATE INDEX idx_series_meta_macro_tracker
    ON series_meta (is_macro_tracker, country_code, macro_category, display_order);

CREATE INDEX idx_series_meta_indicator_code
    ON series_meta (indicator_code);

CREATE INDEX idx_series_data_series_date
    ON series_data (series_id, date_value);

/* ---------------------------------------------------------
   9) 확인용 쿼리
   --------------------------------------------------------- */
/*
SELECT * FROM data_source;
SELECT * FROM series_meta;
SELECT * FROM series_data;
SELECT * FROM chart_rule;
SELECT * FROM release_meta;
*/

USE macro_data;

SELECT COUNT(*) FROM series_meta;
SELECT COUNT(*) FROM series_data;
SELECT COUNT(*) FROM chart_rule;

select * from series_meta;
select * from series_data;

DROP DATABASE macro_data;
CREATE DATABASE macro_data;

SELECT
    m.series_code,
    m.indicator_name_ko,
    m.frequency,
    d.date_value,
    d.value_num
FROM series_meta m
JOIN series_data d ON m.series_id = d.series_id
WHERE m.series_code IN ('T10YIE')
ORDER BY m.series_code, d.date_value DESC;

SELECT
    release_id,
    release_code,
    release_name,
    release_name_ko,
    release_frequency,
    release_time_local,
    timezone_name
FROM release_meta
WHERE release_code IN ('EMPLOYMENT_SITUATION', 'CPI', 'JOLTS');

SELECT series_code, series_name, frequency, unit, is_macro_tracker, is_active
FROM series_meta
WHERE series_code IN ('DFEDTAR', 'DFEDTARU');

SHOW INDEX FROM series_meta;
SHOW INDEX FROM series_data;

CREATE INDEX idx_series_meta_country_category_active
ON series_meta (country_code, macro_category, is_active);

CREATE INDEX idx_series_data_date
ON series_data (date_value);

