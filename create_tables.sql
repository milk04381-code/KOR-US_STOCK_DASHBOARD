CREATE DATABASE macro_data;
USE macro_data;

CREATE TABLE data_source (
    source_id INT AUTO_INCREMENT PRIMARY KEY,
    source_code VARCHAR(50) NOT NULL UNIQUE,
    source_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    base_url VARCHAR(255),
    description TEXT
);

CREATE TABLE series_meta (
    series_id INT AUTO_INCREMENT PRIMARY KEY,
    series_code VARCHAR(100) NOT NULL UNIQUE,
    series_name VARCHAR(255) NOT NULL,
    source_id INT NOT NULL,
    category_name VARCHAR(100),
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
    FOREIGN KEY (source_id) REFERENCES data_source(source_id)
);

CREATE TABLE series_data (
    data_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    series_id INT NOT NULL,
    date_value DATE NOT NULL,
    value_num DECIMAL(20,6),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_series_date (series_id, date_value),
    FOREIGN KEY (series_id) REFERENCES series_meta(series_id)
);

CREATE TABLE chart_rule (
    rule_id INT AUTO_INCREMENT PRIMARY KEY,
    unit_group VARCHAR(50),
    frequency VARCHAR(30),
    recommended_axis VARCHAR(10),
    recommended_chart_type VARCHAR(30),
    use_secondary_axis BOOLEAN DEFAULT FALSE,
    rule_description TEXT
);

INSERT INTO data_source (source_code, source_name, source_type, base_url, description)
VALUES
('MANUAL', 'Manual Input', 'manual', NULL, '테스트용 수동 입력'),
('FRED', 'FRED', 'api', 'https://fred.stlouisfed.org/', 'FRED 데이터'),
('NBER', 'NBER', 'api', 'https://www.nber.org/', '경기침체 기준');

INSERT INTO series_meta
(series_code, series_name, source_id, category_name, frequency, unit, unit_group, chart_type, default_axis, default_color, is_recession_series, start_date, end_date)
VALUES
('KOSPI', 'KOSPI Index', 1, '국내주식', 'daily', 'index', 'index', 'line', 'left', 'blue', FALSE, '2020-01-01', '2026-12-31'),
('EXPORT_YOY', '한국 수출 증가율', 1, '매크로', 'monthly', '%', 'percent', 'line', 'right', 'red', FALSE, '2020-01-01', '2026-12-31'),
('USREC', 'NBER Recession Indicator', 2, '경기침체', 'monthly', 'indicator', 'indicator', 'area', 'left', 'gray', TRUE, '1854-12-01', '2026-12-31');

INSERT INTO series_data (series_id, date_value, value_num)
VALUES
(1, '2024-01-01', 2600.00),
(1, '2024-02-01', 2650.00),
(1, '2024-03-01', 2700.00),
(1, '2024-04-01', 2750.00),
(1, '2024-05-01', 2720.00),

(2, '2024-01-01', 5.0),
(2, '2024-02-01', 7.0),
(2, '2024-03-01', 3.0),
(2, '2024-04-01', 4.0),
(2, '2024-05-01', 6.0),

(3, '2024-01-01', 0),
(3, '2024-02-01', 0),
(3, '2024-03-01', 0),
(3, '2024-04-01', 0),
(3, '2024-05-01', 0);

INSERT INTO chart_rule
(unit_group, frequency, recommended_axis, recommended_chart_type, use_secondary_axis, rule_description)
VALUES
('index', 'daily', 'left', 'line', FALSE, '주가지수는 기본적으로 좌측 축 line'),
('percent', 'monthly', 'right', 'bar', TRUE, '월간 % 데이터는 우측 축 bar'),
('rate', 'daily', 'right', 'line', TRUE, '금리성 데이터는 우측 축 line'),
('indicator', 'monthly', 'left', 'area', FALSE, '침체/indicator 계열은 area');

UPDATE series_meta
SET category_name = '국내 매크로'
WHERE series_code = 'EXPORT_YOY';

USE macro_data;

ALTER TABLE series_meta
    ADD COLUMN source_series_code VARCHAR(100) NULL AFTER notes,
    ADD COLUMN source_series_name VARCHAR(255) NULL AFTER source_series_code,
    ADD COLUMN transform_code VARCHAR(20) NULL AFTER source_series_name,
    ADD COLUMN transform_name VARCHAR(20) NULL AFTER transform_code,
    ADD COLUMN is_transformed BOOLEAN DEFAULT FALSE AFTER transform_name,
    ADD COLUMN source_unit VARCHAR(100) NULL AFTER is_transformed;
    
UPDATE series_meta
SET
    source_series_code = series_code,
    source_series_name = series_name,
    transform_code = 'lin',
    transform_name = 'RAW',
    is_transformed = FALSE,
    source_unit = unit
WHERE series_id > 0
  AND source_series_code IS NULL;
  
UPDATE series_meta
SET
    source_series_code = 'EXPORT',
    source_series_name = '한국 수출',
    transform_code = 'manual',
    transform_name = 'YOY',
    is_transformed = TRUE,
    source_unit = '%'
WHERE series_code = 'EXPORT_YOY';

SELECT
    series_id,
    series_code,
    source_series_code,
    transform_name,
    is_transformed,
    unit,
    source_unit
FROM series_meta
ORDER BY series_id;

SELECT
    rule_id,
    unit,
    frequency,
    recommended_axis,
    recommended_chart_type,
    use_secondary_axis
FROM chart_rule
ORDER BY rule_id;

SHOW COLUMNS FROM chart_rule;

ALTER TABLE chart_rule
    ADD COLUMN unit VARCHAR(50) NULL AFTER rule_id;
    
SHOW COLUMNS FROM chart_rule;

SELECT DATABASE();
SHOW TABLES LIKE 'chart_rule';
SHOW CREATE TABLE chart_rule;
select * FROM chart_rule;

DELETE FROM chart_rule
WHERE rule_id > 0;

INSERT INTO chart_rule
(unit, frequency, recommended_axis, recommended_chart_type, use_secondary_axis, rule_description)
VALUES
('index', 'daily', 'left', 'line', FALSE, '주가지수는 좌측축 line'),
('index', 'monthly', 'left', 'line', FALSE, '월간 index 계열은 좌측축 line'),
('%', 'daily', 'right', 'line', TRUE, '일간 % 계열은 우측축 line'),
('%', 'monthly', 'right', 'line', TRUE, '월간 % 계열은 우측축 line'),
('% YoY', 'monthly', 'right', 'bar', TRUE, '월간 YoY는 우측축 bar'),
('% MoM', 'monthly', 'right', 'bar', TRUE, '월간 MoM는 우측축 bar'),
('% QoQ', 'quarterly', 'right', 'bar', TRUE, '분기 QoQ는 우측축 bar'),
('indicator', 'monthly', 'left', 'area', FALSE, 'indicator는 좌측축 area');

SELECT
    rule_id,
    unit,
    frequency,
    recommended_axis,
    recommended_chart_type,
    use_secondary_axis
FROM chart_rule
ORDER BY rule_id;

select * from chart_rule;

USE macro_data;

CREATE TABLE IF NOT EXISTS chart_rule_backup AS
SELECT * FROM chart_rule;

USE macro_data;

DELETE FROM chart_rule;

INSERT INTO chart_rule
(unit, frequency, recommended_axis, recommended_chart_type, use_secondary_axis, rule_description)
VALUES
('index', 'daily',      'left',  'line', FALSE, '일간 index는 좌측축 line'),
('index', 'monthly',    'left',  'line', FALSE, '월간 index는 좌측축 line'),

('indicator', 'monthly','left',  'area', FALSE, 'indicator는 좌측축 area'),

('%', 'daily',          'right', 'line', TRUE,  '원시 % 는 우측축 line'),
('%', 'monthly',        'right', 'line', TRUE,  '원시 % 는 우측축 line'),
('%', 'quarterly',      'right', 'line', TRUE,  '원시 % 는 우측축 line'),

('% YoY', 'monthly',    'left',  'bar',  FALSE, 'YoY 변화율은 좌측축 bar'),
('% MoM', 'monthly',    'left',  'bar',  FALSE, 'MoM 변화율은 좌측축 bar'),
('% QoQ', 'quarterly',  'left',  'bar',  FALSE, 'QoQ 변화율은 좌측축 bar');

UPDATE chart_rule
SET
    recommended_axis = 'left',
    recommended_chart_type = 'bar',
    use_secondary_axis = FALSE,
    rule_description = 'YoY 변화율은 좌측축 bar'
WHERE rule_id = 9;

SELECT
    rule_id,
    unit,
    frequency,
    recommended_axis,
    recommended_chart_type,
    use_secondary_axis
FROM chart_rule
WHERE rule_id = 9;

SELECT
    rule_id,
    unit,
    frequency,
    recommended_axis,
    recommended_chart_type,
    use_secondary_axis,
    rule_description
FROM chart_rule
WHERE rule_id IN (8, 10);

SELECT * FROM chart_rule;

USE macro_data;

DELETE FROM chart_rule
WHERE rule_id > 0;

INSERT INTO chart_rule
(unit, frequency, recommended_axis, recommended_chart_type, use_secondary_axis, rule_description)
VALUES
('index', 'daily',      'left',  'line', FALSE, '일간 index는 좌측축 line'),
('index', 'monthly',    'left',  'line', FALSE, '월간 index는 좌측축 line'),

('indicator', 'monthly','left',  'area', FALSE, 'indicator는 좌측축 area'),

('%', 'daily',          'right', 'line', TRUE,  '원시 % 는 우측축 line'),
('%', 'monthly',        'right', 'line', TRUE,  '원시 % 는 우측축 line'),
('%', 'quarterly',      'right', 'line', TRUE,  '원시 % 는 우측축 line'),

('% YoY', 'monthly',    'left',  'bar',  FALSE, 'YoY 변화율은 좌측축 bar'),
('% MoM', 'monthly',    'left',  'bar',  FALSE, 'MoM 변화율은 좌측축 bar'),
('% QoQ', 'quarterly',  'left',  'bar',  FALSE, 'QoQ 변화율은 좌측축 bar');

SELECT
    rule_id,
    unit,
    frequency,
    recommended_axis,
    recommended_chart_type,
    use_secondary_axis
FROM chart_rule
ORDER BY rule_id;

USE macro_data;

INSERT INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
VALUES
('INVESTING', 'Investing.com', 'file', 'https://www.investing.com/', 'Investing.com CSV 파일 적재'),
('FINRA', 'FINRA', 'file', 'https://www.finra.org/', 'FINRA XLSX 파일 적재');

SELECT
    m.series_code,
    d.series_id,
    d.date_value,
    d.value_num
FROM series_data d
JOIN series_meta m
  ON d.series_id = m.series_id
WHERE m.series_code = 'KOSPI'
ORDER BY d.date_value DESC;

DELETE d
FROM series_data d
JOIN series_meta m
  ON d.series_id = m.series_id
WHERE m.series_code = 'KOSPI'
  AND d.date_value BETWEEN '2024-01-01' AND '2024-05-01';
  
USE macro_data;

INSERT INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
VALUES
('YAHOO', 'Yahoo Finance', 'api', 'https://finance.yahoo.com/', 'Yahoo Finance 일별 종가 적재');

INSERT INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
VALUES
('OECD', 'OECD API', 'api', 'https://sdmx.oecd.org/', 'OECD Data Explorer API 적재');

select * from data_source;
select * from series_meta;

SELECT
    m.series_code,
    COUNT(*) AS row_count
FROM series_data d
JOIN series_meta m
  ON d.series_id = m.series_id
WHERE m.series_code LIKE 'OECD%'
GROUP BY m.series_code
ORDER BY m.series_code;

SELECT
    m.series_code,
    d.date_value,
    d.value_num
FROM series_data d
JOIN series_meta m
  ON d.series_id = m.series_id
WHERE m.series_code LIKE 'OECD_CPI_G20'
ORDER BY m.series_code, d.date_value DESC
LIMIT 200;

INSERT INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
VALUES
('KOSIS', 'KOSIS OpenAPI', 'api', 'https://kosis.kr/openapi/', 'KOSIS API 적재');

INSERT INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
VALUES (
    'ECOS',
    'Bank of Korea ECOS',
    'api',
    'https://ecos.bok.or.kr/api/',
    '한국은행 ECOS OpenAPI 적재'
    );

INSERT INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
VALUES (
    'CUSTOMS',
    'Korea Customs Service',
    'file',
    NULL,
    '관세청 수출입 총괄 CSV 파일 적재'
);

UPDATE data_source
SET base_url = 'https://tradedata.go.kr/cts/index.do'
WHERE source_code = 'CUSTOMS';

INSERT INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
VALUES (
    'MSCI',
    'MSCI',
    'file',
    NULL,
    'MSCI ACWI XLSX 적재'
);

INSERT INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
VALUES (
    'ENARA',
    'e-나라지표',
    'api',
    'https://www.index.go.kr/',
    'e-나라지표 공유 OpenAPI 적재'
);

INSERT INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
VALUES (
    'CFTC',
    'CFTC Socrata API',
    'api',
    'https://publicreporting.cftc.gov/',
    'CFTC Current Legacy Report API 적재'
);

/* =========================================================
   Step 4 추가안
   목적:
   - 기존 탭 무영향
   - 미국 경제지표 Tracker용 발표일 / 이벤트 / 실제·예상·이전 / 계산룰 추가
   - 기존 series_meta / series_data / chart_rule는 유지
   ========================================================= */

USE macro_data;

/* ---------------------------------------------------------
   0) series_meta 확장
   - 기존 컬럼 의미는 유지
   - macro tracker용 표시/매핑 정보만 추가
   --------------------------------------------------------- */
ALTER TABLE series_meta
    ADD COLUMN country_code VARCHAR(10) NULL AFTER category_name,
    ADD COLUMN macro_category VARCHAR(100) NULL AFTER country_code,
    ADD COLUMN indicator_code VARCHAR(100) NULL AFTER macro_category,
    ADD COLUMN indicator_name_ko VARCHAR(255) NULL AFTER indicator_code,
    ADD COLUMN indicator_name_en VARCHAR(255) NULL AFTER indicator_name_ko,
    ADD COLUMN is_macro_tracker BOOLEAN DEFAULT FALSE AFTER indicator_name_en,
    ADD COLUMN display_order INT NULL AFTER is_macro_tracker,
    ADD COLUMN is_active BOOLEAN DEFAULT TRUE AFTER display_order;


/* ---------------------------------------------------------
   1) release_meta
   - 발표 종류 정의
   - 예: Employment Situation, CPI, Retail Sales
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


/* ---------------------------------------------------------
   2) series_release_map
   - 어떤 시계열이 어떤 발표와 연결되는지 저장
   - 같은 발표에 여러 시계열이 연결될 수 있음
   --------------------------------------------------------- */
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


/* ---------------------------------------------------------
   3) indicator_release_event
   - 실제 발표 이벤트
   - reference_period: 기준월/기준분기
   - release_date: 발표일
   --------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS indicator_release_event (
    event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    release_id INT NOT NULL,
    reference_period DATE NOT NULL,
    release_date DATE NOT NULL,
    release_datetime_local DATETIME NULL,
    release_datetime_utc DATETIME NULL,
    is_preliminary BOOLEAN DEFAULT FALSE,
    is_revision BOOLEAN DEFAULT FALSE,
    source_event_key VARCHAR(100) NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_release_period_datetime (release_id, reference_period, release_datetime_local),
    KEY idx_ire_release_date (release_date),
    KEY idx_ire_release_id (release_id),
    CONSTRAINT fk_indicator_release_event_release
        FOREIGN KEY (release_id) REFERENCES release_meta(release_id)
);


/* ---------------------------------------------------------
   4) indicator_release_value
   - 실제 / 예상 / 이전 수치 저장
   - value_type: actual / expected / previous
   --------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS indicator_release_value (
    value_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id BIGINT NOT NULL,
    series_id INT NOT NULL,
    value_type VARCHAR(30) NOT NULL,
    value_num DECIMAL(20,6) NULL,
    value_text VARCHAR(100) NULL,
    unit_override VARCHAR(50) NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_event_series_type (event_id, series_id, value_type),
    KEY idx_irv_series_id (series_id),
    KEY idx_irv_value_type (value_type),
    CONSTRAINT fk_indicator_release_value_event
        FOREIGN KEY (event_id) REFERENCES indicator_release_event(event_id),
    CONSTRAINT fk_indicator_release_value_series
        FOREIGN KEY (series_id) REFERENCES series_meta(series_id)
);


/* ---------------------------------------------------------
   5) indicator_calc_rule
   - macro_tracker_service.py의 RULE_MAP를 DB로 옮기기 위한 테이블
   - 정책 국면 / 전기 대비 / 속도 / 추세 / 자산 반응 정의
   --------------------------------------------------------- */
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
   6) 권장 seed: release_meta
   - source_id는 이미 data_source에 FRED가 있어야 함
   --------------------------------------------------------- */
INSERT INTO release_meta
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
  ON ds.source_code = 'FRED'
WHERE NOT EXISTS (
    SELECT 1
    FROM release_meta rm
    WHERE rm.release_code = x.release_code
);


/* ---------------------------------------------------------
   7) 권장 seed: indicator_calc_rule 예시
   - series_id는 series_meta에 적재된 뒤 넣는 것이 안전
   - 아래는 예시 insert 형태
   --------------------------------------------------------- */

/*
INSERT INTO indicator_calc_rule
(
    indicator_code,
    country_code,
    macro_category,
    series_id,
    display_name_ko,
    display_order,
    calc_basis,
    comparison_lag,
    comparison_unit,
    speed_calc_method,
    trend_calc_method,
    policy_phase_method,
    asset_stock_series_code,
    asset_bond_series_code,
    asset_fx_series_code,
    stock_reaction_method,
    bond_reaction_method,
    fx_reaction_method,
    use_expected_value,
    use_previous_value,
    supports_revision,
    is_active
)
SELECT
    'US_UNRATE',
    'US',
    '고용',
    sm.series_id,
    '실업률',
    20,
    'level',
    1,
    'release',
    'delta_abs',
    'direction_3pt',
    'rule_based',
    'SP500',
    'US2Y',
    'DXY',
    'close_pct_vs_prev_close',
    'close_bp_vs_prev_close',
    'close_pct_vs_prev_close',
    TRUE,
    TRUE,
    TRUE,
    TRUE
FROM series_meta sm
WHERE sm.series_code = 'UNRATE';
*/


/* ---------------------------------------------------------
   8) 권장 인덱스 보강
   - macro tracker 조회 성능용
   --------------------------------------------------------- */
CREATE INDEX idx_series_meta_macro_tracker
    ON series_meta (is_macro_tracker, country_code, macro_category, display_order);

CREATE INDEX idx_series_meta_indicator_code
    ON series_meta (indicator_code);

CREATE INDEX idx_series_data_series_date
    ON series_data (series_id, date_value);

/* =========================================================
   Step 4 SQL 수정본
   목적:
   1) 기존 탭 무영향 유지
   2) macro tracker용 발표/예상/이전/개정 구조 추가
   3) expected 크롤링(Investing 등) 확장성 반영
   4) previous에 개정치 반영 가능한 구조 확보
   ========================================================= */

USE macro_data;


/* =========================================================
   A. data_source 보강
   - expected 크롤링 source 구분용
   - 기존 source 체계 유지
   ========================================================= */
INSERT INTO data_source (
    source_code,
    source_name,
    source_type,
    base_url,
    description
)
SELECT
    'INVESTING_WEB',
    'Investing.com Economic Calendar',
    'web',
    'https://www.investing.com/economic-calendar',
    'Expected 값 크롤링용 source'
WHERE NOT EXISTS (
    SELECT 1
    FROM data_source
    WHERE source_code = 'INVESTING_WEB'
);


/* =========================================================
   B. series_meta 확장
   - 기존 series_meta 의미 유지
   - macro tracker 표시/분류용 컬럼만 추가
   ========================================================= */
ALTER TABLE series_meta
    ADD COLUMN country_code VARCHAR(10) NULL AFTER category_name,
    ADD COLUMN macro_category VARCHAR(100) NULL AFTER country_code,
    ADD COLUMN indicator_code VARCHAR(100) NULL AFTER macro_category,
    ADD COLUMN indicator_name_ko VARCHAR(255) NULL AFTER indicator_code,
    ADD COLUMN indicator_name_en VARCHAR(255) NULL AFTER indicator_name_ko,
    ADD COLUMN is_macro_tracker BOOLEAN DEFAULT FALSE AFTER indicator_name_en,
    ADD COLUMN display_order INT NULL AFTER is_macro_tracker,
    ADD COLUMN is_active BOOLEAN DEFAULT TRUE AFTER display_order;


/* =========================================================
   C. release_meta
   - 발표 종류 정의
   - 예: Employment Situation, CPI, Retail Sales
   ========================================================= */
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


/* =========================================================
   D. series_release_map
   - 어떤 시계열이 어떤 발표와 연결되는지 저장
   ========================================================= */
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


/* =========================================================
   E. indicator_release_event
   - 실제 발표 이벤트
   - previous 개정 추적을 위해 revision_of_event_id 추가
   ========================================================= */
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


/* =========================================================
   F. indicator_release_value
   - actual / expected / previous 저장
   - expected 크롤링 source / captured_at / URL / official 여부 반영
   ========================================================= */
CREATE TABLE IF NOT EXISTS indicator_release_value (
    value_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id BIGINT NOT NULL,
    series_id INT NOT NULL,

    value_type VARCHAR(30) NOT NULL,          -- actual / expected / previous
    reference_period DATE NULL,               -- 선택: value가 가리키는 기준시점
    value_num DECIMAL(20,6) NULL,
    value_text VARCHAR(100) NULL,
    unit_override VARCHAR(50) NULL,

    source_id INT NULL,                       -- FRED / INVESTING_WEB / 기타
    source_value_key VARCHAR(150) NULL,       -- 원천 페이지 row key 등
    captured_at DATETIME NULL,                -- expected 수집 시각
    source_url VARCHAR(255) NULL,
    is_official BOOLEAN DEFAULT TRUE,         -- actual은 TRUE, 웹 expected는 FALSE 가능

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


/* =========================================================
   G. expected_value_snapshot
   - 선택이지만 강력 추천
   - expected가 하루에 여러 번 바뀌는 경우 raw snapshot 저장
   - indicator_release_value는 최종 반영값
   - snapshot은 수집 이력
   ========================================================= */
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


/* =========================================================
   H. indicator_calc_rule
   - macro_tracker_service.py RULE_MAP를 DB화
   - 자산 반응 기준 포함
   ========================================================= */
CREATE TABLE IF NOT EXISTS indicator_calc_rule (
    calc_rule_id INT AUTO_INCREMENT PRIMARY KEY,
    indicator_code VARCHAR(100) NOT NULL UNIQUE,
    country_code VARCHAR(10) NOT NULL,
    macro_category VARCHAR(100) NULL,
    series_id INT NOT NULL,
    display_name_ko VARCHAR(255) NOT NULL,
    display_order INT NULL,

    calc_basis VARCHAR(30) NOT NULL,          -- level / mom_diff / yoy_diff / custom
    comparison_lag INT NULL,
    comparison_unit VARCHAR(30) NULL,         -- release / month / quarter

    speed_calc_method VARCHAR(50) NULL,
    trend_calc_method VARCHAR(50) NULL,
    policy_phase_method VARCHAR(50) NULL,

    asset_stock_series_code VARCHAR(100) NULL,
    asset_bond_series_code VARCHAR(100) NULL,
    asset_fx_series_code VARCHAR(100) NULL,

    stock_reaction_method VARCHAR(50) NULL,   -- close_pct_vs_prev_close
    bond_reaction_method VARCHAR(50) NULL,    -- close_bp_vs_prev_close
    fx_reaction_method VARCHAR(50) NULL,      -- close_pct_vs_prev_close

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


/* =========================================================
   I. release_meta seed
   - source_id는 data_source에서 FRED를 찾아 사용
   ========================================================= */
INSERT INTO release_meta
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
  ON ds.source_code = 'FRED'
WHERE NOT EXISTS (
    SELECT 1
    FROM release_meta rm
    WHERE rm.release_code = x.release_code
);


/* =========================================================
   J. 인덱스 보강
   - macro tracker 조회 성능용
   ========================================================= */
CREATE INDEX idx_series_meta_macro_tracker
    ON series_meta (is_macro_tracker, country_code, macro_category, display_order);

CREATE INDEX idx_series_meta_indicator_code
    ON series_meta (indicator_code);

CREATE INDEX idx_series_data_series_date
    ON series_data (series_id, date_value);


/* =========================================================
   K. indicator_calc_rule 예시 insert
   - 실제 series_meta 적재 후 실행
   - 예: UNRATE
   ========================================================= */
/*
INSERT INTO indicator_calc_rule
(
    indicator_code,
    country_code,
    macro_category,
    series_id,
    display_name_ko,
    display_order,
    calc_basis,
    comparison_lag,
    comparison_unit,
    speed_calc_method,
    trend_calc_method,
    policy_phase_method,
    asset_stock_series_code,
    asset_bond_series_code,
    asset_fx_series_code,
    stock_reaction_method,
    bond_reaction_method,
    fx_reaction_method,
    use_expected_value,
    use_previous_value,
    supports_revision,
    is_active
)
SELECT
    'US_UNRATE',
    'US',
    '고용',
    sm.series_id,
    '실업률',
    20,
    'level',
    1,
    'release',
    'delta_abs',
    'direction_3pt',
    'rule_based',
    'SP500',
    'US2Y',
    'DXY',
    'close_pct_vs_prev_close',
    'close_bp_vs_prev_close',
    'close_pct_vs_prev_close',
    TRUE,
    TRUE,
    TRUE,
    TRUE
FROM series_meta sm
WHERE sm.series_code = 'UNRATE';
*/