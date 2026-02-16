-- ============================================================
-- Construction Market Intelligence Platform
-- PostgreSQL 15+ Schema
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- 1. COMPANIES (Central entity)
-- ============================================================
CREATE TABLE companies (
    stir            VARCHAR(9) PRIMARY KEY,
    canonical_name  VARCHAR(500) NOT NULL,
    raw_names       JSONB DEFAULT '[]'::jsonb,

    -- From reyting.mc.uz
    region              VARCHAR(100),
    rating_letter       VARCHAR(5),
    rating_score        NUMERIC(6,2),
    employee_count      INTEGER,
    specialist_count    INTEGER,

    -- Aggregated from tender_results (set by enrichment pipeline)
    total_wins              INTEGER DEFAULT 0,
    total_contract_value    NUMERIC(18,2) DEFAULT 0,
    avg_discount_pct        NUMERIC(5,2),
    first_tender_date       DATE,
    last_tender_date        DATE,
    active_regions          JSONB DEFAULT '[]'::jsonb,

    -- Classification (set by enrichment pipeline)
    company_type        VARCHAR(30) DEFAULT 'unknown',

    -- Metadata
    first_seen_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    rating_fetched_at   TIMESTAMPTZ,
    source              VARCHAR(50)
);

CREATE INDEX idx_companies_rating_score          ON companies (rating_score DESC NULLS LAST);
CREATE INDEX idx_companies_rating_letter         ON companies (rating_letter);
CREATE INDEX idx_companies_total_wins            ON companies (total_wins DESC);
CREATE INDEX idx_companies_total_contract_value  ON companies (total_contract_value DESC);
CREATE INDEX idx_companies_region                ON companies (region);
CREATE INDEX idx_companies_type                  ON companies (company_type);
CREATE INDEX idx_companies_name_trgm             ON companies USING gin (canonical_name gin_trgm_ops);


-- ============================================================
-- 2. TENDER_RESULTS (from etender.uzex.uz)
-- ============================================================
CREATE TABLE tender_results (
    id                  SERIAL PRIMARY KEY,
    deal_id             BIGINT UNIQUE NOT NULL,

    -- Financial
    start_cost          NUMERIC(18,2),
    deal_cost           NUMERIC(18,2),
    discount_pct        NUMERIC(5,2) GENERATED ALWAYS AS (
                            CASE WHEN start_cost > 0
                                 THEN ROUND((start_cost - deal_cost) / start_cost * 100, 2)
                                 ELSE 0
                            END
                        ) STORED,

    -- Parties
    customer_name       VARCHAR(500),
    provider_stir       VARCHAR(9),
    provider_name       VARCHAR(500),

    -- Deal metadata
    deal_date           DATE,
    deal_description    TEXT,
    participants_count  INTEGER,
    region              VARCHAR(100),

    -- Raw data preservation
    raw_data            JSONB,

    scraped_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_provider
        FOREIGN KEY (provider_stir) REFERENCES companies (stir) ON DELETE SET NULL
);

CREATE INDEX idx_tender_deal_date       ON tender_results (deal_date DESC);
CREATE INDEX idx_tender_provider        ON tender_results (provider_stir);
CREATE INDEX idx_tender_customer        ON tender_results (customer_name);
CREATE INDEX idx_tender_deal_cost       ON tender_results (deal_cost DESC);
CREATE INDEX idx_tender_region          ON tender_results (region);
CREATE INDEX idx_tender_desc_trgm       ON tender_results USING gin (deal_description gin_trgm_ops);


-- ============================================================
-- 3. RATING_CATEGORIES (reference: 6 categories)
-- ============================================================
CREATE TABLE rating_categories (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(50) UNIQUE NOT NULL,
    name_uz         VARCHAR(200) NOT NULL,
    name_ru         VARCHAR(200),
    display_order   INTEGER NOT NULL,
    max_possible_score NUMERIC(6,2)
);


-- ============================================================
-- 4. RATING_CRITERIA (reference: 71 indicators)
-- ============================================================
CREATE TABLE rating_criteria (
    id              SERIAL PRIMARY KEY,
    category_id     INTEGER NOT NULL REFERENCES rating_categories (id),
    code            VARCHAR(200) UNIQUE,
    name_uz         TEXT NOT NULL,
    name_ru         TEXT,
    source_agency   TEXT,
    max_points      NUMERIC(6,2),
    display_order   INTEGER
);

CREATE INDEX idx_criteria_category ON rating_criteria (category_id);


-- ============================================================
-- 5. COMPANY_RATINGS (EAV: one row per company per criterion)
-- ============================================================
CREATE TABLE company_ratings (
    id              SERIAL PRIMARY KEY,
    company_stir    VARCHAR(9) NOT NULL REFERENCES companies (stir),
    criterion_id    INTEGER NOT NULL REFERENCES rating_criteria (id),

    raw_value       TEXT,
    earned_points   NUMERIC(6,2),
    max_points      NUMERIC(6,2),

    rating_date     DATE,
    scraped_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_company_criterion_date
        UNIQUE (company_stir, criterion_id, rating_date)
);

CREATE INDEX idx_company_ratings_stir      ON company_ratings (company_stir);
CREATE INDEX idx_company_ratings_criterion ON company_ratings (criterion_id);
CREATE INDEX idx_company_ratings_earned    ON company_ratings (earned_points DESC);


-- ============================================================
-- 6. COMPANY_RATING_SNAPSHOTS (full JSONB backup)
-- ============================================================
CREATE TABLE company_rating_snapshots (
    id              SERIAL PRIMARY KEY,
    company_stir    VARCHAR(9) NOT NULL REFERENCES companies (stir),
    rating_date     DATE,

    rating_letter   VARCHAR(5),
    total_score     NUMERIC(6,2),

    categories_json JSONB,
    indicators_json JSONB,
    raw_html        TEXT,

    scraped_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_snapshot_stir_date
        UNIQUE (company_stir, rating_date)
);

CREATE INDEX idx_snapshots_stir ON company_rating_snapshots (company_stir);


-- ============================================================
-- 7. SCRAPE_LOGS (pipeline execution tracking)
-- ============================================================
CREATE TABLE scrape_logs (
    id                  SERIAL PRIMARY KEY,
    source              VARCHAR(50) NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    status              VARCHAR(20) NOT NULL DEFAULT 'running',

    records_found       INTEGER DEFAULT 0,
    records_inserted    INTEGER DEFAULT 0,
    records_updated     INTEGER DEFAULT 0,
    records_skipped     INTEGER DEFAULT 0,
    records_failed      INTEGER DEFAULT 0,

    last_page_scraped   INTEGER,
    error_message       TEXT,
    details             JSONB
);

CREATE INDEX idx_scrape_logs_source ON scrape_logs (source, started_at DESC);


-- ============================================================
-- 8. VIEWS
-- ============================================================

-- Company leaderboard
CREATE OR REPLACE VIEW v_company_leaderboard AS
SELECT
    c.stir,
    c.canonical_name,
    c.region,
    c.rating_letter,
    c.rating_score,
    c.total_wins,
    c.total_contract_value,
    c.avg_discount_pct,
    c.employee_count,
    c.specialist_count,
    RANK() OVER (ORDER BY c.total_wins DESC)            AS rank_by_wins,
    RANK() OVER (ORDER BY c.total_contract_value DESC)  AS rank_by_volume,
    RANK() OVER (ORDER BY c.rating_score DESC NULLS LAST) AS rank_by_rating
FROM companies c
WHERE c.total_wins > 0;

-- Recent tenders (last 12 months)
CREATE OR REPLACE VIEW v_recent_tenders AS
SELECT
    t.deal_id,
    t.deal_date,
    t.provider_stir,
    t.provider_name,
    c.canonical_name,
    c.rating_letter,
    t.customer_name,
    t.start_cost,
    t.deal_cost,
    t.discount_pct,
    t.participants_count,
    t.deal_description,
    t.region
FROM tender_results t
LEFT JOIN companies c ON t.provider_stir = c.stir
WHERE t.deal_date >= CURRENT_DATE - INTERVAL '12 months';
