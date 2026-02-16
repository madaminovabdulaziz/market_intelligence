-- Migration 001: Add company_type column for filtering non-construction companies
-- from competitor analysis.
--
-- Values: 'contractor', 'consultant', 'laboratory', 'assessor', 'supplier', 'other', 'unknown'
-- Default: 'unknown' — classification is done by the enrichment pipeline.

ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_type VARCHAR(30) DEFAULT 'unknown';

CREATE INDEX IF NOT EXISTS idx_companies_type ON companies (company_type);
