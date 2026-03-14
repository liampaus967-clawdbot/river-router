-- Flow Percentiles Schema
-- Historical flow analysis for Vermont river reaches

-- ============================================
-- RAW HISTORY TABLE
-- Stores daily flow/velocity averages per comid
-- ~51M rows for 10 years of Vermont data
-- ============================================

CREATE TABLE IF NOT EXISTS flow_history (
    id BIGSERIAL PRIMARY KEY,
    comid BIGINT NOT NULL,
    date DATE NOT NULL,
    year INT NOT NULL,
    week_of_year INT NOT NULL,  -- 1-52 for weekly aggregation
    day_of_year INT NOT NULL,   -- 1-365 for daily lookup
    
    -- Flow metrics (daily average from NWM)
    streamflow_cms FLOAT,       -- cubic meters per second
    velocity_ms FLOAT,          -- meters per second
    
    -- Metadata
    source VARCHAR(20) DEFAULT 'nwm_retrospective',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(comid, date)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_fh_comid ON flow_history(comid);
CREATE INDEX IF NOT EXISTS idx_fh_date ON flow_history(date);
CREATE INDEX IF NOT EXISTS idx_fh_comid_week ON flow_history(comid, week_of_year);
CREATE INDEX IF NOT EXISTS idx_fh_comid_doy ON flow_history(comid, day_of_year);
CREATE INDEX IF NOT EXISTS idx_fh_year ON flow_history(year);


-- ============================================
-- PERCENTILES TABLE
-- Pre-computed percentile breakpoints per comid per week
-- ~730K rows for Vermont (14K comids × 52 weeks)
-- ============================================

CREATE TABLE IF NOT EXISTS flow_percentiles (
    comid BIGINT NOT NULL,
    week_of_year INT NOT NULL,  -- 1-52
    
    -- Flow percentiles (cms)
    flow_min FLOAT,
    flow_p05 FLOAT,
    flow_p10 FLOAT,
    flow_p25 FLOAT,
    flow_p50 FLOAT,  -- median
    flow_p75 FLOAT,
    flow_p90 FLOAT,
    flow_p95 FLOAT,
    flow_max FLOAT,
    flow_mean FLOAT,
    flow_stddev FLOAT,
    
    -- Velocity percentiles (m/s)
    vel_min FLOAT,
    vel_p05 FLOAT,
    vel_p10 FLOAT,
    vel_p25 FLOAT,
    vel_p50 FLOAT,
    vel_p75 FLOAT,
    vel_p90 FLOAT,
    vel_p95 FLOAT,
    vel_max FLOAT,
    vel_mean FLOAT,
    vel_stddev FLOAT,
    
    -- Metadata
    sample_count INT,           -- number of daily samples in this week
    sample_years INT,           -- number of years of data
    date_range_start DATE,      -- earliest date in sample
    date_range_end DATE,        -- latest date in sample
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (comid, week_of_year)
);

-- Index for joining with current data
CREATE INDEX IF NOT EXISTS idx_fp_comid ON flow_percentiles(comid);


-- ============================================
-- HELPER VIEW: Current Flow with Percentile
-- Joins river_velocities with percentiles for current week
-- ============================================

CREATE OR REPLACE VIEW current_flow_percentiles AS
SELECT 
    rv.comid,
    rv.gnis_name,
    rv.streamflow_cms AS current_flow,
    rv.velocity_ms AS current_velocity,
    rv.nwm_updated_at,
    
    -- Flow percentile context
    fp.flow_p10,
    fp.flow_p25,
    fp.flow_p50,
    fp.flow_p75,
    fp.flow_p90,
    fp.flow_mean,
    
    -- Calculate flow category
    CASE 
        WHEN rv.streamflow_cms IS NULL THEN 'Unknown'
        WHEN rv.streamflow_cms <= fp.flow_p10 THEN 'Very Low'
        WHEN rv.streamflow_cms <= fp.flow_p25 THEN 'Low'
        WHEN rv.streamflow_cms <= fp.flow_p75 THEN 'Normal'
        WHEN rv.streamflow_cms <= fp.flow_p90 THEN 'High'
        ELSE 'Very High'
    END AS flow_category,
    
    -- Approximate percentile (linear interpolation)
    CASE
        WHEN rv.streamflow_cms IS NULL THEN NULL
        WHEN rv.streamflow_cms <= fp.flow_min THEN 0
        WHEN rv.streamflow_cms >= fp.flow_max THEN 100
        ELSE ROUND(
            (rv.streamflow_cms - fp.flow_min) / 
            NULLIF(fp.flow_max - fp.flow_min, 0) * 100
        )::INT
    END AS flow_percentile,
    
    -- Velocity context
    fp.vel_p50 AS median_velocity,
    
    -- How many years of history
    fp.sample_years,
    
    rv.geom
    
FROM river_velocities rv
LEFT JOIN flow_percentiles fp 
    ON rv.comid = fp.comid 
    AND fp.week_of_year = EXTRACT(WEEK FROM NOW())::INT;


-- ============================================
-- HELPER FUNCTION: Get percentile for a value
-- More accurate percentile calculation
-- ============================================

CREATE OR REPLACE FUNCTION calculate_flow_percentile(
    current_flow FLOAT,
    p05 FLOAT, p10 FLOAT, p25 FLOAT, p50 FLOAT, 
    p75 FLOAT, p90 FLOAT, p95 FLOAT
) RETURNS INT AS $$
BEGIN
    IF current_flow IS NULL THEN RETURN NULL; END IF;
    IF current_flow <= p05 THEN RETURN 5; END IF;
    IF current_flow <= p10 THEN RETURN 5 + (current_flow - p05) / NULLIF(p10 - p05, 0) * 5; END IF;
    IF current_flow <= p25 THEN RETURN 10 + (current_flow - p10) / NULLIF(p25 - p10, 0) * 15; END IF;
    IF current_flow <= p50 THEN RETURN 25 + (current_flow - p25) / NULLIF(p50 - p25, 0) * 25; END IF;
    IF current_flow <= p75 THEN RETURN 50 + (current_flow - p50) / NULLIF(p75 - p50, 0) * 25; END IF;
    IF current_flow <= p90 THEN RETURN 75 + (current_flow - p75) / NULLIF(p90 - p75, 0) * 15; END IF;
    IF current_flow <= p95 THEN RETURN 90 + (current_flow - p90) / NULLIF(p95 - p90, 0) * 5; END IF;
    RETURN 95;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- ============================================
-- SAMPLE QUERY: Vermont rivers with percentiles
-- ============================================

-- SELECT 
--     comid, gnis_name, current_flow, flow_category, flow_percentile
-- FROM current_flow_percentiles
-- WHERE ST_Intersects(geom, ST_MakeEnvelope(-73.5, 42.7, -71.5, 45.1, 4326))
-- ORDER BY flow_percentile DESC
-- LIMIT 20;
