-- ============================================================================
-- RIVER REACH PROFILES - Denormalized metrics table for LLM chatbot
-- ============================================================================
-- Purpose: Pre-compute river reach metrics, POI associations, and difficulty
--          scores to optimize LLM queries (reduce joins, save tokens)
-- 
-- Run order:
--   1. Create tables and functions (this file)
--   2. Run initial population: CALL populate_river_reach_profiles();
--   3. Schedule refresh jobs (hourly for flow, daily for POIs)
-- ============================================================================

-- Drop existing objects if rebuilding
DROP TABLE IF EXISTS us.river_reach_profiles CASCADE;
DROP FUNCTION IF EXISTS calculate_difficulty_score CASCADE;
DROP FUNCTION IF EXISTS parse_rapid_class CASCADE;
DROP FUNCTION IF EXISTS calculate_paddling_score CASCADE;
DROP FUNCTION IF EXISTS calculate_beginner_score CASCADE;

-- ============================================================================
-- MAIN TABLE
-- ============================================================================
CREATE TABLE us.river_reach_profiles (
    comid BIGINT PRIMARY KEY,
    
    -- ========================================================================
    -- CORE RIVER INFO (from river_edges)
    -- ========================================================================
    gnis_name TEXT,
    state TEXT,
    region TEXT,
    stream_order INT,
    length_km NUMERIC(10,3),
    
    -- Elevation & gradient
    min_elev_m NUMERIC(10,2),
    max_elev_m NUMERIC(10,2),
    total_drop_m NUMERIC(10,2),
    gradient_m_per_km NUMERIC(10,4),
    avg_slope NUMERIC(10,6),
    
    -- Location (centroid for proximity searches)
    centroid_lat NUMERIC(10,6),
    centroid_lon NUMERIC(10,6),
    centroid_geom GEOMETRY(Point, 4326),
    
    -- ========================================================================
    -- REAL-TIME FLOW (updated hourly from NWM)
    -- ========================================================================
    current_velocity_ms NUMERIC(10,4),
    current_flow_cms NUMERIC(12,4),
    flow_status TEXT,  -- 'very_low', 'low', 'normal', 'high', 'flood'
    flow_updated_at TIMESTAMPTZ,
    
    -- ========================================================================
    -- HISTORICAL FLOW CONTEXT (from nearest gauge)
    -- ========================================================================
    gauge_site_no TEXT,
    gauge_name TEXT,
    gauge_distance_m NUMERIC(10,2),
    p10 NUMERIC(12,4),  -- 10th percentile (low flow)
    p25 NUMERIC(12,4),  -- 25th percentile
    p50 NUMERIC(12,4),  -- Median flow
    p75 NUMERIC(12,4),  -- 75th percentile
    p90 NUMERIC(12,4),  -- 90th percentile (high flow)
    
    -- ========================================================================
    -- POI COUNTS (for fast filtering)
    -- ========================================================================
    access_point_count INT DEFAULT 0,
    campground_count INT DEFAULT 0,
    dam_count INT DEFAULT 0,
    rapid_count INT DEFAULT 0,
    waterfall_count INT DEFAULT 0,
    portage_count INT DEFAULT 0,
    
    -- ========================================================================
    -- POI DETAILS (JSONB for flexibility)
    -- ========================================================================
    -- Each array contains objects: {name, type, lat, lon, distance_m, ...}
    access_points JSONB DEFAULT '[]'::jsonb,
    campgrounds JSONB DEFAULT '[]'::jsonb,
    dams JSONB DEFAULT '[]'::jsonb,
    rapids JSONB DEFAULT '[]'::jsonb,
    waterfalls JSONB DEFAULT '[]'::jsonb,
    
    -- ========================================================================
    -- DIFFICULTY SCORING INPUTS
    -- ========================================================================
    max_rapid_class INT DEFAULT 0,        -- 0-6 (Class I-VI)
    rapid_density NUMERIC(10,4) DEFAULT 0, -- rapids per km
    
    -- ========================================================================
    -- COMPUTED SCORES
    -- ========================================================================
    -- Difficulty (1.00 - 5.00)
    difficulty_score NUMERIC(4,2),
    difficulty_level INT,              -- 1-5 rounded
    difficulty_label TEXT,             -- 'Flatwater', 'Easy', 'Moderate', 'Difficult', 'Expert'
    difficulty_factors JSONB,          -- Breakdown of scoring factors
    
    -- Activity scores (1.00 - 5.00, higher = better for activity)
    paddling_score NUMERIC(4,2),
    fishing_score NUMERIC(4,2),
    scenery_score NUMERIC(4,2),
    beginner_friendly_score NUMERIC(4,2),
    
    -- ========================================================================
    -- SEARCH OPTIMIZATION
    -- ========================================================================
    search_text TSVECTOR,              -- Full-text search index
    
    -- ========================================================================
    -- METADATA
    -- ========================================================================
    created_at TIMESTAMPTZ DEFAULT NOW(),
    profile_updated_at TIMESTAMPTZ DEFAULT NOW(),
    pois_updated_at TIMESTAMPTZ,
    difficulty_updated_at TIMESTAMPTZ
);

-- ============================================================================
-- INDEXES
-- ============================================================================
CREATE INDEX idx_rrp_name_search ON us.river_reach_profiles USING gin(search_text);
CREATE INDEX idx_rrp_state ON us.river_reach_profiles(state);
CREATE INDEX idx_rrp_stream_order ON us.river_reach_profiles(stream_order);
CREATE INDEX idx_rrp_difficulty ON us.river_reach_profiles(difficulty_level);
CREATE INDEX idx_rrp_paddling ON us.river_reach_profiles(paddling_score DESC NULLS LAST);
CREATE INDEX idx_rrp_beginner ON us.river_reach_profiles(beginner_friendly_score DESC NULLS LAST);
CREATE INDEX idx_rrp_location ON us.river_reach_profiles USING gist(centroid_geom);
CREATE INDEX idx_rrp_flow_status ON us.river_reach_profiles(flow_status);
CREATE INDEX idx_rrp_access_count ON us.river_reach_profiles(access_point_count) WHERE access_point_count > 0;

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Parse rapid class string to integer
CREATE OR REPLACE FUNCTION parse_rapid_class(class_text TEXT) 
RETURNS INT AS $$
BEGIN
    RETURN CASE UPPER(TRIM(COALESCE(class_text, '')))
        WHEN 'I' THEN 1
        WHEN 'I+' THEN 1
        WHEN 'II' THEN 2
        WHEN 'II+' THEN 2
        WHEN 'II-III' THEN 2
        WHEN 'III' THEN 3
        WHEN 'III+' THEN 3
        WHEN 'III-IV' THEN 3
        WHEN 'IV' THEN 4
        WHEN 'IV+' THEN 4
        WHEN 'IV-V' THEN 4
        WHEN 'V' THEN 5
        WHEN 'V+' THEN 5
        WHEN 'VI' THEN 6
        ELSE 0
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- DIFFICULTY SCORING FUNCTION
-- ============================================================================
-- Weights calibrated to American Whitewater International Scale:
--   Class I:  Easy - Fast moving water with riffles and small waves
--   Class II: Novice - Straightforward rapids with wide, clear channels
--   Class III: Intermediate - Rapids with moderate, irregular waves
--   Class IV: Advanced - Intense, powerful rapids requiring precise handling
--   Class V:  Expert - Extremely long, obstructed, or very violent rapids
--   Class VI: Extreme - Nearly impossible, very dangerous
--
-- Weight justification (based on whitewater safety literature):
--   - Rapid class is primary indicator (40%) - direct correlation to skill needed
--   - Hazards critical for safety (25%) - waterfalls/dams are binary dangers
--   - Gradient indicates water speed (20%) - steeper = faster = harder
--   - Rapid density for sustained difficulty (15%) - pool-drop vs continuous
-- ============================================================================
CREATE OR REPLACE FUNCTION calculate_difficulty_score(
    p_max_rapid_class INT,
    p_rapid_density NUMERIC,
    p_waterfall_count INT,
    p_dam_count INT,
    p_gradient_m_per_km NUMERIC,
    p_total_drop_m NUMERIC
) RETURNS JSONB AS $$
DECLARE
    -- Weight configuration (tune these based on real-world validation)
    W_RAPID_CLASS CONSTANT NUMERIC := 0.40;
    W_HAZARDS CONSTANT NUMERIC := 0.25;
    W_GRADIENT CONSTANT NUMERIC := 0.20;
    W_DENSITY CONSTANT NUMERIC := 0.15;
    
    v_rapid_score NUMERIC;
    v_density_score NUMERIC;
    v_hazard_score NUMERIC;
    v_gradient_score NUMERIC;
    v_final_score NUMERIC;
    v_level INT;
    v_label TEXT;
BEGIN
    -- 1. RAPID CLASS SCORE (0-5)
    -- Maps International Scale to our 1-5 difficulty
    v_rapid_score := CASE COALESCE(p_max_rapid_class, 0)
        WHEN 0 THEN 1.0    -- No rapids = flatwater
        WHEN 1 THEN 1.8    -- Class I
        WHEN 2 THEN 2.6    -- Class II
        WHEN 3 THEN 3.5    -- Class III
        WHEN 4 THEN 4.3    -- Class IV
        WHEN 5 THEN 4.8    -- Class V
        ELSE 5.0           -- Class VI
    END;
    
    -- 2. RAPID DENSITY SCORE
    -- 0/km = 1.0, 0.5/km = 2.0, 1.0/km = 3.0, 2.0/km = 4.0, 3+/km = 5.0
    v_density_score := LEAST(5.0, GREATEST(1.0, 
        1.0 + (COALESCE(p_rapid_density, 0) * 1.33)
    ));
    
    -- 3. HAZARD SCORE (waterfalls & dams)
    -- Each waterfall: +1.5 (often unrunnable)
    -- Each dam: +1.0 (portage required, hydraulic danger)
    -- Minimum 1.0, capped at 5.0
    v_hazard_score := LEAST(5.0, GREATEST(1.0,
        1.0 + 
        (COALESCE(p_waterfall_count, 0) * 1.5) + 
        (COALESCE(p_dam_count, 0) * 1.0)
    ));
    
    -- 4. GRADIENT SCORE
    -- Based on standard whitewater gradient classifications:
    --   < 1 m/km:  Flatwater (1.0)
    --   1-5 m/km:  Easy (2.0)
    --   5-15 m/km: Moderate (3.0)
    --   15-30 m/km: Steep (4.0)
    --   > 30 m/km: Very Steep (5.0)
    v_gradient_score := CASE
        WHEN COALESCE(p_gradient_m_per_km, 0) < 1 THEN 1.0
        WHEN p_gradient_m_per_km < 5 THEN 1.5 + (p_gradient_m_per_km / 10)
        WHEN p_gradient_m_per_km < 15 THEN 2.5 + ((p_gradient_m_per_km - 5) / 20)
        WHEN p_gradient_m_per_km < 30 THEN 3.5 + ((p_gradient_m_per_km - 15) / 30)
        ELSE 5.0
    END;
    
    -- WEIGHTED FINAL SCORE
    v_final_score := (
        (v_rapid_score * W_RAPID_CLASS) +
        (v_density_score * W_DENSITY) +
        (v_hazard_score * W_HAZARDS) +
        (v_gradient_score * W_GRADIENT)
    );
    
    -- Ensure bounds [1.0, 5.0]
    v_final_score := GREATEST(1.0, LEAST(5.0, v_final_score));
    
    -- Round to level (1-5)
    v_level := ROUND(v_final_score)::INT;
    
    -- Human-readable label
    v_label := CASE v_level
        WHEN 1 THEN 'Flatwater'
        WHEN 2 THEN 'Easy'
        WHEN 3 THEN 'Moderate'
        WHEN 4 THEN 'Difficult'
        WHEN 5 THEN 'Expert'
    END;
    
    RETURN jsonb_build_object(
        'score', ROUND(v_final_score, 2),
        'level', v_level,
        'label', v_label,
        'factors', jsonb_build_object(
            'rapid_class', jsonb_build_object(
                'input', p_max_rapid_class, 
                'score', ROUND(v_rapid_score, 2), 
                'weight', W_RAPID_CLASS
            ),
            'rapid_density', jsonb_build_object(
                'input', ROUND(COALESCE(p_rapid_density, 0), 2), 
                'score', ROUND(v_density_score, 2), 
                'weight', W_DENSITY
            ),
            'hazards', jsonb_build_object(
                'waterfalls', COALESCE(p_waterfall_count, 0), 
                'dams', COALESCE(p_dam_count, 0), 
                'score', ROUND(v_hazard_score, 2), 
                'weight', W_HAZARDS
            ),
            'gradient', jsonb_build_object(
                'input_m_per_km', ROUND(COALESCE(p_gradient_m_per_km, 0), 2), 
                'score', ROUND(v_gradient_score, 2), 
                'weight', W_GRADIENT
            )
        ),
        'weights_version', '1.0'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- ACTIVITY SCORING FUNCTIONS
-- ============================================================================

-- Paddling score: Higher = better for paddling
CREATE OR REPLACE FUNCTION calculate_paddling_score(
    p_access_count INT,
    p_difficulty_level INT,
    p_flow_status TEXT,
    p_length_km NUMERIC,
    p_campground_count INT
) RETURNS NUMERIC AS $$
DECLARE
    v_score NUMERIC := 2.5;  -- Start at middle
BEGIN
    -- Access points: Need at least 2 for a proper run
    IF COALESCE(p_access_count, 0) >= 2 THEN
        v_score := v_score + 1.0;
    ELSIF p_access_count = 1 THEN
        v_score := v_score + 0.3;
    ELSE
        v_score := v_score - 1.0;
    END IF;
    
    -- Flow status: Normal/high is best
    v_score := v_score + CASE COALESCE(p_flow_status, 'unknown')
        WHEN 'normal' THEN 0.5
        WHEN 'high' THEN 0.3
        WHEN 'low' THEN -0.3
        WHEN 'very_low' THEN -0.8
        WHEN 'flood' THEN -1.0
        ELSE 0
    END;
    
    -- Length: 5-20km is ideal day trip
    IF COALESCE(p_length_km, 0) BETWEEN 5 AND 20 THEN
        v_score := v_score + 0.5;
    ELSIF p_length_km BETWEEN 2 AND 30 THEN
        v_score := v_score + 0.2;
    END IF;
    
    -- Camping nearby: Bonus for multi-day options
    IF COALESCE(p_campground_count, 0) > 0 THEN
        v_score := v_score + 0.3;
    END IF;
    
    RETURN GREATEST(1.0, LEAST(5.0, v_score));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Beginner-friendly score: Higher = more suitable for beginners
CREATE OR REPLACE FUNCTION calculate_beginner_score(
    p_difficulty_level INT,
    p_access_count INT,
    p_flow_status TEXT
) RETURNS NUMERIC AS $$
DECLARE
    v_score NUMERIC := 3.0;
BEGIN
    -- Difficulty: Lower is better for beginners
    v_score := v_score + CASE COALESCE(p_difficulty_level, 3)
        WHEN 1 THEN 1.5
        WHEN 2 THEN 0.8
        WHEN 3 THEN 0
        WHEN 4 THEN -1.0
        ELSE -2.0
    END;
    
    -- Good access is important for beginners
    IF COALESCE(p_access_count, 0) >= 2 THEN
        v_score := v_score + 0.5;
    END IF;
    
    -- Stable flow conditions
    IF COALESCE(p_flow_status, '') IN ('normal', 'low') THEN
        v_score := v_score + 0.3;
    ELSIF p_flow_status IN ('high', 'flood') THEN
        v_score := v_score - 0.5;
    END IF;
    
    RETURN GREATEST(1.0, LEAST(5.0, v_score));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- POPULATION PROCEDURE
-- ============================================================================
-- Call this to initially populate or fully rebuild the profiles table
-- ============================================================================
CREATE OR REPLACE PROCEDURE populate_river_reach_profiles()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMPTZ;
    v_count INT;
BEGIN
    v_start_time := NOW();
    RAISE NOTICE 'Starting river reach profiles population at %', v_start_time;
    
    -- ========================================================================
    -- STEP 1: Insert base river data
    -- ========================================================================
    RAISE NOTICE 'Step 1: Inserting base river data...';
    
    INSERT INTO us.river_reach_profiles (
        comid, gnis_name, state, region, stream_order, length_km,
        min_elev_m, max_elev_m, total_drop_m, gradient_m_per_km, avg_slope,
        centroid_lat, centroid_lon, centroid_geom
    )
    SELECT 
        r.comid,
        r.gnis_name,
        COALESCE(s.stusps, r.region) as state,
        r.region,
        r.stream_order,
        r.lengthkm,
        r.min_elev_m,
        r.max_elev_m,
        r.max_elev_m - r.min_elev_m as total_drop_m,
        CASE WHEN r.lengthkm > 0 
             THEN (r.max_elev_m - r.min_elev_m) / r.lengthkm 
             ELSE 0 END as gradient_m_per_km,
        r.slope,
        ST_Y(ST_Centroid(r.geom)),
        ST_X(ST_Centroid(r.geom)),
        ST_Centroid(r.geom)
    FROM public.river_edges r
    LEFT JOIN us.states s ON ST_Intersects(ST_Centroid(r.geom), s.geom)
    WHERE r.gnis_name IS NOT NULL
      AND r.stream_order >= 2  -- Skip tiny tributaries
    ON CONFLICT (comid) DO UPDATE SET
        gnis_name = EXCLUDED.gnis_name,
        state = EXCLUDED.state,
        stream_order = EXCLUDED.stream_order,
        length_km = EXCLUDED.length_km,
        min_elev_m = EXCLUDED.min_elev_m,
        max_elev_m = EXCLUDED.max_elev_m,
        total_drop_m = EXCLUDED.total_drop_m,
        gradient_m_per_km = EXCLUDED.gradient_m_per_km,
        profile_updated_at = NOW();
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Inserted/updated % river reaches', v_count;
    
    -- ========================================================================
    -- STEP 2: Aggregate POI counts and details
    -- ========================================================================
    RAISE NOTICE 'Step 2: Aggregating POIs...';
    CALL refresh_poi_associations();
    
    -- ========================================================================
    -- STEP 3: Link gauges and flow percentiles
    -- ========================================================================
    RAISE NOTICE 'Step 3: Linking gauges...';
    CALL refresh_gauge_associations();
    
    -- ========================================================================
    -- STEP 4: Update flow data from NWM
    -- ========================================================================
    RAISE NOTICE 'Step 4: Updating flow data...';
    CALL refresh_flow_data();
    
    -- ========================================================================
    -- STEP 5: Calculate difficulty scores
    -- ========================================================================
    RAISE NOTICE 'Step 5: Calculating difficulty scores...';
    CALL refresh_difficulty_scores();
    
    -- ========================================================================
    -- STEP 6: Calculate activity scores
    -- ========================================================================
    RAISE NOTICE 'Step 6: Calculating activity scores...';
    CALL refresh_activity_scores();
    
    -- ========================================================================
    -- STEP 7: Build search index
    -- ========================================================================
    RAISE NOTICE 'Step 7: Building search index...';
    UPDATE us.river_reach_profiles
    SET search_text = to_tsvector('english', 
        COALESCE(gnis_name, '') || ' ' ||
        COALESCE(state, '') || ' ' ||
        COALESCE(difficulty_label, '')
    );
    
    RAISE NOTICE 'Population complete in %', NOW() - v_start_time;
END;
$$;

-- ============================================================================
-- REFRESH PROCEDURES (for scheduled jobs)
-- ============================================================================

-- Refresh POI associations (run daily)
CREATE OR REPLACE PROCEDURE refresh_poi_associations()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Access points (within 2km of river centroid)
    UPDATE us.river_reach_profiles rp
    SET 
        access_point_count = sub.cnt,
        access_points = sub.details,
        pois_updated_at = NOW()
    FROM (
        SELECT 
            r.comid,
            COUNT(a.id) as cnt,
            COALESCE(jsonb_agg(jsonb_build_object(
                'id', a.id,
                'name', a.name,
                'type', a.access_type,
                'lat', a.lat,
                'lon', a.lon
            ) ORDER BY a.name) FILTER (WHERE a.id IS NOT NULL), '[]'::jsonb) as details
        FROM us.river_reach_profiles r
        LEFT JOIN us.access_points a ON ST_DWithin(
            r.centroid_geom::geography,
            a.geom::geography,
            2000  -- 2km radius
        )
        GROUP BY r.comid
    ) sub
    WHERE rp.comid = sub.comid;
    
    -- Campgrounds (within 5km)
    UPDATE us.river_reach_profiles rp
    SET campground_count = sub.cnt, campgrounds = sub.details
    FROM (
        SELECT 
            r.comid,
            COUNT(c.id) as cnt,
            COALESCE(jsonb_agg(jsonb_build_object(
                'id', c.id,
                'name', c.name,
                'type', c.tourism,
                'near_water', c.near_water,
                'lat', c.lat,
                'lon', c.lon
            ) ORDER BY c.name) FILTER (WHERE c.id IS NOT NULL), '[]'::jsonb) as details
        FROM us.river_reach_profiles r
        LEFT JOIN us.campgrounds c ON ST_DWithin(
            r.centroid_geom::geography,
            c.geom::geography,
            5000
        )
        GROUP BY r.comid
    ) sub
    WHERE rp.comid = sub.comid;
    
    -- Dams (linked by nearest_comid)
    UPDATE us.river_reach_profiles rp
    SET dam_count = sub.cnt, dams = sub.details
    FROM (
        SELECT 
            r.comid,
            COUNT(d.id) as cnt,
            COALESCE(jsonb_agg(jsonb_build_object(
                'id', d.id,
                'name', d.dam_name,
                'hazard', d.hazard_potential,
                'height_ft', d.dam_height_ft,
                'lat', d.latitude,
                'lon', d.longitude
            ) ORDER BY d.dam_name) FILTER (WHERE d.id IS NOT NULL), '[]'::jsonb) as details
        FROM us.river_reach_profiles r
        LEFT JOIN us.dams d ON d.nearest_comid = r.comid
        GROUP BY r.comid
    ) sub
    WHERE rp.comid = sub.comid;
    
    -- Rapids (linked by nearest_comid)
    UPDATE us.river_reach_profiles rp
    SET rapid_count = sub.cnt, rapids = sub.details
    FROM (
        SELECT 
            r.comid,
            COUNT(rp2.id) as cnt,
            COALESCE(jsonb_agg(jsonb_build_object(
                'id', rp2.id,
                'name', rp2.name,
                'class', rp2.rapid_class,
                'lat', rp2.lat,
                'lon', rp2.lon
            ) ORDER BY rp2.name) FILTER (WHERE rp2.id IS NOT NULL), '[]'::jsonb) as details
        FROM us.river_reach_profiles r
        LEFT JOIN us.rapids rp2 ON rp2.nearest_comid = r.comid
        GROUP BY r.comid
    ) sub
    WHERE rp.comid = sub.comid;
    
    -- Waterfalls (linked by nearest_comid)
    UPDATE us.river_reach_profiles rp
    SET waterfall_count = sub.cnt, waterfalls = sub.details
    FROM (
        SELECT 
            r.comid,
            COUNT(w.id) as cnt,
            COALESCE(jsonb_agg(jsonb_build_object(
                'id', w.id,
                'name', w.name,
                'height', w.height,
                'lat', w.lat,
                'lon', w.lon
            ) ORDER BY w.name) FILTER (WHERE w.id IS NOT NULL), '[]'::jsonb) as details
        FROM us.river_reach_profiles r
        LEFT JOIN us.waterfalls w ON w.nearest_comid = r.comid
        GROUP BY r.comid
    ) sub
    WHERE rp.comid = sub.comid;
    
    RAISE NOTICE 'POI associations refreshed';
END;
$$;

-- Refresh gauge associations (run daily)
CREATE OR REPLACE PROCEDURE refresh_gauge_associations()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE us.river_reach_profiles rp
    SET 
        gauge_site_no = g.site_no,
        gauge_name = g.site_name,
        p10 = fp.p10,
        p25 = fp.p25,
        p50 = fp.p50,
        p75 = fp.p75,
        p90 = fp.p90
    FROM us.gauges g
    LEFT JOIN us.flow_percentiles fp ON g.site_no = fp.site_id
    WHERE g.comid = rp.comid;
    
    RAISE NOTICE 'Gauge associations refreshed';
END;
$$;

-- Refresh flow data (run hourly)
CREATE OR REPLACE PROCEDURE refresh_flow_data()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE us.river_reach_profiles rp
    SET 
        current_velocity_ms = nv.velocity_ms,
        current_flow_cms = nv.streamflow_cms,
        flow_updated_at = nv.updated_at,
        flow_status = CASE 
            WHEN rp.p10 IS NULL THEN 'unknown'
            WHEN nv.streamflow_cms < rp.p10 THEN 'very_low'
            WHEN nv.streamflow_cms < rp.p25 THEN 'low'
            WHEN nv.streamflow_cms > rp.p90 THEN 'flood'
            WHEN nv.streamflow_cms > rp.p75 THEN 'high'
            ELSE 'normal'
        END
    FROM public.nwm_velocity nv
    WHERE rp.comid = nv.comid;
    
    RAISE NOTICE 'Flow data refreshed';
END;
$$;

-- Refresh difficulty scores (run daily or after POI updates)
CREATE OR REPLACE PROCEDURE refresh_difficulty_scores()
LANGUAGE plpgsql
AS $$
BEGIN
    -- First calculate max rapid class per reach
    UPDATE us.river_reach_profiles rp
    SET max_rapid_class = COALESCE(
        (SELECT MAX(parse_rapid_class(r.rapid_class))
         FROM us.rapids r WHERE r.nearest_comid = rp.comid), 0
    ),
    rapid_density = CASE 
        WHEN COALESCE(rp.length_km, 0) > 0 
        THEN rp.rapid_count::NUMERIC / rp.length_km
        ELSE 0 
    END;
    
    -- Then calculate full difficulty score
    UPDATE us.river_reach_profiles rp
    SET 
        difficulty_factors = calculate_difficulty_score(
            rp.max_rapid_class,
            rp.rapid_density,
            rp.waterfall_count,
            rp.dam_count,
            rp.gradient_m_per_km,
            rp.total_drop_m
        ),
        difficulty_updated_at = NOW();
    
    -- Extract score components
    UPDATE us.river_reach_profiles
    SET 
        difficulty_score = (difficulty_factors->>'score')::NUMERIC,
        difficulty_level = (difficulty_factors->>'level')::INT,
        difficulty_label = difficulty_factors->>'label';
    
    RAISE NOTICE 'Difficulty scores refreshed';
END;
$$;

-- Refresh activity scores (run after difficulty/flow updates)
CREATE OR REPLACE PROCEDURE refresh_activity_scores()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE us.river_reach_profiles
    SET 
        paddling_score = calculate_paddling_score(
            access_point_count,
            difficulty_level,
            flow_status,
            length_km,
            campground_count
        ),
        beginner_friendly_score = calculate_beginner_score(
            difficulty_level,
            access_point_count,
            flow_status
        );
    
    RAISE NOTICE 'Activity scores refreshed';
END;
$$;

-- ============================================================================
-- EXAMPLE QUERIES FOR LLM
-- ============================================================================
/*
-- Find beginner-friendly rivers in Vermont with good access
SELECT 
    gnis_name,
    difficulty_label,
    difficulty_score,
    flow_status,
    access_point_count,
    beginner_friendly_score
FROM us.river_reach_profiles
WHERE state = 'VT'
  AND difficulty_level <= 2
  AND access_point_count >= 2
ORDER BY beginner_friendly_score DESC
LIMIT 10;

-- Find challenging whitewater with rapids
SELECT 
    gnis_name,
    difficulty_label,
    difficulty_factors,
    max_rapid_class,
    rapid_count,
    current_flow_cms,
    flow_status
FROM us.river_reach_profiles
WHERE difficulty_level >= 4
  AND rapid_count > 0
  AND flow_status = 'normal'
ORDER BY difficulty_score DESC
LIMIT 10;

-- Search by name with full context
SELECT 
    gnis_name, state,
    difficulty_label,
    current_flow_cms, flow_status,
    access_points, campgrounds, rapids, dams
FROM us.river_reach_profiles
WHERE search_text @@ to_tsquery('white & river')
ORDER BY paddling_score DESC
LIMIT 5;
*/

-- ============================================================================
-- COMMENTS
-- ============================================================================
COMMENT ON TABLE us.river_reach_profiles IS 
'Denormalized river reach metrics for LLM chatbot optimization. Pre-computes POI associations, difficulty scores, and flow context to minimize query complexity and token usage.';

COMMENT ON COLUMN us.river_reach_profiles.difficulty_score IS 
'Composite difficulty rating 1.0-5.0 based on rapids, hazards, and gradient. See difficulty_factors for breakdown.';

COMMENT ON COLUMN us.river_reach_profiles.difficulty_factors IS 
'JSONB breakdown of difficulty scoring factors with weights for transparency and tuning.';
