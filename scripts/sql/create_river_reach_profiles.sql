-- ============================================================================
-- RIVER REACH PROFILES - Denormalized metrics table for LLM chatbot
-- ============================================================================
-- Purpose: Pre-compute river reach metrics, POI associations, and difficulty
--          scores to optimize LLM queries (reduce joins, save tokens)
--
-- DESIGN:
--   - river_summary: One row per RIVER (aggregated by gnis_name)
--   - river_reach_profiles: One row per REACH (by comid)
--   - Access points are linked to specific reaches for routing
--   - Hydroseq enables ordering reaches upstream→downstream
-- 
-- Run order:
--   1. Create tables and functions (this file)
--   2. Run initial population: CALL populate_river_profiles();
--   3. Schedule refresh jobs (hourly for flow, daily for POIs)
-- ============================================================================

-- Drop existing objects if rebuilding
DROP TABLE IF EXISTS us.river_summary CASCADE;
DROP TABLE IF EXISTS us.river_reach_profiles CASCADE;
DROP FUNCTION IF EXISTS calculate_difficulty_score CASCADE;
DROP FUNCTION IF EXISTS parse_rapid_class CASCADE;
DROP FUNCTION IF EXISTS calculate_paddling_score CASCADE;
DROP FUNCTION IF EXISTS calculate_beginner_score CASCADE;
DROP FUNCTION IF EXISTS estimate_float_time_hours CASCADE;

-- ============================================================================
-- RIVER SUMMARY TABLE (One row per named river)
-- ============================================================================
-- Use this for high-level queries: "Tell me about the Lamoille River"
-- ============================================================================
CREATE TABLE us.river_summary (
    id SERIAL PRIMARY KEY,
    gnis_name TEXT NOT NULL,
    
    -- Location
    primary_state TEXT,              -- State with most river length
    states TEXT[],                   -- All states the river passes through
    
    -- Aggregated metrics
    total_length_km NUMERIC(10,2),
    reach_count INT,
    min_stream_order INT,
    max_stream_order INT,
    
    -- Elevation profile
    min_elev_m NUMERIC(10,2),
    max_elev_m NUMERIC(10,2),
    total_drop_m NUMERIC(10,2),
    avg_gradient_m_per_km NUMERIC(10,4),
    
    -- POI totals for entire river
    total_access_points INT DEFAULT 0,
    total_campgrounds INT DEFAULT 0,
    total_dams INT DEFAULT 0,
    total_rapids INT DEFAULT 0,
    total_waterfalls INT DEFAULT 0,
    
    -- Difficulty (max across all reaches)
    max_difficulty_score NUMERIC(4,2),
    max_difficulty_level INT,
    max_difficulty_label TEXT,
    avg_difficulty_score NUMERIC(4,2),
    
    -- Typical conditions
    typical_flow_status TEXT,        -- Most common flow status
    
    -- Activity scores (averaged)
    paddling_score NUMERIC(4,2),
    beginner_friendly_score NUMERIC(4,2),
    
    -- Bounding box for map display
    bbox_min_lat NUMERIC(10,6),
    bbox_max_lat NUMERIC(10,6),
    bbox_min_lon NUMERIC(10,6),
    bbox_max_lon NUMERIC(10,6),
    
    -- Search
    search_text TSVECTOR,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(gnis_name)
);

CREATE INDEX idx_rs_name ON us.river_summary(gnis_name);
CREATE INDEX idx_rs_state ON us.river_summary(primary_state);
CREATE INDEX idx_rs_search ON us.river_summary USING gin(search_text);
CREATE INDEX idx_rs_difficulty ON us.river_summary(max_difficulty_level);
CREATE INDEX idx_rs_length ON us.river_summary(total_length_km);

-- ============================================================================
-- RIVER REACH PROFILES TABLE (One row per reach/COMID)
-- ============================================================================
-- Use this for routing: "Find a 2-hour section with put-in/take-out"
-- ============================================================================
CREATE TABLE us.river_reach_profiles (
    comid BIGINT PRIMARY KEY,
    
    -- ========================================================================
    -- RIVER IDENTITY
    -- ========================================================================
    gnis_name TEXT,                  -- Links to river_summary
    state TEXT,
    region TEXT,
    
    -- ========================================================================
    -- NETWORK POSITION (for routing/ordering)
    -- ========================================================================
    hydroseq BIGINT,                 -- Hydrologic sequence (lower = downstream)
    from_node BIGINT,
    to_node BIGINT,
    stream_order INT,
    
    -- ========================================================================
    -- REACH METRICS
    -- ========================================================================
    length_km NUMERIC(10,3),
    min_elev_m NUMERIC(10,2),
    max_elev_m NUMERIC(10,2),
    total_drop_m NUMERIC(10,2),
    gradient_m_per_km NUMERIC(10,4),
    avg_slope NUMERIC(10,6),
    
    -- ========================================================================
    -- FLOAT TIME ESTIMATION
    -- ========================================================================
    estimated_float_hours NUMERIC(6,2),  -- Based on length + velocity
    
    -- ========================================================================
    -- LOCATION
    -- ========================================================================
    centroid_lat NUMERIC(10,6),
    centroid_lon NUMERIC(10,6),
    centroid_geom GEOMETRY(Point, 4326),
    
    -- ========================================================================
    -- REAL-TIME FLOW (updated hourly from NWM)
    -- ========================================================================
    current_velocity_ms NUMERIC(10,4),
    current_flow_cms NUMERIC(12,4),
    flow_status TEXT,
    flow_updated_at TIMESTAMPTZ,
    
    -- ========================================================================
    -- HISTORICAL FLOW CONTEXT
    -- ========================================================================
    gauge_site_no TEXT,
    gauge_name TEXT,
    p10 NUMERIC(12,4),
    p25 NUMERIC(12,4),
    p50 NUMERIC(12,4),
    p75 NUMERIC(12,4),
    p90 NUMERIC(12,4),
    
    -- ========================================================================
    -- POI COUNTS ON THIS REACH
    -- ========================================================================
    access_point_count INT DEFAULT 0,
    campground_count INT DEFAULT 0,
    dam_count INT DEFAULT 0,
    rapid_count INT DEFAULT 0,
    waterfall_count INT DEFAULT 0,
    
    -- ========================================================================
    -- ACCESS POINTS WITH DETAILS
    -- ========================================================================
    -- Stored with position info so LLM can identify put-ins vs take-outs
    -- [{id, name, type, lat, lon, position: "upstream"|"midstream"|"downstream"}]
    access_points JSONB DEFAULT '[]'::jsonb,
    
    -- ========================================================================
    -- OTHER POI DETAILS
    -- ========================================================================
    campgrounds JSONB DEFAULT '[]'::jsonb,
    dams JSONB DEFAULT '[]'::jsonb,
    rapids JSONB DEFAULT '[]'::jsonb,
    waterfalls JSONB DEFAULT '[]'::jsonb,
    
    -- ========================================================================
    -- DIFFICULTY SCORING
    -- ========================================================================
    max_rapid_class INT DEFAULT 0,
    rapid_density NUMERIC(10,4) DEFAULT 0,
    difficulty_score NUMERIC(4,2),
    difficulty_level INT,
    difficulty_label TEXT,
    difficulty_factors JSONB,
    
    -- ========================================================================
    -- ACTIVITY SCORES
    -- ========================================================================
    paddling_score NUMERIC(4,2),
    beginner_friendly_score NUMERIC(4,2),
    
    -- ========================================================================
    -- FLAGS
    -- ========================================================================
    has_put_in BOOLEAN DEFAULT FALSE,   -- Has usable access point for starting
    has_take_out BOOLEAN DEFAULT FALSE, -- Has usable access point for ending
    
    -- ========================================================================
    -- METADATA
    -- ========================================================================
    created_at TIMESTAMPTZ DEFAULT NOW(),
    profile_updated_at TIMESTAMPTZ DEFAULT NOW(),
    pois_updated_at TIMESTAMPTZ,
    difficulty_updated_at TIMESTAMPTZ
);

-- Indexes
CREATE INDEX idx_rrp_gnis ON us.river_reach_profiles(gnis_name);
CREATE INDEX idx_rrp_state ON us.river_reach_profiles(state);
CREATE INDEX idx_rrp_hydroseq ON us.river_reach_profiles(gnis_name, hydroseq);
CREATE INDEX idx_rrp_stream_order ON us.river_reach_profiles(stream_order);
CREATE INDEX idx_rrp_difficulty ON us.river_reach_profiles(difficulty_level);
CREATE INDEX idx_rrp_location ON us.river_reach_profiles USING gist(centroid_geom);
CREATE INDEX idx_rrp_flow_status ON us.river_reach_profiles(flow_status);
CREATE INDEX idx_rrp_has_access ON us.river_reach_profiles(gnis_name) 
    WHERE has_put_in = TRUE OR has_take_out = TRUE;
CREATE INDEX idx_rrp_float_time ON us.river_reach_profiles(estimated_float_hours);

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

-- Estimate float time based on length and velocity
-- Default velocity: 3 km/h (typical lazy river float)
CREATE OR REPLACE FUNCTION estimate_float_time_hours(
    p_length_km NUMERIC,
    p_velocity_ms NUMERIC
) RETURNS NUMERIC AS $$
DECLARE
    v_velocity_kmh NUMERIC;
BEGIN
    -- Convert m/s to km/h, use default if no velocity data
    v_velocity_kmh := COALESCE(p_velocity_ms * 3.6, 3.0);
    
    -- Minimum velocity to avoid division issues
    v_velocity_kmh := GREATEST(v_velocity_kmh, 1.0);
    
    RETURN ROUND(COALESCE(p_length_km, 0) / v_velocity_kmh, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- DIFFICULTY SCORING FUNCTION
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
    v_rapid_score := CASE COALESCE(p_max_rapid_class, 0)
        WHEN 0 THEN 1.0
        WHEN 1 THEN 1.8
        WHEN 2 THEN 2.6
        WHEN 3 THEN 3.5
        WHEN 4 THEN 4.3
        WHEN 5 THEN 4.8
        ELSE 5.0
    END;
    
    v_density_score := LEAST(5.0, GREATEST(1.0, 
        1.0 + (COALESCE(p_rapid_density, 0) * 1.33)
    ));
    
    v_hazard_score := LEAST(5.0, GREATEST(1.0,
        1.0 + (COALESCE(p_waterfall_count, 0) * 1.5) + (COALESCE(p_dam_count, 0) * 1.0)
    ));
    
    v_gradient_score := CASE
        WHEN COALESCE(p_gradient_m_per_km, 0) < 1 THEN 1.0
        WHEN p_gradient_m_per_km < 5 THEN 1.5 + (p_gradient_m_per_km / 10)
        WHEN p_gradient_m_per_km < 15 THEN 2.5 + ((p_gradient_m_per_km - 5) / 20)
        WHEN p_gradient_m_per_km < 30 THEN 3.5 + ((p_gradient_m_per_km - 15) / 30)
        ELSE 5.0
    END;
    
    v_final_score := (
        (v_rapid_score * W_RAPID_CLASS) +
        (v_density_score * W_DENSITY) +
        (v_hazard_score * W_HAZARDS) +
        (v_gradient_score * W_GRADIENT)
    );
    
    v_final_score := GREATEST(1.0, LEAST(5.0, v_final_score));
    v_level := ROUND(v_final_score)::INT;
    
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
            'rapid_class', jsonb_build_object('input', p_max_rapid_class, 'score', ROUND(v_rapid_score, 2), 'weight', W_RAPID_CLASS),
            'rapid_density', jsonb_build_object('input', ROUND(COALESCE(p_rapid_density, 0), 2), 'score', ROUND(v_density_score, 2), 'weight', W_DENSITY),
            'hazards', jsonb_build_object('waterfalls', COALESCE(p_waterfall_count, 0), 'dams', COALESCE(p_dam_count, 0), 'score', ROUND(v_hazard_score, 2), 'weight', W_HAZARDS),
            'gradient', jsonb_build_object('input_m_per_km', ROUND(COALESCE(p_gradient_m_per_km, 0), 2), 'score', ROUND(v_gradient_score, 2), 'weight', W_GRADIENT)
        ),
        'weights_version', '1.0'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- ACTIVITY SCORING FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION calculate_paddling_score(
    p_access_count INT,
    p_difficulty_level INT,
    p_flow_status TEXT,
    p_length_km NUMERIC,
    p_campground_count INT
) RETURNS NUMERIC AS $$
DECLARE
    v_score NUMERIC := 2.5;
BEGIN
    IF COALESCE(p_access_count, 0) >= 2 THEN
        v_score := v_score + 1.0;
    ELSIF p_access_count = 1 THEN
        v_score := v_score + 0.3;
    ELSE
        v_score := v_score - 1.0;
    END IF;
    
    v_score := v_score + CASE COALESCE(p_flow_status, 'unknown')
        WHEN 'normal' THEN 0.5
        WHEN 'high' THEN 0.3
        WHEN 'low' THEN -0.3
        WHEN 'very_low' THEN -0.8
        WHEN 'flood' THEN -1.0
        ELSE 0
    END;
    
    IF COALESCE(p_length_km, 0) BETWEEN 5 AND 20 THEN
        v_score := v_score + 0.5;
    ELSIF p_length_km BETWEEN 2 AND 30 THEN
        v_score := v_score + 0.2;
    END IF;
    
    IF COALESCE(p_campground_count, 0) > 0 THEN
        v_score := v_score + 0.3;
    END IF;
    
    RETURN GREATEST(1.0, LEAST(5.0, v_score));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION calculate_beginner_score(
    p_difficulty_level INT,
    p_access_count INT,
    p_flow_status TEXT
) RETURNS NUMERIC AS $$
DECLARE
    v_score NUMERIC := 3.0;
BEGIN
    v_score := v_score + CASE COALESCE(p_difficulty_level, 3)
        WHEN 1 THEN 1.5
        WHEN 2 THEN 0.8
        WHEN 3 THEN 0
        WHEN 4 THEN -1.0
        ELSE -2.0
    END;
    
    IF COALESCE(p_access_count, 0) >= 2 THEN
        v_score := v_score + 0.5;
    END IF;
    
    IF COALESCE(p_flow_status, '') IN ('normal', 'low') THEN
        v_score := v_score + 0.3;
    ELSIF p_flow_status IN ('high', 'flood') THEN
        v_score := v_score - 0.5;
    END IF;
    
    RETURN GREATEST(1.0, LEAST(5.0, v_score));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- ROUTE FINDING FUNCTION
-- ============================================================================
-- Find a contiguous route on a river with target float time
-- Returns reaches in order with cumulative stats
-- ============================================================================
CREATE OR REPLACE FUNCTION find_float_route(
    p_river_name TEXT,
    p_target_hours NUMERIC,
    p_tolerance_hours NUMERIC DEFAULT 0.5,
    p_max_difficulty INT DEFAULT 5
) RETURNS TABLE (
    route_id INT,
    comid BIGINT,
    gnis_name TEXT,
    reach_order INT,
    length_km NUMERIC,
    float_hours NUMERIC,
    cumulative_hours NUMERIC,
    difficulty_level INT,
    has_put_in BOOLEAN,
    has_take_out BOOLEAN,
    access_points JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH ordered_reaches AS (
        SELECT 
            rrp.comid,
            rrp.gnis_name,
            ROW_NUMBER() OVER (ORDER BY rrp.hydroseq DESC) as reach_order,
            rrp.length_km,
            rrp.estimated_float_hours,
            rrp.difficulty_level,
            rrp.has_put_in,
            rrp.has_take_out,
            rrp.access_points as aps
        FROM us.river_reach_profiles rrp
        WHERE rrp.gnis_name ILIKE p_river_name
          AND COALESCE(rrp.difficulty_level, 1) <= p_max_difficulty
        ORDER BY rrp.hydroseq DESC
    ),
    route_windows AS (
        SELECT 
            r1.reach_order as start_reach,
            r2.reach_order as end_reach,
            SUM(r3.estimated_float_hours) as total_hours,
            SUM(r3.length_km) as total_km
        FROM ordered_reaches r1
        CROSS JOIN ordered_reaches r2
        JOIN ordered_reaches r3 
            ON r3.reach_order BETWEEN r1.reach_order AND r2.reach_order
        WHERE r2.reach_order >= r1.reach_order
        GROUP BY r1.reach_order, r2.reach_order
        HAVING SUM(r3.estimated_float_hours) BETWEEN (p_target_hours - p_tolerance_hours) 
                                                  AND (p_target_hours + p_tolerance_hours)
    ),
    best_route AS (
        SELECT 
            rw.*,
            -- Prefer routes with put-in at start and take-out at end
            (SELECT has_put_in FROM ordered_reaches WHERE reach_order = rw.start_reach) as start_has_access,
            (SELECT has_take_out FROM ordered_reaches WHERE reach_order = rw.end_reach) as end_has_access
        FROM route_windows rw
        ORDER BY 
            (SELECT has_put_in FROM ordered_reaches WHERE reach_order = rw.start_reach)::int +
            (SELECT has_take_out FROM ordered_reaches WHERE reach_order = rw.end_reach)::int DESC,
            ABS(rw.total_hours - p_target_hours) ASC
        LIMIT 1
    )
    SELECT 
        1 as route_id,
        orp.comid,
        orp.gnis_name,
        orp.reach_order::INT,
        orp.length_km,
        orp.estimated_float_hours,
        SUM(orp.estimated_float_hours) OVER (ORDER BY orp.reach_order) as cumulative_hours,
        orp.difficulty_level,
        orp.has_put_in,
        orp.has_take_out,
        orp.aps
    FROM ordered_reaches orp
    JOIN best_route br ON orp.reach_order BETWEEN br.start_reach AND br.end_reach
    ORDER BY orp.reach_order;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- MAIN POPULATION PROCEDURE
-- ============================================================================
CREATE OR REPLACE PROCEDURE populate_river_profiles()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMPTZ;
    v_count INT;
BEGIN
    v_start_time := NOW();
    RAISE NOTICE 'Starting river profiles population at %', v_start_time;
    
    -- ========================================================================
    -- STEP 1: Populate reach profiles
    -- ========================================================================
    RAISE NOTICE 'Step 1: Inserting reach data...';
    
    INSERT INTO us.river_reach_profiles (
        comid, gnis_name, state, region, stream_order,
        hydroseq, from_node, to_node,
        length_km, min_elev_m, max_elev_m, total_drop_m, gradient_m_per_km, avg_slope,
        centroid_lat, centroid_lon, centroid_geom
    )
    SELECT 
        r.comid,
        r.gnis_name,
        COALESCE(s.stusps, r.region) as state,
        r.region,
        r.stream_order,
        r.hydroseq,
        r.from_node,
        r.to_node,
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
      AND r.stream_order >= 2
    ON CONFLICT (comid) DO UPDATE SET
        gnis_name = EXCLUDED.gnis_name,
        state = EXCLUDED.state,
        hydroseq = EXCLUDED.hydroseq,
        stream_order = EXCLUDED.stream_order,
        length_km = EXCLUDED.length_km,
        gradient_m_per_km = EXCLUDED.gradient_m_per_km,
        profile_updated_at = NOW();
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Inserted/updated % river reaches', v_count;
    
    -- ========================================================================
    -- STEP 2: Link POIs to reaches
    -- ========================================================================
    RAISE NOTICE 'Step 2: Linking POIs...';
    CALL refresh_poi_associations();
    
    -- ========================================================================
    -- STEP 3: Link gauges
    -- ========================================================================
    RAISE NOTICE 'Step 3: Linking gauges...';
    CALL refresh_gauge_associations();
    
    -- ========================================================================
    -- STEP 4: Update flow data
    -- ========================================================================
    RAISE NOTICE 'Step 4: Updating flow data...';
    CALL refresh_flow_data();
    
    -- ========================================================================
    -- STEP 5: Calculate difficulty
    -- ========================================================================
    RAISE NOTICE 'Step 5: Calculating difficulty...';
    CALL refresh_difficulty_scores();
    
    -- ========================================================================
    -- STEP 6: Calculate float times and activity scores
    -- ========================================================================
    RAISE NOTICE 'Step 6: Calculating float times and scores...';
    CALL refresh_activity_scores();
    
    -- ========================================================================
    -- STEP 7: Build river summary
    -- ========================================================================
    RAISE NOTICE 'Step 7: Building river summary...';
    CALL refresh_river_summary();
    
    RAISE NOTICE 'Population complete in %', NOW() - v_start_time;
END;
$$;

-- ============================================================================
-- REFRESH PROCEDURES
-- ============================================================================

-- Refresh POI associations with reach linkage
CREATE OR REPLACE PROCEDURE refresh_poi_associations()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Access points - link to nearest reach and determine position
    UPDATE us.river_reach_profiles rp
    SET 
        access_point_count = sub.cnt,
        access_points = sub.details,
        has_put_in = sub.cnt > 0,
        has_take_out = sub.cnt > 0,
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
                'lon', a.lon,
                'source', a.source
            ) ORDER BY a.name) FILTER (WHERE a.id IS NOT NULL), '[]'::jsonb) as details
        FROM us.river_reach_profiles r
        LEFT JOIN us.access_points a ON ST_DWithin(
            r.centroid_geom::geography,
            a.geom::geography,
            1000  -- 1km - tighter radius for better accuracy
        )
        GROUP BY r.comid
    ) sub
    WHERE rp.comid = sub.comid;
    
    -- Campgrounds
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
    
    -- Dams
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
    
    -- Rapids
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
    
    -- Waterfalls
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

-- Refresh gauge associations
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

-- Refresh difficulty scores
CREATE OR REPLACE PROCEDURE refresh_difficulty_scores()
LANGUAGE plpgsql
AS $$
BEGIN
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
    
    UPDATE us.river_reach_profiles rp
    SET difficulty_factors = calculate_difficulty_score(
            rp.max_rapid_class, rp.rapid_density, rp.waterfall_count,
            rp.dam_count, rp.gradient_m_per_km, rp.total_drop_m
        ),
        difficulty_updated_at = NOW();
    
    UPDATE us.river_reach_profiles
    SET 
        difficulty_score = (difficulty_factors->>'score')::NUMERIC,
        difficulty_level = (difficulty_factors->>'level')::INT,
        difficulty_label = difficulty_factors->>'label';
    
    RAISE NOTICE 'Difficulty scores refreshed';
END;
$$;

-- Refresh activity scores and float times
CREATE OR REPLACE PROCEDURE refresh_activity_scores()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE us.river_reach_profiles
    SET 
        estimated_float_hours = estimate_float_time_hours(length_km, current_velocity_ms),
        paddling_score = calculate_paddling_score(
            access_point_count, difficulty_level, flow_status, length_km, campground_count
        ),
        beginner_friendly_score = calculate_beginner_score(
            difficulty_level, access_point_count, flow_status
        );
    
    RAISE NOTICE 'Activity scores refreshed';
END;
$$;

-- Build/refresh river summary table
CREATE OR REPLACE PROCEDURE refresh_river_summary()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Clear and rebuild
    TRUNCATE us.river_summary;
    
    INSERT INTO us.river_summary (
        gnis_name, primary_state, states,
        total_length_km, reach_count, min_stream_order, max_stream_order,
        min_elev_m, max_elev_m, total_drop_m, avg_gradient_m_per_km,
        total_access_points, total_campgrounds, total_dams, total_rapids, total_waterfalls,
        max_difficulty_score, max_difficulty_level, max_difficulty_label, avg_difficulty_score,
        paddling_score, beginner_friendly_score,
        bbox_min_lat, bbox_max_lat, bbox_min_lon, bbox_max_lon,
        search_text
    )
    SELECT 
        gnis_name,
        MODE() WITHIN GROUP (ORDER BY state) as primary_state,
        ARRAY_AGG(DISTINCT state) FILTER (WHERE state IS NOT NULL) as states,
        SUM(length_km) as total_length_km,
        COUNT(*) as reach_count,
        MIN(stream_order) as min_stream_order,
        MAX(stream_order) as max_stream_order,
        MIN(min_elev_m) as min_elev_m,
        MAX(max_elev_m) as max_elev_m,
        MAX(max_elev_m) - MIN(min_elev_m) as total_drop_m,
        AVG(gradient_m_per_km) as avg_gradient_m_per_km,
        SUM(access_point_count) as total_access_points,
        SUM(campground_count) as total_campgrounds,
        SUM(dam_count) as total_dams,
        SUM(rapid_count) as total_rapids,
        SUM(waterfall_count) as total_waterfalls,
        MAX(difficulty_score) as max_difficulty_score,
        MAX(difficulty_level) as max_difficulty_level,
        MAX(difficulty_label) as max_difficulty_label,
        AVG(difficulty_score) as avg_difficulty_score,
        AVG(paddling_score) as paddling_score,
        AVG(beginner_friendly_score) as beginner_friendly_score,
        MIN(centroid_lat) as bbox_min_lat,
        MAX(centroid_lat) as bbox_max_lat,
        MIN(centroid_lon) as bbox_min_lon,
        MAX(centroid_lon) as bbox_max_lon,
        to_tsvector('english', gnis_name || ' ' || COALESCE(MODE() WITHIN GROUP (ORDER BY state), ''))
    FROM us.river_reach_profiles
    WHERE gnis_name IS NOT NULL
    GROUP BY gnis_name;
    
    RAISE NOTICE 'River summary refreshed';
END;
$$;

-- ============================================================================
-- EXAMPLE QUERIES FOR LLM
-- ============================================================================
/*
-- Q1: "Tell me about the Lamoille River"
SELECT 
    gnis_name, primary_state, states,
    total_length_km, reach_count,
    total_access_points, total_rapids, total_dams,
    max_difficulty_label, avg_difficulty_score,
    paddling_score
FROM us.river_summary
WHERE gnis_name ILIKE '%lamoille%';

-- Q2: "Find a 2-hour float on the Lamoille with put-in and take-out"
SELECT * FROM find_float_route('Lamoille River', 2.0, 0.5);

-- Q3: "What are the access points on the White River?"
SELECT 
    rp.gnis_name,
    rp.hydroseq,
    rp.access_points,
    rp.estimated_float_hours
FROM us.river_reach_profiles rp
WHERE rp.gnis_name ILIKE '%white river%'
  AND rp.access_point_count > 0
ORDER BY rp.hydroseq DESC;

-- Q4: "Find easy rivers near Burlington, VT for beginners"
SELECT 
    rs.gnis_name,
    rs.total_length_km,
    rs.max_difficulty_label,
    rs.total_access_points,
    rs.beginner_friendly_score
FROM us.river_summary rs
WHERE rs.primary_state = 'VT'
  AND rs.max_difficulty_level <= 2
  AND rs.total_access_points >= 2
ORDER BY rs.beginner_friendly_score DESC
LIMIT 10;
*/

-- ============================================================================
-- COMMENTS
-- ============================================================================
COMMENT ON TABLE us.river_summary IS 
'Aggregated river-level metrics. One row per named river. Use for high-level queries.';

COMMENT ON TABLE us.river_reach_profiles IS 
'Per-reach metrics with POI details. Use hydroseq for ordering. Use for routing queries.';

COMMENT ON FUNCTION find_float_route IS
'Find a contiguous route on a river matching target float time. Returns ordered reaches with access point info.';

-- ============================================================================
-- ADDENDUM: River Identification
-- ============================================================================
-- Rivers are uniquely identified by gnis_name + state combination
-- The river_summary table should use this for lookups

-- Add unique constraint on name + state
ALTER TABLE us.river_summary 
    DROP CONSTRAINT IF EXISTS river_summary_gnis_name_key;
ALTER TABLE us.river_summary 
    ADD CONSTRAINT river_summary_name_state_unique UNIQUE (gnis_name, primary_state);

-- Add composite index for fast lookups
CREATE INDEX IF NOT EXISTS idx_rs_name_state 
    ON us.river_summary(gnis_name, primary_state);

CREATE INDEX IF NOT EXISTS idx_rrp_name_state 
    ON us.river_reach_profiles(gnis_name, state);

-- ============================================================================
-- UPDATED ROUTE FINDING FUNCTION (with state parameter)
-- ============================================================================
DROP FUNCTION IF EXISTS find_float_route;

CREATE OR REPLACE FUNCTION find_float_route(
    p_river_name TEXT,
    p_state TEXT,                    -- Required: state to disambiguate
    p_target_hours NUMERIC,
    p_tolerance_hours NUMERIC DEFAULT 0.5,
    p_max_difficulty INT DEFAULT 5
) RETURNS TABLE (
    route_id INT,
    comid BIGINT,
    gnis_name TEXT,
    state TEXT,
    reach_order INT,
    length_km NUMERIC,
    float_hours NUMERIC,
    cumulative_hours NUMERIC,
    difficulty_level INT,
    has_put_in BOOLEAN,
    has_take_out BOOLEAN,
    access_points JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH ordered_reaches AS (
        SELECT 
            rrp.comid,
            rrp.gnis_name,
            rrp.state,
            ROW_NUMBER() OVER (ORDER BY rrp.hydroseq DESC) as reach_order,
            rrp.length_km,
            rrp.estimated_float_hours,
            rrp.difficulty_level,
            rrp.has_put_in,
            rrp.has_take_out,
            rrp.access_points as aps
        FROM us.river_reach_profiles rrp
        WHERE rrp.gnis_name ILIKE p_river_name
          AND rrp.state = p_state
          AND COALESCE(rrp.difficulty_level, 1) <= p_max_difficulty
        ORDER BY rrp.hydroseq DESC
    ),
    route_windows AS (
        SELECT 
            r1.reach_order as start_reach,
            r2.reach_order as end_reach,
            SUM(r3.estimated_float_hours) as total_hours,
            SUM(r3.length_km) as total_km
        FROM ordered_reaches r1
        CROSS JOIN ordered_reaches r2
        JOIN ordered_reaches r3 
            ON r3.reach_order BETWEEN r1.reach_order AND r2.reach_order
        WHERE r2.reach_order >= r1.reach_order
        GROUP BY r1.reach_order, r2.reach_order
        HAVING SUM(r3.estimated_float_hours) BETWEEN (p_target_hours - p_tolerance_hours) 
                                                  AND (p_target_hours + p_tolerance_hours)
    ),
    best_route AS (
        SELECT 
            rw.*,
            (SELECT or1.has_put_in FROM ordered_reaches or1 WHERE or1.reach_order = rw.start_reach) as start_has_access,
            (SELECT or2.has_take_out FROM ordered_reaches or2 WHERE or2.reach_order = rw.end_reach) as end_has_access
        FROM route_windows rw
        ORDER BY 
            (SELECT or1.has_put_in FROM ordered_reaches or1 WHERE or1.reach_order = rw.start_reach)::int +
            (SELECT or2.has_take_out FROM ordered_reaches or2 WHERE or2.reach_order = rw.end_reach)::int DESC,
            ABS(rw.total_hours - p_target_hours) ASC
        LIMIT 1
    )
    SELECT 
        1 as route_id,
        orp.comid,
        orp.gnis_name,
        orp.state,
        orp.reach_order::INT,
        orp.length_km,
        orp.estimated_float_hours,
        SUM(orp.estimated_float_hours) OVER (ORDER BY orp.reach_order) as cumulative_hours,
        orp.difficulty_level,
        orp.has_put_in,
        orp.has_take_out,
        orp.aps
    FROM ordered_reaches orp
    JOIN best_route br ON orp.reach_order BETWEEN br.start_reach AND br.end_reach
    ORDER BY orp.reach_order;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- UPDATED EXAMPLE QUERIES
-- ============================================================================
/*
-- Q1: "Tell me about the White River in Vermont" (not Arkansas, not Indiana)
SELECT * FROM us.river_summary 
WHERE gnis_name ILIKE '%white river%' AND primary_state = 'VT';

-- Q2: "Find a 2-hour float on the Lamoille in VT"
SELECT * FROM find_float_route('Lamoille River', 'VT', 2.0);

-- Q3: "What rivers named 'White River' exist?" (disambiguation query)
SELECT gnis_name, primary_state, total_length_km, max_difficulty_label
FROM us.river_summary 
WHERE gnis_name ILIKE '%white river%'
ORDER BY total_length_km DESC;
*/

-- ============================================================================
-- DISSOLVED RIVER IDENTIFICATION
-- ============================================================================
-- Assigns unique river_id to each physically distinct river
-- Rivers with same name but disconnected networks get different IDs
-- e.g., Connecticut River (4 states, connected) = 1 ID
--       Birch Creek MT vs Birch Creek VT (disconnected) = 2 IDs
-- ============================================================================

-- Add river_id to reach profiles
ALTER TABLE us.river_reach_profiles 
    ADD COLUMN IF NOT EXISTS river_id INT;

-- Add river_id to summary (will now be truly unique per physical river)
ALTER TABLE us.river_summary
    ADD COLUMN IF NOT EXISTS river_id INT;

-- Drop old constraints that assumed name+state uniqueness
ALTER TABLE us.river_summary 
    DROP CONSTRAINT IF EXISTS river_summary_name_state_unique;
ALTER TABLE us.river_summary 
    DROP CONSTRAINT IF EXISTS river_summary_gnis_name_key;

-- ============================================================================
-- FUNCTION: Assign river IDs based on network connectivity
-- ============================================================================
-- Uses connected components algorithm via recursive CTE
-- Reaches with same gnis_name that are connected = same river_id
-- ============================================================================
CREATE OR REPLACE PROCEDURE assign_river_ids()
LANGUAGE plpgsql
AS $$
DECLARE
    v_river_id INT := 0;
    v_river_name TEXT;
    v_seed_comid BIGINT;
    v_count INT;
BEGIN
    RAISE NOTICE 'Assigning river IDs based on network connectivity...';
    
    -- Reset all river_ids
    UPDATE us.river_reach_profiles SET river_id = NULL;
    
    -- Process each unique river name
    FOR v_river_name IN 
        SELECT DISTINCT gnis_name 
        FROM us.river_reach_profiles 
        WHERE gnis_name IS NOT NULL
        ORDER BY gnis_name
    LOOP
        -- Find all unassigned reaches for this river name
        -- and group them by connectivity
        LOOP
            -- Find a seed reach that hasn't been assigned yet
            SELECT comid INTO v_seed_comid
            FROM us.river_reach_profiles
            WHERE gnis_name = v_river_name
              AND river_id IS NULL
            LIMIT 1;
            
            -- Exit if no more unassigned reaches for this name
            EXIT WHEN v_seed_comid IS NULL;
            
            -- Increment river_id for this new connected component
            v_river_id := v_river_id + 1;
            
            -- Find all reaches connected to the seed (same name, connected network)
            WITH RECURSIVE connected AS (
                -- Start with the seed reach
                SELECT comid, from_node, to_node
                FROM public.river_edges
                WHERE comid = v_seed_comid
                
                UNION
                
                -- Add reaches that connect upstream or downstream
                SELECT e.comid, e.from_node, e.to_node
                FROM public.river_edges e
                JOIN connected c ON (
                    e.from_node = c.to_node OR   -- e flows into c
                    e.to_node = c.from_node      -- c flows into e
                )
                WHERE e.gnis_name = v_river_name
                  AND e.comid NOT IN (SELECT comid FROM connected)
            )
            UPDATE us.river_reach_profiles rp
            SET river_id = v_river_id
            FROM connected c
            WHERE rp.comid = c.comid
              AND rp.gnis_name = v_river_name;
            
            GET DIAGNOSTICS v_count = ROW_COUNT;
            
            IF v_count > 0 THEN
                RAISE NOTICE 'River ID %: % (%  reaches)', v_river_id, v_river_name, v_count;
            END IF;
        END LOOP;
    END LOOP;
    
    RAISE NOTICE 'Assigned % unique river IDs', v_river_id;
END;
$$;

-- ============================================================================
-- SIMPLER ALTERNATIVE: Using HUC watershed codes
-- ============================================================================
-- If the recursive approach is too slow, use HUC codes as a proxy:
-- Same name + same HUC4 (or HUC6) = same river
-- This is faster but slightly less accurate for rivers crossing HUC boundaries
-- ============================================================================
CREATE OR REPLACE PROCEDURE assign_river_ids_by_huc()
LANGUAGE plpgsql
AS $$
DECLARE
    v_river_id INT := 0;
BEGIN
    RAISE NOTICE 'Assigning river IDs by HUC watershed...';
    
    -- Create temp table with name + HUC groupings
    CREATE TEMP TABLE river_groups AS
    SELECT 
        gnis_name,
        LEFT(r.region, 4) as huc4,  -- Use HUC4 for grouping
        MIN(comid) as min_comid
    FROM us.river_reach_profiles rp
    JOIN public.river_edges r ON rp.comid = r.comid
    WHERE gnis_name IS NOT NULL
    GROUP BY gnis_name, LEFT(r.region, 4);
    
    -- Assign sequential IDs
    UPDATE us.river_reach_profiles rp
    SET river_id = rg.row_num
    FROM (
        SELECT 
            gnis_name, 
            huc4,
            ROW_NUMBER() OVER (ORDER BY gnis_name, huc4) as row_num
        FROM river_groups
    ) rg
    JOIN public.river_edges r ON rp.comid = r.comid
    WHERE rp.gnis_name = rg.gnis_name
      AND LEFT(r.region, 4) = rg.huc4;
    
    DROP TABLE river_groups;
    
    SELECT MAX(river_id) INTO v_river_id FROM us.river_reach_profiles;
    RAISE NOTICE 'Assigned % unique river IDs', v_river_id;
END;
$$;

-- ============================================================================
-- UPDATE RIVER SUMMARY TO USE RIVER_ID
-- ============================================================================
CREATE OR REPLACE PROCEDURE refresh_river_summary()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE us.river_summary;
    
    INSERT INTO us.river_summary (
        river_id, gnis_name, primary_state, states,
        total_length_km, reach_count, min_stream_order, max_stream_order,
        min_elev_m, max_elev_m, total_drop_m, avg_gradient_m_per_km,
        total_access_points, total_campgrounds, total_dams, total_rapids, total_waterfalls,
        max_difficulty_score, max_difficulty_level, max_difficulty_label, avg_difficulty_score,
        paddling_score, beginner_friendly_score,
        bbox_min_lat, bbox_max_lat, bbox_min_lon, bbox_max_lon,
        search_text
    )
    SELECT 
        river_id,
        gnis_name,
        MODE() WITHIN GROUP (ORDER BY state) as primary_state,
        ARRAY_AGG(DISTINCT state ORDER BY state) FILTER (WHERE state IS NOT NULL) as states,
        SUM(length_km) as total_length_km,
        COUNT(*) as reach_count,
        MIN(stream_order) as min_stream_order,
        MAX(stream_order) as max_stream_order,
        MIN(min_elev_m) as min_elev_m,
        MAX(max_elev_m) as max_elev_m,
        MAX(max_elev_m) - MIN(min_elev_m) as total_drop_m,
        AVG(gradient_m_per_km) as avg_gradient_m_per_km,
        SUM(access_point_count) as total_access_points,
        SUM(campground_count) as total_campgrounds,
        SUM(dam_count) as total_dams,
        SUM(rapid_count) as total_rapids,
        SUM(waterfall_count) as total_waterfalls,
        MAX(difficulty_score) as max_difficulty_score,
        MAX(difficulty_level) as max_difficulty_level,
        MAX(difficulty_label) as max_difficulty_label,
        AVG(difficulty_score) as avg_difficulty_score,
        AVG(paddling_score) as paddling_score,
        AVG(beginner_friendly_score) as beginner_friendly_score,
        MIN(centroid_lat) as bbox_min_lat,
        MAX(centroid_lat) as bbox_max_lat,
        MIN(centroid_lon) as bbox_min_lon,
        MAX(centroid_lon) as bbox_max_lon,
        to_tsvector('english', 
            gnis_name || ' ' || 
            COALESCE(array_to_string(ARRAY_AGG(DISTINCT state), ' '), '')
        )
    FROM us.river_reach_profiles
    WHERE gnis_name IS NOT NULL
      AND river_id IS NOT NULL
    GROUP BY river_id, gnis_name;
    
    RAISE NOTICE 'River summary refreshed with river_id';
END;
$$;

-- Create unique constraint on river_id
CREATE UNIQUE INDEX IF NOT EXISTS idx_rs_river_id ON us.river_summary(river_id);

-- Index for looking up by river_id
CREATE INDEX IF NOT EXISTS idx_rrp_river_id ON us.river_reach_profiles(river_id);

-- ============================================================================
-- UPDATED ROUTE FINDING (uses river_id)
-- ============================================================================
DROP FUNCTION IF EXISTS find_float_route;

CREATE OR REPLACE FUNCTION find_float_route(
    p_river_id INT,                  -- Use river_id instead of name+state
    p_target_hours NUMERIC,
    p_tolerance_hours NUMERIC DEFAULT 0.5,
    p_max_difficulty INT DEFAULT 5
) RETURNS TABLE (
    route_id INT,
    comid BIGINT,
    river_id INT,
    gnis_name TEXT,
    state TEXT,
    reach_order INT,
    length_km NUMERIC,
    float_hours NUMERIC,
    cumulative_hours NUMERIC,
    difficulty_level INT,
    has_put_in BOOLEAN,
    has_take_out BOOLEAN,
    access_points JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH ordered_reaches AS (
        SELECT 
            rrp.comid,
            rrp.river_id,
            rrp.gnis_name,
            rrp.state,
            ROW_NUMBER() OVER (ORDER BY rrp.hydroseq DESC) as reach_order,
            rrp.length_km,
            rrp.estimated_float_hours,
            rrp.difficulty_level,
            rrp.has_put_in,
            rrp.has_take_out,
            rrp.access_points as aps
        FROM us.river_reach_profiles rrp
        WHERE rrp.river_id = p_river_id
          AND COALESCE(rrp.difficulty_level, 1) <= p_max_difficulty
        ORDER BY rrp.hydroseq DESC
    ),
    route_windows AS (
        SELECT 
            r1.reach_order as start_reach,
            r2.reach_order as end_reach,
            SUM(r3.estimated_float_hours) as total_hours
        FROM ordered_reaches r1
        CROSS JOIN ordered_reaches r2
        JOIN ordered_reaches r3 
            ON r3.reach_order BETWEEN r1.reach_order AND r2.reach_order
        WHERE r2.reach_order >= r1.reach_order
        GROUP BY r1.reach_order, r2.reach_order
        HAVING SUM(r3.estimated_float_hours) BETWEEN (p_target_hours - p_tolerance_hours) 
                                                  AND (p_target_hours + p_tolerance_hours)
    ),
    best_route AS (
        SELECT rw.*
        FROM route_windows rw
        ORDER BY 
            (SELECT or1.has_put_in FROM ordered_reaches or1 WHERE or1.reach_order = rw.start_reach)::int +
            (SELECT or2.has_take_out FROM ordered_reaches or2 WHERE or2.reach_order = rw.end_reach)::int DESC,
            ABS(rw.total_hours - p_target_hours) ASC
        LIMIT 1
    )
    SELECT 
        1 as route_id,
        orp.comid,
        orp.river_id,
        orp.gnis_name,
        orp.state,
        orp.reach_order::INT,
        orp.length_km,
        orp.estimated_float_hours,
        SUM(orp.estimated_float_hours) OVER (ORDER BY orp.reach_order) as cumulative_hours,
        orp.difficulty_level,
        orp.has_put_in,
        orp.has_take_out,
        orp.aps
    FROM ordered_reaches orp
    JOIN best_route br ON orp.reach_order BETWEEN br.start_reach AND br.end_reach
    ORDER BY orp.reach_order;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- HELPER: Find river_id by name (with disambiguation)
-- ============================================================================
CREATE OR REPLACE FUNCTION find_river_id(
    p_river_name TEXT,
    p_state TEXT DEFAULT NULL
) RETURNS TABLE (
    river_id INT,
    gnis_name TEXT,
    states TEXT[],
    total_length_km NUMERIC,
    reach_count INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        rs.river_id,
        rs.gnis_name,
        rs.states,
        rs.total_length_km,
        rs.reach_count
    FROM us.river_summary rs
    WHERE rs.gnis_name ILIKE '%' || p_river_name || '%'
      AND (p_state IS NULL OR p_state = ANY(rs.states))
    ORDER BY rs.total_length_km DESC;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================
/*
-- Find all rivers named "White River" and their IDs
SELECT * FROM find_river_id('White River');
-- Returns:
-- river_id | gnis_name    | states           | total_length_km
-- 1234     | White River  | {VT}             | 89.2
-- 5678     | White River  | {AR,MO}          | 1102.4
-- 9012     | White River  | {IN}             | 362.1

-- Find specifically the White River that flows through Vermont
SELECT * FROM find_river_id('White River', 'VT');

-- Get a 2-hour float on river_id 1234 (White River, VT)
SELECT * FROM find_float_route(1234, 2.0);

-- Connecticut River (flows through 4 states) = single river_id
SELECT * FROM find_river_id('Connecticut River');
-- Returns ONE row with states = {NH, VT, MA, CT}
*/
