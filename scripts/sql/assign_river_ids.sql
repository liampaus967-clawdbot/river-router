-- ============================================================================
-- ASSIGN UNIQUE RIVER IDs TO river_edges
-- ============================================================================
-- One-time batch process to assign unique river_id to each distinct river
-- Uses network connectivity (from_node/to_node) to identify connected components
-- 
-- Results:
--   - Connecticut River (4 states, connected) → 1 river_id
--   - White River VT vs White River AR (disconnected) → 2 different river_ids
--   - Birch Creek in MT vs Birch Creek in ID → separate river_ids
--
-- Run time estimate: 30-60 minutes for 2.9M reaches
-- ============================================================================

-- Step 1: Add river_id column if not exists
ALTER TABLE public.river_edges ADD COLUMN IF NOT EXISTS river_id INT;

-- Step 2: Create index for faster lookups during processing
CREATE INDEX IF NOT EXISTS idx_re_gnis_name ON public.river_edges(gnis_name) WHERE gnis_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_re_from_node ON public.river_edges(from_node);
CREATE INDEX IF NOT EXISTS idx_re_to_node ON public.river_edges(to_node);

-- Step 3: Create working table for connected components
DROP TABLE IF EXISTS temp_river_components;
CREATE UNLOGGED TABLE temp_river_components (
    comid BIGINT PRIMARY KEY,
    gnis_name TEXT,
    component_id BIGINT  -- Will hold the minimum comid in each connected component
);

-- Step 4: Initialize - each reach starts as its own component
INSERT INTO temp_river_components (comid, gnis_name, component_id)
SELECT comid, gnis_name, comid
FROM public.river_edges
WHERE gnis_name IS NOT NULL;

CREATE INDEX idx_trc_gnis ON temp_river_components(gnis_name);
CREATE INDEX idx_trc_component ON temp_river_components(component_id);

-- Step 5: Build edge list for connectivity (only between same-named reaches)
DROP TABLE IF EXISTS temp_river_edges;
CREATE UNLOGGED TABLE temp_river_edges AS
SELECT DISTINCT
    a.comid as comid1,
    b.comid as comid2
FROM public.river_edges a
JOIN public.river_edges b ON (
    a.to_node = b.from_node OR   -- a flows into b
    a.from_node = b.to_node      -- b flows into a
)
WHERE a.gnis_name IS NOT NULL
  AND a.gnis_name = b.gnis_name  -- Same river name
  AND a.comid < b.comid;         -- Avoid duplicates

CREATE INDEX idx_tre_comid1 ON temp_river_edges(comid1);
CREATE INDEX idx_tre_comid2 ON temp_river_edges(comid2);

-- ============================================================================
-- Step 6: Iterative label propagation to find connected components
-- ============================================================================
-- This propagates the minimum comid through the network until stable
-- Typically converges in 10-20 iterations for river networks
-- ============================================================================
DO $$
DECLARE
    v_updated INT := 1;
    v_iteration INT := 0;
BEGIN
    RAISE NOTICE 'Starting connected components algorithm...';
    
    WHILE v_updated > 0 LOOP
        v_iteration := v_iteration + 1;
        
        -- Propagate minimum component_id along edges
        WITH updates AS (
            SELECT 
                trc.comid,
                LEAST(
                    trc.component_id,
                    MIN(trc2.component_id)
                ) as new_component_id
            FROM temp_river_components trc
            JOIN temp_river_edges tre ON trc.comid = tre.comid1 OR trc.comid = tre.comid2
            JOIN temp_river_components trc2 ON (
                (tre.comid1 = trc.comid AND tre.comid2 = trc2.comid) OR
                (tre.comid2 = trc.comid AND tre.comid1 = trc2.comid)
            )
            WHERE trc.gnis_name = trc2.gnis_name
            GROUP BY trc.comid, trc.component_id
            HAVING LEAST(trc.component_id, MIN(trc2.component_id)) < trc.component_id
        )
        UPDATE temp_river_components trc
        SET component_id = updates.new_component_id
        FROM updates
        WHERE trc.comid = updates.comid;
        
        GET DIAGNOSTICS v_updated = ROW_COUNT;
        RAISE NOTICE 'Iteration %: % updates', v_iteration, v_updated;
    END LOOP;
    
    RAISE NOTICE 'Converged after % iterations', v_iteration;
END $$;

-- ============================================================================
-- Step 7: Assign sequential river_ids from component_ids
-- ============================================================================
DROP TABLE IF EXISTS temp_river_id_map;
CREATE TABLE temp_river_id_map AS
SELECT 
    component_id,
    ROW_NUMBER() OVER (ORDER BY component_id) as river_id
FROM (
    SELECT DISTINCT component_id FROM temp_river_components
) sub;

CREATE INDEX idx_trim_component ON temp_river_id_map(component_id);

-- Step 8: Update river_edges with the final river_id
UPDATE public.river_edges re
SET river_id = trim.river_id
FROM temp_river_components trc
JOIN temp_river_id_map trim ON trc.component_id = trim.component_id
WHERE re.comid = trc.comid;

-- Step 9: Create index on river_id
CREATE INDEX IF NOT EXISTS idx_re_river_id ON public.river_edges(river_id);

-- Step 10: Report results
DO $$
DECLARE
    v_total_rivers INT;
    v_total_reaches INT;
    v_multi_state_rivers INT;
BEGIN
    SELECT COUNT(DISTINCT river_id) INTO v_total_rivers FROM public.river_edges WHERE river_id IS NOT NULL;
    SELECT COUNT(*) INTO v_total_reaches FROM public.river_edges WHERE river_id IS NOT NULL;
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'RIVER ID ASSIGNMENT COMPLETE';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Total unique rivers: %', v_total_rivers;
    RAISE NOTICE 'Total reaches with river_id: %', v_total_reaches;
END $$;

-- Cleanup temp tables
DROP TABLE IF EXISTS temp_river_components;
DROP TABLE IF EXISTS temp_river_edges;
DROP TABLE IF EXISTS temp_river_id_map;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================
/*
-- Check Connecticut River (should be ONE river_id across multiple states)
SELECT river_id, COUNT(*) as reaches, 
       COUNT(DISTINCT region) as regions,
       MIN(hydroseq), MAX(hydroseq)
FROM public.river_edges 
WHERE gnis_name = 'Connecticut River'
GROUP BY river_id;

-- Check how many "White River"s exist (should be multiple river_ids)
SELECT river_id, COUNT(*) as reaches
FROM public.river_edges
WHERE gnis_name = 'White River'
GROUP BY river_id
ORDER BY reaches DESC;

-- Find multi-state rivers
WITH river_states AS (
    SELECT 
        river_id, 
        gnis_name,
        COUNT(DISTINCT region) as state_count,
        COUNT(*) as reaches
    FROM public.river_edges
    WHERE river_id IS NOT NULL AND region IS NOT NULL
    GROUP BY river_id, gnis_name
)
SELECT * FROM river_states WHERE state_count > 1 ORDER BY reaches DESC LIMIT 20;
*/
