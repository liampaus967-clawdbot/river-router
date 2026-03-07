# River Router - Product Roadmap

## Current State ✅

| Feature | Status |
|---------|--------|
| Paddle A to B routing | ✅ Implemented |
| Estimated trip time from velocities | ✅ Implemented |
| Flow direction arrows (static) | ✅ Implemented |
| Paddle speed input for ETA | ✅ Implemented |
| Basic hazard data (dams, rapids) | ✅ Partial |

---

## Phase 1 Improvements

### 1. Upstream vs Downstream Detection
**Priority:** 🔴 HIGH (foundational for other features)

**Current behavior:** App only allows downstream routing, gives error for upstream attempts.

**Changes needed:**
- [ ] Modify `buildGraph()` to support bidirectional routing
- [ ] Detect route direction by comparing start/end elevations
- [ ] Add warning UI when user routes upstream
- [ ] Adjust ETA calculation for upstream (subtract velocity from paddle speed)
- [ ] Prevent impossible routes (paddle speed < stream velocity upstream)

**Files to modify:**
- `app/api/route/route.ts` - routing logic
- `app/page.tsx` - UI for warnings

---

### 2. Color Rivers by Velocity
**Priority:** 🔴 HIGH

**Database:** `river_velocities` table created with joined geometry + NWM velocities

**Steps:**
1. ✅ Create `river_velocities` table
2. [ ] Export to GeoJSON/MBTiles
3. [ ] Upload to Mapbox (Liam handles)
4. [ ] Update frontend to style by velocity property

**Color scale suggestion:**
- 0-0.5 m/s: Blue (slow/pool)
- 0.5-1.0 m/s: Green (moderate)
- 1.0-2.0 m/s: Yellow (faster)
- 2.0+ m/s: Red (fast/technical)

---

### 3. Factor in Current Velocities
**Priority:** 🟡 MEDIUM (after upstream detection)

**Physics:**
```
downstream_speed = paddle_speed + stream_velocity
upstream_speed = paddle_speed - stream_velocity
trip_time = distance / effective_speed
```

**Implementation:**
- [ ] Calculate per-segment effective speed
- [ ] Sum segment times for total ETA
- [ ] Show warning if upstream speed < 0.5 m/s (grueling paddle)
- [ ] Show error if upstream speed <= 0 (impossible)

---

### 4. Runnability Indicators (Hybrid Approach)

**Data sources:**
1. **American Whitewater** - scraped/API rapids data
2. **Derived scoring** - velocity + gradient calculation
3. **User reports** - future crowdsourcing

**Derived formula:**
```
gradient_ft_mi = elevation_drop_ft / length_mi
difficulty_score = f(velocity_ms, gradient_ft_mi, stream_order)

Classifications:
- Class I (Easy): gradient < 10 ft/mi, velocity < 1 m/s
- Class II (Novice): gradient 10-20 ft/mi, velocity 1-1.5 m/s
- Class III (Intermediate): gradient 20-40 ft/mi, velocity 1.5-2.5 m/s
- Class IV+ (Advanced): gradient > 40 ft/mi OR velocity > 2.5 m/s
```

**Implementation:**
- [ ] Scrape American Whitewater for known rapids
- [ ] Calculate derived scores for all segments
- [ ] Merge: AW data where available, derived elsewhere
- [ ] Add to tileset and frontend

---

### 5. USGS Gauge Integration

**Status:** Script created at `scripts/usgs_gauges.py`

**Tables:**
- `usgs_gauges` - site locations (site_no, lat/lng, HUC, drainage area)
- `usgs_readings` - live readings (streamflow, gage height, temp)
- `usgs_statistics` - historical percentiles for comparison

**Commands:**
```bash
python scripts/usgs_gauges.py populate       # Fetch all US gauges
python scripts/usgs_gauges.py populate VT NH # Specific states
python scripts/usgs_gauges.py fetch          # Get live readings
```

**Integration steps:**
- [ ] Run `populate` to load gauge sites
- [ ] Add API endpoint to fetch nearby gauges
- [ ] Display gauges as map layer
- [ ] Show live readings in popup
- [ ] Periodic fetch via cron

---

### 6. Historical Flow Comparisons

**Using USGS statistics (after gauge integration)**

**Display format:**
```
Current flow: 450 cfs
This is: 72nd percentile (higher than usual)
Median for today: 280 cfs
```

**Implementation:**
- [ ] Fetch daily statistics from USGS
- [ ] Store in `usgs_statistics` table
- [ ] Compare current reading to day-of-year percentiles
- [ ] Show comparison in gauge popups and route summary

---

## Phase 2 (Future)

### Water Temperature Modeling
**Scope:** Large standalone project
- NHD + satellite data + air temp
- ML model for prediction
- Thermal refuge identification

---

## Data Sources - Hazards & Portages

| Source | Data | Access | Priority |
|--------|------|--------|----------|
| **USACE National Inventory of Dams** | 90k+ dams | Free download | 🔴 HIGH |
| **American Whitewater** | Rapids, hazards | Scrape/API | 🔴 HIGH |
| **OpenStreetMap** | Weirs, dams, rapids | Overpass API | 🟡 MED |
| **USGS GNIS** | Named falls, rapids | Free | 🟡 MED |
| **State agencies** | Fish ladders, low-head dams | Varies | 🟢 LOW |

---

## Next Actions (Priority Order)

1. **Finish `river_velocities` table** → Export for Mapbox
2. **Populate USGS gauges** → `python usgs_gauges.py populate`
3. **Add upstream routing** → Modify route.ts
4. **Add velocity adjustment** → Effective speed calculation
5. **Download dam data** → National Inventory of Dams
6. **Scrape American Whitewater** → Rapids database

---

*Last updated: 2026-02-14*
