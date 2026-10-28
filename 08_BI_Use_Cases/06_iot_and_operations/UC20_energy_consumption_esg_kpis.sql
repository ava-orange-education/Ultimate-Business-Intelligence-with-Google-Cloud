-- ============================================================================================================================
-- FILE        : UC20_energy_consumption_esg_kpis.sql
-- PROJECT     : NordLeaf BI Implementation
-- DOMAIN      : IoT & Operations / ESG & Sustainability
-- USE CASE    : UC-20 — Energy Consumption & ESG KPIs
-- PRIORITY    : P1 — Board-level ESG Reporting Requirement
-- LAYER       : EXT → STG → DWH → MGMT
-- AUTHOR      : NordLeaf Data Engineering Team
-- CREATED     : 2024-01-01
-- UPDATED     : 2024-01-01
-- ============================================================================================================================
--
-- ARCHITECTURE DIAGRAM
-- ────────────────────
--
--   sensor_events (Power kW) ──┐
--                              ├──► Energy KPI View [MGMT] ──► ESG Dashboard (Looker Studio)
--   fact_production ───────────┘                          └──► Dataplex Data Catalog (ESG lineage)
--
-- DATA FLOW:
--   1. Power sensors on each paper machine write kW readings to nordleaf_dwh.sensor_events (streaming)
--   2. Production output (tonnes of paper per shift/day) stored in nordleaf_dwh.fact_production
--   3. This SQL joins power sensor data to production tonnes to compute energy intensity (kWh/tonne)
--   4. Carbon emissions calculated using Finnish national grid emission factor (0.079 kg CO2e/kWh)
--   5. Monthly KPIs and EU Green Deal progress metrics published to nordleaf_mgmt.v_energy_consumption_kpi
--   6. Dataplex tags applied to view for ESG data lineage and governance compliance
--
-- ============================================================================================================================
-- BUSINESS PROBLEM
-- ────────────────
-- NordLeaf must demonstrate measurable energy reduction under the EU Green Deal (European Climate Law 2021).
-- Paper and pulp manufacturing is one of Europe's most energy-intensive industries.
-- NordLeaf's 5 Finnish mills (Tampere, Oulu, Turku, Espoo, Jyväskylä) consume heavy electricity via:
--   - Paper machine drives (main motors up to 5,000 kW)
--   - Steam dryer sections (thermal energy)
--   - Pumps, refiners, fans, and ancillary equipment
--
-- The Board and ESG committee require monthly reporting on:
--   (a) kWh consumed per tonne of paper (energy intensity)
--   (b) Scope 2 CO2e emissions per tonne (market-based method)
--   (c) Progress vs EU Green Deal 2030 target and world-class benchmark
--   (d) Energy cost per tonne (EUR) for margin analysis
--
-- ============================================================================================================================
-- EMISSION AND ENERGY CONSTANTS
-- ──────────────────────────────
--   Finnish grid emission factor  : 0.079 kg CO2e / kWh  (2023, Statistics Finland)
--                                   Finland benefits from high nuclear (Olkiluoto 3) and hydro share
--                                   EU average: ~0.233 kg CO2e/kWh for comparison
--
--   Finnish industrial electricity : ~0.12 EUR / kWh  (Nord Pool 2023 average, industrial tariff)
--
-- KPI TARGETS
-- ────────────
--   Energy Intensity Target    : ≤ 750 kWh / tonne  (world-class papermaking benchmark)
--   World-Class Best Practice  : < 600 kWh / tonne  (Nordic kraft leaders)
--   Scope 2 Emissions Target   : < 60 kg CO2e / tonne
--
-- EU GREEN DEAL 2030 TRAJECTORY
-- ──────────────────────────────
--   2005 Baseline (assumed)    : 1,200 kWh / tonne  (pre-modernisation NordLeaf estimate)
--   Required 2030 target       : 1,200 × 0.45 = 540 kWh / tonne (55% reduction from 2005)
--   Current gap                : Computed dynamically in SECTION 7
--   Years remaining to 2030    : 2030 - EXTRACT(YEAR FROM CURRENT_DATE())
--
-- ============================================================================================================================
-- KPIs PRODUCED BY THIS USE CASE
-- ────────────────────────────────
--   1. Daily / Monthly energy intensity (kWh/tonne) per mill
--   2. Scope 2 carbon emissions (kg CO2e/tonne) per mill
--   3. Energy cost (EUR/tonne) per mill
--   4. Month-over-month trend (LAG comparison) for all KPIs
--   5. % achievement vs world-class benchmark (750 kWh/tonne)
--   6. % progress vs EU Green Deal 2030 target (540 kWh/tonne)
--   7. Shift-level energy efficiency (morning / afternoon / night)
--   8. Mill energy efficiency ranking (WORLD CLASS / GOOD / AVERAGE / POOR)
--   9. Projected annual improvement rate needed to hit 2030 target
--
-- ============================================================================================================================


-- ============================================================================================================================
-- SECTION 1: HOURLY ENERGY CONSUMPTION FROM IoT SENSOR EVENTS (Power Sensor, kW)
-- Purpose : Aggregate raw kW readings to hourly kWh per mill
-- Method  : avg_kW in an hour × 1 hour = kWh for that hour (power × time = energy)
-- Window  : Last 90 days (supports monthly trend analysis)
-- ============================================================================================================================

WITH hourly_power AS (

  SELECT
    se.mill_key,
    TIMESTAMP_TRUNC(se.timestamp, HOUR)             AS hour_bucket,

    -- Average power draw during this hour (kW)
    ROUND(AVG(se.sensor_value), 2)                  AS avg_power_kw,

    -- Count of readings — for data quality check (expect ~6/hour per machine)
    COUNT(*)                                        AS reading_count,

    -- Number of distinct machines contributing Power readings this hour
    COUNT(DISTINCT se.machine_id)                   AS active_machine_count,

    -- Min/max power draw in this hour (for anomaly context)
    MIN(se.sensor_value)                            AS min_power_kw,
    MAX(se.sensor_value)                            AS max_power_kw,

    -- Energy (kWh) = average kW × 1 hour
    -- This is correct because: if avg demand over 1 hour = X kW, energy = X * 1h = X kWh
    ROUND(AVG(se.sensor_value) * 1.0, 2)            AS energy_kwh_this_hour

  FROM nordleaf_dwh.sensor_events se
  WHERE se.sensor_type = 'Power'
    AND se.sensor_value > 0                          -- exclude zero/null power readings (machine offline)
    AND se.sensor_value <= 6000                      -- hard cap: exclude sensor errors above 6,000 kW
    AND se.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  GROUP BY se.mill_key, TIMESTAMP_TRUNC(se.timestamp, HOUR)

),


-- ============================================================================================================================
-- SECTION 2A: AGGREGATE HOURLY TO DAILY ENERGY PER MILL
-- Purpose : Sum hourly kWh to get total daily kWh per mill
-- ============================================================================================================================

daily_energy AS (

  SELECT
    hp.mill_key,
    dm.mill_name,
    dm.mill_location,
    DATE(hp.hour_bucket)                            AS production_date,

    -- Total energy consumed by this mill today (kWh)
    ROUND(SUM(hp.energy_kwh_this_hour), 2)          AS total_kwh_per_day,

    -- Average hourly power draw (useful for capacity planning)
    ROUND(AVG(hp.avg_power_kw), 2)                  AS avg_daily_power_kw,

    -- Peak hourly demand (grid capacity planning)
    ROUND(MAX(hp.max_power_kw), 2)                  AS peak_power_kw_today,

    -- Data quality: average readings per machine per hour (expect ~6)
    ROUND(AVG(hp.reading_count), 1)                 AS avg_readings_per_hour,

    -- Operational hours: hours where mill was actively consuming power
    COUNT(DISTINCT hp.hour_bucket)                  AS active_hours_today

  FROM hourly_power hp
  JOIN nordleaf_dwh.dim_mill dm ON hp.mill_key = dm.mill_key
  GROUP BY hp.mill_key, dm.mill_name, dm.mill_location, DATE(hp.hour_bucket)

),


-- ============================================================================================================================
-- SECTION 2B: DAILY PRODUCTION TONNES PER MILL (from fact_production)
-- Purpose : Get daily paper production output in tonnes — the denominator for energy intensity
-- Note    : fact_production has one row per shift; we sum to get daily total per mill
-- ============================================================================================================================

daily_production AS (

  SELECT
    fp.mill_key,
    dd.full_date                                    AS production_date,
    SUM(fp.production_tonnes)                       AS daily_tonnes_produced,
    SUM(fp.production_hours)                        AS daily_production_hours,
    AVG(fp.machine_speed_m_per_min)                 AS avg_machine_speed_m_per_min,
    COUNT(DISTINCT fp.shift_id)                     AS shifts_run_today,

    -- Production efficiency: actual vs target tonnes
    ROUND(
      SAFE_DIVIDE(SUM(fp.production_tonnes), NULLIF(SUM(fp.target_tonnes), 0)) * 100, 2
    )                                               AS production_efficiency_pct

  FROM nordleaf_dwh.fact_production fp
  JOIN nordleaf_dwh.dim_date dd ON fp.date_key = dd.date_key
  WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND fp.production_tonnes > 0                    -- exclude non-production days
  GROUP BY fp.mill_key, dd.full_date

),


-- ============================================================================================================================
-- SECTION 2C: JOIN ENERGY TO PRODUCTION — COMPUTE DAILY ENERGY INTENSITY
-- Purpose : Combine kWh with production tonnes to get kWh/tonne (core energy intensity KPI)
-- ============================================================================================================================

daily_energy_intensity AS (

  SELECT
    de.mill_key,
    de.mill_name,
    de.mill_location,
    de.production_date,
    de.total_kwh_per_day,
    de.avg_daily_power_kw,
    de.peak_power_kw_today,
    de.active_hours_today,
    dp.daily_tonnes_produced,
    dp.daily_production_hours,
    dp.avg_machine_speed_m_per_min,
    dp.shifts_run_today,
    dp.production_efficiency_pct,

    -- CORE KPI 1: Energy Intensity (kWh per tonne of paper produced)
    ROUND(
      SAFE_DIVIDE(de.total_kwh_per_day, NULLIF(dp.daily_tonnes_produced, 0)), 2
    )                                               AS energy_intensity_kwh_per_tonne,

    -- Benchmark comparison: how far above/below the 750 kWh/tonne world-class target?
    ROUND(
      SAFE_DIVIDE(de.total_kwh_per_day, NULLIF(dp.daily_tonnes_produced, 0)) - 750, 2
    )                                               AS vs_world_class_target_kwh_per_tonne,

    -- Benchmark flag
    CASE
      WHEN SAFE_DIVIDE(de.total_kwh_per_day, NULLIF(dp.daily_tonnes_produced, 0)) < 600
        THEN 'WORLD_CLASS'              -- best-in-class Nordic paper mills
      WHEN SAFE_DIVIDE(de.total_kwh_per_day, NULLIF(dp.daily_tonnes_produced, 0)) < 750
        THEN 'GOOD'                     -- above world-class benchmark
      WHEN SAFE_DIVIDE(de.total_kwh_per_day, NULLIF(dp.daily_tonnes_produced, 0)) < 900
        THEN 'AVERAGE'                  -- meets minimum acceptable standards
      ELSE 'POOR'                       -- below acceptable; requires energy efficiency programme
    END                                             AS energy_benchmark_category

  FROM daily_energy de
  JOIN daily_production dp
    ON  de.mill_key        = dp.mill_key
    AND de.production_date = dp.production_date

),


-- ============================================================================================================================
-- SECTION 3: SCOPE 2 CARBON EMISSIONS CALCULATION
-- Purpose : Convert kWh to CO2e using Finnish national grid emission factor
-- Method  : Scope 2 (market-based) = kWh × grid_emission_factor_kg_per_kwh
-- Factor  : 0.079 kg CO2e/kWh (Statistics Finland 2023; includes nuclear, hydro, wind, gas)
-- Target  : < 60 kg CO2e/tonne of paper produced
-- ============================================================================================================================

daily_carbon AS (

  SELECT
    dei.*,

    -- Finnish grid emission factor (2023)
    0.079                                           AS grid_emission_factor_kg_co2e_per_kwh,

    -- CORE KPI 2: Total daily Scope 2 emissions (kg CO2e)
    ROUND(dei.total_kwh_per_day * 0.079, 2)         AS scope2_emissions_kg_co2e,

    -- Scope 2 emissions in tonnes CO2e (for standard ESG reporting)
    ROUND(dei.total_kwh_per_day * 0.079 / 1000, 4)  AS scope2_emissions_tonnes_co2e,

    -- CORE KPI 3: Scope 2 emission intensity (kg CO2e per tonne of paper)
    ROUND(
      SAFE_DIVIDE(dei.total_kwh_per_day * 0.079, NULLIF(dei.daily_tonnes_produced, 0)), 4
    )                                               AS scope2_intensity_kg_co2e_per_tonne,

    -- For context: what would emissions be on the EU average grid? (0.233 kg CO2e/kWh)
    ROUND(
      SAFE_DIVIDE(dei.total_kwh_per_day * 0.233, NULLIF(dei.daily_tonnes_produced, 0)), 4
    )                                               AS scope2_intensity_eu_avg_grid_kg_co2e_per_tonne,

    -- Carbon intensity vs 60 kg CO2e/tonne target
    CASE
      WHEN SAFE_DIVIDE(dei.total_kwh_per_day * 0.079, NULLIF(dei.daily_tonnes_produced, 0)) < 60
        THEN 'ON_TARGET'
      WHEN SAFE_DIVIDE(dei.total_kwh_per_day * 0.079, NULLIF(dei.daily_tonnes_produced, 0)) < 75
        THEN 'NEAR_TARGET'
      ELSE 'ABOVE_TARGET'
    END                                             AS scope2_target_status

  FROM daily_energy_intensity dei

),


-- ============================================================================================================================
-- SECTION 4: MONTHLY ENERGY KPIs PER MILL WITH MoM TREND
-- Purpose : Aggregate daily data to monthly level; compute MoM % change using LAG()
-- Output  : energy_intensity, scope2 intensity, cost per tonne — all with MoM deltas
-- ============================================================================================================================

monthly_kpis AS (

  SELECT
    mill_key,
    mill_name,
    mill_location,
    FORMAT_DATE('%Y-%m', production_date)           AS year_month,
    EXTRACT(YEAR FROM production_date)              AS report_year,
    EXTRACT(MONTH FROM production_date)             AS report_month,

    -- Volume
    ROUND(SUM(total_kwh_per_day), 0)                AS monthly_total_kwh,
    ROUND(SUM(daily_tonnes_produced), 0)            AS monthly_total_tonnes,
    COUNT(DISTINCT production_date)                 AS production_days_in_month,

    -- Energy Intensity (kWh/tonne)
    ROUND(
      SAFE_DIVIDE(SUM(total_kwh_per_day), NULLIF(SUM(daily_tonnes_produced), 0)), 2
    )                                               AS monthly_energy_intensity,

    -- Scope 2 emissions
    ROUND(SUM(scope2_emissions_kg_co2e), 0)         AS monthly_scope2_kg_co2e,
    ROUND(SUM(scope2_emissions_tonnes_co2e), 2)     AS monthly_scope2_tonnes_co2e,
    ROUND(
      SAFE_DIVIDE(SUM(scope2_emissions_kg_co2e), NULLIF(SUM(daily_tonnes_produced), 0)), 4
    )                                               AS monthly_scope2_intensity_kg_per_tonne,

    -- Average production efficiency
    ROUND(AVG(production_efficiency_pct), 2)        AS avg_production_efficiency_pct,

    -- Peak power (max of daily peaks)
    ROUND(MAX(peak_power_kw_today), 0)              AS monthly_peak_power_kw

  FROM daily_carbon
  GROUP BY mill_key, mill_name, mill_location,
           FORMAT_DATE('%Y-%m', production_date),
           EXTRACT(YEAR FROM production_date),
           EXTRACT(MONTH FROM production_date)

),

monthly_kpis_with_trend AS (

  SELECT
    mk.*,

    -- MoM Energy Intensity: previous month via LAG()
    LAG(mk.monthly_energy_intensity) OVER (
      PARTITION BY mk.mill_key
      ORDER BY mk.year_month
    )                                               AS prev_month_energy_intensity,

    -- MoM % change in energy intensity (negative = improvement)
    ROUND(
      SAFE_DIVIDE(
        mk.monthly_energy_intensity
        - LAG(mk.monthly_energy_intensity) OVER (PARTITION BY mk.mill_key ORDER BY mk.year_month),
        NULLIF(LAG(mk.monthly_energy_intensity) OVER (PARTITION BY mk.mill_key ORDER BY mk.year_month), 0)
      ) * 100, 2
    )                                               AS mom_energy_intensity_change_pct,

    -- MoM Scope 2 intensity change
    ROUND(
      SAFE_DIVIDE(
        mk.monthly_scope2_intensity_kg_per_tonne
        - LAG(mk.monthly_scope2_intensity_kg_per_tonne) OVER (PARTITION BY mk.mill_key ORDER BY mk.year_month),
        NULLIF(LAG(mk.monthly_scope2_intensity_kg_per_tonne) OVER (PARTITION BY mk.mill_key ORDER BY mk.year_month), 0)
      ) * 100, 2
    )                                               AS mom_scope2_intensity_change_pct,

    -- % vs world-class 750 kWh/tonne benchmark
    ROUND(
      SAFE_DIVIDE(mk.monthly_energy_intensity - 750, 750) * 100, 2
    )                                               AS pct_vs_world_class_750_target,

    -- % vs EU Green Deal 2030 target (540 kWh/tonne)
    ROUND(
      SAFE_DIVIDE(mk.monthly_energy_intensity - 540, 540) * 100, 2
    )                                               AS pct_vs_eu_2030_target_540

  FROM monthly_kpis mk

),


-- ============================================================================================================================
-- SECTION 5: SHIFT-LEVEL ENERGY EFFICIENCY (MORNING / AFTERNOON / NIGHT)
-- Purpose : Identify which shifts are most/least energy-efficient per mill
-- Insight : Night shifts often run lighter schedules; some mills show higher energy intensity at night
--           due to lower production volume while fixed loads (lighting, HVAC, standby motors) remain
-- ============================================================================================================================

shift_energy AS (

  SELECT
    fp.mill_key,
    dm.mill_name,
    dd.full_date                                    AS production_date,
    fp.shift_name,                                  -- 'Morning' / 'Afternoon' / 'Night'

    -- Production per shift
    SUM(fp.production_tonnes)                       AS shift_tonnes,
    SUM(fp.production_hours)                        AS shift_hours,
    AVG(fp.machine_speed_m_per_min)                 AS shift_avg_speed,

    -- Approximate kWh for shift period (join to hourly_power using mill+date+hour range)
    -- Morning shift: 06:00–14:00 (8h), Afternoon: 14:00–22:00 (8h), Night: 22:00–06:00 (8h)
    SUM(hp_shift.energy_kwh_this_hour)              AS shift_kwh,

    -- Shift energy intensity
    ROUND(
      SAFE_DIVIDE(
        SUM(hp_shift.energy_kwh_this_hour),
        NULLIF(SUM(fp.production_tonnes), 0)
      ), 2
    )                                               AS shift_energy_intensity_kwh_per_tonne,

    -- Shift efficiency ranking (lower = more efficient)
    RANK() OVER (
      PARTITION BY fp.mill_key, dd.full_date
      ORDER BY
        SAFE_DIVIDE(SUM(hp_shift.energy_kwh_this_hour), NULLIF(SUM(fp.production_tonnes), 0)) ASC
    )                                               AS shift_efficiency_rank_today

  FROM nordleaf_dwh.fact_production fp
  JOIN nordleaf_dwh.dim_date dd ON fp.date_key = dd.date_key
  JOIN nordleaf_dwh.dim_mill dm ON fp.mill_key  = dm.mill_key

  -- Join to hourly power for the matching hours of each shift
  LEFT JOIN (
    SELECT
      mill_key,
      DATE(hour_bucket)    AS power_date,
      EXTRACT(HOUR FROM hour_bucket) AS power_hour,
      energy_kwh_this_hour
    FROM hourly_power
  ) hp_shift
    ON  fp.mill_key   = hp_shift.mill_key
    AND dd.full_date  = hp_shift.power_date
    AND (
      -- Morning shift: hours 6–13
      (fp.shift_name = 'Morning'   AND hp_shift.power_hour BETWEEN 6  AND 13)
      -- Afternoon shift: hours 14–21
      OR (fp.shift_name = 'Afternoon' AND hp_shift.power_hour BETWEEN 14 AND 21)
      -- Night shift: hours 22–23 and 0–5 (spans midnight)
      OR (fp.shift_name = 'Night'     AND (hp_shift.power_hour >= 22 OR hp_shift.power_hour <= 5))
    )

  WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND fp.production_tonnes > 0

  GROUP BY fp.mill_key, dm.mill_name, dd.full_date, fp.shift_name

),


-- ============================================================================================================================
-- SECTION 6: ENERGY COST CALCULATION (EUR)
-- Purpose  : Translate kWh consumption into EUR cost using Finnish Nord Pool industrial tariff
-- Price    : 0.12 EUR/kWh (Nord Pool 2023 average for Finnish industrial consumers)
-- Note     : Real implementation should join to a dim_electricity_price table for daily spot prices
--            Using fixed average here as approximation for ESG reporting
-- ============================================================================================================================

monthly_energy_cost AS (

  SELECT
    mk.mill_key,
    mk.mill_name,
    mk.year_month,
    mk.monthly_total_kwh,
    mk.monthly_total_tonnes,
    mk.monthly_energy_intensity,

    -- Electricity price constant (EUR/kWh — Nord Pool 2023 Finnish industrial average)
    0.12                                            AS electricity_price_eur_per_kwh,

    -- CORE KPI 4: Monthly energy cost (EUR)
    ROUND(mk.monthly_total_kwh * 0.12, 0)           AS monthly_energy_cost_eur,

    -- Energy cost per tonne (EUR/tonne)
    ROUND(
      SAFE_DIVIDE(mk.monthly_total_kwh * 0.12, NULLIF(mk.monthly_total_tonnes, 0)), 2
    )                                               AS energy_cost_eur_per_tonne,

    -- MoM cost change
    LAG(ROUND(SAFE_DIVIDE(mk.monthly_total_kwh * 0.12, NULLIF(mk.monthly_total_tonnes, 0)), 2))
      OVER (PARTITION BY mk.mill_key ORDER BY mk.year_month)
                                                    AS prev_month_cost_eur_per_tonne,

    ROUND(
      SAFE_DIVIDE(
        ROUND(SAFE_DIVIDE(mk.monthly_total_kwh * 0.12, NULLIF(mk.monthly_total_tonnes, 0)), 2)
        - LAG(ROUND(SAFE_DIVIDE(mk.monthly_total_kwh * 0.12, NULLIF(mk.monthly_total_tonnes, 0)), 2))
          OVER (PARTITION BY mk.mill_key ORDER BY mk.year_month),
        NULLIF(
          LAG(ROUND(SAFE_DIVIDE(mk.monthly_total_kwh * 0.12, NULLIF(mk.monthly_total_tonnes, 0)), 2))
            OVER (PARTITION BY mk.mill_key ORDER BY mk.year_month),
          0
        )
      ) * 100, 2
    )                                               AS mom_cost_per_tonne_change_pct,

    -- Annualised energy cost projection (current month × 12)
    ROUND(mk.monthly_total_kwh * 0.12 * 12, 0)     AS annualised_energy_cost_eur,

    -- Cost intensity category
    CASE
      WHEN SAFE_DIVIDE(mk.monthly_total_kwh * 0.12, NULLIF(mk.monthly_total_tonnes, 0)) < 72
        THEN 'COST_EFFICIENT'      -- < EUR 72/tonne = below world-class 600 kWh/tonne * 0.12
      WHEN SAFE_DIVIDE(mk.monthly_total_kwh * 0.12, NULLIF(mk.monthly_total_tonnes, 0)) < 90
        THEN 'ACCEPTABLE'          -- EUR 72–90/tonne
      ELSE 'HIGH_COST'             -- above EUR 90/tonne; energy efficiency investment justified
    END                                             AS energy_cost_category

  FROM monthly_kpis_with_trend mk

),


-- ============================================================================================================================
-- SECTION 7: EU GREEN DEAL PROGRESS TRACKING
-- Purpose  : Compute NordLeaf's trajectory vs EU Climate Law 2030 55% reduction target
-- Baseline : 2005 assumed energy intensity = 1,200 kWh/tonne (pre-modernisation estimate)
-- Target   : 540 kWh/tonne by 2030 (1,200 × 0.45 = 540; 55% reduction)
-- ============================================================================================================================

eu_green_deal_progress AS (

  SELECT
    mk.mill_key,
    mk.mill_name,
    mk.year_month,
    mk.report_year,
    mk.monthly_energy_intensity                     AS current_intensity_kwh_per_tonne,

    -- EU Green Deal constants
    1200.0                                          AS baseline_2005_kwh_per_tonne,
    540.0                                           AS target_2030_kwh_per_tonne,   -- 55% reduction from 2005
    750.0                                           AS world_class_benchmark_kwh_per_tonne,
    2030                                            AS target_year,

    -- Reduction already achieved vs 2005 baseline
    ROUND(1200.0 - mk.monthly_energy_intensity, 2)  AS reduction_achieved_kwh_per_tonne,

    -- % reduction achieved vs 2005 baseline
    ROUND(
      SAFE_DIVIDE(1200.0 - mk.monthly_energy_intensity, 1200.0) * 100, 2
    )                                               AS reduction_pct_achieved_vs_2005,

    -- Remaining gap to 2030 target (positive = still above target; negative = already achieved)
    ROUND(mk.monthly_energy_intensity - 540.0, 2)   AS gap_to_2030_target_kwh_per_tonne,

    -- 2030 target status
    CASE
      WHEN mk.monthly_energy_intensity <= 540  THEN 'TARGET_MET'
      WHEN mk.monthly_energy_intensity <= 600  THEN 'ON_TRACK'
      WHEN mk.monthly_energy_intensity <= 750  THEN 'PROGRESS_NEEDED'
      ELSE 'SIGNIFICANTLY_BEHIND'
    END                                             AS eu_target_status,

    -- Years remaining to 2030
    2030 - mk.report_year                          AS years_remaining_to_2030,

    -- Required annual reduction to hit 540 kWh/tonne by 2030
    -- Formula: (current - target) / years_remaining = required reduction per year
    ROUND(
      SAFE_DIVIDE(
        mk.monthly_energy_intensity - 540.0,
        NULLIF(2030 - mk.report_year, 0)
      ), 2
    )                                               AS required_annual_reduction_kwh_per_tonne,

    -- Required annual improvement rate (%)
    ROUND(
      SAFE_DIVIDE(
        SAFE_DIVIDE(mk.monthly_energy_intensity - 540.0, NULLIF(2030 - mk.report_year, 0)),
        NULLIF(mk.monthly_energy_intensity, 0)
      ) * 100, 2
    )                                               AS required_annual_improvement_rate_pct,

    -- Scope 2 carbon savings vs EU average grid (environmental benefit of Finnish nuclear/hydro)
    -- If NordLeaf was on EU avg grid (0.233), emissions would be 3x higher
    ROUND(
      mk.monthly_total_kwh * (0.233 - 0.079) / 1000, 2
    )                                               AS scope2_saving_vs_eu_avg_grid_tonnes_co2e

  FROM monthly_kpis_with_trend mk

),


-- ============================================================================================================================
-- SECTION 8: MILL ENERGY EFFICIENCY RANKING
-- Purpose : Rank all 5 NordLeaf mills by energy intensity for the current month
-- Output  : RANK() 1–5, category label, gap to best-performing mill
-- ============================================================================================================================

current_month_ranking AS (

  SELECT
    mk.mill_key,
    mk.mill_name,
    mk.year_month,
    mk.monthly_energy_intensity,
    mk.monthly_total_tonnes,
    mk.monthly_scope2_intensity_kg_per_tonne,
    mk.mom_energy_intensity_change_pct,

    -- Rank by energy intensity (1 = most efficient / lowest kWh per tonne)
    RANK() OVER (
      PARTITION BY mk.year_month
      ORDER BY mk.monthly_energy_intensity ASC
    )                                               AS efficiency_rank,

    -- Best mill intensity for this month (for gap analysis)
    MIN(mk.monthly_energy_intensity) OVER (
      PARTITION BY mk.year_month
    )                                               AS best_mill_intensity_this_month,

    -- Gap to best performing mill (kWh/tonne above best)
    ROUND(
      mk.monthly_energy_intensity
      - MIN(mk.monthly_energy_intensity) OVER (PARTITION BY mk.year_month), 2
    )                                               AS gap_to_best_mill_kwh_per_tonne,

    -- Energy efficiency tier
    CASE
      WHEN mk.monthly_energy_intensity < 600  THEN 'WORLD_CLASS'    -- best-in-class Nordic
      WHEN mk.monthly_energy_intensity < 750  THEN 'GOOD'           -- above world-class benchmark
      WHEN mk.monthly_energy_intensity < 900  THEN 'AVERAGE'        -- room for improvement
      ELSE 'POOR'                                                    -- significant investment needed
    END                                             AS efficiency_tier,

    -- Improvement potential: if this mill matched the best mill
    ROUND(
      (mk.monthly_energy_intensity - MIN(mk.monthly_energy_intensity) OVER (PARTITION BY mk.year_month))
      * mk.monthly_total_tonnes
      / 1000, 0                                     -- convert to MWh difference
    )                                               AS improvement_potential_mwh_per_month,

    -- Cost saving potential (EUR) if matching best mill
    ROUND(
      (mk.monthly_energy_intensity - MIN(mk.monthly_energy_intensity) OVER (PARTITION BY mk.year_month))
      * mk.monthly_total_tonnes
      * 0.12, 0
    )                                               AS cost_saving_potential_eur_per_month

  FROM monthly_kpis_with_trend mk
  WHERE mk.year_month = FORMAT_DATE('%Y-%m', DATE_TRUNC(CURRENT_DATE(), MONTH))

)


-- ============================================================================================================================
-- SECTION 9: MGMT VIEW — v_energy_consumption_kpi
-- Purpose  : Consolidated ESG energy KPI view for board reporting, ESG dashboard, and Dataplex catalog
-- Audience : ESG team, Finance, Plant managers, Board sustainability committee
-- Refresh  : Daily scheduled query at 06:00 Finnish time (EET/EEST UTC+2/UTC+3)
-- ============================================================================================================================

CREATE OR REPLACE VIEW nordleaf_mgmt.v_energy_consumption_kpi
OPTIONS (
  description = 'UC-20: NordLeaf energy consumption & ESG KPIs. Daily energy intensity (kWh/tonne), Scope 2 carbon emissions, EUR cost per tonne, EU Green Deal 2030 progress tracking, and mill efficiency ranking across 5 Finnish mills. Finnish grid factor: 0.079 kg CO2e/kWh.'
)
AS

-- 9A: Daily energy intensity detail (operational level — for trend charts)
SELECT
  'DAILY_DETAIL'                      AS report_section,
  dc.mill_key,
  dc.mill_name,
  dc.mill_location,
  dc.production_date,
  FORMAT_DATE('%Y-%m', dc.production_date) AS year_month,
  EXTRACT(YEAR FROM dc.production_date) AS report_year,

  -- Volume
  dc.total_kwh_per_day,
  dc.daily_tonnes_produced,
  dc.active_hours_today,

  -- Core KPIs
  dc.energy_intensity_kwh_per_tonne,
  dc.vs_world_class_target_kwh_per_tonne,
  dc.energy_benchmark_category,

  -- Carbon
  dc.scope2_emissions_kg_co2e,
  dc.scope2_emissions_tonnes_co2e,
  dc.scope2_intensity_kg_co2e_per_tonne,
  dc.scope2_intensity_eu_avg_grid_kg_co2e_per_tonne,
  dc.scope2_target_status,

  -- Machine performance
  dc.avg_machine_speed_m_per_min,
  dc.production_efficiency_pct,
  dc.avg_daily_power_kw,
  dc.peak_power_kw_today,

  -- Monthly trend stubs (not available at daily level)
  CAST(NULL AS FLOAT64)               AS monthly_energy_intensity,
  CAST(NULL AS FLOAT64)               AS mom_energy_intensity_change_pct,
  CAST(NULL AS FLOAT64)               AS energy_cost_eur_per_tonne,
  CAST(NULL AS FLOAT64)               AS monthly_energy_cost_eur,
  CAST(NULL AS FLOAT64)               AS reduction_pct_achieved_vs_2005,
  CAST(NULL AS FLOAT64)               AS gap_to_2030_target_kwh_per_tonne,
  CAST(NULL AS STRING)                AS eu_target_status,
  CAST(NULL AS INT64)                 AS efficiency_rank,
  CAST(NULL AS STRING)                AS efficiency_tier,

  CURRENT_TIMESTAMP()                 AS view_refreshed_at

FROM daily_carbon dc

UNION ALL

-- 9B: Monthly KPI summary with MoM trend and EU Green Deal progress
SELECT
  'MONTHLY_SUMMARY'                   AS report_section,
  mkt.mill_key,
  mkt.mill_name,
  CAST(NULL AS STRING)                AS mill_location,
  CAST(NULL AS DATE)                  AS production_date,
  mkt.year_month,
  mkt.report_year,

  -- Volume
  mkt.monthly_total_kwh               AS total_kwh_per_day,   -- monthly total in this column
  mkt.monthly_total_tonnes            AS daily_tonnes_produced,
  mkt.production_days_in_month        AS active_hours_today,  -- repurposed for production days

  -- Core KPIs
  mkt.monthly_energy_intensity        AS energy_intensity_kwh_per_tonne,
  mkt.pct_vs_world_class_750_target   AS vs_world_class_target_kwh_per_tonne,
  CASE
    WHEN mkt.monthly_energy_intensity < 600 THEN 'WORLD_CLASS'
    WHEN mkt.monthly_energy_intensity < 750 THEN 'GOOD'
    WHEN mkt.monthly_energy_intensity < 900 THEN 'AVERAGE'
    ELSE 'POOR'
  END                                 AS energy_benchmark_category,

  -- Carbon
  mkt.monthly_scope2_kg_co2e         AS scope2_emissions_kg_co2e,
  mkt.monthly_scope2_tonnes_co2e     AS scope2_emissions_tonnes_co2e,
  mkt.monthly_scope2_intensity_kg_per_tonne AS scope2_intensity_kg_co2e_per_tonne,
  CAST(NULL AS FLOAT64)              AS scope2_intensity_eu_avg_grid_kg_co2e_per_tonne,
  CASE
    WHEN mkt.monthly_scope2_intensity_kg_per_tonne < 60 THEN 'ON_TARGET'
    WHEN mkt.monthly_scope2_intensity_kg_per_tonne < 75 THEN 'NEAR_TARGET'
    ELSE 'ABOVE_TARGET'
  END                                 AS scope2_target_status,

  -- Machine performance (monthly averages)
  CAST(NULL AS FLOAT64)              AS avg_machine_speed_m_per_min,
  mkt.avg_production_efficiency_pct  AS production_efficiency_pct,
  CAST(NULL AS FLOAT64)              AS avg_daily_power_kw,
  mkt.monthly_peak_power_kw          AS peak_power_kw_today,

  -- Monthly trend (populated in this section)
  mkt.monthly_energy_intensity,
  mkt.mom_energy_intensity_change_pct,
  mec.energy_cost_eur_per_tonne,
  mec.monthly_energy_cost_eur,

  -- EU Green Deal progress
  egdp.reduction_pct_achieved_vs_2005,
  egdp.gap_to_2030_target_kwh_per_tonne,
  egdp.eu_target_status,

  -- Mill ranking (current month only; NULL for historical months)
  CAST(NULL AS INT64)                AS efficiency_rank,
  CAST(NULL AS STRING)               AS efficiency_tier,

  CURRENT_TIMESTAMP()                AS view_refreshed_at

FROM monthly_kpis_with_trend mkt
LEFT JOIN monthly_energy_cost mec
  ON  mkt.mill_key   = mec.mill_key
  AND mkt.year_month = mec.year_month
LEFT JOIN eu_green_deal_progress egdp
  ON  mkt.mill_key   = egdp.mill_key
  AND mkt.year_month = egdp.year_month

UNION ALL

-- 9C: Current month mill ranking (for leaderboard tile in ESG dashboard)
SELECT
  'MILL_RANKING'                      AS report_section,
  cmr.mill_key,
  cmr.mill_name,
  CAST(NULL AS STRING)                AS mill_location,
  CAST(NULL AS DATE)                  AS production_date,
  cmr.year_month,
  EXTRACT(YEAR FROM CURRENT_DATE())   AS report_year,

  CAST(NULL AS FLOAT64)               AS total_kwh_per_day,
  cmr.monthly_total_tonnes            AS daily_tonnes_produced,
  CAST(NULL AS INT64)                 AS active_hours_today,

  -- Energy intensity with benchmark
  cmr.monthly_energy_intensity        AS energy_intensity_kwh_per_tonne,
  cmr.gap_to_best_mill_kwh_per_tonne  AS vs_world_class_target_kwh_per_tonne,
  cmr.efficiency_tier                 AS energy_benchmark_category,

  -- Carbon
  CAST(NULL AS FLOAT64)               AS scope2_emissions_kg_co2e,
  CAST(NULL AS FLOAT64)               AS scope2_emissions_tonnes_co2e,
  cmr.monthly_scope2_intensity_kg_per_tonne AS scope2_intensity_kg_co2e_per_tonne,
  CAST(NULL AS FLOAT64)               AS scope2_intensity_eu_avg_grid_kg_co2e_per_tonne,
  CAST(NULL AS STRING)                AS scope2_target_status,

  -- Machine performance
  CAST(NULL AS FLOAT64)               AS avg_machine_speed_m_per_min,
  CAST(NULL AS FLOAT64)               AS production_efficiency_pct,
  CAST(NULL AS FLOAT64)               AS avg_daily_power_kw,
  CAST(NULL AS FLOAT64)               AS peak_power_kw_today,

  -- Monthly trend
  cmr.monthly_energy_intensity,
  cmr.mom_energy_intensity_change_pct,
  CAST(NULL AS FLOAT64)               AS energy_cost_eur_per_tonne,
  CAST(NULL AS FLOAT64)               AS monthly_energy_cost_eur,

  -- EU progress
  CAST(NULL AS FLOAT64)               AS reduction_pct_achieved_vs_2005,
  ROUND(cmr.monthly_energy_intensity - 540.0, 2) AS gap_to_2030_target_kwh_per_tonne,
  CAST(NULL AS STRING)                AS eu_target_status,

  -- Ranking columns populated
  cmr.efficiency_rank,
  cmr.efficiency_tier,

  CURRENT_TIMESTAMP()                 AS view_refreshed_at

FROM current_month_ranking cmr;


-- ============================================================================================================================
-- USAGE EXAMPLES
-- ============================================================================================================================

/*
  -- 1. Daily energy intensity for all mills — last 30 days (for trend chart)
  SELECT mill_name, production_date, energy_intensity_kwh_per_tonne,
         scope2_intensity_kg_co2e_per_tonne, energy_benchmark_category
  FROM nordleaf_mgmt.v_energy_consumption_kpi
  WHERE report_section = 'DAILY_DETAIL'
    AND production_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  ORDER BY mill_name, production_date;

  -- 2. Monthly ESG summary for board report (current year)
  SELECT year_month, mill_name,
         monthly_energy_intensity, mom_energy_intensity_change_pct,
         scope2_intensity_kg_co2e_per_tonne,
         energy_cost_eur_per_tonne,
         reduction_pct_achieved_vs_2005,
         gap_to_2030_target_kwh_per_tonne, eu_target_status
  FROM nordleaf_mgmt.v_energy_consumption_kpi
  WHERE report_section = 'MONTHLY_SUMMARY'
    AND report_year = EXTRACT(YEAR FROM CURRENT_DATE())
  ORDER BY year_month, mill_name;

  -- 3. Current month mill energy efficiency leaderboard
  SELECT efficiency_rank, mill_name,
         monthly_energy_intensity,
         efficiency_tier,
         gap_to_best_mill_kwh_per_tonne,
         scope2_intensity_kg_co2e_per_tonne,
         mom_energy_intensity_change_pct
  FROM nordleaf_mgmt.v_energy_consumption_kpi
  WHERE report_section = 'MILL_RANKING'
  ORDER BY efficiency_rank ASC;

  -- 4. Mills that are POOR on energy efficiency — targets for improvement programme
  SELECT mill_name, monthly_energy_intensity, gap_to_2030_target_kwh_per_tonne
  FROM nordleaf_mgmt.v_energy_consumption_kpi
  WHERE report_section = 'MILL_RANKING'
    AND efficiency_tier = 'POOR';

  -- 5. EU Green Deal progress check — are all mills on track for 2030?
  SELECT mill_name, year_month,
         monthly_energy_intensity,
         reduction_pct_achieved_vs_2005,
         gap_to_2030_target_kwh_per_tonne,
         eu_target_status
  FROM nordleaf_mgmt.v_energy_consumption_kpi
  WHERE report_section = 'MONTHLY_SUMMARY'
    AND year_month = FORMAT_DATE('%Y-%m', DATE_TRUNC(CURRENT_DATE(), MONTH))
  ORDER BY gap_to_2030_target_kwh_per_tonne DESC;
*/

-- ============================================================================================================================
-- END OF FILE: UC20_energy_consumption_esg_kpis.sql
-- ============================================================================================================================
