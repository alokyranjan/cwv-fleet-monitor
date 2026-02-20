-- DDL reference for the cwv_monthly table (extracted CrUX data with denormalized dealer metadata)
-- Actual creation is handled by scripts/setup_dataset.py
-- Partitioned by RANGE_BUCKET(yyyymm) and clustered by origin, oem_brand, region

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.cwv_monthly` (
    -- Dimensions
    yyyymm              INT64 NOT NULL,
    date                DATE,
    origin              STRING NOT NULL,
    device              STRING,
    rank                INT64,

    -- Dealer metadata (denormalized from origins)
    dealer_name         STRING,
    dealer_group        STRING,
    oem_brand           STRING,
    region              STRING,
    state               STRING,
    platform_version    STRING,

    -- P75 values
    p75_lcp             FLOAT64,
    p75_fcp             FLOAT64,
    p75_inp             FLOAT64,
    p75_cls             FLOAT64,
    p75_ttfb            FLOAT64,

    -- LCP distribution
    fast_lcp            FLOAT64,
    avg_lcp             FLOAT64,
    slow_lcp            FLOAT64,

    -- FCP distribution
    fast_fcp            FLOAT64,
    avg_fcp             FLOAT64,
    slow_fcp            FLOAT64,

    -- INP distribution
    fast_inp            FLOAT64,
    avg_inp             FLOAT64,
    slow_inp            FLOAT64,

    -- CLS distribution
    small_cls           FLOAT64,
    medium_cls          FLOAT64,
    large_cls           FLOAT64,

    -- TTFB distribution
    fast_ttfb           FLOAT64,
    avg_ttfb            FLOAT64,
    slow_ttfb           FLOAT64,

    -- Device density
    desktopDensity      FLOAT64,
    phoneDensity        FLOAT64,
    tabletDensity       FLOAT64,

    -- Navigation types
    nav_navigate                STRING,
    nav_navigate_cache          STRING,
    nav_reload                  STRING,
    nav_restore                 STRING,
    nav_back_forward            STRING,
    nav_back_forward_cache      STRING,
    nav_prerender               STRING,

    -- Round-trip time distribution
    low_rtt             FLOAT64,
    medium_rtt          FLOAT64,
    high_rtt            FLOAT64,

    -- Meta
    extracted_at        TIMESTAMP
)
PARTITION BY RANGE_BUCKET(yyyymm, GENERATE_ARRAY(202001, 203001, 1))
CLUSTER BY origin, oem_brand, region;
