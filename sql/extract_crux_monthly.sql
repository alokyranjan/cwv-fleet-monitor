-- Extraction query: pulls CrUX data for a specific month, joined with origins metadata
-- Parameters: {project}, {dataset}, {target_yyyymm}

INSERT INTO `{project}.{dataset}.cwv_monthly`
SELECT
    crux.yyyymm,
    crux.date,
    crux.origin,
    crux.device,
    crux.rank,

    -- Denormalized dealer metadata
    o.dealer_name,
    o.dealer_group,
    o.oem_brand,
    o.region,
    o.state,
    o.platform_version,

    -- P75 values
    crux.p75_lcp,
    crux.p75_fcp,
    crux.p75_inp,
    crux.p75_cls,
    crux.p75_ttfb,

    -- LCP distribution
    crux.fast_lcp,
    crux.avg_lcp,
    crux.slow_lcp,

    -- FCP distribution
    crux.fast_fcp,
    crux.avg_fcp,
    crux.slow_fcp,

    -- INP distribution
    crux.fast_inp,
    crux.avg_inp,
    crux.slow_inp,

    -- CLS distribution
    crux.small_cls,
    crux.medium_cls,
    crux.large_cls,

    -- TTFB distribution
    crux.fast_ttfb,
    crux.avg_ttfb,
    crux.slow_ttfb,

    -- Device density
    crux.desktopDensity,
    crux.phoneDensity,
    crux.tabletDensity,

    -- Navigation types
    crux.navigation_types_navigate,
    crux.navigation_types_navigate_cache,
    crux.navigation_types_reload,
    crux.navigation_types_restore,
    crux.navigation_types_back_forward,
    crux.navigation_types_back_forward_cache,
    crux.navigation_types_prerender,

    -- RTT
    crux.low_rtt,
    crux.medium_rtt,
    crux.high_rtt,

    -- Meta
    CURRENT_TIMESTAMP()

FROM `chrome-ux-report.materialized.device_summary` crux
INNER JOIN `{project}.{dataset}.origins` o
    ON crux.origin = o.origin
WHERE
    crux.yyyymm = {target_yyyymm}
    AND o.is_active = TRUE
