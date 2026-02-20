-- Fleet health summary with month-over-month comparison
-- Parameters: {project}, {dataset}, {target_yyyymm}, {prev_yyyymm}

WITH current_month AS (
    SELECT
        origin,
        device,
        p75_lcp,
        p75_inp,
        p75_cls,
        -- CWV pass: LCP <= 2500 AND INP <= 200 AND CLS <= 0.1
        CASE WHEN p75_lcp <= 2500 AND p75_inp <= 200 AND p75_cls <= 0.1 THEN 1 ELSE 0 END AS cwv_pass,
        CASE WHEN p75_lcp <= 2500 THEN 1 ELSE 0 END AS lcp_pass,
        CASE WHEN p75_inp <= 200 THEN 1 ELSE 0 END AS inp_pass,
        CASE WHEN p75_cls <= 0.1 THEN 1 ELSE 0 END AS cls_pass
    FROM `{project}.{dataset}.cwv_monthly`
    WHERE yyyymm = {target_yyyymm}
),

prev_month AS (
    SELECT
        origin,
        device,
        p75_lcp,
        p75_inp,
        p75_cls,
        CASE WHEN p75_lcp <= 2500 AND p75_inp <= 200 AND p75_cls <= 0.1 THEN 1 ELSE 0 END AS cwv_pass,
        CASE WHEN p75_lcp <= 2500 THEN 1 ELSE 0 END AS lcp_pass,
        CASE WHEN p75_inp <= 200 THEN 1 ELSE 0 END AS inp_pass,
        CASE WHEN p75_cls <= 0.1 THEN 1 ELSE 0 END AS cls_pass
    FROM `{project}.{dataset}.cwv_monthly`
    WHERE yyyymm = {prev_yyyymm}
),

current_stats AS (
    SELECT
        COUNT(DISTINCT origin) AS origins_with_data,
        ROUND(100.0 * AVG(cwv_pass), 1) AS cwv_pass_rate,
        ROUND(100.0 * AVG(lcp_pass), 1) AS lcp_pass_rate,
        ROUND(100.0 * AVG(inp_pass), 1) AS inp_pass_rate,
        ROUND(100.0 * AVG(cls_pass), 1) AS cls_pass_rate,
        ROUND(AVG(p75_lcp), 0) AS avg_p75_lcp,
        ROUND(AVG(p75_inp), 0) AS avg_p75_inp,
        ROUND(AVG(p75_cls), 3) AS avg_p75_cls
    FROM current_month
),

prev_stats AS (
    SELECT
        ROUND(100.0 * AVG(cwv_pass), 1) AS cwv_pass_rate,
        ROUND(100.0 * AVG(lcp_pass), 1) AS lcp_pass_rate,
        ROUND(100.0 * AVG(inp_pass), 1) AS inp_pass_rate,
        ROUND(100.0 * AVG(cls_pass), 1) AS cls_pass_rate,
        ROUND(AVG(p75_lcp), 0) AS avg_p75_lcp,
        ROUND(AVG(p75_inp), 0) AS avg_p75_inp,
        ROUND(AVG(p75_cls), 3) AS avg_p75_cls
    FROM prev_month
),

coverage AS (
    SELECT
        COUNT(DISTINCT origin) AS total_active_origins
    FROM `{project}.{dataset}.origins`
    WHERE is_active = TRUE
)

SELECT
    c.origins_with_data,
    cov.total_active_origins,
    ROUND(100.0 * c.origins_with_data / NULLIF(cov.total_active_origins, 0), 1) AS coverage_pct,
    c.cwv_pass_rate,
    c.lcp_pass_rate,
    c.inp_pass_rate,
    c.cls_pass_rate,
    c.avg_p75_lcp,
    c.avg_p75_inp,
    c.avg_p75_cls,
    -- MoM deltas (NULL if no previous month)
    ROUND(c.cwv_pass_rate - p.cwv_pass_rate, 1) AS cwv_pass_rate_delta,
    ROUND(c.lcp_pass_rate - p.lcp_pass_rate, 1) AS lcp_pass_rate_delta,
    ROUND(c.inp_pass_rate - p.inp_pass_rate, 1) AS inp_pass_rate_delta,
    ROUND(c.cls_pass_rate - p.cls_pass_rate, 1) AS cls_pass_rate_delta,
    ROUND(c.avg_p75_lcp - p.avg_p75_lcp, 0) AS avg_p75_lcp_delta,
    ROUND(c.avg_p75_inp - p.avg_p75_inp, 0) AS avg_p75_inp_delta,
    ROUND(c.avg_p75_cls - p.avg_p75_cls, 3) AS avg_p75_cls_delta
FROM current_stats c
CROSS JOIN prev_stats p
CROSS JOIN coverage cov
