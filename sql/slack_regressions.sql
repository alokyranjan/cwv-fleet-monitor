-- Sites that regressed: good -> poor, plus largest LCP increases
-- Parameters: {project}, {dataset}, {target_yyyymm}, {prev_yyyymm}

WITH cur AS (
    SELECT origin, device, dealer_name, p75_lcp, p75_inp, p75_cls
    FROM `{project}.{dataset}.cwv_monthly`
    WHERE yyyymm = {target_yyyymm}
),

prev AS (
    SELECT origin, device, p75_lcp, p75_inp, p75_cls
    FROM `{project}.{dataset}.cwv_monthly`
    WHERE yyyymm = {prev_yyyymm}
),

joined AS (
    SELECT
        c.origin,
        c.device,
        c.dealer_name,
        c.p75_lcp AS current_lcp,
        p.p75_lcp AS prev_lcp,
        c.p75_inp AS current_inp,
        p.p75_inp AS prev_inp,
        c.p75_cls AS current_cls,
        p.p75_cls AS prev_cls
    FROM cur c
    INNER JOIN prev p ON c.origin = p.origin AND c.device = p.device
),

-- LCP regression: was good (<= 2500), now poor (> 4000)
lcp_regressions AS (
    SELECT
        'lcp_regression' AS category,
        origin, device, dealer_name,
        current_lcp AS current_value,
        prev_lcp AS prev_value,
        ROUND(current_lcp - prev_lcp, 0) AS delta
    FROM joined
    WHERE prev_lcp <= 2500 AND current_lcp > 4000
),

-- INP regression: was good (<= 200), now poor (> 500)
inp_regressions AS (
    SELECT
        'inp_regression' AS category,
        origin, device, dealer_name,
        current_inp AS current_value,
        prev_inp AS prev_value,
        ROUND(current_inp - prev_inp, 0) AS delta
    FROM joined
    WHERE prev_inp <= 200 AND current_inp > 500
),

-- CLS regression: was good (<= 0.1), now poor (> 0.25)
cls_regressions AS (
    SELECT
        'cls_regression' AS category,
        origin, device, dealer_name,
        current_cls AS current_value,
        prev_cls AS prev_value,
        ROUND(current_cls - prev_cls, 3) AS delta
    FROM joined
    WHERE prev_cls <= 0.1 AND current_cls > 0.25
),

-- Top 10 largest LCP increases (regardless of threshold)
lcp_increases AS (
    SELECT
        'lcp_increase' AS category,
        origin, device, dealer_name,
        current_lcp AS current_value,
        prev_lcp AS prev_value,
        ROUND(current_lcp - prev_lcp, 0) AS delta
    FROM joined
    WHERE current_lcp > prev_lcp
    ORDER BY delta DESC
    LIMIT 10
)

SELECT * FROM lcp_regressions
UNION ALL
SELECT * FROM inp_regressions
UNION ALL
SELECT * FROM cls_regressions
UNION ALL
SELECT * FROM lcp_increases
ORDER BY category, delta DESC
