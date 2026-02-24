-- Sites that improved: poor -> good
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

-- LCP improvement: was poor (> 4000), now good (<= 2500)
lcp_improvements AS (
    SELECT
        'lcp_improvement' AS category,
        origin, device, dealer_name,
        current_lcp AS current_value,
        prev_lcp AS prev_value,
        ROUND(current_lcp - prev_lcp, 0) AS delta
    FROM joined
    WHERE prev_lcp > 4000 AND current_lcp <= 2500
),

-- INP improvement: was poor (> 500), now good (<= 200)
inp_improvements AS (
    SELECT
        'inp_improvement' AS category,
        origin, device, dealer_name,
        current_inp AS current_value,
        prev_inp AS prev_value,
        ROUND(current_inp - prev_inp, 0) AS delta
    FROM joined
    WHERE prev_inp > 500 AND current_inp <= 200
),

-- CLS improvement: was poor (> 0.25), now good (<= 0.1)
cls_improvements AS (
    SELECT
        'cls_improvement' AS category,
        origin, device, dealer_name,
        current_cls AS current_value,
        prev_cls AS prev_value,
        ROUND(current_cls - prev_cls, 3) AS delta
    FROM joined
    WHERE prev_cls > 0.25 AND current_cls <= 0.1
)

SELECT * FROM lcp_improvements
UNION ALL
SELECT * FROM inp_improvements
UNION ALL
SELECT * FROM cls_improvements
ORDER BY category, delta ASC
