-- Top 5 worst sites per core metric
-- Parameters: {project}, {dataset}, {target_yyyymm}

WITH ranked AS (
    SELECT
        origin,
        device,
        dealer_name,
        p75_lcp,
        p75_inp,
        p75_cls,
        ROW_NUMBER() OVER (ORDER BY p75_lcp DESC) AS lcp_rank,
        ROW_NUMBER() OVER (ORDER BY p75_inp DESC) AS inp_rank,
        ROW_NUMBER() OVER (ORDER BY p75_cls DESC) AS cls_rank
    FROM `{project}.{dataset}.cwv_monthly`
    WHERE yyyymm = {target_yyyymm}
)

SELECT
    origin,
    device,
    dealer_name,
    p75_lcp,
    p75_inp,
    p75_cls,
    lcp_rank,
    inp_rank,
    cls_rank
FROM ranked
WHERE lcp_rank <= 5 OR inp_rank <= 5 OR cls_rank <= 5
ORDER BY lcp_rank
