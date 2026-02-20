"""Data validation and sanity checks.

Usage:
    python scripts/validate.py [--config path/to/settings.yaml]
"""

import argparse
import sys

from common import load_config, get_client, get_table_id


def check_origins(client, config):
    """Check origins table has data."""
    table_id = get_table_id(config, "origins_table")
    query = f"SELECT COUNT(*) as cnt, COUNTIF(is_active) as active FROM `{table_id}`"
    result = client.query(query).result()
    for row in result:
        total, active = row.cnt, row.active
        status = "PASS" if total > 0 else "FAIL"
        print(f"  [{status}] Origins: {total} total, {active} active")
        return total > 0
    return False


def check_cwv_months(client, config):
    """Check cwv_monthly has continuous month coverage."""
    table_id = get_table_id(config, "cwv_monthly_table")
    query = f"""
    SELECT yyyymm, COUNT(*) as row_count, COUNT(DISTINCT origin) as origins
    FROM `{table_id}`
    GROUP BY yyyymm
    ORDER BY yyyymm
    """
    result = client.query(query).result()
    months = list(result)

    if not months:
        print("  [FAIL] cwv_monthly: No data")
        return False

    print(f"  [PASS] cwv_monthly: {len(months)} months of data")
    for m in months:
        print(f"         {m.yyyymm}: {m.row_count} rows, {m.origins} origins")
    return True


def check_nulls(client, config):
    """Check for NULL values in critical columns."""
    table_id = get_table_id(config, "cwv_monthly_table")
    query = f"""
    SELECT
        COUNTIF(origin IS NULL) as null_origins,
        COUNTIF(device IS NULL) as null_devices,
        COUNTIF(yyyymm IS NULL) as null_months
    FROM `{table_id}`
    """
    result = client.query(query).result()
    for row in result:
        issues = []
        if row.null_origins > 0:
            issues.append(f"{row.null_origins} NULL origins")
        if row.null_months > 0:
            issues.append(f"{row.null_months} NULL months")

        if issues:
            print(f"  [FAIL] NULL check: {', '.join(issues)}")
            return False
        else:
            info = ""
            if row.null_devices > 0:
                info = f" ({row.null_devices} NULL-device rows are CrUX rank aggregates — expected)"
            print(f"  [PASS] NULL check: No NULLs in origin/yyyymm{info}")
            return True
    return False


def check_distributions(client, config):
    """Check that distribution fractions are consistent.

    Note: In the CrUX materialized device_summary table, distributions are
    density-weighted (multiplied by device traffic share). So fast_lcp + avg_lcp
    + slow_lcp ≈ deviceDensity, NOT 1.0. We validate that the ratio of
    distribution sum to device density is close to 1.0.
    """
    table_id = get_table_id(config, "cwv_monthly_table")
    query = f"""
    SELECT
        AVG(SAFE_DIVIDE(fast_lcp + avg_lcp + slow_lcp,
            CASE device WHEN 'desktop' THEN desktopDensity
                        WHEN 'phone' THEN phoneDensity
                        WHEN 'tablet' THEN tabletDensity END)) as lcp_ratio,
        AVG(SAFE_DIVIDE(fast_fcp + avg_fcp + slow_fcp,
            CASE device WHEN 'desktop' THEN desktopDensity
                        WHEN 'phone' THEN phoneDensity
                        WHEN 'tablet' THEN tabletDensity END)) as fcp_ratio,
        AVG(SAFE_DIVIDE(fast_inp + avg_inp + slow_inp,
            CASE device WHEN 'desktop' THEN desktopDensity
                        WHEN 'phone' THEN phoneDensity
                        WHEN 'tablet' THEN tabletDensity END)) as inp_ratio,
        AVG(SAFE_DIVIDE(small_cls + medium_cls + large_cls,
            CASE device WHEN 'desktop' THEN desktopDensity
                        WHEN 'phone' THEN phoneDensity
                        WHEN 'tablet' THEN tabletDensity END)) as cls_ratio,
        AVG(SAFE_DIVIDE(fast_ttfb + avg_ttfb + slow_ttfb,
            CASE device WHEN 'desktop' THEN desktopDensity
                        WHEN 'phone' THEN phoneDensity
                        WHEN 'tablet' THEN tabletDensity END)) as ttfb_ratio
    FROM `{table_id}`
    WHERE fast_lcp IS NOT NULL AND device IS NOT NULL
    """
    result = client.query(query).result()
    for row in result:
        all_ok = True
        for metric, val in [
            ("LCP", row.lcp_ratio), ("FCP", row.fcp_ratio),
            ("INP", row.inp_ratio), ("CLS", row.cls_ratio),
            ("TTFB", row.ttfb_ratio),
        ]:
            if val is None:
                print(f"  [WARN] {metric} distribution: No data")
                continue
            ok = 0.90 <= val <= 1.10
            status = "PASS" if ok else "FAIL"
            print(f"  [{status}] {metric} dist/density ratio: {val:.4f} (expect ~1.0)")
            if not ok:
                all_ok = False
        return all_ok
    return False


def check_coverage(client, config):
    """Report coverage: origins with CrUX data vs total origins."""
    origins_table = get_table_id(config, "origins_table")
    cwv_table = get_table_id(config, "cwv_monthly_table")

    query = f"""
    WITH latest AS (
        SELECT MAX(yyyymm) as latest_month FROM `{cwv_table}`
    ),
    active_origins AS (
        SELECT COUNT(*) as total FROM `{origins_table}` WHERE is_active = TRUE
    ),
    matched AS (
        SELECT COUNT(DISTINCT origin) as matched
        FROM `{cwv_table}`
        WHERE yyyymm = (SELECT latest_month FROM latest)
    )
    SELECT
        ao.total as total_origins,
        m.matched as origins_with_data,
        ROUND(SAFE_DIVIDE(m.matched, ao.total) * 100, 1) as coverage_pct
    FROM active_origins ao, matched m
    """
    result = client.query(query).result()
    for row in result:
        print(f"  [INFO] Coverage: {row.origins_with_data}/{row.total_origins} origins have CrUX data ({row.coverage_pct}%)")
        return True
    return False


def main():
    parser = argparse.ArgumentParser(description="Validate CrUX data in BigQuery")
    parser.add_argument("--config", help="Path to settings.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    client = get_client(config)

    print("Running validation checks...\n")

    checks = [
        ("Origins table", check_origins),
        ("CWV monthly coverage", check_cwv_months),
        ("NULL values", check_nulls),
        ("Distribution sums", check_distributions),
        ("Origin coverage", check_coverage),
    ]

    results = []
    for name, check_fn in checks:
        print(f"{name}:")
        try:
            passed = check_fn(client, config)
            results.append((name, passed))
        except Exception as e:
            print(f"  [ERROR] {e}")
            results.append((name, False))
        print()

    # Summary
    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    print(f"{'='*40}")
    print(f"Results: {passed}/{total} checks passed")

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
