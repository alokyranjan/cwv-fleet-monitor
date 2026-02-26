"""Post a formatted Slack summary after monthly CrUX extraction.

Usage:
    python scripts/notify_slack.py [--month YYYYMM] [--config path/to/settings.yaml] [--dry-run]
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error

from common import load_config, get_client, read_sql, format_sql


# ---------------------------------------------------------------------------
# Month helpers
# ---------------------------------------------------------------------------

def compute_prev_month(yyyymm):
    """Return the previous month as YYYYMM integer. 202601->202512."""
    year, month = yyyymm // 100, yyyymm % 100
    if month == 1:
        return (year - 1) * 100 + 12
    return year * 100 + (month - 1)


def check_prev_month_exists(client, config, prev_yyyymm):
    """Return True if cwv_monthly has data for prev_yyyymm."""
    project = config["gcp"]["project_id"]
    dataset = config["bigquery"]["dataset_name"]
    query = (
        f"SELECT COUNT(*) AS cnt FROM `{project}.{dataset}.cwv_monthly` "
        f"WHERE yyyymm = {prev_yyyymm}"
    )
    for row in client.query(query).result():
        return row.cnt > 0
    return False


def format_month_label(yyyymm):
    """202601 -> '2026-01'."""
    return f"{yyyymm // 100}-{yyyymm % 100:02d}"


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_fleet_summary(client, config, target_yyyymm, prev_yyyymm):
    """Run fleet summary SQL and return a dict of stats."""
    sql = read_sql("slack_fleet_summary.sql")
    query = format_sql(sql, config,
                       target_yyyymm=target_yyyymm,
                       prev_yyyymm=prev_yyyymm)
    rows = list(client.query(query).result())
    if not rows:
        return None
    row = rows[0]
    return {col: getattr(row, col) for col in row.keys()}


def fetch_regressions(client, config, target_yyyymm, prev_yyyymm):
    """Run regressions SQL, return dict grouped by category."""
    sql = read_sql("slack_regressions.sql")
    query = format_sql(sql, config,
                       target_yyyymm=target_yyyymm,
                       prev_yyyymm=prev_yyyymm)
    results = {}
    for row in client.query(query).result():
        cat = row.category
        results.setdefault(cat, []).append({
            "origin": row.origin,
            "device": row.device,
            "dealer_name": row.dealer_name,
            "current_value": row.current_value,
            "prev_value": row.prev_value,
            "delta": row.delta,
        })
    return results


def fetch_worst_offenders(client, config, target_yyyymm):
    """Run worst offenders SQL, return dict grouped by metric."""
    sql = read_sql("slack_worst_offenders.sql")
    query = format_sql(sql, config, target_yyyymm=target_yyyymm)
    results = {"lcp": [], "inp": [], "cls": []}
    for row in client.query(query).result():
        entry = {
            "origin": row.origin,
            "device": row.device,
            "dealer_name": row.dealer_name,
            "p75_lcp": row.p75_lcp,
            "p75_inp": row.p75_inp,
            "p75_cls": row.p75_cls,
        }
        if row.lcp_rank <= 5:
            results["lcp"].append((row.lcp_rank, entry))
        if row.inp_rank <= 5:
            results["inp"].append((row.inp_rank, entry))
        if row.cls_rank <= 5:
            results["cls"].append((row.cls_rank, entry))
    # Sort by rank
    for metric in results:
        results[metric] = [e for _, e in sorted(results[metric])]
    return results


def fetch_improvements(client, config, target_yyyymm, prev_yyyymm):
    """Run improvements SQL, return dict grouped by category."""
    sql = read_sql("slack_improvements.sql")
    query = format_sql(sql, config,
                       target_yyyymm=target_yyyymm,
                       prev_yyyymm=prev_yyyymm)
    results = {}
    for row in client.query(query).result():
        cat = row.category
        results.setdefault(cat, []).append({
            "origin": row.origin,
            "device": row.device,
            "dealer_name": row.dealer_name,
            "current_value": row.current_value,
            "prev_value": row.prev_value,
            "delta": row.delta,
        })
    return results


# ---------------------------------------------------------------------------
# Slack message building (Block Kit)
# ---------------------------------------------------------------------------

def _delta_str(value, suffix="pp", decimals=1, invert=False):
    """Format a delta value with arrow. invert=True means lower is better."""
    if value is None:
        return "N/A"
    sign = -1 if invert else 1
    arrow = "\u25b2" if (value * sign) > 0 else "\u25bc" if (value * sign) < 0 else "\u25c6"
    prefix = "+" if value > 0 else ""
    return f"{arrow} {prefix}{value:.{decimals}f}{suffix}"


def _format_value(value, metric):
    """Format a metric value for display."""
    if value is None:
        return "N/A"
    if metric == "cls":
        return f"{value:.3f}"
    return f"{value:,.0f}ms"


def _strip_origin(origin):
    """Remove protocol prefix from origin for display."""
    if origin and origin.startswith("https://"):
        return origin[8:]
    if origin and origin.startswith("http://"):
        return origin[7:]
    return origin or "unknown"


def _site_examples(items, metric, max_examples=5):
    """Build example string like 'e.g., site.com (phone): 2,100 -> 5,200ms'."""
    if not items:
        return ""
    examples = []
    for item in items[:max_examples]:
        site = _strip_origin(item["origin"])
        prev_val = _format_value(item["prev_value"], metric)
        cur_val = _format_value(item["current_value"], metric)
        examples.append(f"{site} ({item['device']}): {prev_val} \u2192 {cur_val}")
    text = ", ".join(examples)
    remaining = len(items) - max_examples
    if remaining > 0:
        text += f" ... and {remaining} more"
    return text


def build_slack_message(summary, regressions, worst, improvements, config, target_yyyymm, has_prev):
    """Build Block Kit JSON payload for Slack."""
    month_label = format_month_label(target_yyyymm)
    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f"\U0001f4ca CrUX Monthly Report \u2014 {month_label}"}
    })

    # --- Fleet Health Snapshot ---
    if summary:
        cwv_delta = _delta_str(summary.get("cwv_pass_rate_delta"))
        coverage_text = (
            f"{summary['origins_with_data']:,} / {summary['total_active_origins']:,} origins "
            f"({summary['coverage_pct']}%)"
        )

        lcp_rate_delta = _delta_str(summary.get("lcp_pass_rate_delta"))
        inp_rate_delta = _delta_str(summary.get("inp_pass_rate_delta"))
        cls_rate_delta = _delta_str(summary.get("cls_pass_rate_delta"))
        fcp_rate_delta = _delta_str(summary.get("fcp_pass_rate_delta"))
        ttfb_rate_delta = _delta_str(summary.get("ttfb_pass_rate_delta"))

        lcp_p75 = _format_value(summary.get("avg_p75_lcp"), "lcp")
        inp_p75 = _format_value(summary.get("avg_p75_inp"), "inp")
        cls_p75 = _format_value(summary.get("avg_p75_cls"), "cls")
        fcp_p75 = _format_value(summary.get("avg_p75_fcp"), "fcp")
        ttfb_p75 = _format_value(summary.get("avg_p75_ttfb"), "ttfb")

        lcp_p75_delta = _delta_str(summary.get("avg_p75_lcp_delta"), suffix="ms", decimals=0, invert=True)
        inp_p75_delta = _delta_str(summary.get("avg_p75_inp_delta"), suffix="ms", decimals=0, invert=True)
        cls_p75_delta = _delta_str(summary.get("avg_p75_cls_delta"), suffix="", decimals=3, invert=True)
        fcp_p75_delta = _delta_str(summary.get("avg_p75_fcp_delta"), suffix="ms", decimals=0, invert=True)
        ttfb_p75_delta = _delta_str(summary.get("avg_p75_ttfb_delta"), suffix="ms", decimals=0, invert=True)

        fleet_text = (
            f"*Overall CWV Pass Rate:* {summary['cwv_pass_rate']}%  {cwv_delta}\n"
            f"*Coverage:* {coverage_text}\n\n"
            f"*LCP*  {summary['lcp_pass_rate']}% {lcp_rate_delta}  |  p75: {lcp_p75} ({lcp_p75_delta})\n"
            f"*INP*  {summary['inp_pass_rate']}% {inp_rate_delta}  |  p75: {inp_p75} ({inp_p75_delta})\n"
            f"*CLS*  {summary['cls_pass_rate']}% {cls_rate_delta}  |  p75: {cls_p75} ({cls_p75_delta})\n"
            f"*FCP*  {summary.get('fcp_pass_rate', 'N/A')}% {fcp_rate_delta}  |  p75: {fcp_p75} ({fcp_p75_delta})\n"
            f"*TTFB*  {summary.get('ttfb_pass_rate', 'N/A')}% {ttfb_rate_delta}  |  p75: {ttfb_p75} ({ttfb_p75_delta})"
        )

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Fleet Health Snapshot*\n\n{fleet_text}"}
        })

    # --- Regressions ---
    blocks.append({"type": "divider"})
    if has_prev and regressions:
        reg_lines = ["*Regressions (Good \u2192 Poor)*\n"]

        for metric_key, label in [("lcp_regression", "LCP"), ("inp_regression", "INP"), ("cls_regression", "CLS")]:
            items = regressions.get(metric_key, [])
            metric = metric_key.split("_")[0]
            if items:
                examples = _site_examples(items[:3], metric, max_examples=3)
                reg_lines.append(f"*{label}:* {len(items)} sites (e.g., {examples})")
            else:
                reg_lines.append(f"*{label}:* 0 sites")

        # Largest LCP increases
        lcp_increases = regressions.get("lcp_increase", [])
        if lcp_increases:
            reg_lines.append("\n*Largest LCP increases:*")
            for i, item in enumerate(lcp_increases[:5], 1):
                site = _strip_origin(item["origin"])
                prev_v = _format_value(item["prev_value"], "lcp")
                cur_v = _format_value(item["current_value"], "lcp")
                delta_v = _format_value(abs(item["delta"]), "lcp")
                reg_lines.append(f"  {i}. {site} ({item['device']}): {prev_v} \u2192 {cur_v} (+{delta_v})")

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(reg_lines)}
        })
    elif not has_prev:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Regressions (Good \u2192 Poor)*\n\nNo previous month data for comparison."}
        })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Regressions (Good \u2192 Poor)*\n\nNo regressions detected."}
        })

    # --- Worst Offenders ---
    if worst:
        blocks.append({"type": "divider"})
        worst_lines = ["*Top 5 Worst*\n"]

        for metric, label, value_key in [("lcp", "LCP", "p75_lcp"), ("inp", "INP", "p75_inp"), ("cls", "CLS", "p75_cls")]:
            items = worst.get(metric, [])
            if items:
                worst_lines.append(f"*{label}:*")
                for i, item in enumerate(items[:5], 1):
                    site = _strip_origin(item["origin"])
                    val = _format_value(item[value_key], metric)
                    worst_lines.append(f"  {i}. {site} ({item['device']}) \u2014 {val}")

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(worst_lines)}
        })

    # --- Improvements ---
    blocks.append({"type": "divider"})
    if has_prev and improvements:
        imp_lines = ["*Improvements (Poor \u2192 Good)*\n"]

        for metric_key, label in [("lcp_improvement", "LCP"), ("inp_improvement", "INP"), ("cls_improvement", "CLS")]:
            items = improvements.get(metric_key, [])
            metric = metric_key.split("_")[0]
            if items:
                examples = _site_examples(items[:3], metric, max_examples=3)
                imp_lines.append(f"*{label}:* {len(items)} sites (e.g., {examples})")
            else:
                imp_lines.append(f"*{label}:* 0 sites")

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(imp_lines)}
        })
    elif not has_prev:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Improvements (Poor \u2192 Good)*\n\nNo previous month data for comparison."}
        })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Improvements (Poor \u2192 Good)*\n\nNo improvements detected."}
        })

    # --- Dashboard buttons ---
    grafana = config.get("grafana", {})
    base_url = grafana.get("base_url", "http://localhost:3000")
    fleet_uid = grafana.get("fleet_overview_uid", "fleet-overview")
    worst_uid = grafana.get("worst_offenders_uid", "worst-offenders")

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Fleet Overview"},
                "url": f"{base_url}/d/{fleet_uid}",
            },
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Worst Offenders"},
                "url": f"{base_url}/d/{worst_uid}",
            },
        ],
    })

    return {"blocks": blocks}


# ---------------------------------------------------------------------------
# Slack posting
# ---------------------------------------------------------------------------

def get_webhook_url(config):
    """Resolve webhook URL: env var > config file."""
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if url:
        return url
    return config.get("slack", {}).get("webhook_url") or None


def post_to_slack(webhook_url, payload):
    """POST JSON payload to Slack webhook. Returns True on success."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status == 200:
                return True
            body = resp.read().decode("utf-8", errors="replace")
            print(f"  WARNING: Slack returned status {resp.status}: {body}")
            return False
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  WARNING: Slack HTTP error {e.code}: {body}")
        return False
    except urllib.error.URLError as e:
        print(f"  WARNING: Slack network error: {e.reason}")
        return False


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def post_notification(client, config, target_yyyymm, dry_run=False):
    """Fetch all data, build Slack message, and post it.

    Returns True on success, False on failure.
    """
    # Check if Slack is disabled
    slack_config = config.get("slack", {})
    if not slack_config.get("enabled", True):
        return True

    # Resolve webhook URL (skip if dry-run)
    webhook_url = get_webhook_url(config)
    if not webhook_url and not dry_run:
        print("  WARNING: No Slack webhook URL configured. Set SLACK_WEBHOOK_URL env var or slack.webhook_url in config.")
        return False

    prev_yyyymm = compute_prev_month(target_yyyymm)
    has_prev = check_prev_month_exists(client, config, prev_yyyymm)

    print(f"  Fetching Slack notification data for {target_yyyymm}...")

    # Fleet summary — always fetch (uses prev for deltas, NULLs if missing)
    summary = fetch_fleet_summary(client, config, target_yyyymm, prev_yyyymm)

    # Regressions & improvements — only if previous month exists
    regressions = {}
    improvements = {}
    if has_prev:
        regressions = fetch_regressions(client, config, target_yyyymm, prev_yyyymm)
        improvements = fetch_improvements(client, config, target_yyyymm, prev_yyyymm)

    # Worst offenders — always
    worst = fetch_worst_offenders(client, config, target_yyyymm)

    # Build message
    payload = build_slack_message(summary, regressions, worst, improvements,
                                  config, target_yyyymm, has_prev)

    if dry_run:
        # Use sys.stdout with utf-8 if possible, fall back to ascii-escaped JSON
        try:
            print(json.dumps(payload, indent=2, ensure_ascii=False))
        except UnicodeEncodeError:
            print(json.dumps(payload, indent=2, ensure_ascii=True))
        return True

    # Post
    print("  Posting to Slack...")
    return post_to_slack(webhook_url, payload)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Post CrUX monthly summary to Slack")
    parser.add_argument("--month", type=int, required=True, help="Target month as YYYYMM")
    parser.add_argument("--config", help="Path to settings.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Print payload to stdout instead of posting")
    args = parser.parse_args()

    config = load_config(args.config)
    client = get_client(config)

    success = post_notification(client, config, args.month, dry_run=args.dry_run)
    if not success:
        sys.exit(1)

    if not args.dry_run:
        print("  Slack notification sent successfully.")


if __name__ == "__main__":
    main()
