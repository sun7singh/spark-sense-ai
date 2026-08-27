"""
Test script: EMR cluster/step source + Bedrock provider

Usage:
    aws configure   # if not already done
    python test_emr_bedrock.py --cluster-id j-XXXXXXX --step-id s-XXXXXXX

This calls the diagnose_spark_failure and optimize_spark_performance
tool functions DIRECTLY (bypassing the MCP protocol layer), against a
REAL EMR cluster/step, using Bedrock for the diagnosis.

NOTE: this is the least-tested code path in the server (EMR log
fetching assumes a standard <LogUri>/<step_id>/stderr.gz layout) - if
this fails, the error message will tell us whether it's an AWS
permissions issue or a log-path assumption that needs adjusting.
"""

import argparse
import sys

sys.path.insert(0, "src")

from sparksense_mcp.server import diagnose_spark_failure, optimize_spark_performance


def test_diagnose_failure(cluster_id: str, step_id: str, s3_project: str):
    print("=" * 70)
    print("TEST 1: diagnose_spark_failure (emr + bedrock)")
    print("=" * 70)

    result = diagnose_spark_failure(
        source_type="emr",
        emr_cluster_id=cluster_id,
        emr_step_id=step_id,
        s3_project_location=s3_project,
        provider="bedrock",
    )
    print(result)
    print()


def test_optimize_performance(cluster_id: str, step_id: str, s3_project: str):
    print("=" * 70)
    print("TEST 2: optimize_spark_performance (emr + bedrock)")
    print("=" * 70)

    result = optimize_spark_performance(
        source_type="emr",
        emr_cluster_id=cluster_id,
        emr_step_id=step_id,
        s3_project_location=s3_project,
        current_spark_config="executor.memory=4g, executor.cores=2",
        provider="bedrock",
    )
    print(result)
    print()


def test_fetch_only_no_provider(cluster_id: str, step_id: str, s3_project: str):
    """Sanity-check the EMR fetching logic alone, with zero Bedrock
    cost - run this FIRST to confirm log-fetching works before
    spending anything on the LLM call."""
    print("=" * 70)
    print("TEST 0: EMR fetch only (provider=none) - run this first")
    print("=" * 70)

    result = diagnose_spark_failure(
        source_type="emr",
        emr_cluster_id=cluster_id,
        emr_step_id=step_id,
        s3_project_location=s3_project,
        provider="none",
    )
    print(result)
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster-id", required=True, help="EMR cluster ID, e.g. j-XXXXXXX")
    parser.add_argument("--step-id", required=True, help="EMR step ID, e.g. s-XXXXXXX")
    parser.add_argument("--s3-project", default="", help="Optional S3 URI to source code")
    args = parser.parse_args()

    # Always run the free fetch-only test first
    test_fetch_only_no_provider(args.cluster_id, args.step_id, args.s3_project)

    input("Fetch looked correct? Press Enter to continue to Bedrock calls (costs tokens)...")

    test_diagnose_failure(args.cluster_id, args.step_id, args.s3_project)
    test_optimize_performance(args.cluster_id, args.step_id, args.s3_project)
