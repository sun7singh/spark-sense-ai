"""
Test script: local folder source + Anthropic API provider

Usage:
    export ANTHROPIC_API_KEY="sk-ant-ap****"
    python test_local_anthropic.py

This calls the diagnose_spark_failure and optimize_spark_performance
tool functions DIRECTLY (bypassing the MCP protocol layer) so you can
validate the underlying logic before wiring it into Claude Desktop/Code.
"""

import os
import sys

# Make sure this points to wherever you unzipped/installed the package
sys.path.insert(0, "src")

from sparksense_mcp.server import diagnose_spark_failure, optimize_spark_performance


def test_diagnose_failure():
    print("=" * 70)
    print("TEST 1: diagnose_spark_failure (local + anthropic)")
    print("=" * 70)

    result = diagnose_spark_failure(
        source_type="local",
        local_log_path="./test_data/sample_error.log",
        local_project_path="./test_data/sample_project",  # optional
        provider="anthropic",
        api_key="",  # leave empty to use ANTHROPIC_API_KEY env var
    )
    print(result)
    print()


def test_optimize_performance():
    print("=" * 70)
    print("TEST 2: optimize_spark_performance (local + anthropic)")
    print("=" * 70)

    result = optimize_spark_performance(
        source_type="local",
        local_log_path="./test_data/sample_success_stats.log",
        current_spark_config="executor.memory=4g, executor.cores=2, spark.sql.shuffle.partitions=200",
        provider="anthropic",
        api_key="",
    )
    print(result)
    print()


def test_diagnose_failure_no_provider():
    """Confirms provider='none' works with zero external calls - useful
    to sanity-check fetching logic without spending any API credits."""
    print("=" * 70)
    print("TEST 3: diagnose_spark_failure (local + provider=none, Python)")
    print("=" * 70)

    result = diagnose_spark_failure(
        source_type="local",
        local_log_path="./test_data/sample_error.log",
        local_project_path="./test_data/sample_project",
        provider="none",
    )
    print(result)
    print()


def test_diagnose_failure_scala_no_provider():
    """Same as above but with a Scala stack trace + Scala project files,
    to confirm the Scala/Java extraction path (filename search, since
    Scala traces don't include full paths) works correctly."""
    print("=" * 70)
    print("TEST 4: diagnose_spark_failure (local + provider=none, Scala)")
    print("=" * 70)

    result = diagnose_spark_failure(
        source_type="local",
        local_log_path="./test_data/sample_error_scala.log",
        local_project_path="./test_data/sample_project_scala",
        provider="none",
    )
    print(result)
    print()


def test_diagnose_failure_with_entry_point():
    """Confirms job_entry_point takes priority over auto-extraction when
    explicitly given."""
    print("=" * 70)
    print("TEST 5: diagnose_spark_failure (job_entry_point given, provider=none)")
    print("=" * 70)

    result = diagnose_spark_failure(
        source_type="local",
        local_log_path="./test_data/sample_error_scala.log",
        local_project_path="./test_data/sample_project_scala",
        job_entry_point="CustomerHelper.scala",  # deliberately point at the ACTUAL bug, not the entry file
        provider="none",
    )
    print(result)
    print()


if __name__ == "__main__":
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("WARNING: ANTHROPIC_API_KEY not set. Tests 1 and 2 will fail.")
        print("Run: export ANTHROPIC_API_KEY='sk-ant-...'\n")

    # Cheapest/no-cost tests first — validate fetching + file-selection
    # logic only, no API calls made
    test_diagnose_failure_no_provider()
    test_diagnose_failure_scala_no_provider()
    test_diagnose_failure_with_entry_point()

    # Then the real API-calling tests
    test_diagnose_failure()
    test_optimize_performance()
