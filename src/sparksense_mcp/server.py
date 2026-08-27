"""
SparkSenseAI MCP Server
------------------------
An MCP (Model Context Protocol) server that gives AI agents (Claude
Desktop, Claude Code, Devin, etc.) two capabilities for Apache Spark:

  1. diagnose_spark_failure     - root-cause a failed Spark job
  2. optimize_spark_performance - suggest tuning for a successful-but-slow job

Design principles:
  - Source-agnostic: reads logs/code from either an EMR cluster+step
    (via AWS APIs) or a local folder (via plain filesystem access).
  - Provider-agnostic: reasoning can be done by Bedrock, direct
    Anthropic API, OpenAI, or left entirely to the calling agent
    (provider="none") - this server never bundles or requires any
    credentials of its own.
  - No shared billing, no shared access: every user brings their own
    credentials (AWS and/or LLM API key) if and when a step needs them.

Install:
    pip install spark-sense-ai
"""

import glob
import json
import os
from typing import Optional

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("sparksense")

DEFAULT_BEDROCK_MODEL_ID = os.environ.get(
    "SPARKSENSE_BEDROCK_MODEL_ID",
    "global.anthropic.claude-haiku-4-5-20251001-v1:0",
)
DEFAULT_AWS_REGION = os.environ.get("SPARKSENSE_AWS_REGION", "ap-south-1")


# ---------------------------------------------------------------------------
# Smart file selection: job_entry_point > stack-trace extraction > fallback
# ---------------------------------------------------------------------------

MAX_EXTRACTED_FILES = 10

# Prefixes that indicate framework/library internals, never the user's own
# job code. Kept as simple string-prefix checks - not exhaustive, but
# covers the overwhelming majority of noise in real Spark stack traces.
_PYTHON_INTERNAL_MARKERS = ("site-packages", "dist-packages", "/pyspark/", "/py4j/")
_JVM_INTERNAL_PACKAGE_PREFIXES = (
    "org.apache.spark", "org.apache.hadoop", "scala.", "java.", "javax.",
    "sun.", "py4j.",
)

_PYTHON_TRACE_RE = r'File "([^"]+)", line (\d+)'
_JVM_TRACE_RE = r"at ([\w.$]+)\.\w+\(([\w$]+\.(?:scala|java)):(\d+)\)"


def _extract_referenced_files(error_log: str) -> list:
    """Parse an error log for BOTH Python-style and Scala/Java-style stack
    trace references, filter out known framework/library internals, and
    return a de-duplicated list of filenames/paths that likely belong to
    the user's own job code.

    Handles mixed-language traces (very common for PySpark, where a
    Python traceback bottoms out into a JVM stack trace) by scanning for
    both patterns in the same log rather than picking one exclusively.
    """
    import re

    candidates = []

    # Python: direct file paths
    for match in re.finditer(_PYTHON_TRACE_RE, error_log):
        path = match.group(1)
        if any(marker in path for marker in _PYTHON_INTERNAL_MARKERS):
            continue
        candidates.append(("path", path))

    # Scala/Java: package.Class + filename only (no full path given)
    for match in re.finditer(_JVM_TRACE_RE, error_log):
        package_class, filename = match.group(1), match.group(2)
        if any(package_class.startswith(p) for p in _JVM_INTERNAL_PACKAGE_PREFIXES):
            continue
        candidates.append(("filename", filename))

    # De-duplicate while preserving order (closest-to-top-of-trace first)
    seen = set()
    deduped = []
    for kind, value in candidates:
        if value not in seen:
            seen.add(value)
            deduped.append((kind, value))

    return deduped[:MAX_EXTRACTED_FILES]


def _resolve_extracted_files_to_paths(extracted: list, project_path: str) -> list:
    """Turn extracted (kind, value) pairs into actual file paths that
    exist under project_path.

    - kind="path" (Python): check the path directly, and also try it
      relative to project_path (stack traces often show absolute paths
      from a different machine than the one running this tool).
    - kind="filename" (Scala/Java): search project_path recursively for
      a file matching that exact filename.
    """
    resolved = []
    for kind, value in extracted:
        if kind == "path":
            if os.path.isfile(value):
                resolved.append(value)
                continue
            candidate = os.path.join(project_path, os.path.basename(value))
            if os.path.isfile(candidate):
                resolved.append(candidate)
                continue
            matches = glob.glob(
                os.path.join(project_path, "**", os.path.basename(value)), recursive=True
            )
            if matches:
                resolved.append(matches[0])
        else:  # kind == "filename"
            matches = glob.glob(os.path.join(project_path, "**", value), recursive=True)
            if matches:
                resolved.append(matches[0])

    # De-duplicate, preserve order, cap again as a final safety net
    seen = set()
    final = []
    for p in resolved:
        if p not in seen:
            seen.add(p)
            final.append(p)
    return final[:MAX_EXTRACTED_FILES]


def _read_files_as_text(paths: list) -> str:
    parts = []
    for fp in paths:
        try:
            with open(fp, "r", errors="replace") as f:
                parts.append(f"--- {fp} ---\n{f.read()}")
        except OSError as exc:
            parts.append(f"--- {fp} ---\n[Could not read file: {exc}]")
    return "\n\n".join(parts)


def _select_project_code(
    job_entry_point: str,
    error_log: str,
    project_path: str,
) -> str:
    """Implements the file-selection priority:
      1. job_entry_point given -> use ONLY that file
      2. else -> parse the log's stack trace, fetch up to MAX_EXTRACTED_FILES matches
      3. else -> fallback: broad folder scan, capped at MAX_EXTRACTED_FILES
    Returns "" if project_path isn't provided/usable at all.
    """
    if not project_path or not os.path.isdir(project_path):
        return ""

    # 1. Explicit hint takes priority, no auto-extraction needed
    if job_entry_point:
        candidate = job_entry_point
        if not os.path.isfile(candidate):
            candidate = os.path.join(project_path, job_entry_point)
        if os.path.isfile(candidate):
            return _read_files_as_text([candidate])
        # Given but not found - fall through to auto-extraction rather
        # than silently returning nothing.

    # 2. Auto-extract from the stack trace
    if error_log:
        extracted = _extract_referenced_files(error_log)
        resolved = _resolve_extracted_files_to_paths(extracted, project_path)
        if resolved:
            return _read_files_as_text(resolved)

    # 3. Fallback: broad scan, capped
    code_files = []
    for ext in ("*.py", "*.scala", "*.sql", "*.conf"):
        code_files.extend(glob.glob(os.path.join(project_path, "**", ext), recursive=True))
    return _read_files_as_text(code_files[:MAX_EXTRACTED_FILES])


# ---------------------------------------------------------------------------
# Source resolution: EMR vs. local
# ---------------------------------------------------------------------------

def _fetch_from_emr(
    cluster_id: str, step_id: str, s3_project_location: str = "", job_entry_point: str = "",
) -> dict:
    """Fetch a Spark step's log and (optionally) its project source code
    from AWS EMR + S3. Requires AWS credentials to be configured in the
    environment (aws configure / IAM role / env vars) - never passed as
    a parameter."""
    import boto3  # lazy import: only required if EMR source is actually used

    emr = boto3.client("emr", region_name=DEFAULT_AWS_REGION)
    s3 = boto3.client("s3", region_name=DEFAULT_AWS_REGION)

    step = emr.describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]
    status = step.get("Status", {})

    # EMR step logs live under <cluster log URI>/<step_id>/stderr / stdout
    cluster = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]
    log_uri = cluster.get("LogUri", "")

    log_text = ""
    if log_uri:
        bucket, _, prefix = log_uri.replace("s3://", "").partition("/")
        key = f"{prefix}{step_id}/stderr.gz" if prefix else f"{step_id}/stderr.gz"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            log_text = obj["Body"].read().decode("utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001
            log_text = f"[Could not auto-fetch stderr.gz: {exc}]"

    project_code = ""
    if s3_project_location:
        project_code = _select_s3_project_code(job_entry_point, log_text, s3_project_location)

    return {
        "status": status,
        "log": log_text,
        "project_code": project_code,
    }


def _select_s3_project_code(job_entry_point: str, error_log: str, s3_project_location: str) -> str:
    """S3 equivalent of _select_project_code - same priority logic
    (entry point > stack-trace extraction > fallback scan), but reading
    from S3 instead of local disk."""
    import boto3

    s3 = boto3.client("s3", region_name=DEFAULT_AWS_REGION)
    bucket, _, prefix = s3_project_location.replace("s3://", "").partition("/")

    def _read_key(key: str) -> str:
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            return f"--- s3://{bucket}/{key} ---\n{body.decode('utf-8', errors='replace')}"
        except Exception as exc:  # noqa: BLE001
            return f"--- s3://{bucket}/{key} ---\n[Could not read: {exc}]"

    # List all candidate code files under the prefix once, reused below
    paginator = s3.get_paginator("list_objects_v2")
    all_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith((".py", ".scala", ".sql", ".conf", ".json")):
                all_keys.append(obj["Key"])

    # 1. Explicit entry point
    if job_entry_point:
        matches = [k for k in all_keys if k.endswith(job_entry_point) or os.path.basename(k) == job_entry_point]
        if matches:
            return _read_key(matches[0])

    # 2. Auto-extract from stack trace
    if error_log:
        extracted = _extract_referenced_files(error_log)
        matched_keys = []
        for kind, value in extracted:
            target = os.path.basename(value)
            for k in all_keys:
                if os.path.basename(k) == target:
                    matched_keys.append(k)
                    break
        if matched_keys:
            return "\n\n".join(_read_key(k) for k in matched_keys[:MAX_EXTRACTED_FILES])

    # 3. Fallback: capped scan
    return "\n\n".join(_read_key(k) for k in all_keys[:MAX_EXTRACTED_FILES])


def _fetch_from_local(log_path: str, project_path: str = "", job_entry_point: str = "") -> dict:
    """Fetch log content and (optionally) project source code from the
    local filesystem. No AWS credentials required."""
    log_text = ""
    if os.path.isfile(log_path):
        with open(log_path, "r", errors="replace") as f:
            log_text = f.read()
    elif os.path.isdir(log_path):
        files = glob.glob(os.path.join(log_path, "**", "*.log"), recursive=True)
        parts = []
        for fp in files[:10]:
            with open(fp, "r", errors="replace") as f:
                parts.append(f"--- {fp} ---\n{f.read()}")
        log_text = "\n\n".join(parts)
    else:
        log_text = f"[Path not found: {log_path}]"

    project_code = _select_project_code(job_entry_point, log_text, project_path)

    return {"status": {}, "log": log_text, "project_code": project_code}


def _resolve_source(
    source_type: str,
    emr_cluster_id: str,
    emr_step_id: str,
    s3_project_location: str,
    local_log_path: str,
    local_project_path: str,
    job_entry_point: str = "",
) -> dict:
    if source_type == "emr":
        if not emr_cluster_id or not emr_step_id:
            raise ValueError("emr_cluster_id and emr_step_id are required when source_type='emr'")
        return _fetch_from_emr(emr_cluster_id, emr_step_id, s3_project_location, job_entry_point)
    elif source_type == "local":
        if not local_log_path:
            raise ValueError("local_log_path is required when source_type='local'")
        return _fetch_from_local(local_log_path, local_project_path, job_entry_point)
    else:
        raise ValueError(f"Unknown source_type '{source_type}'. Use 'emr' or 'local'.")


# ---------------------------------------------------------------------------
# Provider resolution: bedrock / anthropic / openai / none
# ---------------------------------------------------------------------------

def _call_bedrock(system_prompt: str, user_prompt: str, max_tokens: int = 1024) -> str:
    import boto3  # lazy import: only required if provider="bedrock" is actually used

    client = boto3.client("bedrock-runtime", region_name=DEFAULT_AWS_REGION)
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}],
    }
    response = client.invoke_model(modelId=DEFAULT_BEDROCK_MODEL_ID, body=json.dumps(body))
    payload = json.loads(response["body"].read())
    return payload["content"][0]["text"]


def _call_anthropic(system_prompt: str, user_prompt: str, api_key: str, max_tokens: int = 1024) -> str:
    import anthropic  # local import: only required if this provider is used

    client = anthropic.Anthropic(api_key=api_key or os.environ.get("ANTHROPIC_API_KEY"))
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=max_tokens,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return message.content[0].text


def _call_openai(system_prompt: str, user_prompt: str, api_key: str, max_tokens: int = 1024) -> str:
    import openai  # local import: only required if this provider is used

    client = openai.OpenAI(api_key=api_key or os.environ.get("OPENAI_API_KEY"))
    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    return response.choices[0].message.content


def _resolve_diagnosis(
    provider: str,
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    fetched: dict,
) -> str:
    """Either call the requested provider, or - if provider is 'none' -
    return the fetched raw content so the CALLING AGENT (Devin, Claude
    Desktop, Claude Code) can reason about it directly, with no LLM call
    made by this server at all."""
    if provider == "bedrock":
        return _call_bedrock(system_prompt, user_prompt)
    elif provider == "anthropic":
        return _call_anthropic(system_prompt, user_prompt, api_key)
    elif provider == "openai":
        return _call_openai(system_prompt, user_prompt, api_key)
    elif provider == "none":
        return (
            "No LLM provider configured - returning fetched content for "
            "the calling agent to analyze directly.\n\n"
            f"{user_prompt}"
        )
    else:
        raise ValueError(
            f"Unknown provider '{provider}'. Use 'bedrock', 'anthropic', 'openai', or 'none'."
        )


# ---------------------------------------------------------------------------
# Tool 1: diagnose_spark_failure
# ---------------------------------------------------------------------------

FAILURE_SYSTEM_PROMPT = """You are an expert Apache Spark engineer. Given a
failed Spark job's error log (and optionally its source code), diagnose
the root cause and suggest a concrete fix.

Respond in this exact structure:

ROOT CAUSE:
<specific to what's in the log>

EVIDENCE:
<the specific log lines/stack trace elements that support this diagnosis>

SUGGESTED FIX:
<concrete code or configuration change>

CONFIDENCE: <High / Medium / Low>
"""


@mcp.tool(
    annotations={
        "readOnlyHint": True,       # only reads logs/code, never modifies anything
        "destructiveHint": False,   # no data is ever deleted or overwritten
        "idempotentHint": True,     # same log/code input -> same type of diagnosis, no side effects accumulate
        "openWorldHint": True,      # may call external systems (AWS EMR/S3, Bedrock, Anthropic, OpenAI) depending on config
    }
)
def diagnose_spark_failure(
    source_type: str,
    emr_cluster_id: str = "",
    emr_step_id: str = "",
    s3_project_location: str = "",
    local_log_path: str = "",
    local_project_path: str = "",
    job_entry_point: str = "",
    provider: str = "none",
    api_key: str = "",
) -> str:
    """Diagnose why an Apache Spark job failed.

    Args:
        source_type: Where to fetch the log/code from - "emr" or "local".
        emr_cluster_id: EMR cluster ID (required if source_type="emr").
        emr_step_id: EMR step ID (required if source_type="emr").
        s3_project_location: Optional S3 URI to the job's source code
            (used only if source_type="emr").
        local_log_path: Path to a local log file or folder (required if
            source_type="local").
        local_project_path: Optional local folder containing the job's
            source code (used only if source_type="local").
        job_entry_point: Optional filename/relative path of the specific
            job file that ran (e.g. "jobs/customer_order_join.py"). If
            given, ONLY this file is used as code context - skips
            auto-extraction entirely. Best used when the caller already
            knows which job failed. If omitted, the tool automatically
            parses the error log's stack trace (Python and/or Scala/Java
            patterns - handles mixed PySpark traces) to find the
            relevant file(s) in the project, up to 10 files, filtering
            out framework/library internals. This keeps large,
            multi-job projects from having their entire codebase sent
            to the model - only the code actually implicated by the
            failure is included.
        provider: Which LLM does the reasoning - "bedrock", "anthropic",
            "openai", or "none" (default). "none" returns the fetched
            log/code as-is, for the CALLING AGENT to diagnose itself -
            no LLM call is made by this tool in that case.
        api_key: API key for "anthropic" or "openai" providers. If
            omitted, reads from the ANTHROPIC_API_KEY / OPENAI_API_KEY
            environment variable. Not used for "bedrock" (uses locally
            configured AWS credentials) or "none".

    Returns:
        A structured diagnosis (root cause, evidence, fix, confidence)
        if a provider is set, or the raw fetched log/code for the
        calling agent to analyze if provider="none".
    """
    fetched = _resolve_source(
        source_type, emr_cluster_id, emr_step_id, s3_project_location,
        local_log_path, local_project_path, job_entry_point,
    )

    user_prompt = f"ERROR LOG:\n{fetched['log']}\n"
    if fetched["project_code"]:
        user_prompt += f"\nSOURCE CODE:\n{fetched['project_code']}\n"

    return _resolve_diagnosis(provider, api_key, FAILURE_SYSTEM_PROMPT, user_prompt, fetched)


# ---------------------------------------------------------------------------
# Tool 2: optimize_spark_performance
# ---------------------------------------------------------------------------

PERFORMANCE_SYSTEM_PROMPT = """You are an expert Apache Spark performance
tuning engineer. Given execution logs/stats from a SUCCESSFUL Spark job
(stage durations, shuffle read/write sizes, task counts, executor
config, partition counts, skew indicators), identify bottlenecks and
recommend specific, actionable improvements.

Only suggest changes clearly justified by the data provided. If the
data doesn't show a clear bottleneck, say so honestly.

Respond in this exact structure:

OBSERVATIONS:
<what the stats/log show>

RECOMMENDATIONS:
<numbered list of specific, concrete changes - code, config, or both>

ESTIMATED IMPACT: <High / Medium / Low>
"""


@mcp.tool(
    annotations={
        "readOnlyHint": True,       # only reads logs/code, never modifies anything
        "destructiveHint": False,   # no data is ever deleted or overwritten
        "idempotentHint": True,     # same log/code input -> same type of analysis, no side effects accumulate
        "openWorldHint": True,      # may call external systems (AWS EMR/S3, Bedrock, Anthropic, OpenAI) depending on config
    }
)
def optimize_spark_performance(
    source_type: str,
    emr_cluster_id: str = "",
    emr_step_id: str = "",
    s3_project_location: str = "",
    local_log_path: str = "",
    local_project_path: str = "",
    job_entry_point: str = "",
    current_spark_config: str = "",
    provider: str = "none",
    api_key: str = "",
) -> str:
    """Analyze a successful Spark job's execution log/stats and suggest
    performance optimizations.

    Args:
        source_type: Where to fetch the log/code from - "emr" or "local".
        emr_cluster_id: EMR cluster ID (required if source_type="emr").
        emr_step_id: EMR step ID (required if source_type="emr").
        s3_project_location: Optional S3 URI to the job's source code.
        local_log_path: Path to a local log file/folder (required if
            source_type="local").
        local_project_path: Optional local folder with source code.
        job_entry_point: Optional filename/relative path of the specific
            job file to focus on. If given, ONLY this file is used -
            skips auto-extraction. If omitted, the tool parses the
            execution log for stack-trace-like file references (Python
            and/or Scala/Java) to narrow down which of the project's
            files are relevant, up to 10 files. Useful for large,
            multi-job repositories.
        current_spark_config: Optional text describing the current Spark
            configuration (executor memory, cores, shuffle partitions,
            etc.), to ground recommendations in what's configurable.
        provider: "bedrock", "anthropic", "openai", or "none" (default -
            returns fetched content for the calling agent to analyze).
        api_key: API key for "anthropic"/"openai". Reads from env var if
            omitted. Unused for "bedrock" or "none".

    Returns:
        A structured analysis (observations, recommendations, estimated
        impact) if a provider is set, or the raw fetched content for the
        calling agent to analyze if provider="none".
    """
    fetched = _resolve_source(
        source_type, emr_cluster_id, emr_step_id, s3_project_location,
        local_log_path, local_project_path, job_entry_point,
    )

    user_prompt = f"EXECUTION LOG/STATS:\n{fetched['log']}\n"
    if current_spark_config.strip():
        user_prompt += f"\nCURRENT SPARK CONFIG:\n{current_spark_config}\n"
    if fetched["project_code"]:
        user_prompt += f"\nSOURCE CODE:\n{fetched['project_code']}\n"

    return _resolve_diagnosis(provider, api_key, PERFORMANCE_SYSTEM_PROMPT, user_prompt, fetched)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    """Entrypoint used by the `sparksense-mcp` console script."""
    mcp.run()


if __name__ == "__main__":
    main()
