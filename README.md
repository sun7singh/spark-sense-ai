# spark-sense-ai

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![M8ven Score](https://m8ven.ai/badge/mcp/sun7singh-spark-sense-ai-1wc85l)](https://m8ven.ai/mcp/sun7singh-spark-sense-ai-1wc85l)

An **MCP (Model Context Protocol) server** that gives AI agents — Claude
Desktop, Claude Code, Devin, or any MCP-compatible client — two
capabilities for working with Apache Spark jobs:

- 🔴 **`diagnose_spark_failure`** — a Spark job failed; get a root cause
  and a concrete fix, grounded in the actual error log and the specific
  code that failed.
- 🟢 **`optimize_spark_performance`** — a Spark job succeeded but is slow
  or expensive; get targeted, evidence-based tuning recommendations.

Built by a data engineer with 12+ years of hands-on Apache Spark
experience, to bring that same debugging instinct — "which file is
this error actually about, and why" — into an AI-assisted workflow.

---

## Why this exists

Spark failures are usually diagnosable from the log alone — but reading
a 200-line stack trace, matching it back to the right file in a large,
multi-job codebase, and knowing which of a dozen possible causes it
actually is, takes real Spark experience. This tool automates that
first pass: it finds the *relevant* code (not the whole repository),
hands it to an LLM alongside the log, and gets back a structured
diagnosis you can verify and act on.

---

## What makes it different

| Design choice | Why it matters |
|---|---|
| **Source-agnostic** — EMR (cluster + step ID) *or* a local folder | Works whether your job runs on AWS or on-prem/locally |
| **Provider-agnostic** — Bedrock, Anthropic, OpenAI, *or none* | No vendor lock-in; `provider="none"` lets the calling agent (e.g. Devin) reason about the fetched content itself, with no LLM call made by this server at all |
| **Smart file selection** | Large projects run many jobs — this tool parses the error log's stack trace (Python *and* Scala/Java, including mixed PySpark traces) to pull in only the specific file(s) implicated in the failure, capped at 10 files, instead of dumping an entire codebase into the prompt |
| **No bundled credentials, ever** | Every user brings their own AWS and/or LLM credentials. Nothing here shares billing or access between users |

---

## Sample output

Given [this sample Scala error log](tests/test_data/sample_error_scala.log)
and its [matching project files](tests/test_data/sample_project_scala/),
`diagnose_spark_failure` (with a provider configured) returns:

```
ROOT CAUSE:
CustomerHelper.validate() calls .trim() on the "email" field without
checking for null first. Records with a missing email cause a
NullPointerException, which aborts the job after 4 failed task retries.

EVIDENCE:
- Caused by: java.lang.NullPointerException: Cannot invoke "String.trim()"
  because "email" is null
- at com.company.jobs.CustomerHelper$.validate(CustomerHelper.scala:22)
- Source shows: email.trim().nonEmpty with no null check beforehand

SUGGESTED FIX:
def validate(row: Row): Boolean = {
  val email = Option(row.getAs[String]("email"))
  email.exists(_.trim.nonEmpty)
}

CONFIDENCE: High
```

Notice the tool automatically pulled in `CustomerHelper.scala` (the file
the trace's `Caused by` actually points to), not the whole project or
even the top-level `CustomerOrderJoin.scala` entry file — because the
stack-trace parser resolved the deepest relevant frame.

---

## Installation

```bash
pip install spark-sense-ai
```

Install extras only for what you'll actually use:

```bash
pip install spark-sense-ai[aws]         # for EMR source or Bedrock provider
pip install spark-sense-ai[anthropic]   # for provider="anthropic"
pip install spark-sense-ai[openai]      # for provider="openai"
pip install spark-sense-ai[all]         # everything
```

`provider="none"` with `source_type="local"` needs **no extras at all**
— just the base `mcp` dependency.

---

## The four ways to use it

| # | Log/code source | LLM provider | Extras needed | AWS credentials needed? |
|---|---|---|---|---|
| 1 | EMR cluster + step | Bedrock | `[aws]` | Yes — for fetching **and** diagnosis |
| 2 | EMR cluster + step | Anthropic / OpenAI | `[aws]` + `[anthropic\|openai]` | Yes — only for fetching |
| 3 | Local folder | None (agent reasons, e.g. inside Devin) | none | No |
| 4 | Local folder | Anthropic / OpenAI | `[anthropic\|openai]` | No |

AWS credentials, when needed, are picked up automatically from your
existing setup (`aws configure`, an attached IAM role, or standard
`AWS_*` environment variables) — **never** passed as a tool parameter.

---

## Setup

### Claude Desktop

Edit `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "sparksense": {
      "command": "sparksense-mcp",
      "env": {
        "SPARKSENSE_AWS_REGION": "ap-south-1"
      }
    }
  }
}
```

### Claude Code

```bash
claude mcp add sparksense -- sparksense-mcp
```

### Devin

See [Devin's MCP docs](https://docs.devin.ai/desktop/cascade/mcp) for
the current config method for your Devin agent mode (Cascade and Devin
Local use slightly different config locations). Point it at the
`sparksense-mcp` command the same way as above.

---

## Usage examples

> "My Spark job failed — EMR cluster j-ABC123, step s-XYZ789. Use
> sparksense to diagnose it with Bedrock."

> "Here's my local job's log at `./logs/error.log` and code at `./src`
> — diagnose the failure."

> "I know it's `jobs/customer_order_join.py` that failed — use
> sparksense with that as the entry point."

> "Use sparksense to fetch the log at `./logs/job.log` — I'll review it
> myself." *(`provider="none"` — the tool just fetches; the calling
> agent does the reasoning)*

> "My job succeeded but took 40 minutes. Use sparksense to check the
> execution stats for optimization opportunities."

---

## Tool reference

### `diagnose_spark_failure`

| Parameter | Required | Notes |
|---|---|---|
| `source_type` | Yes | `"emr"` or `"local"` |
| `emr_cluster_id` | If `source_type="emr"` | |
| `emr_step_id` | If `source_type="emr"` | |
| `s3_project_location` | No | S3 URI to source code |
| `local_log_path` | If `source_type="local"` | File or folder |
| `local_project_path` | No | Local source code folder |
| `job_entry_point` | No | Specific filename/relative path to use directly, skipping auto-extraction — best when you already know which job failed |
| `provider` | No (default `"none"`) | `"bedrock"` / `"anthropic"` / `"openai"` / `"none"` |
| `api_key` | No | For anthropic/openai; else reads `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` |

### `optimize_spark_performance`

Same parameters as above, plus:

| Parameter | Required | Notes |
|---|---|---|
| `current_spark_config` | No | Executor memory, cores, shuffle partitions, etc. |

### File selection logic (both tools)

```
1. job_entry_point given?
     → use ONLY that file. No auto-extraction.

2. Else, parse the error log for:
     → Python:      File "<path>", line <N>
     → Scala/Java:  at <package>.<Class>.<method>(<Filename>:<N>)
     (handles mixed PySpark traces — Python frames bottoming into JVM
     frames — by scanning for both patterns in the same log)
     → filters out framework/library internals (site-packages, pyspark,
       org.apache.spark, scala.*, java.*, etc.)
     → fetches up to 10 matched files

3. Else, fallback: broad scan of the project folder, capped at 10 files
```

---

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `SPARKSENSE_AWS_REGION` | `ap-south-1` | Region for EMR/S3/Bedrock calls |
| `SPARKSENSE_BEDROCK_MODEL_ID` | `global.anthropic.claude-haiku-4-5-20251001-v1:0` | Bedrock model to use |
| `ANTHROPIC_API_KEY` | — | Used if `provider="anthropic"` and no `api_key` param given |
| `OPENAI_API_KEY` | — | Used if `provider="openai"` and no `api_key` param given |

---

## Testing

```bash
git clone https://github.com/YOUR_GITHUB_USERNAME/spark-sense-ai.git
cd spark-sense-ai
pip install -e ".[all]"

# Local source + Anthropic provider, includes Python and Scala samples
export ANTHROPIC_API_KEY="sk-ant-..."
python tests/test_local_anthropic.py

# EMR source + Bedrock provider (needs a real EMR cluster/step)
aws configure
python tests/test_emr_bedrock.py --cluster-id j-XXXXXXX --step-id s-XXXXXXX
```

Both scripts run a free, no-API-call sanity check first (`provider="none"`)
before making any billed LLM calls.

---

## Roadmap

- [ ] Automatic trigger via Lambda/EventBridge on EMR/Glue job completion
- [ ] Databricks as a third `source_type`
- [ ] Structured Spark History Server API integration
- [ ] Skew detection with partition-level statistics

## License

MIT — see [LICENSE](LICENSE).
