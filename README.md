<h1 align="center">
    <strong>data load tool (dlt) — verified sources repository</strong>
</h1>

<div align="center">
  <a target="_blank" href="https://dlthub.com/community" style="background:none">
    <img src="https://img.shields.io/badge/slack-join-dlt.svg?labelColor=191937&color=6F6FF7&logo=slack" />
  </a>
  <a target="_blank" href="https://pypi.org/project/dlt/" style="background:none">
    <img src="https://img.shields.io/pypi/v/dlt?labelColor=191937&color=6F6FF7">
  </a>
  <a target="_blank" href="https://pypi.org/project/dlt/" style="background:none">
    <img src="https://img.shields.io/pypi/pyversions/dlt?labelColor=191937&color=6F6FF7">
  </a>
</div>
<br>

This repository contains verified dlt sources and demo pipelines for each source that you can use as a starting point for your project.

> We launched **dltHub** in March 2026. You can now run your dlt pipelines on a managed, agent-native platform with transformations, data quality and observability built in. [Learn more](https://dlthub.com/) or [deploy your first pipeline](https://dlthub.com/docs/hub/getting-started/onboarding).

## How to add a verified source to your dlt project
`dlt` offers an `init` command that will clone and inject any source with an example pipeline from this repository into your project, setup the credentials and python dependencies. Please follow the step by step instructions in our [docs](https://dlthub.com/docs/walkthroughs/add-a-verified-source).

### We encourage you to hack the code
Verified sources code is added to your project with the expectation that you will hack or customize code yourself - as you need it.

Ready to run it in production? [Deploy it with dltHub](#run-it-in-production-with-dlthub).

## Run it in production with dltHub

[dltHub](https://dlthub.com/) is an agent-native data engineering platform built around `dlt`. It uses the same sources, destinations, and pipelines you already write, and adds what you need to run them in production:

- **Agent-native development**: build pipelines from prompts with Claude Code, Codex, or Cursor, with per-source context files and scaffolding commands
- **[Transformations](https://dlthub.com/docs/hub/transformations)** and **[data quality checks](https://dlthub.com/docs/hub/data-quality)** on the data you load
- **[Managed infrastructure](https://dlthub.com/docs/hub/pipeline-operations/overview)** for pipelines, notebooks, and data apps, with `dev` and `prod` profiles and one-command deploys
- **[Observability](https://dlthub.com/docs/hub/ingestion/dashboard)**: monitor runs and troubleshoot from the workspace dashboard

`dlt` stays open source under the Apache 2.0 license. dltHub is a commercial product governed by the [dltHub License](https://dlthub.com/docs/hub/license).

**[Deploy your first pipeline →](https://dlthub.com/docs/hub/getting-started/onboarding)** One command sets up a workspace, connects it to dltHub, and has your coding agent deploy and run a sample pipeline:

```sh
uvx dlthub-start@latest
```

## How to contact us and get help
Join our slack by following the [invitation link](https://dlthub.com/community)

## Reporting a source bug
Follow this link: [bug report template](https://github.com/dlt-hub/verified-sources/issues/new?template=bug-report.md)

## How to contribute
* 🚀 We are happy to accept **bugfixes and improvements** to existing sources!
* 📣 **We accept new sources but only those that cannot be easily implemented** via `REST API`, `sql_database` or `filesystem` or with **vibe coding**. 
  - see issues for source requests!
  - queues and brokers like google pub sub or rabbitMQ
  - SAP/ERP/HEALTH
  - HR platforms
  - Graph QL
* 📣 **Before starting, announce your PR** with [source request template](https://github.com/dlt-hub/verified-sources/issues/new?template=bug-report.md)

Find step by step instruction as well as troubleshooting help in [CONTRIBUTING.md](CONTRIBUTING.md).
