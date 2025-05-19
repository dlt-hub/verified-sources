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

## How to add a verified source to your dlt project
`dlt` offers an `init` command that will clone and inject any source with an example pipeline from this repository into your project, setup the credentials and python dependencies. Please follow the step by step instructions in our [docs](https://dlthub.com/docs/walkthroughs/add-a-verified-source).

### We encourage you to hack the code
Verified sources code is added to your project with the expectation that you will hack or customize code yourself - as you need it.


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


## Building with LLMs
See README [`ai`](ai/README.md) folder for details.
