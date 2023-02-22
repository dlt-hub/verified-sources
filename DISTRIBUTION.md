# How the pipelines are distributed to our users

We are trying a distribution model that allows our user to easily customize and hack the pipeline code. So our pipelines are not distributed as black boxes (ie. `pip` packages) but clones into user's project from this very repo.

This may look a little bit weird from software engineering perspective but we want to try it. Going back to `pip` package model is quite easy and we may do it on top or instead of the model below.

> Please give use feedback in the issues of this repo!

## Use `dlt init` to add pipeline to your project

We'll use the [dlt init](https://dlthub.com/docs/command-line-interface) command to distribute pipelines. The information below is WIP. This feature works currently only with [init repo](https://github.com/dlt-hub/python-dlt-init-template)


1. Pipelines are distributed by `dlt init` command that can be issued several times.
2. Pipelines come from `contrib` repo and if there’s no pipeline there, the `init` repo is used (this is what we currently do)
3. The pipelines are added to the current project as source code: the folder (ie. `pipedrive`) and the example script (`pipedrive_pipeline.py`) are added to the project.
4. The freelancer/end user is able to copy and paste and hack the code of the pipeline.
5. The subsequent `dlt init` with existing pipeline will update the code only if it was not modified!
6. Versioning: using `git` for distribution provides enough versioning information. (tags, branches, commit ids)

For example if someone issues `dlt init chess bigquery`:

1. `dlt` clones the repo and finds the `chess` in `pipelines` folder.
2. it copies the `chess` folder and `chess_pipeline.py` to user's project folder
3. it modifies the example script `chess_pipeline.py` to use `bigquery` to load data
4. it inspects the `dlt.resource` and `dlt.source` functions in `chess` module and generates config/credentials sections


## From pipeline project to customer / end user
We want our end users to hack the pipelines too! So still no black boxes

1. The implementer should customize and hack the pipelines from `dlt init` as they wish
2. The implementer should generate deployment with `dlt deploy`
3. The distribution to customer happens as source code via git repository.
4. The customer is able to hack and customize the pipeline.
