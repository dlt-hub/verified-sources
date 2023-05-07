# Pipelines contrib repo


# Add pipelines from here to your project
`dlt` offers an `init` command that will clone and inject any pipeline from this repository into your project, setup the credentials and python dependencies. Please follow our [docs](https://dlthub.com/docs/walkthroughs/add-a-pipeline)

# Contact us and get help
Join our slack by following the [invitation link](https://join.slack.com/t/dlthub-community/shared_invite/zt-1n5193dbq-rCBmJ6p~ckpSFK4hCF2dYA)

If you added a pipeline and something does not work: `technical-help` channel

If you want to contribute pipeline, customization or a fix: `dlt-contributors` channel

# Submit new pipelines or bugfixes

> 💡 **If you want to share your working pipeline with the community**

1. Follow the [guide](SHARE_PIPELINE.md) on how to prepare your pipeline started with `dlt init` to be shared here.
2. Create an issue that describes the pipeline using [community pipeline template](https://github.com/dlt-hub/pipelines/issues/new?template=new-community-pipeline.md)
3. Fork the [pipelines]() repository
4. Create a feature branch in your fork
5. Commit to that branch when you work. Please use descriptive commit names
6. Make a PR to a master branch of this repository (upstream) [from your fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork)
7. We'll do code reviews quickly

> 💡 **If you want us or someone else to build a new pipeline**

Here's the [pipeline request template](https://github.com/dlt-hub/pipelines/issues/new?template=pipeline-request.md)

> 💡 **If you want to report a bug in one of the pipelines**

Use the [bug report template](https://github.com/dlt-hub/pipelines/issues/new?template=bug-report.md)

# Read the docs on building blocks

If you are new to `dlt` complete the [Getting started](https://dlthub.com/docs/getting-started) and the [Walkthroughs](https://dlthub.com/docs/walkthroughs/create-a-pipeline) so you have a feeling what is dlt and **how people will use your pipeline**.

We strongly suggest that you build your pipelines out of existing **building blocks**.

* Declare your [resources](https://dlthub.com/docs/general-usage/resource) and group them in [sources](https://dlthub.com/docs/general-usage/source) using Python decorators.
* [Connect the transformers to the resources](https://dlthub.com/docs/general-usage/resource#feeding-data-from-one-resource-into-another) to load additional data or enrich it
* [Create your resources dynamically from data](https://dlthub.com/docs/general-usage/source#create-resources-dynamically)
* [Append, replace and merge your tables](https://dlthub.com/docs/general-usage/incremental-loading)
* [Transform your data before loading](https://dlthub.com/docs/general-usage/resource#customize-resources) and see some [examples of customizations like column renames and anonymization](https://dlthub.com/docs/customizations/customizing-pipelines/renaming_columns)
* [Set up "last value" incremental loading](https://dlthub.com/docs/general-usage/incremental-loading#incremental-loading-with-last-value)
* [Dispatch data to several tables from a single resource](https://dlthub.com/docs/general-usage/resource#dispatch-data-to-many-tables)
* [Set primary and merge keys, define the columns nullability and data types](https://dlthub.com/docs/general-usage/resource#define-schema)
* [Pass config and credentials into your sources and resources](https://dlthub.com/docs/general-usage/credentials)
* Use google oauth2 and service account credentials, database connection strings and define your own complex credentials: see examples below

Concepts to grasp
* [Credentials](https://dlthub.com/docs/general-usage/credentials) and their ["under the hood"](https://github.com/dlt-hub/dlt/blob/devel/docs/technical/secrets_and_config.md)
* [Schemas, naming conventions and data normalization](https://dlthub.com/docs/general-usage/schema).
* [How we distribute pipelines to our users](DISTRIBUTION.md)

Building blocks used right:
* [Create dynamic resources for tables by reflecting a whole database](https://github.com/dlt-hub/pipelines/blob/master/pipelines/sql_database/sql_database.py#L50)
* [Incrementally dispatch github events to separate tables](https://github.com/dlt-hub/pipelines/blob/master/pipelines/github/__init__.py#L70)
* [Read the participants for each deal using transformers and pipe operator](https://github.com/dlt-hub/pipelines/blob/master/pipelines/pipedrive/__init__.py#L67)
* [Read the events for each ticket by attaching transformer to resource explicitly](https://github.com/dlt-hub/pipelines/blob/master/pipelines/hubspot/__init__.py#L125)
* [Set `tags` column data type to complex to load them as JSON/struct](https://github.com/dlt-hub/pipelines/blob/master/pipelines/zendesk/__init__.py#L108)
* Typical use of `merge` with incremental load for endpoints returning a list of updates in [Shopify pipeline]().
* A `dlt` mega-combo in pipedrive pipeline, where the deals from `deal` endpoint are [fed into]() `deals_flow` resource to obtain events for a particular deal. [Both resources use `merge` write disposition and incremental load to get just the newest updates](). [The `deals_flow` is dispatching different event types to separate tables with `dlt.mark.with_table_name`]().
* An example of using JSONPath expression to get cursor value for incremental loading. In pipedrive some objects have `timestamp` property and others `update_time`. [The dlt.sources.incremental('update_time|modified') expression lets you bind the incremental to either]().
* If your source/resource needs google credentials, just use `dlt` build it credentials as we do in [google sheets]() and [google analytics]()
* If your source/resource accepts several different credential types look how [we deal with 3 different types of Zendesk credentilas]()
* See database connection string credentials [applied to sql_database pipeline]()

# Contribute pipeline step by step

Code of the community and verified pipelines reside in `pipelines` folder. Each pipeline has its own **pipeline folder** (ie. `chess`) where the `dlt.source` and `dlt.resource` functions are present. The internal organization of this folder is up to the contributor. For each pipeline there's a also a script with the example usages (ie. `chess_pipeline.py`). The intention is to show the user how the sources/resources may be called and let the user to copy the code from it.

> 💡 if you are sharing a pipeline created with `dlt init` here is a [guide](SHARE_PIPELINE.md). Below you can find in-depth information.

## Steps to add a new pipeline `<name>`

1. Create a folder (**pipeline folder**) with your pipeline `<name>` in `pipelines`. Place all your code in that folder.
2. Place (decorated) source/resource functions in the **main module** named as **pipeline folder** (the `__init__.py` also works)
3. Try to separate your code where the part that you want people to hack stays in **main module** and the rest goes to some helper modules.
4. Create a demo/usage script with the name `<name>_pipeline.py` and place it in `pipelines`. Make it work with `postgres` or `duckdb` so it is easy to try them out
5. Add pipeline specific dependencies as described below
6. Add example credentials to this repo as described below.
7. Add one liner module docstring to the `__init__.py` in **pipeline folder**. `dlt init --list-pipelines` will use this line as pipeline description.
8. The pipeline must pass linter stage.

## Pipeline specific dependencies.
If pipeline requires additional dependencies that are not available in `dlt` they may be added as follows:

1. Use `poetry` to add it to the group with the same name as pipeline. Example: chess pipeline uses `python-chess` to decode game moves. Dependency was added with `poetry add -G chess python-chess`
2. Add `requirements.txt` file in **pipeline folder** and add the dependency there.

## Python module import structure
**Use relative imports**. Your code will be imported as source code and everything under **pipeline folder** must be self-contained and isolated. Example (from `google_sheets`)
```python
from .helpers.data_processing import get_spreadsheet_id
from .helpers.api_calls import api_auth
from .helpers import api_calls
```

In your **Create a demo/usage script** use normal imports - as you would use in standalone script.
Example (from `pipedrive`):
```python
import dlt
from pipedrive import pipedrive_source
```

## The demo/usage pipeline script: `<name>_pipeline.py`
This script is distributed by `dlt init` with the other pipeline `<name>` files. It will be a first touch point with your users. It will be used by them as a starting point or as a source of code snippets. The ideal content for the script:
1. Shows a few usage examples with different source/resource arguments combinations that you think are the most common cases for your user.
2. If you provide any customizations/transformations then show how to use them.
3. Any code snippet that will speed the user up.

Examples:
* [chess](pipelines/chess_pipeline.py)
* [pipedrive](pipelines/pipedrive_pipeline.py)


## Run your demo scripts
It would be perfect if you are able to run the demo scripts. If you are contributing a working pipeline you can probably re-use your test accounts, data and credentials. You can test the scripts by loading to local `duckdb` or `postgres` as explained later.

Your working dir must be `pipelines` otherwise `dlt` will not find the `.dlt` folder with secrets.

### Common credentials and configuration
**All pipeline tests and usage/example scripts share the same config and credential files** that are present in `pipelines/.dlt`.

This makes running locally much easier and `dlt` configuration is flexible enough to apply to many pipelines in one folder.

### Destination credentials
Please look at `example.secrets.toml` in `.dlt` folder on how to configure `postgres`, `redshift` and `bigquery` destination credentials. Those credentials are shared by all pipelines.

Then you can create your `secrets.toml` with the credentials you need. The `duckdb` and `postgres` destinations work locally and we suggest you use them for initial testing.

As explained in technical docs, both native form (ie. database connection string) or dictionary representation (a python dict with *host* *database* *password* etc.) can be used.

### Adding source config and credentials
If you add a new pipeline that require a secret value, please add a placeholder to `example.secrets.toml`. When adding the source config and secrets please follow the [section layout for sources](https://github.com/dlt-hub/dlt/blob/devel/docs/technical/secrets_and_config.md#default-layout-and-default-key-lookup-during-injection). We have a lot of pipelines so we must use precise section layout (up to module level):

`[sources.<python module name where source and resources are placed>]`

This way we can isolate credentials for each pipeline.

### Local Postgres instance
There's compose file with fully prepared postgres instance [here](tests/postgres/README.md)

## Go a step further by adding test data and automatic tests
We may distribute a pipeline without tests and daily CI running as **community pipeline**. **verified pipelines** require following additional steps

1. Place your tests in `tests/<name>`.
2. To run your tests you'll need to create test accounts, data sets, credentials etc. Talk to dlt team on slack. We may provide you with the required accounts and credentials.

# Development
`python-dlt` uses `poetry` to manage, build and version the package. It also uses `make` to automate tasks. To start
```sh
make install-poetry  # will install poetry, to be run outside virtualenv
```
then
```sh
make dev  # will install all deps including dev
```
Executing `poetry shell` and working in it is very convenient at this moment.

## Python version
Use python 3.8 for development which is the lowest supported version for `python-dlt`. You'll need `distutils` and `venv`:

```shell
sudo apt-get install python3.8
sudo apt-get install python3.8-distutils
sudo apt install python3.8-venv
```
You may also use `pyenv` as [poetry](https://python-poetry.org/docs/managing-environments/) suggests.

## Typing and linting
`python-dlt` uses `mypy` and `flake8` with several plugins for linting. We do not reorder imports or reformat code. To lint the code do `make lint`.

**Code does not need to be typed** - but it is better if it is - `mypy` is able to catch a lot of problems in the code. If your pipeline is typed file named `py.typed` to the folder where your pipeline code is. (see `chess` pipeline for example)

**Function input argument of sources and resources should be typed** that allows `dlt` to validate input arguments at runtime, say which are secrets and generate the secret and config files automatically.

## `dlt init` compatibility
All the pipelines will be parsed and installed with `dlt init` **during the linting stage**. Those tests are implemented in `tests/test_dlt_init.py`. This is required for the PR to be accepted on CI.

### Adding `__init__.py` files
Linting step requires properly constructed python packages so it will ask for `__init__` files to be created. That can be automated with
```sh
./check-package.sh --fix
```
executed from the top repo folder


# Sharing and obtaining source credentials, test accounts, destination access

1. If you are contributing and want to test against `redshift` and `bigquery`, **ping the dlt team on slack**. You'll get a `toml` file fragment with the credentials that you can paste into your `secrets.toml`
2. If you contributed a pipeline and created any credentials, test accounts, test dataset please include them in the tests or share them with `dlt` team so we can configure the CI job. If sharing is not possible please help us to reproduce your test cases so CI job will pass.

# Testing
We use `pytest` for testing. Every test is running within a set of fixtures that provide the following environment (see `conftest.py`):
1. they load secrets and config from `pipelines/.dlt` so the same values are used when you run your pipeline from command line and in tests
2. it sets the working directory for each pipeline to `_storage` folder and makes sure it is empty before each test
3. it drops all datasets from the destination after each test
4. it runs each test with the original environment variables so you can modify `os.environ`

Look at `tests/chess/test_chess_pipeline.py` for an example. The line
```python
@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
```
makes sure that each test runs against all destinations (as defined in `ALL_DESTINATIONS` global variables)

The simplest possible test just creates pipeline and then issues a run on a source. More advanced test will use `sql_client` to check the data and access the schemas to check the table structure.

Please also look at the [test helpers](tests/utils.py) that you can use to assert the load infos, get counts of elements in tables, select and assert the data in tables etc.

## Guidelines for writing tests
Your tests will be run both locally and on CI. It means that a few instances of your test may be executed in parallel and they will be sharing resources. A few simple rules make that possible.
1. Always use `full_refresh` when creating pipelines in test. This will make sure that data is loaded into new schema/dataset. Fixtures in `conftest.py` will drop datasets created during load.
2. When creating any fixtures for your tests, make sure that fixture is unique for your test instance.
> If you create database or schema or table, add random suffix/prefix to it und use in your test
>
> If you create an account ie. an user with a name and this name is uniq identifier, also add random suffix/prefix
3. Cleanup after your fixtures - delete accounts, drop schemas and databases
4. Add code to `tests/utils.py` only if this is helpful for all tests. Put your specific helpers in your own directory.

## Mandatory tests for pipelines
Tests in `tests/test_dlt_init.py` are executed as part of linting stage and must be passing. They make sure that pipeline can be distributed with `dlt init`.

## Running tests selectively
1. When developing, limit the destinations to local ie. duckdb by setting the environment variable:
```
ALL_DESTINATIONS='["duckdb"]' pytest tests/chess
```

there's also ` make test-local` command that will run all the tests on `duckdb` and `postgres`
