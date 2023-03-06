# Pipelines contrib repo


# Using pipelines in your project
`dlt` offers an `init` command that will clone and inject any pipeline from this repository into your project, setup the credentials and python dependencies. Please follow our [docs](https://dlthub.com/docs/command-line-interface)

# How to contact us and get help
Join our slack by following the [invitation link](https://join.slack.com/t/dlthub-community/shared_invite/zt-1n5193dbq-rCBmJ6p~ckpSFK4hCF2dYA)

For people using the pipelines: `technical-help` channel

For contributors: `dlt-contributors` channel

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

## python version
Use python 3.8 for development which is the lowest supported version for `python-dlt`. You'll need `distutils` and `venv`:

```shell
sudo apt-get install python3.8
sudo apt-get install python3.8-distutils
sudo apt install python3.8-venv
```
You may also use `pyenv` as [poetry](https://python-poetry.org/docs/managing-environments/) suggests.

## typing and linting
`python-dlt` uses `mypy` and `flake8` with several plugins for linting. We do not reorder imports or reformat code. To lint the code do `make lint`.

**Code does not need to be typed** - but it is better if it is - `mypy` is able to catch a lot of problems in the code. If your pipeline is typed file named `py.typed` to the folder where your pipeline code is. (see `chess` pipeline for example)

**Function input argument of sources and resources should be typed** that allows `dlt` to validate input arguments at runtime, say which are secrets and generate the secret and config files automatically.

### Adding `__init__.py` files
Linting step requires properly constructed python packages so it will ask for `__init__` files to be created. That can be automated with
```sh
./check-package.sh --fix
```
executed from the top repo folder

## Submitting new pipelines or bugfixes

1. Create an issue that describes the pipeline or the problem being fixed
2. Make a feature branch
3. Follow the guidelines from **Repository structure** chapter
4. Commit to that branch when you work. Please use descriptive commit names
5. Make a PR to master branch

## Documentation for contributors

You can find the official `dlt` documentation at [our docs site](https://dlthub.com/docs). This documentation is oriented at newcomers that often are not professional programmers. In other words: it is good to get first grasp on how to create a pipeline.

For contributors we have [in-depth technical documentation](https://github.com/dlt-hub/dlt/tree/devel/docs/technical) that may not be polished but is much more comprehensive. The chapter on [config and credentials](https://github.com/dlt-hub/dlt/blob/devel/docs/technical/secrets_and_config.md) is a must-read.

# Repository structure

All repo code reside in `pipelines` folder. Each pipeline has its own **pipeline folder** (ie. `chess`) where the `dlt.source` and `dlt.resource` functions are present. The internal organization of this folder is up to the contributor. For each pipeline there's a also a script with the example usages (ie. `chess_pipeline.py`). The intention is to show the user how the sources/resources may be called and let the user to copy the code from it.

## Steps to add a new pipeline

1. Create a folder (**pipeline folder**) with your pipeline name in `pipelines`. Place all your code in that folder.
2. Place (decorated) source/resource functions in the **main module** named as **pipeline folder** (the `__init__.py` also works)
3. Try to separate your code where the part that you want people to hack stays in **main module** and the rest goes to some helper modules.
4. Create a demo/usage script with the name `<pipeline_folder>_pipeline.py` and place it in `pipelines`. Make it work with `postgres` or `duckdb` so it is easy to try them out
5. Add pipeline specific dependencies as described below
6. Place your tests in `tests/<pipeline folder>`. To run your tests you'll need to create test accounts, data sets, credentials etc. Talk to dlt team on slack. We may provide you with the required accounts and credentials.
7. Add example credentials to this repo as described below.
8. Optional: add one liner module docstring to the `__init__.py` in **pipeline folder**. `dlt init --list-pipelines` will use this line as pipeline description.
9. The pipeline must pass CI: linter and tests stage. If you created any accounts or credentials, this data must be shared or via this repo or as is described later. We'll add it to our CI secrets

## Pipeline specific dependencies.
If pipeline requires additional dependencies that are not available in `python-dlt` they may be added as follows:

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

## Common credentials and configuration
As mentioned above the tech doc on [config and credentials](https://github.com/dlt-hub/dlt/blob/devel/docs/technical/secrets_and_config.md) is a must-read.

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

## WIP: Common code

## Running example pipelines
Your working dir must be `pipelines` otherwise `dlt` will not find the `.dlt` folder with secrets.

# Sharing and obtaining source credentials, test accounts, destination access

1. If you are contributing and want to test against `redshift` and `bigquery`, **ping the dlt team on slack**. You'll get a `toml` file fragment with the credentials that you can paste into your `secrets.toml`
2. If you contributed a pipeline and created any credentials, test accounts, test dataset please include them in the tests or share them with `dlt` team so we can configure the CI job. If sharing is not possible please help us to reproduce your test cases so CI job will pass.

## Running CI jobs from fork.
TBD. but is seems you need all destination and source credentials. **Please ping us on slack** and you'll obtain two `toml` fragments which need to be added to forked repo as Repository Secrets:
1. DESTINATIONS_SECRETS
2. SOURCES_SECRETS

# How Pipelines will be distributed
The reason for the structure above is to use `dlt init` command to let user add the pipelines to their own project. `dlt init` is able to add pipelines as pieces of code, not as dependencies.

Please read the [detailed information](DISTRIBUTION.md) on our distribution model

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
TBD.

## Running tests selectively
1. When developing, limit the destinations to local ie. duckdb by setting the environment variable:
```
ALL_DESTINATIONS='["duckdb"]' pytest tests/chess
```

there's also ` make test-local` command that will run all the tests on `duckdb` and `postgres`


## Test Postgres instance
There's compose file with fully prepared postgres instance [here](tests/postgres/README.md)

# Continuous integration
We have CI on github actions. Workflows need full set of credentials for sources and destinations to run. We put those as `toml` fragments in
1. DESTINATIONS_SECRETS - fragment with all destination credentials
2. SOURCES_SECRETS - fragment with all sources credentials

**If you are contributing from fork ping us on slack to get those**

Selective running of tests is not yet implemented. When done we'll run only the tests for the pipelines that were modified by given PR.
