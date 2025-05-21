<h1 align="center">
    <strong>data load tool (dlt) — contributing</strong>
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


The following guide will walk you through contributing new sources or changes to existing sources
and their demo pipelines and contains a troubleshooting section. Please also read
[DISTRIBUTION.md](docs/DISTRIBUTION.md) to understand how our sources are distributed to the users.

## Before you start
* 🚀 We are happy to accept **bugfixes and improvements** to existing sources!
* 📣 **We accept new sources but only those that cannot be easily implemented** via `REST API`, `sql_database` or `filesystem` or with **vibe coding**. 
  - see issues for source requests!
  - queues and brokers like google pub sub or rabbitMQ
  - SAP/ERP/HEALTH
  - HR platforms
  - Graph QL



## What you can do here

- Contribute a change to an existing verified source or its demo pipeline: Go to the
  ["Walkthrough: Fix, improve, customize, document an existing source"](#Walkthrough-fix-improve-customize-document-an-existing-pipeline)
  section.
- Contribute a new verified source: Go to the
  ["Walkthrough: Create and contribute a new source"](#Walkthrough-create-and-contribute-a-new-source)
  section.
- Join our slack to get support from us by following the
  [invitation link](https://dlthub.com/community).

## Walkthrough: Fix, improve, customize, document an existing pipeline

In this section you will learn how to contribute changes to an existing pipeline.

1. Ensure you have followed all steps in the coding prerequisites section and the `format-lint`
   command works.
2. Start changing the code of an existing source. The typical development workflow will look
   something like this (the code examples assume you are changing the `chess` source):
   1. Make changes to the code of your source, for example adding new resources or fixing bugs.
   2. Execute the source example pipeline script, for example `python chess_pipeline.py` (from
      `sources` folder!) and see if there are any errors and wether the expected data ends up in
      your destination.
   3. Adjust your tests to test for the new features you have added or changes you have made in
      `./tests/chess/test_chess_source.py` and run the tests against duckdb locally with this command:
      `pytest tests/chess`.
   4. Run the linter and formatter to check for any problems: `make lint-code`.
3. Proceed to the pull request section to [create a pull request to the main repo](#making-a-pull-request-to-the-main-repo-from-fork).

## Walkthrough: Create and contribute a new source

In this section you will learn how to contribute a new source including tests and a demo pipeline
for that source. It is helpful to also read through the above section to see all the steps that are
part of source development.

1. Before starting development on a new source, please open a ticket
   [here](https://github.com/dlt-hub/verified-sources/issues/new?assignees=&labels=verified+source&projects=&template=build-new-verified-source.md&title=%25source+name%5D+verified+source)
   and let us know what you have planned.
2. We will acknowledge your ticket and figure out how to proceed. **This mostly has to do with
   creating a test account for the desired source and providing you with the needed credentials**. We
   will also ensure that no parallel development is happening.

Now you can get to coding. As a starting point we will copy the `chess` source. The `chess` example
is two very basic sources with a few resources. For an even simpler starting point, you can use the
`pokemon` source as the starting point with the same method. Please also read
[DISTRIBUTION.md](docs/DISTRIBUTION.md) before you start this guide to get an understanding of how
your source will be distributed to other users once it is accepted into our repo.

1. Ensure you have followed all steps in the coding prerequisites section and the format-lint
   command works.

2. We will copy the chess source as a starting point. There is a convenient script that creates a
   copy of the chess source to a new name. Run it with `python tools/new_source.py my_source`. This
   will create a new example script and **source folder** in the `sources` directory and a new test
   folder in the `tests` directory. You will have to update a few imports to make it work.

3. You are now set to start development on your new source.

4. You can now implement your verified source. Consult our extensive docs on how to create dlt
   sources and pipelines at
   [dlthub create pipeline walkthrough](https://dlthub.com/docs/walkthroughs/create-a-pipeline).

5. Read the rest of this document and [BUILDING-BLOCKS.md](docs/BUILDING-BLOCKS.md) for information
   on various topics.

6. Proceed to the pull request section to [create a pull request to the main repo](#making-a-pull-request-to-the-main-repo-from-fork).

## Walkthrough: Modify or add rules files for LLM-enabled IDEs
In this section, you will learn how to contribute rules files.
1. Follow the [coding prerequisites](#coding-prerequisites) to setup the repository
2. On your branch, add or modify rules files under the `/ai` directory
3. Verify that the rules are properly formatted and work with the target IDE.
4. Proceed to the pull request section to [create a pull request to the main repo](#making-a-pull-request-to-the-main-repo-from-fork). Please explain for what use cases these rules are useful and share what IDE version you're using.

## Coding Prerequisites

To start development in the verified sources repository, there are a few steps you need to do to
ensure you have a working setup.

### 1. Prepare the repository

1. Fork the [verified-sources](https://github.com/dlt-hub/verified-sources) repository on GitHub,
   alternatively check out this repository if you have the right to create Pull Requests directly.
2. Clone the forked repository:
    ```sh
    git clone https://github.com/dlt-hub/verified-sources.git
    ```

3. Make a feature branch in the fork:
    ```sh
    cd verified-sources
    git checkout -b <your-branch-name>
    ```

### 2. Prepare the development environment

Development on the verified sources repository depends on Python 3.9 or higher and `uv` being
available as well as the needed dependencies being installed. Make is used to automate tasks.

1. Install uv: https://docs.astral.sh/uv/getting-started/installation/

2. Install the python dependencies:
    ```sh
    make dev
    ```

3. Activate the virtual environment (Linux example):
    ```sh
    activate .venv/bin/activate
    ```

4. To verify that the dependencies are set up correctly you can run the linter / formatter:
    ```sh
    make format-lint
    ```

If this command fails, something is not set up correctly yet.

## Things to remember before doing PR

### 1. Specify dlt version and additional dependencies (requirements file)

A `requirements.txt` file must be added to the **source folder**
including a versioned dependency on `dlt` itself.
This is to specify which version of dlt the source is developed against, and ensures that users
are notified to update `dlt` if the source depends on new features or backwards incompatible changes.

The `dlt` dependency should be added in `requirements.txt` with a version range and without extras, example:

```
dlt>=0.3.5,<0.4.0
```

If your source requires additional dependencies that are not available in `dlt` they may be added as
follows:

1. Use `uv` to add it to the group with the same name as the source. Example: the chess source uses
   `python-chess` to decode game moves. The dependency was added with
   `uv add --group chess python-chess`.
2. Add the dependency to the `requirements.txt` file in the **source folder**.

### 2. Make sure you use relative imports in your source module

**Use relative imports**. Your code will be imported as source code and everything under **source
folder** must be self-contained and isolated. Example (from `google_sheets`):

```python
from .helpers.data_processing import get_spreadsheet_id
from .helpers.api_calls import api_auth
from .helpers import api_calls
```

### 3. Write example script `<name>_pipeline.py`

This script is distributed by `dlt init` with the other source `<name>` files. It will be a first
touch point with your users. It will be used by them as a starting point or as a source of code
snippets. The ideal content for the script:

1. Shows a few usage examples with different source/resource arguments combinations that you think
   are the most common cases for your user.
2. If you provide any customizations/transformations then show how to use them.
3. Any code snippet that will speed the user up.

Examples:

- [pokemon](sources/pokemon_pipeline.py)
- [chess](sources/chess_pipeline.py)
- [pipedrive](sources/pipedrive_pipeline.py)



### 3. Add secrets and configs to run the local tests

**All source tests and usage/example scripts share the same config and credential files** that are
present in `sources/.dlt`.

This makes running locally much easier and `dlt` configuration is flexible enough to apply to many
sources in one folder.

Please add your credentials/secrets using `sources.<source_name>` ie.
```toml
[sources.github]
access_token="ghp_KZCEQl****"
```

this will become handy when [you'll use our github CI](#github-ci-setting-credentials-and-running-tests) or [run local tests](#testing).

### 4. Make sure that tests are passing


## Making a pull request to the main repo (from fork)

1. Ensure the linter and formatter pass by running:
    ```sh
    make lint-code
    ```
2. Ensure the example script of the source you have added/changed runs:
    ```sh
    python my_source_pipeline.py
    ```
3. Add all files and commit them to your feature branch:
    ```sh
    commit -am "my changes"
    ```
4. Push to your fork.
5. Make a PR to a master branch of this repository (upstream).
   [from your fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).
   You can see the result of our automated testing and linting scripts in the PR on GitHub.
6. If you are connecting to a new datasource, we will need instructions on how to connect there or
   reproduce the test data.
7. Wait for our code review/merge.

### Requirements for your PR to be accepted

1. Your PR must pass the linting and testing stages on the GitHub pull request. For more information
   consult the sections about formatting, linting and testing.
2. Your code must have [Google style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) in all relevant sections.
3. Your code needs to be typed. Consult the section about typing for more information.
4. If you create a new source or make significant changes to an existing source, please add or
   update tests accordingly.
5. The source folder must contain a `requirements.txt` file including `dlt` as a dependency and
   additional dependencies needed to run the source (if any).


## Typing, linting and formatting

`python-dlt` uses `mypy` and `flake8` with several plugins for linting and `black` with default
settings to format the code To lint the code and run the formatter do `make lint-code`. Do this
before you commit so you can be sure that the CI will pass.

**Code needs to be typed** - `mypy` is able to catch a lot of problems in the code. See the `chess`
source for example.

## Testing

We use `pytest` for testing. Every test is running within a set of fixtures that provide the
following environment (see `conftest.py`):

1. They load secrets and config from `sources/.dlt` so the same values are used when you run your
   pipeline from command line and in tests.
2. It sets the working directory for each pipeline to `_storage` folder and makes sure it is empty
   before each test.
3. It drops all datasets from the destination after each test.
4. It runs each test with the original environment variables so you can modify `os.environ`.

Look at `tests/chess/test_chess_source.py` for an example. The line

```python
@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
```

makes sure that each test runs against all destinations (as defined in the `ALL_DESTINATIONS` global
variables).

The simplest possible test just creates a pipeline with your source and then issues a run on a
source. More advanced test will use `sql_client` to check the data and access the schemas to check
the table structure.

Please also look at the [test helpers](tests/utils.py) that you can use to assert the load infos,
get counts of elements in tables, select and assert the data in tables etc.

### Guidelines for writing tests

Your tests will be run both locally and on CI. It means that a few instances of your test may be
executed in parallel, and they will be sharing resources. A few simple rules make that possible.

1. Always use `dev_mode` when creating pipelines in test. This will make sure that data is
   loaded into a new schema/dataset. Fixtures in `conftest.py` will drop datasets created during load.
2. When creating any fixtures for your tests, make sure that fixture is unique for your test
   instance.

> If you create database or schema or table, add random suffix/prefix to it und use in your test.
>
> If you create an account i.e. a user with a name and this name is uniq identifier, also add random
> suffix/prefix.

3. Cleanup after your fixtures - delete accounts, drop schemas and databases.
4. Add code to `tests/utils.py` only if this is helpful for all tests. Put your specific helpers in
   your own directory.

### Mandatory tests for sources

Tests in `tests/test_dlt_init.py` are executed as part of linting stage and must be passing. They
make sure that sources can be distributed with `dlt init`.

### Running tests selectively

When developing, limit the destinations to local i.e. Postgres by setting the environment
variable:
```
ALL_DESTINATIONS='["postgres"]' pytest tests/chess
```

There's also `make test-local` command that will run all the tests on `duckdb` and `postgres`.

## Github CI: setting credentials and running tests
### If you do pull request from your fork to our master branch:
- linter and init checks will run immediately
- we will review your code
- [we will setup secrets/credentials on our side (with your help)](#sharing-and-obtaining-source-credentials-test-accounts-destination-access)
- we will assign a label **ci from fork** that will enable your verified sources tests against duckdb and postgres

Overall following checks must pass:
* mypy and linter
* `dlt init` test where we make sure you provided all information to your verified source module for the distribution to correctly happen
* tests for your source must pass on **postgres** and **duckdb**

### If you do pull requests within your fork: to your forked master branch
If you prefer to run your checks on your own CI, do the following:
1. Go to **settings of your fork** https://github.com/**account name**/**repo name**/settings/secrets/actions
2. Add new Repository Secrets with a name **DLT_SECRETS_TOML**
3. Paste the `toml` fragment with source credentials [that you added to your secrets.toml](#3-add-secrets-and-configs-to-run-the-local-tests) - remember to include section name:

```toml
[sources.github]
access_token="ghp_KZCEQlC8***"
```
In essence **DLT_SECRETS_TOML** is just your `secrets.toml` file and will be used as such by the CI runner.


### Sharing and obtaining source credentials, test accounts, destination access
Typically we create a common test account for your source [before you started coding](#Walkthrough-create-and-contribute-a-new-source). This is an ideal situation - we can reuse your tests directly and can merge your work quickly.

If you contributed a source and created own credentials, test accounts or test datasets please
   include them in the tests or share them with `dlt` team so we can configure the CI job. If
   sharing is not possible please help us to reproduce your test cases so CI job will pass.

### Contributor access to upstream repository
We are happy to add you as contributor to avoid the hurdles of setting up credentials. This also let's you run your tests on BigQuery/Redshift etc. Just *ping the dlt team on slack**.


## Advanced topics

### Ensuring the correct Python version

Use Python 3.9 for development which is the lowest supported version for `dlt`. You'll need
`distutils` and `venv`:

```shell
sudo apt-get install python3.9
sudo apt-get install python3.9-distutils
sudo apt install python3.9-venv
```

`uv` will manage virtual environments for you.

### Destination credentials

Please look at `example.secrets.toml` in `.dlt` folder on how to configure `postgres`, `redshift`
and `bigquery` destination credentials. Those credentials are shared by all sources.

Then you can create your `secrets.toml` with the credentials you need. The `duckdb` and `postgres`
destinations work locally, and we suggest you use them for initial testing.

As explained in technical docs, both native form (i.e. database connection string) or dictionary
representation (a python dict with *host* *database* *password* etc.) can be used.

### Local Postgres instance

There's compose file with fully prepared postgres instance [here](tests/postgres/README.md).
