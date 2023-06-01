# Contributing
The following guide will walk you through contributing new pipelines or changes to existing pipelines and contains a troubleshooting section. Please also read [DISTRIBUTION.md](DISTRIBUTION.md) to understand how our pipelines are distributed to the users.

## Walktrough: Prerequesites

To start development in the pipelines repository, there are a few steps you need to do to ensure you have the setup. 


### 1. Prepare the repository

1. Fork the [pipelines]() repository on github, alternative check out this repository if you have the right to create Pull Requests directly.
2. Clone the forked repository
```sh
git clone git@github.com:rudolfix/pipelines.git
```
3. Make a feature branch in the fork
```sh
cd pipelines
git checkout -b <your-branch-name>
```

### 2. Prepare the development environment
Development on the pipelines repository depends on python 3.8 or higher and poetry being available as well as the needed dependencies being installed. Make is used to automate tasks

1. Install poetry: 
```sh
make install-poetry
```
2. Install the python dependencies:
```sh
make dev
```
3. Activate the poetry shell:
```sh
poetry shell
```
4. To verify that the dependencies are set up correctly you can run the linter / formatter:
```sh
make format-lint
```
If this command fails, something is not set up correctly yet.


## Walktrough: Create and contribute a new pipeline
In this section you will learn how to contribute a new pipeline based on the `chess` pipeline. The `chess` pipeline is a very basic pipeline with two sources and a few resources. For an even simpler starting point, you can use the `pokemon` pipeline as the starting point with the same method. Please also read [DISTRIBUTION.md](DISTRIBUTION.md) before you start this guide to get an understand of how your pipeline will be distributed to other users once it is accepted into our repo.

1. Ensure you have followed all steps in the prerequesites section.
2. We will copy the chess pipeline as a starting point. You can copy any other pipeline you wish to serve as your starting point. Another fairly simple pipeline is the chess pipeline for example. We will assume your new pipeline is called `animals`.
	1. Duplicate the `chess` folder in` ./pipelines` and rename it to `animals`. This folder will contain all your sources and resources, as well as some settings and helpers code (if any).
	2. Duplicate the `chess_pipeline.py` in `./pipelines` and rename it to `chess_pipeline.py`. This folder contains the example script that users will see when they install your pipeline with `dlt init`. Inside your `chess_pipeline.py` change `from chess import source` to `from animals import source` to make sure the correct source is used in your animals example script.
	3. Duplicate the `chess` folder in the `./tests` and rename it to `animals`. Inside the test_pipeline file again make sure that the right source is imported. These are the tests for your pipeline that you can run to make sure everything works. Run the tests now with
```sh
ALL_DESTINATIONS='["duckdb"]' pytest tests/animals
```
to see if they work. They should :).
	4. You are now set to start development on your animals pipeline.
3. You can now implement your custom pipeline. Consult our extensive docs on how to create dlt pipelines at [dlthub docs](https://dlthub.com/docs/intro). The typical development workflow will look something like this:
	1. Make changes to your pipeline code, in our example you could add resources like `fish` or `predators`.
	2. Execute the pipeline example script `python animals_pipeline.py` and see if there are any errors and wether the expected data ends up in your destination.
	3. Adjust your tests to test for the new features you have added in `./tests/animals/test_pipeline.py` and run the tests again duckdb locally with this command: 
```sh
ALL_DESTINATIONS='["duckdb"]' pytest tests/animals
```
	4. Run the linter and formatter to check for any problems: 
```sh
make lint-code
```
4. Read the rest of this document for information on various topics.
5. Proceed to the pull request section to create a pull request to the main repo.


## Walktrough: Contribute changes walkthrough
In this section you will learn how to contribute changes to an existing pipeline. It is helpful to also read through the above section to see all the very basic things that are part of a pipeline.

1. Ensure you have followed all steps in the prerequesites section and the format-lint command works.
2. Make the changes you want to do in the pipeline. 
3. Proceed to the pull request section to create a pull request to the main repo.


## Making a pull request to the main repo

1. Ensure the linter and formatter pass by running 
```sh
make lint-code
```
2. Ensure the example script of the pipeline you have added/changed runs
```
python my_pipeline.py
```
3. Add all files and commit them to your feature branch
```sh
commit -am "my changes"
```
4. Push to your fork
5. Make a PR to a master branch of this repository (upstream) [from your fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork). You can see the result of our automated testing and linting scripts in the PR on Github.
6. Wait for our code review/merge.



## Requirements for your PR to be accepted
1. Your PR must pass the linting and testing stages on the github pull request. For more information consult the sections about formatting, linting and testing.
2. Your code must have google style docstrings in all relevant sections.
3. Your code needs to be typed. Consult the section about typing for more information.
4. If you create a new pipeline or make significant changes to an existing pipeline, please add or update tests accordingly.


## Pipeline specific dependencies
If your pipeline requires additional dependencies that are not available in `dlt` they may be added as follows:

1. Use `poetry` to add it to the group with the same name as pipeline. Example: chess pipeline uses `python-chess` to decode game moves. The dependency was added with `poetry add -G chess python-chess`
2. Add a `requirements.txt` file in **pipeline folder** and add the dependency there.

## Secrets and settings
**All pipeline tests and usage/example scripts share the same config and credential files** that are present in `pipelines/.dlt`.

This makes running locally much easier and `dlt` configuration is flexible enough to apply to many pipelines in one folder.

## The example script  `<name>_pipeline.py`
This script is distributed by `dlt init` with the other pipeline `<name>` files. It will be a first touch point with your users. It will be used by them as a starting point or as a source of code snippets. The ideal content for the script:

1. Shows a few usage examples with different source/resource arguments combinations that you think are the most common cases for your user.
2. If you provide any customizations/transformations then show how to use them.
3. Any code snippet that will speed the user up.

Examples:

* [chess](pipelines/chess_pipeline.py)
* [pipedrive](pipelines/pipedrive_pipeline.py)

## Typing, linting and formatting

`python-dlt` uses `mypy` and `flake8` with several plugins for linting and `black` with default settings to format the code To lint the code and run the formatter do `make lint-code`. Do this before you commit so you can be sure that the CI will pass.

**Code needs to be typed** - `mypy` is able to catch a lot of problems in the code. If your pipeline is typed file named `py.typed` to the folder where your pipeline code is. (see `chess` pipeline for example)

## Testing

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

### Guidelines for writing tests
Your tests will be run both locally and on CI. It means that a few instances of your test may be executed in parallel and they will be sharing resources. A few simple rules make that possible.

1. Always use `full_refresh` when creating pipelines in test. This will make sure that data is loaded into new schema/dataset. Fixtures in `conftest.py` will drop datasets created during load.
2. When creating any fixtures for your tests, make sure that fixture is unique for your test instance.
> If you create database or schema or table, add random suffix/prefix to it und use in your test
>
> If you create an account ie. an user with a name and this name is uniq identifier, also add random suffix/prefix
3. Cleanup after your fixtures - delete accounts, drop schemas and databases
4. Add code to `tests/utils.py` only if this is helpful for all tests. Put your specific helpers in your own directory.


### Mandatory tests for pipelines
Tests in `tests/test_dlt_init.py` are executed as part of linting stage and must be passing. They make sure that pipeline can be distributed with `dlt init`.

### Running tests selectively
1. When developing, limit the destinations to local ie. duckdb by setting the environment variable:
```
ALL_DESTINATIONS='["duckdb"]' pytest tests/chess
```

there's also ` make test-local` command that will run all the tests on `duckdb` and `postgres`


## Advanced topics

### Ensuring the correct python version
Use python 3.8 for development which is the lowest supported version for `python-dlt`. You'll need `distutils` and `venv`:

```shell
sudo apt-get install python3.8
sudo apt-get install python3.8-distutils
sudo apt install python3.8-venv
```
You may also use `pyenv` as [poetry](https://python-poetry.org/docs/managing-environments/) suggests.

### Python module import structure
**Use relative imports**. Your code will be imported as source code and everything under **pipeline folder** must be self-contained and isolated. Example (from `google_sheets`)
```python
from .helpers.data_processing import get_spreadsheet_id
from .helpers.api_calls import api_auth
from .helpers import api_calls
```

### Sharing and obtaining source credentials, test accounts, destination access

1. If you are contributing and want to test against `redshift` and `bigquery`, **ping the dlt team on slack**. You'll get a `toml` file fragment with the credentials that you can paste into your `secrets.toml`
2. If you contributed a pipeline and created any credentials, test accounts, test dataset please include them in the tests or share them with `dlt` team so we can configure the CI job. If sharing is not possible please help us to reproduce your test cases so CI job will pass.

### Source config and credentials
If you add a new pipeline that require a secret value, please add a placeholder to `example.secrets.toml`. When adding the source config and secrets please follow the [section layout for sources](https://github.com/dlt-hub/dlt/blob/devel/docs/technical/secrets_and_config.md#default-layout-and-default-key-lookup-during-injection). We have a lot of pipelines so we must use precise section layout (up to module level):

`[sources.<python module name where source and resources are placed>]`

This way we can isolate credentials for each pipeline.

### Destination credentials
Please look at `example.secrets.toml` in `.dlt` folder on how to configure `postgres`, `redshift` and `bigquery` destination credentials. Those credentials are shared by all pipelines.

Then you can create your `secrets.toml` with the credentials you need. The `duckdb` and `postgres` destinations work locally and we suggest you use them for initial testing.

As explained in technical docs, both native form (ie. database connection string) or dictionary representation (a python dict with *host* *database* *password* etc.) can be used.

### Local Postgres instance
There's compose file with fully prepared postgres instance [here](tests/postgres/README.md)



## Troubleshooting




