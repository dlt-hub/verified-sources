# Share a pipeline
This walkthrough describe how to prepare and share your pipeline that you created with `dlt init`. It assumes that your pipeline is already working be it locally or on production. It also assumes that you still use a simple file layout created by [dlt init](https://dlthub.com/docs/walkthroughs/create-a-pipeline)

## 1. Prepare the repository

1. Fork the [pipelines]() repository
2. Clone the forked repository
```sh
git clone git@github.com:rudolfix/pipelines.git
```
3. Make a feature branch in the fork
```sh
cd pipelines
git checkout -b rfix/weatherapi
```

## 2. Prepare your project

The share your pipeline with the community you need to slightly modify your project. `dlt init` may place your pipeline along many others in someone's else project and following changes are required.

We'll use pipeline that you created in our [create a pipeline walkthrough](https://dlthub.com/docs/walkthroughs/create-a-pipeline)

1. Choose a name for your pipeline ie. **weatherapi**
2. Create a folder named `weatherapi`
3. Add file `__init__.py` to that folder and move there your sources and resources. In our case we move the `weatherapi_resource` and `weatherapi_source` functions to the new file. Remember to import `dlt` and other dependencies.
4. Rename your pipeline script `weatherapi.py` to `weatherapi_pipeline.py`. Import your sources and resources from the `weatherapi` folder.
5. Add a module level docstring to your newly created `__init__.py` with the pipeline description. This will be visible to the users when they do `dlt init -l`

Your `__init__.py` file should look more or less like this. Mind the docstring at the top:

```python
"""Loads weather data from weatherapi.com"""
import dlt
from dlt.sources.helpers import requests


@dlt.source
def weatherapi_source(api_secret_key=dlt.secrets.value):
    return weatherapi_resource(api_secret_key)



@dlt.resource(write_disposition="append")
def weatherapi_resource(api_secret_key=dlt.secrets.value):
    url = "https://api.weatherapi.com/v1/current.json"
    params = {
        "q": "NYC",
        "key": api_secret_key
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    yield response.json()

```

Your demo script should look more or less like this.
```python
import dlt
from weatherapi import weatherapi_source


def load_new_york_current_weather():
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='weatherapi', destination='duckdb', dataset_name='weatherapi_data')

    # run the pipeline with your parameters
    load_info = pipeline.run(weatherapi_source())

    # pretty print the information on data that was loaded
    print(load_info)


if __name__=='__main__':
    load_new_york_current_weather()
```
6. Make sure that your `requirements.txt` contain the dependencies that your pipeline needs. Make sure it contains the `dlt` dependency with version on which you test the pipeline (`dlt --version`)

7. Run your pipeline. It should work exactly as before.
```
python weatherapi_pipeline.py
```

> 💡 `weatherapi_pipeline.py` is something that we call **demo script**. If there's something important/interesting/useful about your pipeline that you want to show to other - add it!


## 3. Add your pipeline to pipeline repo.

Shared pipelines are added to `pipelines` folder of the `pipelines` repo.
1. Copy the `weatherapi` folder with all files, the `weatherapi_pipeline.py` and `requirements.txt` file to `pipelines` folder
```sh
cp -R weatherapi requirements.txt weatherapi_pipeline.py ../../forks/pipelines/pipelines/
```
(my clone is in `forks/pipelines/`)

2. Try to verify that your pipeline is correctly added. This is optional. Our CI will also do it but we'll save time if you do.

> 🥇 You'll need `make` and `poetry` to be installed. Head on to the root folder of `pipelines` repo and
> - make install-poetry
> - make dev
> - make lint-dlt-init
>
> the last makes sure that all pipelines

3. Try to run the pipeline in the new location.

## 4. Make a PR

1. Add all files and commit it
```sh
```
2. Push to your fork
3. Go back to [README.md](README.md) on info on creating issue with pipeline announcement and opening a PR to our master
