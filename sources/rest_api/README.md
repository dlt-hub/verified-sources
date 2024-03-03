# REST API Source Generator
A declarative way to define dlt sources for REST APIs.


## What is this?
> Happy APIs are all alike
>
>    \- E. T. Lev Tolstoy, Senior Data Engineer

This is not a standard dlt source, this is a source generator. Most of the REST APIs that we encounter behave in similar way, the REST API Source Generator attempts to provide a declarative way to define a dlt source for those APIs.

## How to use it
Probably the easier way is to show you how a source for the Pokemon APIs would look like:

```python
pokemon_config = {
    "client": {
        "base_url": "https://pokeapi.co/api/v2/",
    },
    "resources": [
        "berry",
        "location",
        {
            "name": "pokemon_list",
            "endpoint": "pokemon",
        },
        {
            "name": "pokemon",
            "endpoint": {
                "path": "pokemon/{name}",
                "params": {
                    "name": {
                        "type": "resolve",
                        "resource": "pokemon_list",
                        "field": "name",
                    },
                },
            },
        },
    ],
}

pokemon_source = rest_api_source(pokemon_config)
```
Here a short summary:
- The `client` node contains the base URL of the endpoints that we want to collect.
- The `resources` which correspond to the API endpoints.

We have a couple of simple resources (`berry` and `location`). The API endpoint is also the name of the dlt resource, and the name of the destination table. They don't need additional configuration.

The next resource leverages some additional configuration. The endpoint `pokemon/` returns a list of pokemons, but it can be used also as `pokemon/{id or name}` to return a single pokemon. In this case we want the list, so we decided to rename the resource to `pokemon_list`, while the endpoint stays `pokemon/`. We do not specify the name of the destination table, so it will match the resource name.

And now the `pokemon` one. This is actually a child endpoint of the `pokemon_list`: for each pokemon we want to get further details. So we need to make this resource a bit more smart, the endpoint `path` needs to be explicit, and we have to specify how the value of `name` will be resolved from another resource; this is actually telling the generator that `pokemon` needs to be queries for each pokemon in `pokemon_list`.

## Anatomy of the config object

The config object passed to the REST API Source Generator has three main elements:

```python
{
    "client": {
        ...
    },
    "resource_defaults": {
        ...
    },
    "resources": {
        ...
    ,
}
```

`client` contains the configuration to connect to the APIs endpoints (e.g. base URL, authentication method, default behaviour for the paginator, and more).

`resource_defaults` contains the default values to configure the dlt resources returned by this source.

`resources` object contains the configuration for each resource. 

The configuration with smallers scope will overwrite the one with the wider one:

    Resource Configuration > Resource Defaults Configuration > Client Configuration

## Reference

### `client`

### `resource_defaults`

### `resources`