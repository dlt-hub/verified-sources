# REST API Generic Source
A declarative way to define dlt sources for REST APIs.


## What is this?
> Happy APIs are all alike
>
>    \- E. T. Lev Tolstoy, Senior Data Engineer

This is not a standard dlt source, this is a configurable source which will behave differently depending of the config object passed. Most of the REST APIs that we encounter behave in similar way, the REST API Generic Source attempts to provide a declarative way to define a dlt source for those APIs.

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

And now the `pokemon` one. This is actually a child endpoint of the `pokemon_list`: for each pokemon we want to get further details. So we need to make this resource a bit more smart, the endpoint `path` needs to be explicit, and we have to specify how the value of `name` will be resolved from another resource; this is actually telling the generic source that `pokemon` needs to be queried for each pokemon in `pokemon_list`.

## Anatomy of the config object

> **_TIP:_**  Import `RESTAPIConfig` from the `rest_api` module to have convenient tips.

The config object passed to the REST API Generic Source has three main elements:

```python
my_config: RESTAPIConfig = {
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

#### `auth` [optional]
Use the auth property to pass a token or a `HTTPBasicAuth` object for more complex authentication methods. Here some practical examples:

1. Simple token (read from the `.dlt/secrets.toml` file):
```python
my_api_config: RESTAPIConfig = {
    "client": {
        "base_url": "https://my_api.com/api/v1/",
        "auth": {
            "token": dlt.secrets["sources.my_api.access_token"],
        },
    },
    ...
}
```

2. 
```python
from requests.auth import HTTPBasicAuth

basic_auth = HTTPBasicAuth(dlt.secrets["sources.my_api.access_token"], "")

my_api_config: RESTAPIConfig = {
    "client": {
        "base_url": "https://my_api.com/api/v1/",
        "auth": basic_auth,
    },
    ...
}
```

#### `base_url`
The base URL that will be prepended to the endpoints specified in the `resources` objects. Example

```python
    "base_url": "https://my_api.com/api/v1/",
```

#### `paginator` [optional]
The paginator property specify the default paginator to be used for the endpoint responses.

//TODO describe paginators, the possible strings, and the disctionary to be used 

Possible paginators are:
- BasePaginator - 
- HeaderLinkPaginator - String alias `"header_links"` -
- JSONResponsePaginator -  String alias `"json_links"` - The pagination metainformation are in a node of the JSON response. 

    Usage example for a response with the url of the next page located at `paging.next`:
    ```python
    "paginator": JSONResponsePaginator(
                        next_key=["paging", "next"]
                    )
    ```
- SinglePagePaginator -  String alias `"single_page"` - The response will be interepreted as a single page response, ignoring possible pagination metadata.
- UnspecifiedPaginator - String alias `"auto"` - 


#### `request_client` [optional]
This property allows to passa a custom `requests` client.

### `resource_defaults`
This property allows to pass default properties and behaviour to the dlt resources created by the REST API Generic Source. Beside the properties mentioned in this documentation, a resource accepts all the arguments that usually are passed to a [dlt resource](https://dlthub.com/docs/general-usage/resource).

#### `endpoint`
A string indicating the endpoint or an `endpoint` object (see [below](#endpoint-1)).

#### `include_from_parent` [optional]
A list of fields, from the parent resource, which will be included in the resource output.

#### `name`
Name of the dlt `resource` and the name of the associated table that will be created.

#### `params`
The query parameters for the endpoint url.

For child resource, you can use values from the parent resource for params. The syntax is the following:

```python
    "PARAM_NAME": {
        "type": "resolve",
        "resource": "PARENT_RESOURCE_NAME",
        "field": "PARENT_RESOURCE_FIELD",
    },
```

An example of use:
```python
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
```
#### `parent`
// TODO

#### `path`
The url of the endpoint. If you need to include URL parameters, they can be included using `{}`, for example:
```python
    "path": "pokemon/{name}",
```
In case you need to include query parameters, use the [params](#params) property.


### `resources`
An array of resources. Each resource is a string or a resource object.

Simple resources with their name corresponding to the endpoint can be simple strings. For example:
```python
    "resources": [
        "berry",
        "location",
    ]
```
Resources with the name different from the endpoint string will be:
```python
    "resources": [
        {
            "name": "pokemon_list",
            "endpoint": "pokemon",
        },
    ]
```
In case you need to have a resource with a name different from the table created, you can pass the property `table_name` too.

For the other property see the [resource_defaults](#resource_defaults) above.