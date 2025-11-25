# Rules

## Build the REST API config with cursor-based pagination

## Prerequisities to writing a source

1. VERY IMPORTANT. When writing a new source, you should have an example available in the rest_api_pipeline.py file. 
Use this example or the github rest api source example from dlt's documentation on rest api for the general structure of the code. If you do not see this file rest_api_pipeline.py, ask the user to add it
2. Recall OpenAPI spec. You will figure out the same information that the OpenAPI spec contains for each API.
3. In particular:
- API base url
- type of authentication
- list of endpoints with method GET (you can read data for those)
4. You will figure out additional information that is required for successful data extraction
- type of pagination
- if data from an endpoint can be loaded incrementally
- unwrapping end user data from a response
- write disposition of the endpoint: append, replace, merge
- in case of merge, you need to find primary key that can be compound
5. Some endpoints take data from other endpoints. For example, in the github rest api source example from dlt's documentation, the `comments` endpoint needs `post id` to get the list of comments per particular post. You'll need to figure out such connections
6. **ASK USER IF YOU MISS CRUCIAL INFORMATION** You will make sure the user has provided you with enough information to figure out the above. Below are the most common possibilities
- open api spec (file or link)
- any other api definition, for example Airbyte low code yaml
- a source code in Python, java or c# of such connector or API client
- a documentation of the api or endpoint
7. In case you find more than 10 endpoints and you do not get instructions which you should add to the source, ask user.
8. Make sure you use the right pagination and use exactly the arguments that are available in the pagination guide. do not try to guess anything. remember that we have many paginator types that are configured differently
9. When creating pipeline instance add progress="log" as parameter `pipeline = dlt.pipeline(..., progress="log")`
10. When fixing a bug report focus only on a single cause. ie. incremental, pagination or authentication or wrong dict fields
11. You should have references for paginator types, authenticator types and general reference for rest api in you context. **DO NOT GUESS. DO NOT INVENT CODE. YOU SHOULD HAVE DOCUMENTATION FOR EVERYTHING YOU NEED. IF NOT - ASK USER**


## Look for Required Client Settings
When scanning docs or legacy code, first extract the API-level configuration including:

Base URL:
• The API's base URL (e.g. "https://api.pipedrive.com/").

Authentication:
• The type of authentication used (commonly "api_key" or "bearer").
• The name/key (e.g. "api_token") and its placement (usually in the query).
• Use secrets (e.g. dlt.secrets["api_token"]) to keep credentials secure.

Headers (optional):
• Check if any custom headers are required.

## Authentication Methods
Configure the appropriate authentication method:

API Key Authentication:
```python
"auth": {
    "type": "api_key",
    "name": "api_key",
    "api_key": dlt.secrets["api_key"],
    "location": "query"  # or "header"
}
```

Bearer Token Authentication:
```python
"auth": {
    "type": "bearer",
    "token": dlt.secrets["bearer_token"]
}
```

Basic Authentication:
```python
"auth": {
    "type": "basic",
    "username": dlt.secrets["username"],
    "password": dlt.secrets["password"]
}
```

OAuth2 Authentication:
```python
"auth": {
    "type": "oauth2",
    "token_url": "https://auth.example.com/oauth/token",
    "client_id": dlt.secrets["client_id"],
    "client_secret": dlt.secrets["client_secret"],
    "scopes": ["read", "write"]
}
```

## Find right pagination type
These are the available paginator types to be used in `paginator` field of `endpoint`:

* `json_link`: The link to the next page is in the body (JSON) of the response
* `header_link`: The links to the next page are in the response headers
* `offset`: The pagination is based on an offset parameter, with the total items count either in the response body or explicitly provided
* `page_number`: The pagination is based on a page number parameter, with the total pages count either in the response body or explicitly provided
* `cursor`: The pagination is based on a cursor parameter, with the value of the cursor in the response body (JSON)
* `single_page`: The response will be interpreted as a single-page response, ignoring possible pagination metadata


## Different Paginations per Endpoint are possible
When analyzing the API documentation, carefully check for multiple pagination strategies:

• Different Endpoint Types:
  - Some endpoints might use cursor-based pagination
  - Others might use offset-based pagination
  - Some might use page-based pagination
  - Some might use link-based pagination

• Documentation Analysis:
  - Look for sections describing different pagination methods
  - Check if certain endpoints have special pagination requirements
  - Verify if pagination parameters differ between endpoints
  - Look for examples showing different pagination patterns

• Implementation Strategy:
  - Configure pagination at the endpoint level rather than globally
  - Use the appropriate paginator type for each endpoint
  - Document which endpoints use which pagination strategy
  - Test pagination separately for each endpoint type

## Select the right data from the response
In each endpoint the interesting data (typically an array of objects) may be wrapped
differently. You can unwrap this data by using `data_selector`

Data Selection Patterns:
```python
"endpoint": {
    "data_selector": "data.items.*",  # Basic array selection
    "data_selector": "data.*.items",  # Nested array selection
    "data_selector": "data.{id,name,created_at}",  # Field selection
}
```

## Resource Defaults & Endpoint Details
Ensure that the default settings applied across all resources are clearly delineated:

Defaults:
• Specify the default primary key (e.g., "id").
• Define the write disposition (e.g., "merge").
• Include common endpoint parameters (for example, a default limit value like 50).

Resource-Specific Configurations:
• For each resource, extract the endpoint path, method, and any additional query parameters.
• If incremental loading is supported, include the minimal incremental configuration (using fields like "start_param", "cursor_path", and "initial_value"), but try to keep it within the REST API config portion.

## Incremental Loading Configuration
Configure incremental loading for efficient data extraction. Your task is to get only new data from
the endpoint.

Typically you will identify query parameter that allows to get items that are newer than certain date:

```py
{
    "path": "posts",
    "data_selector": "results",
    "params": {
        "created_since": "{incremental.start_value}",  # Uses cursor value in query parameter
    },
    "incremental": {
        "cursor_path": "created_at",
        "initial_value": "2024-01-25T00:00:00Z",
    },
}
```


## End to end example
Below is an annotated template that illustrates how your output should look. Use it as a reference to guide your extraction:

```python
import dlt
from dlt.sources.rest_api import rest_api_source

# Build the REST API config with cursor-based pagination
source = rest_api_source({
    "client": {
        "base_url": "https://api.pipedrive.com/",  # Extract this from the docs/legacy code
        "auth": {
            "type": "api_key",                    # Use the documented auth type
            "name": "api_token",
            "api_key": dlt.secrets["api_token"],    # Replace with secure token reference
            "location": "query"                     # Typically a query parameter for API keys
        }
    },
    "resource_defaults": {
        "primary_key": "id",                        # Default primary key for resources
        "write_disposition": "merge",               # Default write mode
        "endpoint": {
            "params": {
                "limit": 50                         # Default query parameter for pagination size
            }
        }
    },
    "resources": [
        {
            "name": "deals",                        # Example resource name extracted from code or docs
            "endpoint": {
                "path": "v1/recents",               # Endpoint path to be appended to base_url
                "method": "GET",                    # HTTP method (default is GET)
                "params": {
                    "items": "deal"
                    "since_timestamp": "{incremental.start_value}"
                },
                "data_selector": "data.*",          # JSONPath to extract the actual data
                "paginator": {                      # Endpoint-specific paginator
                    "type": "offset",
                    "offset": 0,
                    "limit": 100
                },
                "incremental": {                    # Optional incremental configuration
                    "cursor_path": "update_time",
                    "initial_value": "2023-01-01 00:00:00"
                }
            }
        }
    ]
})

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive_rest",
        destination="duckdb",
        dataset_name="pipedrive_data"
    )
    pipeline.run(source)
```

## How to Apply This Rule
Extraction:
• Search both the REST API docs and any legacy pipeline code for all mentions of "cursor" or "pagination".
• Identify the exact keys and JSONPath expressions needed for the cursor field.
• Look for authentication requirements and rate limiting information.
• Identify any dependent resources and their relationships.
• Check for multiple pagination strategies across different endpoints.

Configuration Building:
• Assemble the configuration in a dictionary that mirrors the structure in the example.
• Ensure that each section (client, resource defaults, resources) is as declarative as possible.
• Implement proper state management and incremental loading where applicable.
• Configure rate limiting based on API requirements.
• Configure pagination at the endpoint level when multiple strategies exist.

Verification:
• Double-check that the configuration uses the REST API config keys correctly.
• Verify that no extraneous Python code is introduced.
• Test the configuration with mock responses.
• Verify rate limiting and error handling.
• Test pagination separately for each endpoint type.

Customization:
• Allow for adjustments (like modifying the "initial_value") where incremental loading is desired.
• Customize rate limiting parameters based on API requirements.
• Adjust batch sizes and pagination parameters as needed.
• Implement custom error handling and retry logic where necessary.
• Handle different pagination strategies appropriately.

## Guidelines

# Guidelines
1. dlt means "data load tool". It is an open source Python library installable via `pip install dlt`.
2. To create a new pipeline, use `dlt init <source> <destination>`.
3. The dlt library comes with the `dlt` CLI. Add the `--help` flag to any command to verify its specs. 
4. The preferred way to configure dlt (sources, resources, destinations, etc.) is to use `.dlt/config.toml` and `.dlt/secrets.toml`
5. During development, always set `dev_mode=True` when creating a dlt Pipeline. `pipeline = dlt.pipeline(..., dev_mode=True)`. This allows to reset the pipeline's schema and state between iterations.
6. Use type annotations only if you're certain you're properly importing the types.
7. Use dlt's REST API source if loading data from the web.
8. Use dlt's SQL source when loading data from an SQL database or backend.
9. Use dlt's filesystem source if loading data from files (CSV, PDF, Parquet, JSON, and more). This works for local filesystems and cloud buckets (AWS, Azure, GCP, Minio, etc.).

## REST API Parameter Extraction Guide

# REST API Parameter Extraction Guide

This rule helps identify and extract ALL necessary parameters from API documentation to build a dlt REST API source. **Crucially, configuration parameters like pagination and incremental loading can vary significantly between different API endpoints. Do not assume a single global strategy applies to all resources.**

## 1. Base Configuration Parameters (Client Level)

These settings usually apply globally but *can* sometimes be overridden at the resource level.

### Client Settings
Look for these in the API documentation (often in "Overview", "Getting Started", "Authentication"):
- **Base URL**:
  - Aliases: "API endpoint", "root URL", "service URL"
  - Example Format: `https://api.example.com/v1/`
  - Find the main entry point for the API version you need.

- **Authentication**:
  - Keywords: "Authentication", "Authorization", "API Keys", "Security"
  - Common Types & dlt Mappings:
    - API Key: Look for "API Key", "Access Token", "Secret Token", "Subscription Key". Map API key value to `api_key`, name to `name`, location (`query` or `header`) to `location`.
    - Bearer Token: Look for "Bearer Token", "JWT". Map token value to `token`.
    - OAuth2: Look for "OAuth", "Client ID", "Client Secret", "Scopes", "Token URL". Map to `client_id`, `client_secret`, `scopes`, `token_url`.
    - Basic Auth: Look for "Basic Authentication". Map to `username` and `password`.
  - Note where credentials go (header, query parameter, request body).
  - **Secret Handling:**
    - **Pattern 1: Using `@dlt.source` or `@dlt.resource` Decorators (Recommended when applicable):**
      Define your source/resource function with arguments having defaults like `api_key: str = dlt.secrets.value` or `client_secret: str = dlt.secrets["specific_key"]`. `dlt` injects the resolved secret when calling the decorated function. You can then use the argument variable directly.
      ```python
      @dlt.source
      def my_api_source(api_key: str = dlt.secrets.value):
          config = {...
              "auth": {"type": "api_key", "api_key": api_key, ...}
              ...
          }
          yield rest_api_source(config)
      ```
    - **Pattern 2: Calling `rest_api_source` Directly (Requires Explicit Resolution):**
      If calling `rest_api_source` *without* a `@dlt.source/resource` decorator on the calling function, you **must resolve the secret explicitly *before* creating the configuration dictionary**. Using `dlt.secrets.value` directly in the dictionary or as a default function argument *will not work* in this context.
      ```python
      def my_api_source_direct():
          # Resolve secret explicitly first
          actual_key = dlt.secrets["my_api_key"]
          
          config = {
              "client": {
                  "auth": {
                      "type": "api_key",
                      "api_key": actual_key, # Use resolved value
                      ...
                  }
              }
          }
          return rest_api_source(config)
      
      # Pipeline call
      pipeline.run(my_api_source_direct())
      ```

- **Global Headers** (Optional):
  - Keywords: "Headers", "Request Headers", "Required Headers"
  - Common Headers: `Accept: application/json`, `Content-Type: application/json`, `User-Agent`.
  - Look for any custom headers required for *all* requests (e.g., `X-Api-Version`). Resource-specific headers go in the resource config.

## 2. Resource / Endpoint Parameters

**Crucially, examine the documentation for EACH resource/endpoint individually.**

### Endpoint Configuration
For each endpoint/resource (e.g., `/users`, `/orders/{order_id}`), find:
- **Path**:
  - Keywords: "Endpoints", "Resources", "API Methods", "Routes"
  - Format: `/resource`, `/v1/resource`. Note any path parameters like `{id}`.
  - This path is appended to the `base_url`.

- **Method**:
  - Usually explicit: `GET`, `POST`, `PUT`, `DELETE`.
  - Default is `GET` if not specified.

- **Resource-Specific Query Parameters**:
  - Keywords: "Parameters", "Query Parameters", "Optional Parameters", "Filtering", "Sorting"
  - Examples:
    - Filtering: `status=active`, `type=customer`, `created_after=...`
    - Sorting: `sort=created_at`, `order=desc`
    - Fields: `fields=id,name,email` (for selecting specific fields)
  - **Note:** Pagination and incremental parameters are covered separately below, but are often listed here too.

- **Request Body** (for `POST`, `PUT`, `PATCH`):
  - Keywords: "Request Body", "Payload", "Data"
  - Note the expected structure (usually JSON).

### Data Selection (Response Parsing)
- Keywords: "Response", "Response Body", "Example Response", "Schema"
- **Identify the JSON path** to the list/array of actual data items within the response.
- Common patterns & dlt `data_selector`:
  - `{"data": [...]}` -> `data`
  - `{"results": [...]}` -> `results`
  - `{"items": [...]}` -> `items`
  - `{"data": {"records": [...]}}` -> `data.records`
  - Sometimes the root is the list: `[{...}, {...}]` -> `.` or `[*] `(or no selector needed)

## 3. Pagination Parameters (Check Per Endpoint!)

**APIs often use different pagination methods for different endpoints. Check EACH endpoint's documentation for its specific pagination details.**

- **Identify the Strategy**: Look for sections titled "Pagination", "Paging", "Handling Large Responses", or examples showing how to get the next set of results.
- **Common Strategies & dlt Mapping**:
  - **Cursor-based**:
    - Keywords: `cursor`, `next_cursor`, `next_page_token`, `continuation_token`, `after`, `marker`
    - Identify: Where is the *next* cursor value found in the response? (e.g., `pagination.next_cursor`, `meta.next`, `links.next.href`). Map this to `cursor_path`.
    - Identify: What is the *query parameter name* to send the next cursor? (e.g., `cursor`, `page_token`, `after`). Map this to `cursor_param`.
    - Identify: What is the parameter for page size? Map to `limit_param` (set this in `endpoint.params`, not the paginator dict).
    - dlt `type`: `cursor`
  - **Offset-based**:
    - Keywords: `offset`, `skip`, `start`, `startIndex`
    - Identify: Parameter name for the starting index/offset. Map to `offset_param`.
    - Identify: Parameter for page size/limit. Map to `limit_param`.
    - Identify: Optional path to total items count in response (e.g., `summary.total`, `total_count`). Map to `total_path`.
    - dlt `type`: `offset`
  - **Page-based**:
    - Keywords: `page`, `page_number`, `pageNum`
    - Identify: Parameter name for the page number. Map to `page_param`.
    - Identify: Parameter for page size/limit. Map to `limit_param`.
    - Identify: Optional path to total pages or total items in response. Map to `total_path`.
    - dlt `type`: `page`
  - **Link Header-based**:
    - Check response headers for a `Link` header (e.g., `Link: <url>; rel="next"`).
    - dlt `type`: `link_header`, `next_url_path`: `next` (usually)
  - **No Pagination**: Some simple endpoints (e.g., fetching a single item by ID, small config lists) might not be paginated.

- **Configure at Resource Level**: If pagination differs between endpoints, define the `paginator` dictionary within the specific resource's `endpoint` configuration in `dlt`, overriding the client/default level.

## 4. Incremental Loading Parameters (Check Per Endpoint!)

Look for ways to fetch only new or updated data since the last run. This also often varies by endpoint. **The `incremental` configuration dictionary *always* requires the `cursor_path` field to be defined, even if `start_param` is also used.**

- **Identify Strategy & Parameters**:
  - **Timestamp-based**:
    - Keywords: `since`, `updated_since`, `modified_since`, `start_time`, `from_date`
    - Identify: The query parameter name used to filter by time (Optional). Map to `start_param`.
    - Identify: The field *in the response items* that contains the relevant timestamp (e.g., `updated_at`, `modified_ts`, `last_activity_date`). **Map this to `cursor_path` (Required)**. `dlt` uses this path to find the value for the next incremental run's state.
    - Note the required date format.
  - **ID-based / Event-based**:
    - Keywords: `since_id`, `min_id`, `last_event_id`, `sequence_number`, `offset` (if used like a cursor)
    - Identify: The query parameter name used to filter by ID/sequence (Optional). Map to `start_param`.
    - Identify: The field *in the response items* containing the ID/sequence. **Map this to `cursor_path` (Required)**.
  - **Cursor-based (using pagination cursor)**:
    - Sometimes the pagination cursor itself can be used for incremental loading if it's persistent and ordered (less common, often needs verification).
    - Map the response cursor path to `cursor_path` (**Required**) and the query parameter to `start_param` (Optional).

- **Initial Value**: Determine a safe starting point (e.g., a specific date `"2023-01-01T00:00:00Z"`, `0` for IDs). Map to `initial_value`.
- **Optional End Param**: If the API supports filtering up to a certain point (e.g., `end_date`, `max_id`), identify the parameter name (map to `end_param`) and potentially a value (map to `end_value`).
- **Optional Conversion**: If the `cursor_path` value needs transformation before being used in `start_param` or `end_param`, define a function and map it to `convert`.
- **Configure at Resource Level**: Define the `incremental` dictionary within the specific resource's `endpoint` configuration if the strategy or fields differ from others.

## 5. Common Documentation Patterns & Examples

(Keep existing examples, they are helpful)

### Authentication Section
```markdown
## Authentication
To authenticate, include your API key in the `Authorization: Bearer <your_token>` header.
```

### Endpoint Documentation Example (with variations)
```markdown
## List Orders
GET /v2/orders

Fetches a list of orders. This endpoint uses offset-based pagination.

Query Parameters:
- limit (integer, optional, default: 50): Max items per page.
- offset (integer, optional, default: 0): Number of items to skip.
- status (string, optional): Filter by status (e.g., 'completed', 'pending').
```

```markdown
## Get Activity Stream
GET /activities/stream

Fetches recent activities. Uses cursor-based pagination.

Query Parameters:
- page_size (integer, optional, default: 100): Number of activities.
- next_page_cursor (string, optional): Cursor from the previous response's `meta.next_page` field.

Response:
{
  "activities": [...],
  "meta": {
    "next_page": "aabbccddeeff"
  }
}
```

## 6. Enhanced Parameter Mapping (API Terminology -> dlt Config)

Map diverse API documentation terms to consistent `dlt` parameters. Identify the API's term first, then find the corresponding `dlt` key.

```yaml
client:
  base_url:
    common_api_terms: ["Base URL", "API Endpoint", "Root URL", "Service URL"]
    dlt_parameter: "client.base_url"
    notes: "Include version path (e.g., /v1/)"
  
  auth:
    api_key_value:
      common_api_terms: ["API Key", "Access Token", "Secret", "Token", "Key"]
      dlt_parameter: "client.auth.api_key"
      notes: "Handled via Secret Handling patterns"
    
    api_key_param_name:
      common_api_terms: ["api_key", "token", "key", "access_token"]
      dlt_parameter: "client.auth.name"
      notes: "Query param name or Header name"
    
    api_key_location:
      common_api_terms: ["Query parameter", "Header"]
      dlt_parameter: "client.auth.location"
      notes: "query or header"
    
    bearer_token:
      common_api_terms: ["Bearer Token", "JWT"]
      dlt_parameter: "client.auth.token"
      notes: "Handled via Secret Handling patterns"

pagination:
  note: "Define per-resource if strategies differ!"
  
  next_cursor_source:
    common_api_terms: ["next_cursor", "next_page", "nextToken", "marker"]
    dlt_parameter: "paginator.cursor_path"
    notes: "JSON path in response"
  
  next_cursor_param:
    common_api_terms: ["cursor", "page_token", "after", "next", "marker"]
    dlt_parameter: "paginator.cursor_param"
    notes: "Query param name to send cursor"
  
  offset_param:
    common_api_terms: ["offset", "skip", "start", "startIndex"]
    dlt_parameter: "paginator.offset_param"
    notes: "Query param name"
  
  page_number_param:
    common_api_terms: ["page", "page_number", "pageNum"]
    dlt_parameter: "paginator.page_param"
    notes: "Query param name"
  
  page_size_param:
    common_api_terms: ["limit", "per_page", "page_size", "count", "maxItems"]
    dlt_parameter: "paginator.limit_param"
    notes: "Query param name"
  
  total_items_source:
    common_api_terms: ["total", "total_count", "total_results", "count"]
    dlt_parameter: "paginator.total_path"
    notes: "Optional JSON path in response"
  
  link_header_relation:
    common_api_terms: ["next", "last"]
    dlt_parameter: "paginator.next_url_path"
    notes: "rel value in Link header"

incremental:
  note: "Define per-resource if strategies differ!"
  
  timestamp_param:
    common_api_terms: ["since", "updated_since", "modified_since", "from"]
    dlt_parameter: "incremental.start_param"
    notes: "Query param name"
  
  timestamp_source:
    common_api_terms: ["updated_at", "modified", "last_updated", "ts"]
    dlt_parameter: "incremental.cursor_path"
    notes: "JSON path in response item"
  
  id_sequence_param:
    common_api_terms: ["since_id", "min_id", "after_id", "sequence"]
    dlt_parameter: "incremental.start_param"
    notes: "Query param name"
  
  id_sequence_source:
    common_api_terms: ["id", "event_id", "sequence_id", "_id"]
    dlt_parameter: "incremental.cursor_path"
    notes: "JSON path in response item"
  
  initial_value:
    common_api_terms: ["N/A"]
    dlt_parameter: "incremental.initial_value"
    notes: "Start value for first run"

data:
  data_array_path:
    common_api_terms: ["data", "results", "items", "records", "entries"]
    dlt_parameter: "endpoint.data_selector"
    notes: "JSON path to the list of items"
```

## 7. Verification Checklist

Before finalizing the configuration:
1.  Verify Base URL format and version.
2.  Confirm Authentication method and *all* required parameters/headers.
3.  Verify Secret Handling pattern matches how the source is called.
4.  **For EACH resource:** Identify its specific pagination strategy (cursor, offset, page, link, none).
5.  **For EACH resource:** Extract the correct pagination parameters (`cursor_path`, `cursor_param`, `offset_param`, `page_param`, `limit_param` etc.) based on its strategy.
6.  **For EACH resource:** Determine if incremental loading is possible and identify its strategy (timestamp, ID, etc.).
7.  **For EACH resource:** Extract the correct incremental parameters (`cursor_path`, `initial_value`, `start_param`, etc.) based on its strategy.
8.  Validate the `data_selector` path for each resource by checking example responses.
9.  Check for any required Global Headers AND Resource-Specific Headers.

## dlt REST API Pagination Configuration Guide

# dlt REST API Pagination Configuration Guide
Use this rule when writing REST API Source to configure right pagination type for an Endpoint

This rule explains how to configure different pagination strategies for the `dlt` `rest_api` source. Understanding the API's specific pagination method is crucial for correct configuration.

If you are unsure what type of pagination to use due to lack of information from the api, consider curl-ing for responses (you can probably find credentials in secrets if needed)

We will use class based paginators and not declartive so if you search online in dlthub docs, make sure you do the right type

**Key Principle: Endpoint-Specific Pagination**

While you can set a default paginator at the `client` level, many APIs use *different* pagination methods for different endpoints. Always check the documentation for *each specific endpoint* you intend to load.

If an endpoint uses a different pagination method than the default, define its `paginator` configuration within that specific resource's `endpoint` section to override the client-level setting.

## DLT RESTClient Paginators Guide

To specify the pagination configuration, use the `paginator` field in the `client` or `endpoint` configurations. You should use a dictionary with a string alias in the `type` field along with the required parameters

#### Example

Suppose the API response for `https://api.example.com/posts` contains a `next` field with the URL to the next page:

```json
{
    "data": [
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"}
    ],
    "pagination": {
        "next": "https://api.example.com/posts?page=2"
    }
}
```

You can configure the pagination for the `posts` resource like this:

```py
{
    "path": "posts",
    "paginator": {
        "type": "json_link",
        "next_url_path": "pagination.next",
    }
}
```

Currently, pagination is supported only for GET requests. To handle POST requests with pagination, you need to implement a custom paginator

These are the available paginator types to be used in `paginator` field:

* `json_link`: The link to the next page is in the body (JSON) of the response
* `header_link`: The links to the next page are in the response headers
* `offset`: The pagination is based on an offset parameter, with the total items count either in the response body or explicitly provided
* `page_number`: The pagination is based on a page number parameter, with the total pages count either in the response body or explicitly provided
* `cursor`: The pagination is based on a cursor parameter, with the value of the cursor in the response body (JSON)
* `single_page`: The response will be interpreted as a single-page response, ignoring possible pagination metadata


### Paginator arguments

#### json_link
Description: Paginator for APIs where the next page’s URL is included in the response JSON body (e.g. in a field like "next" or within a "pagination" object)​
dlthub.com

Parameters:
`next_url_path` (str, optional): JSONPath to the key in the response JSON that contains the next page URL​

When to Use
Use `json_link` when the API’s JSON response includes a direct link (URL) to the next page of results​

A common pattern is a field such as "next" or a nested key that holds the full URL for the next page. For example, an API might return a JSON structure like:
```json
{
  "data": [ ... ],
  "pagination": {
    "next": "https://api.example.com/posts?page=2"
  }
}
```

In the above, the "pagination.next" field provides the URL for the next page​

By specifying next_url_path="pagination.next", the `json_link` will extract that URL and request the next page automatically. This paginator is appropriate whenever the response body itself contains the next page URL, often indicated by keys like "next", "next_url", or a pagination object with a next link.

#### header_link

Description: Paginator for APIs where the next page’s URL is provided in an HTTP header (commonly the Link header with rel="next")​

Parameters
`links_next_key` (str, optional): The relation key in the Link response header that identifies the next page’s URL​. Default is "next". Example: If the header is Link: <https://api.example.com/items?page=2>; rel="next", the default links_next_key="next" will capture the URL for the next page.

When to Use
Use `header_link` when the API provides pagination links via HTTP headers rather than in the JSON body. This is common in APIs (like GitHub’s) that return a Link header containing URLs for next/prev pages. For example, an HTTP response might include:
```
Link: <https://api.example.com/items?page=2>; rel="next"
Link: <https://api.example.com/items?page=5>; rel="last"
```
In such cases, the `header_link` will parse the Link header, find the URL tagged with rel="next", and follow it​

You should use this paginator if the API documentation or responses indicate that pagination is controlled by header links. (Typically, look for a header named “Link” or similar, with URIs and relation types.) Note: By default, links_next_key="next" works for standard cases. If an API uses a different relation name in the Link header, you can specify that (e.g. HeaderLinkPaginator(links_next_key="pagination-next")).


#### offset
Description: Paginator for APIs that use numeric offset/limit parameters in query strings to paginate results​.
Each request fetches a set number of items (limit), and subsequent requests use an increasing offset.

Parameters
`limit` (int, required): The maximum number of items to retrieve per request (page size)​.
`offset` (int, optional): The starting offset for the first request​. Defaults to 0 (beginning of dataset).
`offset_param` (str, optional): Query parameter name for the offset value​. Default is "offset".
`limit_param` (str, optional): Query parameter name for the page size limit​. Default is "limit".
`total_path` (str or None, optional): JSONPath to the total number of items in the response. If provided, it helps determine when to stop pagination based on total count​. By default this is "total", assuming the response JSON has a field "total" for total item count. Use None if the API doesn’t return a total.
`maximum_offset` (int, optional): A cap on the maximum offset to reach​. If set, pagination stops when offset >= maximum_offset + limit.
`stop_after_empty_page` (bool, optional): Whether to stop when an empty page (no results) is encountered​. Defaults to True. If True, the paginator will halt as soon as a request returns zero items (useful for APIs that don’t provide a total count).
​
. To illustrate, if the first response looks like:
```json
{
  "items": [ ... ],
  "total": 1000
}
```
the paginator knows there are 1000 total items​
 and will continue until the offset reaches 1000 (or the final partial page). If the API does not provide a total count, OffsetPaginator will rely on getting an empty result page to stop by default​. You can also set maximum_offset to limit the number of items fetched (e.g., for testing, or if the API has an implicit max).

When to Use
Use `offset` for APIs that use offset-based pagination. Indicators include endpoint documentation or query parameters like offset (or skip/start) and limit(orpage_size`), and often a field in the response that gives the total count of items. For example, an API endpoint might be called as:
```
GET https://api.example.com/items?offset=0&limit=100
```
and return data with a structure like:
```json
{
  "items": [ ... ],
  "total": 1000
}
```
Here, the presence of offset/limit parameters and a "total" count in the JSON indicates offset-based pagination​. Choose `offset` when you see this pattern. This paginator will automatically increase the offset by the given limit each time, until it either reaches the total count (if known) or encounters an empty result set (if stop_after_empty_page=True). If the API lacks a total count and can continuously scroll, ensure you provide a stopping condition (like maximum_offset) or rely on an empty page to avoid infinite pagination.

#### page_number
Description: Paginator for APIs that use page number indexing in their queries (e.g. page=1, page=2, ... in the URL)​. It increments the page number on each request.

Parameters
`base_page` (int, optional): The starting page index as expected by the API​. This defines what number represents the first page (commonly 0 or 1). Default is 0.
`page` (int, optional): The page number for the first request. If not provided, it defaults to the value of base_page​. (Typically you either use base_page to set the start, or directly give an initial page number.)
`page_param` (str, optional): The query parameter name used for the page number​. Default is "page".
`total_path` (str or None, optional): JSONPath to the total number of pages (or total items) in the response​. If the API provides a total page count or total item count, you can specify its JSON field (e.g. "total_pages"). Defaults to "total" (common key for total count)​. If set to None or not present, the paginator will rely on other stopping criteria.
`maximum_page` (int, optional): The maximum page number to request​. If provided, pagination will stop once this page is reached (useful to limit page count during testing or to avoid excessive requests).
`stop_after_empty_page` (bool, optional): Whether to stop when an empty page is encountered (no results)​. Default is True. If False, you should ensure there is another stop condition (like total_path or maximum_page) to prevent infinite loops.

For example, if a response is:
```json
{
  "items": [ ... ],
  "total_pages": 10
}
```
the paginator knows there are 10 pages in total​ and will not go beyond that. If the API does not provide a total count of pages, PageNumberPaginator will paginate until an empty result page is returned by default​. You can also manually limit pages by maximum_page if needed (e.g., stop after page 5). Setting stop_after_empty_page=False can force it to continue even through empty pages, but then you must have a total_path or maximum_page to avoid infinite loops​


When to Use
Use `page_number` for APIs that indicate pagination through a page number parameter. Clues include endpoints documented like /resource?page=1, /resource?page=2, etc., or the presence of terms like "page" or "page_number" in the API docs. Often, the response will include something like a "total_pages" field or a "page" field in the payload to help manage pagination. For example:
```
GET https://api.example.com/items?page=1
```
Response:
```json
{
  "items": [ ... ],
  "total_pages": 10,
  "page": 1
}
```

In this scenario, the presence of "page" in the request and a total count of pages in the response suggests using a page-number-based paginator​. Choose `page_number` when the API paginates by page index. It will increment the page number on each call. Be mindful of whether the first page is indexed as 0 or 1 in that API (set base_page accordingly). If a total page count is given (e.g., "total_pages" or "last_page"), pass the appropriate JSON path via total_path so the paginator knows when to stop. If no total count is given, the paginator will stop when no more data is returned (or when you hit a maximum_page if you set one).

`cursor`
Description: Paginator for APIs that use a cursor or token in the JSON response to indicate the next page. The next cursor value is extracted from the response body and passed as a query parameter in the subsequent request​

Parameters
`cursor_path` (str, optional): JSONPath to the cursor/token in the response JSON​. Defaults to "cursors.next", which corresponds to a common pattern where the JSON has a "cursors" object with a "next" field.
`cursor_param` (str, optional): The name of the query parameter to send the cursor in for the next request​. Defaults to "after". This is the parameter that the API expects on the URL (or body) to fetch the next page (for example, many APIs use ?after=<token> or ?cursor=<token> in the query string).

When to Use
Use `cursor` when the API provides a continuation token or cursor in the JSON response rather than a direct URL. This is common in APIs where responses include a field like "next_cursor", "next_page_token", or a nested structure for cursors. For example, a response might look like:
```json
{
  "records": [ ... ],
  "cursors": {
    "next": "cursor_string_for_next_page"
  }
}
```

In this case, the value "cursor_string_for_next_page" is a token that the client must send in the next request to get the following page of results. The documentation might say something like “use the next cursor from the response for the next page, via a cursor query parameter.” Indicators for this paginator:
The presence of a field in the JSON that looks like a cryptic token (often base64 or long string) for pagination.
API docs using terminology like “cursor”, “continuation token”, “next token”, or showing request examples with parameters such as after, nextToken, cursor, etc.
Choose JSONResponseCursorPaginator if the API’s pagination is driven by such tokens in the response body. You will configure cursor_path to point at the JSON field containing the token (e.g. "cursors.next" as default, or "next_cursor", etc.), and cursor_param to the name of the query parameter the API expects (commonly "cursor" or "after"). The paginator will then automatically extract the token and append it as ?cursor=<token> (or your specified param name) on subsequent calls​

