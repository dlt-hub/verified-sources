# Jira

[Jira](https://www.atlassian.com/software/jira) by Atlassian helps teams manage projects and tasks
efficiently, prioritize work, and collaborate.

Resources that can be loaded using this verified source are:

| Name      | Description                                                                              |
| --------- | ---------------------------------------------------------------------------------------- |
| issues    | individual pieces of work to be completed                                                |
| users     | all users on the site, including inactive and deleted accounts                           |
| workflows | the key aspect of managing and tracking the progress of issues or tasks within a project |
| workflow_statuses | names and categories of the statuses the workflows reference                     |
| projects  | a collection of tasks that need to be completed to achieve a certain outcome             |

The `workflows` resource reads `rest/api/3/workflows/search`. It returns team-managed workflows as
well, and its table has different columns than the deprecated endpoint produced.

The `workflows__statuses` child table holds a `status_reference` per status and no name. Join it to
`workflow_statuses.status_reference` for the name, description and category. That resource reads the
same response as `workflows` under a sibling key, so it needs no extra permission. `rest/api/3/status`
omits unused statuses and `rest/api/3/statuses/search` omits project scoped ones, so neither resolves
every reference.

### Upgrading an existing dataset

The `workflows` table changed shape. Its `id` was an object and arrived as `id__name` and
`id__entity_id`; it is now a text `id` and the declared primary key. `has_draft_workflow`,
`is_default`, `projects` and `schemes` are gone, and `name`, `is_editable`, `scope` and `version`
are new. Drop the `workflows` table before the first run on this version, or load into a fresh
dataset. The other resources keep their columns.

## Initialize the pipeline

```bash
dlt init jira duckdb
```

Here, we chose `duckdb` as the destination. Alternatively, you can also choose `redshift`,
`bigquery`, or any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Setup verified source

To grab Jira credentials and configure the verified source, please refer to the
[full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/jira#setup-guide)

## Add credential

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can
   securely store your access tokens and other sensitive information. It's important to handle this
   file with care and keep it safe. Here's what the file looks like:

   ```toml
   # put your secret values and credentials here. Please do not share this file, and do not push it to GitHub
   [sources.jira]
   subdomain = "set me up!" # please set me up!
   email = "set me up!" # please set me up!
   api_token = "set me up!" # please set me up!
   ```

1. A subdomain in a URL identifies your Jira account. For example, in
   "https://example.atlassian.net", "example" is the subdomain.

1. Use the email address associated with your Jira account.

1. Set up the "api_token" to ensure secure access to your Jira account.

1. Next, follow the instructions in [Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/) to add credentials for
   your chosen destination. This will ensure that your data is properly routed to its final
   destination.

## Run the pipeline

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

1. Now the pipeline can be run by using the command. The example loads two pages per resource and
   two pages per JQL query; remove the `limit` arguments in `jira_pipeline.py` to load
   everything.

   ```bash
   python jira_pipeline.py
   ```

   Each resource requests the largest page its endpoint serves. Pass `page_size` to `jira` or
   `jira_search` to request a smaller one.

1. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `jira_pipeline`, you may also
   use any custom name instead.

### Writing JQL queries

`jira_search` takes a list of JQL queries and loads the issues each one matches. An issue matched
by two queries is loaded once.

`rest/api/3/search/jql` rejects a query that only sorts: `order by created DESC` answers
`400 Unbounded JQL queries are not allowed here. Please add a search restriction to your query.`
Add a filtering clause, as the shipped example does with `created >= -30d`.

`page_limit` bounds each query on its own. `add_limit` on the resource bounds them together, and
the first query absorbs the whole budget.

💡 To explore additional customizations for this pipeline, we recommend referring to the official
`dlt` [Jira](https://dlthub.com/docs/dlt-ecosystem/verified-sources/jira) documentation. It provides
comprehensive information and guidance on how to further customize and tailor the pipeline to suit
your specific needs.
