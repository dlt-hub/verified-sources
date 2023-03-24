import typing as t
from datetime import datetime

import dlt
from asana import Client as AsanaClient


DEFAULT_START_DATE = "2010-01-01T00:00:00.000Z"


def get_client(
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    refresh_token: str,
) -> AsanaClient:
    """Returns an Asana client with a valid access token"""
    asana = AsanaClient.oauth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )
    asana.session.refresh_token(
        asana.session.token_url,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        refresh_token=refresh_token,
    )
    return asana


@dlt.resource(table_name="workspaces", write_disposition="replace")
def workspaces(client: AsanaClient) -> t.Iterator[dict]:
    """Returns a list of workspaces"""
    print("Fetching workspaces...")
    yield from client.workspaces.find_all(
        opt_fields=",".join(
            ["gid", "name", "is_organization", "resource_type", "email_domains"]
        )
    )
    print("Done fetching workspaces.")


@dlt.transformer(
    data_from=workspaces,
    table_name="projects",
    write_disposition="replace",
    selected=False,
)
@dlt.defer
def projects(workspace, client: AsanaClient) -> t.Iterator[dict]:
    """Returns a list of projects for a given workspace"""
    print(f"Fetching projects for workspace {workspace['name']}...")
    try:
        return list(
            client.projects.find_all(
                workspace=workspace["gid"],
                timeout=300,
                opt_fields=",".join(
                    [
                        "name",
                        "gid",
                        "owner",
                        "current_status",
                        "custom_fields",
                        "default_view",
                        "due_date",
                        "due_on",
                        "is_template",
                        "created_at",
                        "modified_at",
                        "start_on",
                        "archived",
                        "public",
                        "members",
                        "followers",
                        "color",
                        "notes",
                        "icon",
                        "permalink_url",
                        "workspace",
                        "team",
                        "resource_type",
                        "current_status_update",
                        "custom_field_settings",
                        "completed",
                        "completed_at",
                        "completed_by",
                        "created_from_template",
                        "project_brief",
                    ]
                ),
            )
        )
    finally:
        print(f"Done fetching projects for workspace {workspace['name']}.")


@dlt.transformer(
    data_from=projects,
    table_name="sections",
    write_disposition="replace",
)
@dlt.defer
def sections(project_array, client: AsanaClient) -> t.Iterator[dict]:
    """Fetches all sections for a given project."""
    print(f"Fetching sections for {len(project_array)} projects...")
    try:
        return [
            section
            for project in project_array
            for section in client.sections.get_sections_for_project(
                project_gid=project["gid"],
                timeout=300,
                opt_fields=",".join(
                    [
                        "gid",
                        "resource_type",
                        "name",
                        "created_at",
                        "project",
                        "projects",
                    ]
                ),
            )
        ]
    finally:
        print(f"Done fetching sections for {len(project_array)} projects.")


@dlt.transformer(data_from=workspaces, table_name="tags", write_disposition="replace")
@dlt.defer
def tags(workspace, client: AsanaClient) -> t.Iterator[dict]:
    """Fetches all tags for a given workspace."""
    print(f"Fetching tags for workspace {workspace['name']}...")
    try:
        return [
            tag
            for tag in client.tags.find_all(
                workspace=workspace["gid"],
                timeout=300,
                opt_fields=",".join(
                    [
                        "gid",
                        "resource_type",
                        "created_at",
                        "followers",
                        "name",
                        "color",
                        "notes",
                        "permalink_url",
                        "workspace",
                    ]
                ),
            )
        ]
    finally:
        print(f"Done fetching tags for workspace {workspace['name']}.")


@dlt.transformer(
    data_from=projects,
    table_name="tasks",
    write_disposition="append",
)
def tasks(project_array, client: AsanaClient) -> t.Iterator[dict]:
    """Fetches all tasks for a given project."""
    print(f"Fetching tasks for {len(project_array)} projects...")
    state = dlt.state().setdefault("tasks", {"modified_since": DEFAULT_START_DATE})
    yield from (
        task
        for project in project_array
        for task in client.tasks.find_all(
            project=project["gid"],
            timeout=300,
            modified_since=state["modified_since"],
            opt_fields=",".join(
                [
                    "gid",
                    "resource_type",
                    "name",
                    "approval_status",
                    "assignee_status",
                    "created_at",
                    "assignee",
                    "start_on",
                    "start_at",
                    "due_on",
                    "due_at",
                    "completed",
                    "completed_at",
                    "completed_by",
                    "modified_at",
                    "dependencies",
                    "dependents",
                    "external",
                    "notes",
                    "num_subtasks",
                    "resource_subtype",
                    "followers",
                    "parent",
                    "permalink_url",
                    "tags",
                    "workspace",
                    "custom_fields",
                    "project",
                    "memberships",
                    "memberships.project.name",
                    "memberships.section.name",
                ]
            ),
        )
    )
    state["modified_since"] = datetime.utcnow().isoformat() + "Z"
    print(f"Done fetching tasks for {len(project_array)} projects.")


@dlt.transformer(
    data_from=tasks,
    table_name="stories",
    write_disposition="append",
    selected=False,
)
@dlt.defer
def stories(task, client: AsanaClient) -> t.Iterator[dict]:
    """Fetch stories for a task."""
    print(f"Fetching stories for task {task['name']}...")
    try:
        return [
            story
            for story in client.stories.get_stories_for_task(
                task_gid=task["gid"],
                timeout=300,
                opt_fields=",".join(
                    [
                        "gid",
                        "resource_type",
                        "created_at",
                        "created_by",
                        "resource_subtype",
                        "text",
                        "is_pinned",
                        "assignee",
                        "dependency",
                        "follower",
                        "new_section",
                        "old_section",
                        "new_text_value",
                        "old_text_value",
                        "preview",
                        "project",
                        "source",
                        "story",
                        "tag",
                        "target",
                        "task",
                        "sticker_name",
                        "custom_field",
                        "type",
                    ]
                ),
            )
        ]
    finally:
        print(f"Done fetching stories for task {task['name']}.")


@dlt.transformer(
    data_from=workspaces,
    table_name="teams",
    write_disposition="replace",
)
@dlt.defer
def teams(workspace, client: AsanaClient) -> t.Iterator[dict]:
    """Fetches all teams for a given workspace."""
    print(f"Fetching teams for workspace {workspace['name']}...")
    try:
        return [
            team
            for team in client.teams.find_by_organization(
                organization=workspace["gid"],
                timeout=300,
                opt_fields=",".join(
                    [
                        "gid",
                        "resource_type",
                        "name",
                        "description",
                        "organization",
                        "permalink_url",
                        "visibility",
                    ]
                ),
            )
        ]
    finally:
        print(f"Done fetching teams for workspace {workspace['name']}.")


@dlt.transformer(
    data_from=workspaces,
    table_name="users",
    write_disposition="replace",
)
@dlt.defer
def users(workspace, client: AsanaClient) -> t.Iterator[dict]:
    """Fetches all users for a given workspace."""
    print(f"Fetching users for workspace {workspace['name']}...")
    try:
        return [
            user
            for user in client.users.find_all(
                workspace=workspace["gid"],
                timeout=300,
                opt_fields=",".join(
                    ["gid", "resource_type", "name", "email", "photo", "workspaces"]
                ),
            )
        ]
    finally:
        print(f"Done fetching users for workspace {workspace['name']}.")


@dlt.source(name="asana")
def asana_source(
    client_id: str = dlt.config.value,
    client_secret: str = dlt.secrets.value,
    redirect_uri: str = dlt.config.value,
    refresh_token: str = dlt.secrets.value,
):
    """The Asana dlt source."""
    client = get_client(client_id, client_secret, redirect_uri, refresh_token)
    return (
        workspaces.bind(client=client)(),
        projects.bind(client=client)(),
        sections.bind(client=client)(),
        tags.bind(client=client)(),
        tasks.bind(client=client)(),
        stories.bind(client=client)(),
        teams.bind(client=client)(),
        users.bind(client=client)(),
    )
