"""Fetches Asana workspaces, projects, tasks and other associated objects, using parallel requests wherever possible."""
import typing as t
from datetime import datetime
from typing import Sequence
import dlt
from asana import Client as AsanaClient
from dlt.extract.source import DltResource
from functools import partial

DEFAULT_START_DATE = "2010-01-01T00:00:00.000Z"


def get_client(
    access_token: str,
) -> AsanaClient:
    """Returns an Asana client with a valid access token"""
    asana = AsanaClient.access_token(access_token)
    return asana


@dlt.resource(write_disposition="replace")
def workspaces(access_token: str = dlt.config.value) -> t.Iterator[dict]:
    """Returns a list of workspaces"""
    print("Fetching workspaces...")
    yield from get_client(access_token).workspaces.find_all(
        opt_fields=",".join(
            ["gid", "name", "is_organization", "resource_type", "email_domains"]
        )
    )
    print("Done fetching workspaces.")


@dlt.transformer(
    data_from=workspaces,
    write_disposition="replace",
)
@dlt.defer
def projects(workspace, access_token: str = dlt.config.value) -> t.Iterator[dict]:
    """Returns a list of projects for a given workspace"""
    print(f"Fetching projects for workspace {workspace['name']}...")
    try:
        return list(
            get_client(access_token).projects.find_all(
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
    write_disposition="replace",
)
@dlt.defer
def sections(project_array, access_token: str = dlt.config.value) -> t.Iterator[dict]:
    """Fetches all sections for a given project."""
    print(f"Fetching sections for {len(project_array)} projects...")
    try:
        return [
            section
            for project in project_array
            for section in get_client(access_token).sections.get_sections_for_project(
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


@dlt.transformer(data_from=workspaces, write_disposition="replace")
@dlt.defer
def tags(workspace, access_token: str = dlt.config.value) -> t.Iterator[dict]:
    """Fetches all tags for a given workspace."""
    print(f"Fetching tags for workspace {workspace['name']}...")
    try:
        return [
            tag
            for tag in get_client(access_token).tags.find_all(
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
    write_disposition="append",
)
def tasks(project_array, access_token: str = dlt.config.value) -> t.Iterator[dict]:
    """Fetches all tasks for a given project."""
    print(f"Fetching tasks for {len(project_array)} projects...")
    state = dlt.state().setdefault("tasks", {"modified_since": DEFAULT_START_DATE})
    yield from (
        task
        for project in project_array
        for task in get_client(access_token).tasks.find_all(
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
    write_disposition="append",
)
@dlt.defer
def stories(task, access_token: str = dlt.config.value) -> t.Iterator[dict]:
    """Fetch stories for a task."""
    print(f"Fetching stories for task {task['name']}...")
    try:
        return [
            story
            for story in get_client(access_token).stories.get_stories_for_task(
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
    write_disposition="replace",
)
@dlt.defer
def teams(workspace, access_token: str = dlt.config.value) -> t.Iterator[dict]:
    """Fetches all teams for a given workspace."""
    print(f"Fetching teams for workspace {workspace['name']}...")
    try:
        return [
            team
            for team in get_client(access_token).teams.find_by_organization(
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
    write_disposition="replace",
)
@dlt.defer
def users(workspace, access_token: str = dlt.config.value) -> t.Iterator[dict]:
    """Fetches all users for a given workspace."""
    print(f"Fetching users for workspace {workspace['name']}...")
    try:
        return [
            user
            for user in get_client(access_token).users.find_all(
                workspace=workspace["gid"],
                timeout=300,
                opt_fields=",".join(
                    ["gid", "resource_type", "name", "email", "photo", "workspaces"]
                ),
            )
        ]
    finally:
        print(f"Done fetching users for workspace {workspace['name']}.")


@dlt.source
def asana_source(
) -> Sequence[DltResource]:
    """The Asana dlt source."""
    return (
        workspaces,
        projects,
        sections,
        tags,
        tasks,
        stories,
        teams,
        users,
    )
