"""Fetches Asana workspaces, projects, tasks and other associated objects, using parallel requests wherever possible."""
import typing as t
from typing import Sequence
import dlt

from dlt.extract.source import DltResource
from .const import (
    PROJECT_FIELDS,
    USER_FIELDS,
    DEFAULT_START_DATE,
    TIMEOUT,
    SECTION_FIELDS,
    TAG_FIELDS,
    TASK_FIELDS,
    STORY_FIELDS,
    TEAMS_FIELD,
    WORKSPACE_FIELDS,
)
from .helpers import get_client


@dlt.resource(write_disposition="replace")
def workspaces(access_token: str = dlt.secrets.value) -> t.Iterator[dict]:
    """Returns a list of workspaces"""
    print("Fetching workspaces...")
    yield from get_client(access_token).workspaces.find_all(
        opt_fields=",".join(WORKSPACE_FIELDS)
    )
    print("Done fetching workspaces.")


@dlt.transformer(
    data_from=workspaces,
    write_disposition="replace",
)
@dlt.defer
def projects(workspace, access_token: str = dlt.secrets.value) -> t.Iterator[dict]:
    """Returns a list of projects for a given workspace"""
    print(f"Fetching projects for workspace {workspace['name']}...")
    try:
        return list(
            get_client(access_token).projects.find_all(
                workspace=workspace["gid"],
                timeout=TIMEOUT,
                opt_fields=",".join(PROJECT_FIELDS),
            )
        )
    finally:
        print(f"Done fetching projects for workspace {workspace['name']}.")


@dlt.transformer(
    data_from=projects,
    write_disposition="replace",
)
@dlt.defer
def sections(project_array, access_token: str = dlt.secrets.value) -> t.Iterator[dict]:
    """Fetches all sections for a given project."""
    print(f"Fetching sections for {len(project_array)} projects...")
    try:
        return [
            section
            for project in project_array
            for section in get_client(access_token).sections.get_sections_for_project(
                project_gid=project["gid"],
                timeout=TIMEOUT,
                opt_fields=",".join(SECTION_FIELDS),
            )
        ]
    finally:
        print(f"Done fetching sections for {len(project_array)} projects.")


@dlt.transformer(data_from=workspaces, write_disposition="replace")
@dlt.defer
def tags(workspace, access_token: str = dlt.secrets.value) -> t.Iterator[dict]:
    """Fetches all tags for a given workspace."""
    print(f"Fetching tags for workspace {workspace['name']}...")
    try:
        return [
            tag
            for tag in get_client(access_token).tags.find_all(
                workspace=workspace["gid"],
                timeout=TIMEOUT,
                opt_fields=",".join(TAG_FIELDS),
            )
        ]
    finally:
        print(f"Done fetching tags for workspace {workspace['name']}.")


@dlt.transformer(data_from=projects, write_disposition="merge", primary_key="gid")
def tasks(
    project_array,
    access_token: str = dlt.secrets.value,
    modified_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        "modified_at", initial_value=DEFAULT_START_DATE
    ),
) -> t.Iterator[dict]:
    """Fetches all tasks for a given project."""
    print(f"Fetching tasks for {len(project_array)} projects...")
    yield from (
        task
        for project in project_array
        for task in get_client(access_token).tasks.find_all(
            project=project["gid"],
            timeout=TIMEOUT,
            modified_since=modified_at.start_value,
            opt_fields=",".join(TASK_FIELDS),
        )
    )
    print(f"Done fetching tasks for {len(project_array)} projects.")


@dlt.transformer(
    data_from=tasks,
    write_disposition="append",
)
@dlt.defer
def stories(task, access_token: str = dlt.secrets.value) -> t.Iterator[dict]:
    """Fetch stories for a task."""
    print(f"Fetching stories for task {task['name']}...")
    try:
        return [
            story
            for story in get_client(access_token).stories.get_stories_for_task(
                task_gid=task["gid"],
                timeout=TIMEOUT,
                opt_fields=",".join(STORY_FIELDS),
            )
        ]
    finally:
        print(f"Done fetching stories for task {task['name']}.")


@dlt.transformer(
    data_from=workspaces,
    write_disposition="replace",
)
@dlt.defer
def teams(workspace, access_token: str = dlt.secrets.value) -> t.Iterator[dict]:
    """Fetches all teams for a given workspace."""
    print(f"Fetching teams for workspace {workspace['name']}...")
    try:
        return [
            team
            for team in get_client(access_token).teams.find_by_organization(
                organization=workspace["gid"],
                timeout=TIMEOUT,
                opt_fields=",".join(TEAMS_FIELD),
            )
        ]
    finally:
        print(f"Done fetching teams for workspace {workspace['name']}.")


@dlt.transformer(
    data_from=workspaces,
    write_disposition="replace",
)
@dlt.defer
def users(workspace, access_token: str = dlt.secrets.value) -> t.Iterator[dict]:
    """Fetches all users for a given workspace."""
    print(f"Fetching users for workspace {workspace['name']}...")
    try:
        return [
            user
            for user in get_client(access_token).users.find_all(
                workspace=workspace["gid"],
                timeout=TIMEOUT,
                opt_fields=",".join(USER_FIELDS),
            )
        ]
    finally:
        print(f"Done fetching users for workspace {workspace['name']}.")


@dlt.source
def asana_source(access_token: str = dlt.secrets.value) -> Sequence[DltResource]:
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
