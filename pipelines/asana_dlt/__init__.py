"""
This pipeline provides data extraction from the Asana platform via their API.

It defines several functions to fetch data from different parts of Asana including
workspaces, projects, sections, tags, tasks, stories, teams, and users. These
functions are meant to be used as part of a data loading pipeline.
"""

import typing as t
from typing import Sequence, Iterable, Dict, Any
import dlt

from dlt.extract.source import DltResource
from .settings import (
    PROJECT_FIELDS,
    USER_FIELDS,
    DEFAULT_START_DATE,
    REQUEST_TIMEOUT,
    SECTION_FIELDS,
    TAG_FIELDS,
    TASK_FIELDS,
    STORY_FIELDS,
    TEAMS_FIELD,
    WORKSPACE_FIELDS,
)
from .helpers import get_client


@dlt.resource(write_disposition="replace")
def workspaces(access_token: str = dlt.secrets.value) -> Iterable[Dict[str, Any]]:
    """
    Fetches and returns a list of workspaces from Asana.
    Args:
        access_token (str): The access token to authenticate the Asana API client, provided in the secrets file
    Yields:
        dict: The workspace data.
    """
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
def projects(
    workspace: Dict[str, Any], access_token: str = dlt.secrets.value
) -> Iterable[Dict[str, Any]]:
    """
    Fetches and returns a list of projects for a given workspace from Asana.
    Args:
        workspace (dict): The workspace data.
        access_token (str): The access token to authenticate the Asana API client, provided in the secrets file
    Returns:
        list[dict]: The project data for the given workspace.
    """
    print(f"Fetching projects for workspace {workspace['name']}...")
    try:
        return list(
            get_client(access_token).projects.find_all(
                workspace=workspace["gid"],
                timeout=REQUEST_TIMEOUT,
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
def sections(
    project_array: t.List[Dict[str, Any]], access_token: str = dlt.secrets.value
) -> Iterable[Dict[str, Any]]:
    """
    Fetches all sections for a given project from Asana.
    Args:
        project_array (list): The project data.
        access_token (str): The access token to authenticate the Asana API client, provided in the secrets file
    Returns:
        list[dict]: The sections data for the given project.
    """
    print(f"Fetching sections for {len(project_array)} projects...")
    try:
        return [
            section
            for project in project_array
            for section in get_client(access_token).sections.get_sections_for_project(
                project_gid=project["gid"],
                timeout=REQUEST_TIMEOUT,
                opt_fields=",".join(SECTION_FIELDS),
            )
        ]
    finally:
        print(f"Done fetching sections for {len(project_array)} projects.")


@dlt.transformer(data_from=workspaces, write_disposition="replace")
@dlt.defer
def tags(
    workspace: Dict[str, Any], access_token: str = dlt.secrets.value
) -> Iterable[Dict[str, Any]]:
    """
    Fetches all tags for a given workspace from Asana.
    Args:
        workspace (dict): The workspace data.
        access_token (str): The access token to authenticate the Asana API client, provided in the secrets file
    Returns:
        list[dict]: The tags data for the given workspace.
    """
    print(f"Fetching tags for workspace {workspace['name']}...")
    try:
        return [
            tag
            for tag in get_client(access_token).tags.find_all(
                workspace=workspace["gid"],
                timeout=REQUEST_TIMEOUT,
                opt_fields=",".join(TAG_FIELDS),
            )
        ]
    finally:
        print(f"Done fetching tags for workspace {workspace['name']}.")


@dlt.transformer(data_from=projects, write_disposition="merge", primary_key="gid")
def tasks(
    project_array: t.List[Dict[str, Any]],
    access_token: str = dlt.secrets.value,
    modified_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        "modified_at", initial_value=DEFAULT_START_DATE
    ),
) -> Iterable[Dict[str, Any]]:
    """
    Fetches all tasks for a given project from Asana.
    Args:
        project_array (list): The project data.
        access_token (str): The access token to authenticate the Asana API client, provided in the secrets file

        modified_at (str): The date from which to fetch modified tasks.
    Yields:
        dict: The task data for the given project.
    """
    print(f"Fetching tasks for {len(project_array)} projects...")
    yield from (
        task
        for project in project_array
        for task in get_client(access_token).tasks.find_all(
            project=project["gid"],
            timeout=REQUEST_TIMEOUT,
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
def stories(
    task: Dict[str, Any], access_token: str = dlt.secrets.value
) -> Iterable[Dict[str, Any]]:
    """
    Fetches stories for a task from Asana.
    Args:
        task (dict): The task data.
        access_token (str): The access token to authenticate the Asana API client, provided in the secrets file
    Returns:
        list[dict]: The stories data for the given task.
    """
    print(f"Fetching stories for task {task['name']}...")
    try:
        return [
            story
            for story in get_client(access_token).stories.get_stories_for_task(
                task_gid=task["gid"],
                timeout=REQUEST_TIMEOUT,
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
def teams(
    workspace: Dict[str, Any], access_token: str = dlt.secrets.value
) -> Iterable[Dict[str, Any]]:
    """
    Fetches all teams for a given workspace from Asana.
    Args:
        workspace (dict): The workspace data.
        access_token (str): The access token to authenticate the Asana API client, provided in the secrets file
    Returns:
        list[dict]: The teams data for the given workspace.
    """
    print(f"Fetching teams for workspace {workspace['name']}...")
    try:
        return [
            team
            for team in get_client(access_token).teams.find_by_organization(
                organization=workspace["gid"],
                timeout=REQUEST_TIMEOUT,
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
def users(
    workspace: Dict[str, Any], access_token: str = dlt.secrets.value
) -> Iterable[Dict[str, Any]]:
    """
    Fetches all users for a given workspace from Asana.
    Args:
        workspace (dict): The workspace data.
        access_token (str): The access token to authenticate the Asana API client, provided in the secrets file
    Returns:
        list[dict]: The user data for the given workspace.
    """
    print(f"Fetching users for workspace {workspace['name']}...")
    try:
        return [
            user
            for user in get_client(access_token).users.find_all(
                workspace=workspace["gid"],
                timeout=REQUEST_TIMEOUT,
                opt_fields=",".join(USER_FIELDS),
            )
        ]
    finally:
        print(f"Done fetching users for workspace {workspace['name']}.")


@dlt.source
def asana_source(
    access_token: str = dlt.secrets.value,
) -> Any:  # should be Sequence[DltResource]:
    """
    The main function that runs all the other functions to fetch data from Asana.
    Args:
        access_token (str): The access token to authenticate the Asana API client, provided in the secrets file
    Returns:
        Sequence[DltResource]: A sequence of DltResource objects containing the fetched data.
    """
    return [
        workspaces,
        projects,
        sections,
        tags,
        tasks,
        stories,
        teams,
        users,
    ]
