from collections import namedtuple
from typing import Any, Dict, List, Optional, Tuple

import httpx
import numpy as np
import pandas as pd
from cjwmodule import i18n


MaxNPages = 50
USERS_URL = "https://api.intercom.io/users?per_page=60&sort=created_at"
COMPANIES_URL = "https://api.intercom.io/companies?per_page=60"
TAGS_URL = "https://api.intercom.io/tags"
SEGMENTS_URL = "https://api.intercom.io/segments"


Columns = [
    ("email", ["email"]),
    ("name", ["name"]),
    ("city", ["location_data", "city_name"]),
    ("country", ["location_data", "country_name"]),
    ("session_count", ["session_count"]),
    ("last_request_at", ["last_request_at"]),
    ("social_profiles", ["social_profiles", "social_profiles"]),
    ("companies", ["companies", "companies"]),
    ("segments", ["segments", "segments"]),
    ("tags", ["tags", "tags"]),
    ("timezone", ["location_data", "timezone"]),
    ("created_at", ["created_at"]),
    ("updated_at", ["updated_at"]),
    ("id", ["id"]),
]


def read_raw_value(user: Dict[str, Any], path: List[str]) -> Any:
    obj = user
    try:
        for part in path:
            obj = obj[part]
    except KeyError:
        return None
    return obj


def read_column(users: List[Dict[str, Any]], path: List[str]) -> pd.Series:
    """
    Build an `np.object`-typed Series for the given column.
    """
    values = [read_raw_value(user, path) for user in users]
    return pd.Series(values, dtype=np.object)


def ids_to_names(objs: pd.Series, names: Dict[str, str]) -> pd.Series:
    """
    Convert a Series of List[Dict[str, Any]] with "id" keys to a Series of str.
    """

    def find_names(ds: List[Dict[str, str]]):
        nonlocal names
        # Remember: `names` might have been truncated in `fetch_paginated()`,
        # so `id` isn't guaranteed to be in it. Ignore missing IDs.
        return [names.get(d["id"]) for d in ds if d["id"] in names]

    return pd.Series(
        ["; ".join(find_names(value)) for value in objs.values], dtype=str
    ).astype("category")


def extract_social_media_username(objs: pd.Series, service: str) -> pd.Series:
    """
    Convert a Series of List[Dict[str, Any]] to a Series of Optional[str].
    """

    def find_username(profiles):
        nonlocal service
        try:
            return next(
                profile["username"]
                for profile in profiles
                if profile["name"] == service
            )
        except StopIteration:
            return np.nan

    return pd.Series([find_username(profiles) for profiles in objs], dtype=str)


async def fetch_paginated(
    client, bearer_token: str, url: str, data_key: str
) -> List[Dict[str, Any]]:
    """
    Fetch `url` using `access_token`, following pages.

    * Stop after `MaxNPages` requests.
    * Use `pages.next` URL from response to paginate.
    * Use `data_key` (e.g., "users") from response to find list of results.
    """
    results: List[Dict[str, Any]] = []

    page_url = url  # we'll modify it as we go
    for _ in range(MaxNPages):
        response = await client.get(
            page_url,
            headers={
                "Authorization": f"Bearer {bearer_token}",
                "Accept": "application/json",
            },
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError("Intercom did not return a JSON Object")
        if data_key not in data:
            raise RuntimeError(f'Intercom did not return "{data_key}" data')

        results.extend(data[data_key])

        if "pages" in data and data["pages"]["next"]:
            page_url = data["pages"]["next"]
        else:
            break

    return results


async def fetch_companies(client, bearer_token: str) -> Dict[str, str]:
    """Fetch mapping from company ID to company name."""
    companies = await fetch_paginated(client, bearer_token, COMPANIES_URL, "companies")
    return {
        company["id"]: company["name"]
        for company in companies
        if "name" in company  # sometimes it isn't
    }


async def fetch_segments(client, bearer_token: str) -> Dict[str, str]:
    """Fetch mapping from segment ID to segment name."""
    segments = await fetch_paginated(client, bearer_token, SEGMENTS_URL, "segments")
    return {segment["id"]: segment["name"] for segment in segments}


async def fetch_tags(client, bearer_token: str) -> Dict[str, str]:
    """Fetch mapping from tag ID to tag name."""
    tags = await fetch_paginated(client, bearer_token, TAGS_URL, "tags")
    return {tag["id"]: tag["name"] for tag in tags}


async def fetch_users(client, bearer_token: str) -> List[Dict[str, Any]]:
    return await fetch_paginated(client, bearer_token, USERS_URL, "users")


def build_dataframe(
    users: List[Dict[str, str]],
    companies: Dict[str, str],
    segments: Dict[str, str],
    tags: Dict[str, str],
) -> pd.DataFrame:
    # Turn `users` into columnar data. Its `social_profiles`, `companies`,
    # `segments` and `tags` are all complex objects.
    table = pd.DataFrame({name: read_column(users, path) for name, path in Columns})
    # Convert all the types. (They're all np.object)
    # 'category' is better than np.object for strings that repeat.
    table["city"] = table["city"].astype("category")
    table["country"] = table["country"].astype("category")
    table["session_count"] = table["session_count"].astype(np.int32)
    # dates are passed as UNIX timestamps
    table["last_request_at"] = pd.to_datetime(table["last_request_at"], unit="s")
    table["created_at"] = pd.to_datetime(table["created_at"], unit="s")
    table["updated_at"] = pd.to_datetime(table["updated_at"], unit="s")
    table["companies"] = ids_to_names(table["companies"], companies)
    table["segments"] = ids_to_names(table["segments"], segments)
    table["tags"] = ids_to_names(table["tags"], tags)

    # social_profiles has one list per user, of 0-3 entries that we must
    # extract one by one. Delete that one column and replace it with the three
    # (in the same place, so we get the order we gave in `Columns`).
    index = table.columns.get_loc("social_profiles")
    profiles = table.pop("social_profiles")  # modify table in-place
    table.insert(
        index, "facebook_username", extract_social_media_username(profiles, "facebook")
    )
    table.insert(
        index + 1,
        "linkedin_username",
        extract_social_media_username(profiles, "linkedin"),
    )
    table.insert(
        index + 2,
        "twitter_username",
        extract_social_media_username(profiles, "twitter"),
    )

    return table


async def fetch(params, *, secrets):
    access_token = (secrets.get("access_token") or {}).get("secret")
    if not access_token:
        return i18n.trans("badParam.access_token.empty", "Please sign in to Intercom")
    bearer_token = access_token["access_token"]

    try:
        # 5min timeouts ... and we'll assume Intercom is quick enough
        async with httpx.AsyncClient(timeout=httpx.Timeout(300)) as client:
            users = await fetch_users(client, bearer_token)
            companies = await fetch_companies(client, bearer_token)
            segments = await fetch_segments(client, bearer_token)
            tags = await fetch_tags(client, bearer_token)
    except httpx.RequestError as err:
        return i18n.trans(
            "error.httpError.general",
            "Error querying Intercom: {error}",
            {"error": str(err)},
        )
    except RuntimeError as err:
        return i18n.trans(
            "error.unexpectedIntercomJson.general",
            "Error handling Intercom response: {error}",
            {"error": str(err)},
        )

    return build_dataframe(users, companies, segments, tags)
