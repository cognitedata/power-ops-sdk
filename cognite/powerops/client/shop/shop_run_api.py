from __future__ import annotations

import datetime
import secrets
from collections.abc import Sequence
from typing import Literal, Optional, overload
from urllib.parse import urlparse
from uuid import uuid4

import arrow
import requests
from cognite.client import CogniteClient
from cognite.client.data_classes import UserProfile, filters
from cognite.client.data_classes.events import EventSort
from cognite.client.exceptions import CogniteAPIError

from cognite.powerops.client.shop.shop_run_filter import SHOPRunFilter

from .shop_run import SHOPRun, ShopRunEvent, SHOPRunList

DEFAULT_READ_LIMIT = 25


def _unique_short_str(nbytes: int) -> str:
    return secrets.token_hex(nbytes=nbytes)


class SHOPRunAPI:
    def __init__(self, client: CogniteClient, dataset_id: int, cogshop_version: str):
        self._cdf = client
        self._dataset_id = dataset_id
        self.cogshop_version = cogshop_version

    def trigger_case(self, case_file: str, watercourse: str, source: str | None = None) -> SHOPRun:
        """
        Trigger SHOP for a given case file.

        Args:
            case_file: Case file as a string. Expected to be YAML.
            watercourse: The watercourse to run SHOP this case file is for.
            source: The source of the SHOP trigger. If nothing is passed, the method will
                try to detect the service principal of the current user.

        Returns:
            The new SHOP run created.
        """
        user: UserProfile | None
        try:
            user = self._cdf.iam.user_profiles.me()
        except CogniteAPIError as e:
            if e.code == 404:
                user = None
            else:
                raise

        if source is None and user:
            source = user.user_identifier
        elif source is None:
            source = "manual"

        if user:
            display_name = user.display_name
        else:
            display_name = "Unknown"

        meta = self._cdf.files.upload_bytes(
            content=case_file,
            name=f"Manual Case by {display_name}",
            mime_type="application/yaml",
            external_id=f"cog_shop_manual/{uuid4()!s}",
            metadata={"shop:type": "cog_shop_case", "user": display_name},
            data_set_id=self._dataset_id,
            source=source,
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        now_isoformat = now.isoformat().replace("+00:00", "Z")

        new_event = SHOPRun(
            external_id=f"SHOP_RUN_{now_isoformat}_{_unique_short_str(3)}",
            watercourse=watercourse,
            start=now,
            end=None,
            _case_file_external_id=meta.external_id,
            _shop_files=[],
            shop_version=self.cogshop_version,
            _client=self._cdf,
            source=source,
        )
        self._cdf.events.create(new_event.as_cdf_event(self._dataset_id))

        self._trigger_shop_container(new_event)
        return new_event

    def _trigger_shop_container(self, event: SHOPRun):
        def auth(r: requests.PreparedRequest) -> requests.PreparedRequest:
            auth_header_name, auth_header_value = self._cdf._config.credentials.authorization_header()
            r.headers[auth_header_name] = auth_header_value
            return r

        response = requests.post(
            url=self._shop_url(),
            json={"shopEventExternalId": event.external_id, "cogShopVersion": self.cogshop_version},
            auth=auth,
        )
        response.raise_for_status()

    def _shop_url(self) -> str:
        project = self._cdf.config.project

        cluster = urlparse(self._cdf.config.base_url).netloc.split(".", 1)[0]

        environment = project.split("-")[-1]
        if environment in {"dev", "staging"}:
            stage = ".staging"
        elif environment == "prod":
            stage = ""
        else:
            raise ValueError(f"Can't detect prod/staging from project name: {project!r}")

        return f"https://power-ops-api{stage}.{cluster}.cognite.ai/{project}/run-shop"

    def _load_cdf_event_shop_runs(
        self,
        extra_filters: Optional[list[filters.Filter]] = None,
        limit: int = DEFAULT_READ_LIMIT,
        event_sort: EventSort = None,
    ) -> SHOPRunList:
        is_type = filters.Equals("type", ShopRunEvent.event_type)
        events = self._cdf.events.filter(filters.And(is_type, *extra_filters), limit=limit, sort=event_sort)
        return SHOPRunList.load(events)

    @overload
    def retrieve(self, external_id: str) -> SHOPRun | None:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> SHOPRunList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], ignore_unknown_ids: bool = True
    ) -> SHOPRun | SHOPRunList | None:
        """
        Retrieve a SHOP run.

        Args:
            external_id: The external id(s) of the SHOP run(s) to retrieve.
            ignore_unknown_ids: Whether to ignore unknown ids or raise an error.

        Returns:
            The SHOP run(s). None if it is a single external id, and it is not found.
        """
        if isinstance(external_id, str):
            event = self._cdf.events.retrieve(external_id=external_id)
            if event is None:
                return None
            return SHOPRun.load(event)
        elif isinstance(external_id, Sequence):
            return SHOPRunList.load(
                self._cdf.events.retrieve_multiple(external_ids=external_id, ignore_unknown_ids=ignore_unknown_ids)
            )
        else:
            raise TypeError(f"Invalid type {type(external_id)} for external_id.")

    def retrieve_latest(
        self,
        watercourse: str | list[str] | None = None,
        source: str | list[str] | None = None,
        start_after: str | arrow.Arrow | datetime.datetime | None = None,
        start_before: str | arrow.Arrow | datetime.datetime | None = None,
        end_after: str | arrow.Arrow | datetime.datetime | None = None,
        end_before: str | arrow.Arrow | datetime.datetime | None = None,
        latest_by: Literal[
            "start_time",
            "end_time",
            "created_time",
            "last_updated_time",
        ] = "last_updated_time",
    ) -> SHOPRun | None:
        """
        Retrieves the the latest shop run given the filters.

         Args:
            watercourse: The watercourse(s) to filter on.
            source: The source(s) to filter on.
            start_after: The start time after which the SHOP run must have started.
            start_before: The start time before which the SHOP run must have started.
            end_after: The end time after which the SHOP run must have ended.
            end_before: The end time before which the SHOP run must have ended.
            latest_by: The property to retrieve by . The most reliable is `last_updated_time` and `created_time`.
        Returns:
            The most recent SHOP run, None if nothing matched the filter
        """
        extra_filters = SHOPRunFilter(
            watercourse=watercourse,
            source=source,
            start_after=start_after,
            start_before=start_before,
            end_after=end_after,
            end_before=end_before,
        ).get_filters()
        event_sort = EventSort(property=latest_by, order="desc")

        shop_runs = self._load_cdf_event_shop_runs(extra_filters=extra_filters, limit=1, event_sort=event_sort)
        if len(shop_runs) == 0:
            return None

        return shop_runs[0]

    def list(
        self,
        watercourse: str | list[str] | None = None,
        source: str | list[str] | None = None,
        start_after: str | arrow.Arrow | datetime.datetime | None = None,
        start_before: str | arrow.Arrow | datetime.datetime | None = None,
        end_after: str | arrow.Arrow | datetime.datetime | None = None,
        end_before: str | arrow.Arrow | datetime.datetime | None = None,
        limit: int = DEFAULT_READ_LIMIT,
    ) -> SHOPRunList:
        """List the filtered SHOP runs.
        Provide time stamps strings as YYYY-MM-DD) or otherwise something parsable by `arrow.get()` can parse.

        Args:
            watercourse: The watercourse(s) to filter on.
            source: The source(s) to filter on. WARNING: Most
            start_after: The start time after which the SHOP run must have started.
            start_before: The start time before which the SHOP run must have started.
            end_after: The end time after which the SHOP run must have ended.
            end_before: The end time before which the SHOP run must have ended.
            limit: The maximum number of SHOP runs to return.

        Returns:
            A list of SHOP runs.
        """

        extra_filters = SHOPRunFilter(
            watercourse=watercourse,
            source=source,
            start_after=start_after,
            start_before=start_before,
            end_after=end_after,
            end_before=end_before,
        ).get_filters()
        return self._load_cdf_event_shop_runs(extra_filters=extra_filters, limit=limit)
