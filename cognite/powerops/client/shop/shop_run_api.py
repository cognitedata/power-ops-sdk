from __future__ import annotations

import datetime
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, Optional, Union, overload
from urllib.parse import urlparse

import arrow
import requests
from cognite.client import CogniteClient
from cognite.client.data_classes import UserProfile, filters
from cognite.client.data_classes.events import EventSort
from cognite.client.exceptions import CogniteAPIError

from cognite.powerops.cdf_labels import RelationshipLabel
from cognite.powerops.client.shop.data_classes.dayahead_trigger import (
    Case,
    PrerunFileMetadata,
    SHOPPreRunFile,
)
from cognite.powerops.client.shop.data_classes.shop_case import (
    SHOPCase,
    SHOPFileReference,
    SHOPFileType,
)
from cognite.powerops.client.shop.data_classes.shop_run import (
    SHOPRun,
    SHOPRunEvent,
    SHOPRunList,
)
from cognite.powerops.client.shop.data_classes.shop_run_filter import SHOPRunFilter
from cognite.powerops.utils.cdf.calls import create_event
from cognite.powerops.utils.cdf.resource_creation import simple_relationship
from cognite.powerops.utils.deprecation import deprecated_class
from cognite.powerops.utils.identifiers import new_external_id

DEFAULT_READ_LIMIT = 25

_APICallableForSHOPRunT = Callable[[Union[SHOPRun, Sequence[SHOPRun]]], Union[SHOPRun, Sequence[SHOPRun]]]


#! Will be replaced by `cogshop_api.py`
@deprecated_class
class SHOPRunAPI:
    def __init__(
        self,
        client: CogniteClient,
        dataset_id: int,
    ):
        self._cdf = client
        self._dataset_id = dataset_id
        self._CONCURRENT_CALLS = 5

    def _get_shop_prerun_files(self, file_external_ids: list[str]) -> list[SHOPPreRunFile]:
        prerun_files = self._cdf.files.retrieve_multiple(external_ids=file_external_ids)
        return [SHOPPreRunFile.load_from_metadata(file) for file in prerun_files]

    def trigger_case(self, case: Case, shop_version: str) -> tuple[list, list[SHOPRun]]:
        """
        Trigger a collection of shop runs related to one Case (also referred to as watercourse).
        For each SHOPCase the prerun file will be used to trigger a shop run event in cdf,
        and used to trigger the CogSHOP container.
        Prerun files must exist in CDF.
        """
        shop_events = []
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        plants_per_case = []
        pre_runs = self._get_shop_prerun_files(case.pre_run_file_external_ids)
        for shop_run in pre_runs:
            plants_per_case.extend(shop_run.plants)
            shop_events.append(
                SHOPRun(
                    external_id=new_external_id(prefix="shop_run", now=now),
                    watercourse=case.case_name,
                    start=now,
                    end=None,
                    data_set_id=self._dataset_id,
                    _case_file_external_id=shop_run.pre_run_external_id,
                    _shop_files=(
                        [
                            SHOPFileReference(
                                external_id=case.commands_file,
                                file_type=SHOPFileType.ASCII.value,
                            )
                        ]
                        if case.commands_file
                        else []
                    ),
                    shop_version=shop_version,
                    plants=PrerunFileMetadata._plants_delimiter.join(shop_run.plants),
                    price_scenario=shop_run.price_scenario,
                    _client=self._cdf,
                    source="DayaheadTrigger",
                )
            )

        self._cdf.events.create([new_event.as_cdf_event() for new_event in shop_events])

        with ThreadPoolExecutor(max_workers=self._CONCURRENT_CALLS) as executor:
            futures = [executor.submit(self._trigger_shop_container, shop_event) for shop_event in shop_events]

            for future in futures:
                if exc := future.exception():
                    raise (exc)

        return list(set(plants_per_case)), shop_events

    def trigger_single_casefile(
        self,
        case: SHOPCase,
        source: str | None = None,
        shop_run_external_id: str | None = None,
    ) -> SHOPRun:
        """
        Trigger SHOP for a given case file.

        Args:
            case: SHOPCase instance, contains data for the main case file and references to additional SHOP input files.
            source: The source of the SHOP trigger. If nothing is passed, the method will
                try to detect the service principal of the current user.
            shop_run_external_id: External ID for the SHOP run event, must be unique within the CDF project. If nothing
                is passed, a new external ID will be generated.

        Returns:
            The new SHOP run created.
        """
        if not case.watercourse:
            raise ValueError("Set watercourse on case before triggering.")

        user: UserProfile | None
        try:
            user = self._cdf.iam.user_profiles.me()
        except CogniteAPIError as e:
            if e.code == 404:
                user = None
            else:
                raise

        if source is None:
            source = user.user_identifier if user else "manual"
        display_name = user.display_name if user else "Unknown"
        now = datetime.datetime.now(datetime.timezone.utc)

        # todo: are parts of this worth keeping?

        case_file_meta = self._cdf.files.upload_bytes(
            # On Windows machines, some bytes can get lost in the encoding process
            # when uploading a string to CDF. This is a workaround.
            content=case.yaml.encode("utf-8"),
            name=f"Manual Case by {display_name}",
            mime_type="application/yaml",
            external_id=new_external_id(prefix="cog_shop_manual", now=now),
            metadata={"shop:type": "cog_shop_case", "user": display_name},
            data_set_id=self._dataset_id,
            source=source,
        )

        external_id = shop_run_external_id or new_external_id(now=now)

        shop_run = SHOPRun(
            external_id=external_id,
            watercourse=case.watercourse,
            data_set_id=self._dataset_id,
            start=now,
            end=None,
            _case_file_external_id=case_file_meta.external_id,
            _shop_files=case.shop_files,
            shop_version=case.shop_version,
            _client=self._cdf,
            source=source,
        )

        event = shop_run.as_cdf_event()
        create_event(self._cdf, event)

        relationships = [
            simple_relationship(
                source=event,
                target=case_file_meta,
                label_external_id=RelationshipLabel.CASE_FILE,
            )
        ]

        for shop_file_reference in case.shop_files:
            cdf_file = shop_file_reference.as_cdf_file_metadata()
            relationships.append(
                simple_relationship(
                    source=event,
                    target=cdf_file,
                    label_external_id=RelationshipLabel.EXTRA_FILE,
                )
            )
        self._cdf.relationships.create(relationships)

        self._trigger_shop_container(shop_run)
        return shop_run

    def trigger_shop_run(self, shop_run: SHOPRun) -> SHOPRun:
        self._create(shop_run)
        self._trigger_shop_container(shop_run)
        return shop_run

    def _shop_url_cshaas(self) -> str:
        project = self._cdf.config.project

        cluster = urlparse(self._cdf.config.base_url).netloc.split(".", 1)[0]

        environment = ".staging" if project == "power-ops-staging" else ""

        return f"https://power-ops-api{environment}.{cluster}.cognite.ai/{project}/run-shop-as-service"

    # todo: changes to calling logic here -- for switch to fdm
    def _trigger_shop_container(self, shop_run: SHOPRun):
        def auth(r: requests.PreparedRequest) -> requests.PreparedRequest:
            auth_header_name, auth_header_value = self._cdf._config.credentials.authorization_header()
            r.headers[auth_header_name] = auth_header_value
            return r

        shop_url = self._shop_url_cshaas()
        shop_body = {
            "mode": "asset",
            "runs": [{"event_external_id": shop_run.external_id}],
        }

        response = requests.post(
            url=shop_url,
            json=shop_body,
            auth=auth,
        )
        response.raise_for_status()

    def _load_cdf_event_shop_runs(
        self,
        extra_filters: Optional[list[filters.Filter]] = None,
        limit: int = DEFAULT_READ_LIMIT,
        event_sort: EventSort = None,
    ) -> SHOPRunList:
        is_type = filters.Equals("type", SHOPRunEvent.event_type)
        events = self._cdf.events.filter(filters.And(is_type, *extra_filters), limit=limit, sort=event_sort)
        return SHOPRunList.load(events)

    @overload
    def retrieve(self, external_id: str) -> SHOPRun | None: ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> SHOPRunList: ...

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
            return None if event is None else SHOPRun.load(event)
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

    def versions(self, py_version: str = "3.9") -> list[str]:
        """List the available version of SHOP remotely  in CDF. Does not include versions in CogShop Image.
        SHOP releases should have the following format:
        'SHOP-${{VERSION}}-pyshop-python{py_version}.linux.zip'
        """
        files = self._cdf.files.list(metadata={"shop:type": "shop-release"}, limit=-1)
        remote_versions = []
        for file in files:
            name_parts = file.name.split("-")
            if f"python{py_version}" in name_parts:
                remote_versions.append("-".join(name_parts[:2]))
        return remote_versions

    @overload
    def _create(self, shop_run: SHOPRun | Sequence[SHOPRun]) -> SHOPRun | Sequence[SHOPRun]: ...

    @overload
    def _create(self, shop_run: Sequence[SHOPRun]) -> Sequence[SHOPRun]: ...

    def _create(self, shop_run: SHOPRun | Sequence[SHOPRun]) -> SHOPRun | Sequence[SHOPRun]:
        api_method = self._wrap_event_api(self._cdf.events.create)
        return api_method(shop_run)

    @overload
    def _update(self, shop_run: SHOPRun | Sequence[SHOPRun]) -> SHOPRun | Sequence[SHOPRun]: ...

    @overload
    def _update(self, shop_run: Sequence[SHOPRun]) -> Sequence[SHOPRun]: ...

    def _update(self, shop_run: SHOPRun | Sequence[SHOPRun]) -> SHOPRun | Sequence[SHOPRun]:
        api_method = self._wrap_event_api(self._cdf.events.update)
        return api_method(shop_run)

    @overload
    def _upsert(self, shop_run: SHOPRun | Sequence[SHOPRun]) -> SHOPRun | Sequence[SHOPRun]: ...

    @overload
    def _upsert(self, shop_run: Sequence[SHOPRun]) -> Sequence[SHOPRun]: ...

    def _upsert(self, shop_run: SHOPRun | Sequence[SHOPRun]) -> SHOPRun | Sequence[SHOPRun]:
        api_method = self._wrap_event_api(self._cdf.events.update)
        return api_method(shop_run)

    def _wrap_event_api(self, api_method: Callable) -> _APICallableForSHOPRunT:
        def wrapped(
            shop_run: SHOPRun | Sequence[SHOPRun],
        ) -> SHOPRun | Sequence[SHOPRun]:
            is_single = isinstance(shop_run, SHOPRun)
            shop_runs = [shop_run] if is_single else shop_run
            new_events = api_method([shop_run.as_cdf_event() for shop_run in shop_runs])
            loaded_shop_runs = [SHOPRun.load(new_event) for new_event in new_events]
            return loaded_shop_runs[0] if is_single else loaded_shop_runs

        return wrapped
