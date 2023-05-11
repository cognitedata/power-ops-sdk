from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.powerops.config import WatercourseConfig

if TYPE_CHECKING:
    from cognite.powerops.client.powerops_client import PowerOpsClient


class WatercourseClient:
    """Manage watercourses. Changes are directly applied to CDF."""

    def __init__(self, powerops: PowerOpsClient):
        self.powerops = powerops

    def retrieve(self, external_id: str) -> WatercourseConfig:
        ...

    def update(self, watercourse: WatercourseConfig):
        ...

    def copy(self, watercourse: WatercourseConfig, name: str) -> WatercourseConfig:
        """Create a copy of an existing watercourse, with a new name."""

    def delete(self, watercourse: WatercourseConfig) -> None:
        """Deletes a watercourse from CDF."""
