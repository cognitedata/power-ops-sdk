from datetime import datetime

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList


@classmethod
class SHOPRun(CogniteResource):
    external_id: str
    watercourse: str
    start: datetime
    end: datetime


class SHOPRunList(CogniteResourceList[SHOPRun]):
    """
    This represents a list of SHOP runs.
    """

    _RESOURCE = SHOPRun
