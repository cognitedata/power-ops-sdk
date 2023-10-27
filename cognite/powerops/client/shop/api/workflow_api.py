import itertools
from itertools import chain

from cognite.client import CogniteClient

from cognite.powerops.client.shop.shop_run import SHOPRun
from cognite.powerops.client.shop.shop_run_api import SHOPRunAPI
from cognite.powerops.client.shop.data_classes.workflow import Workflow, ShopCase


class WorkflowAPI:
    def __init__(
            self, client: CogniteClient, data_set: int, cogshop_version: str
    ):
        self._client = client
        self._data_set_api = data_set
        self._cogshop_version = cogshop_version
        self.shop_run = SHOPRunAPI(client, data_set, cogshop_version)

    def trigger_workflow(self, workflow: Workflow) -> list[SHOPRun]:
        """
        loops through the cases to run in the workflow and trigger all the shop runs neccessary for each case
        """
        shop_runs = []
        for case in workflow.cases:
            shop_runs.extend(self.shop_run.trigger_prerun_files(case.pre_runs))
        return shop_runs