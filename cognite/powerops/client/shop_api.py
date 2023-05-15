import random
import time
from typing import Optional

from cognite.client.data_classes import FileMetadata


class ShopRunLog:
    def __init__(self, shop_run_logs: "ShopRunLogs") -> None:
        self._shop_run_logs = shop_run_logs

    def file(self) -> FileMetadata:
        return FileMetadata(external_id="log-123", name="shop_123.log")

    def read(self) -> str:
        return "\n".join(f"log line {i}" for i in range(10))

    def print(self) -> None:
        print(self.read())


class ShopRunLogs:
    def __init__(self, shop_run_result: "ShopRunResult") -> None:
        self._shop_run_result = shop_run_result

    def cplex(self) -> ShopRunLog:
        return ShopRunLog(self)

    def post_run(self) -> ShopRunLog:
        return ShopRunLog(self)

    def shop(self) -> ShopRunLog:
        return ShopRunLog(self)


class ShopRunResult:
    def __init__(self, shop_run: "ShopRun") -> None:
        self._shop_run = shop_run

    @property
    def success(self) -> bool:
        if random.random() > 0.5:
            return True
        return False

    @property
    def error_message(self) -> Optional[str]:
        if not self.success:
            return "(sample) Error: invalid configuration"
        return None

    @property
    def logs(self) -> Optional[ShopRunLogs]:
        return ShopRunLogs(shop_run_result=self)

    @property
    def objective(self) -> Optional[list]:
        raise NotImplementedError


class ShopRun:
    def __init__(self, *, case=None, run_id=None) -> None:
        self.case = case
        self.run_id = run_id

    def is_complete(self) -> bool:
        """
        Query CDF and fetch the status of the run.
        """
        if random.random() > 0.7:
            return True
        return False

    def wait_until_complete(self) -> ShopRunResult:
        while not self.is_complete():
            print("SHOP is still running...")
            time.sleep(3)
        return ShopRunResult(shop_run=self)


class ShopModel:
    def __init__(self) -> None:
        self.model_id = random.randint(1000, 9999)

    def render_yaml(self) -> str:
        return "sintef_shop_model_yaml_representation"

    def update(self):
        raise NotImplementedError


class ShopModelsAPI:
    def __init__(self, shop_api: "ShopAPI"):
        self._shop_api = shop_api

    def list(self) -> list[ShopModel]:
        return ShopModel()

    def retrieve(self, model_id):
        m = ShopModel()
        m.model_id = model_id
        return m


class ShopRunsApi:
    def __init__(self, shop_api: "ShopAPI"):
        self._shop_api = shop_api

    def trigger(self, case) -> ShopRun:
        return ShopRun(case=case)

    def list(self) -> list[ShopRun]:
        raise NotImplementedError

    def retrieve(self, run_id):
        return ShopRun(run_id=run_id)


class ShopAPI:
    def __init__(self, po_client):
        self._po_client = po_client
        self.models = ShopModelsAPI(shop_api=self)
        self.runs = ShopRunsApi(shop_api=self)
