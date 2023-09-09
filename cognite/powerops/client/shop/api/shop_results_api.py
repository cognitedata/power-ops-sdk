from __future__ import annotations

import json
import logging
from json import JSONDecodeError
from typing import Optional

import pandas as pd
from cognite.client import CogniteClient

from cognite.powerops.cdf_labels import RelationshipLabel
from cognite.powerops.client.shop.api.shop_result_files_api import ShopFilesAPI
from cognite.powerops.client.shop.data_classes import ShopRun
from cognite.powerops.client.shop.data_classes.shop_result_files import ShopLogFile, ShopYamlFile
from cognite.powerops.client.shop.data_classes.shop_results import ObjectiveFunction, ShopRunResult
from cognite.powerops.client.shop.data_classes.shop_results_compare import ShopResultsCompare
from cognite.powerops.utils.cdf.calls import retrieve_relationships_from_source_ext_id

logger = logging.getLogger(__name__)


class ShopRunResultsAPI:
    def __init__(self, client: CogniteClient, files_api: ShopFilesAPI):
        self._client = client
        self._files_api = files_api
        self.compare = ShopResultsCompare()

    def retrieve(self, shop_run: ShopRun) -> ShopRunResult:
        """
        Retrieve the results of a SHOP run.

        Args:
            shop_run: The SHOP run to retrieve results for.

        Returns:
            The results of the SHOP run.
        """
        if shop_run.in_progress:
            raise ValueError("ShopRun not completed.")

        post_run = None
        cplex = None
        shop_messages = None

        related_log_files = self._files_api.retrieve_related_files_metadata(
            source_external_id=shop_run.shop_run_event.external_id, label_ext_id=RelationshipLabel.LOG_FILE.value
        )
        for metadata in related_log_files:
            ext_id = metadata.external_id
            if ext_id.endswith(".log") and "cplex" in ext_id:
                cplex: Optional[ShopLogFile] = self._files_api.log_files.retrieve(metadata)
            elif ext_id.endswith(".log") and "shop_messages" in ext_id:
                shop_messages: Optional[ShopLogFile] = self._files_api.log_files.retrieve(metadata)
            elif ext_id.endswith(".yaml"):
                post_run: Optional[ShopYamlFile] = self._files_api.yaml_files.retrieve(metadata)
            else:
                logger.error("Unknown file type")
        return ShopRunResult(self.retrieve_objective_function, shop_run, cplex, shop_messages, post_run)

    def retrieve_objective_function(self, shop_run: ShopRun) -> ObjectiveFunction:
        """
        Retrieve the objective function of a SHOP run.

        Args:
            shop_run: The SHOP run to retrieve the objective function for.

        Returns:
            The objective function of the SHOP run.

        """
        # TODO: ability to retrieve the objective function from the post run yaml file
        relationships = retrieve_relationships_from_source_ext_id(
            self._client,
            shop_run.shop_run_event.external_id,
            RelationshipLabel.OBJECTIVE_SEQUENCE,
            target_types=["sequence"],
        )
        sequences = self._client.sequences.retrieve_multiple(external_ids=[r.target_external_id for r in relationships])
        for seq in sequences:
            # sometimes the label is given to the non-objective sequence too
            if "objective" not in seq.name.lower():
                continue
            # Data is inserted as a single row DataFrame
            seq_data = self._client.sequences.data.retrieve_dataframe(
                external_id=seq.external_id, start=0, end=-1
            ).to_dict("records")[0]
            # This shape will always be (72, 7) 72 fields and 7 properties of each field
            column_definitions_df = pd.DataFrame(seq.columns)

            try:
                penalty_breakdown = json.loads(seq.metadata.get("shop:penalty_breakdown", "{}"))
                if not isinstance(penalty_breakdown, dict):
                    penalty_breakdown = {}
            except JSONDecodeError:
                penalty_breakdown = {}

            return ObjectiveFunction(
                external_id=seq.external_id,
                data=seq_data,
                watercourse=seq.metadata.get("shop:watercourse", ""),
                penalty_breakdown=penalty_breakdown,
                field_definitions_df=column_definitions_df,
            )

        logger.error("Objective function sequence not found")
        raise ValueError("Objective function sequence not found")
