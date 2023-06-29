from typing import Optional
from uuid import uuid4

import yaml
from cognite.client import CogniteClient

from cognite.powerops.preprocessor import knockoff_logging as logging
from cognite.powerops.preprocessor.cogreader import CogReader

logger = logging.getLogger(__name__)


def main(
    client: CogniteClient,
    fdm_space_external_id: str,
    fdm_case_external_id: str,
    output_data_set_id: int,
    fdm_model_external_id: Optional[str] = None,
    fdm_model_version: Optional[int] = None,
) -> dict:
    cog_reader = CogReader(
        client=client,
        fdm_space_external_id=fdm_space_external_id,
        fdm_case_external_id=fdm_case_external_id,
        fdm_model_external_id=fdm_model_external_id,
        fdm_model_version=fdm_model_version,
    ).run()

    cog_shop_case = cog_reader.cog_shop_case.to_dict()
    cog_shop_case_yaml = yaml.dump(cog_shop_case, allow_unicode=True, encoding="utf-8")

    cog_shop_file_list = cog_reader.get_cog_shop_file_list()

    xid = f"cog_shop_preprocessor/{fdm_space_external_id}/{fdm_case_external_id}/{str(uuid4())}"

    cog_shop_case_file_md = client.files.upload_bytes(
        content=cog_shop_case_yaml,
        name=f"{fdm_space_external_id}-{fdm_case_external_id}-case.yaml",
        mime_type="application/yaml",
        external_id=xid,
        metadata={
            "fdm:space_external_id": fdm_space_external_id,
            "fdm:case_external_id": fdm_case_external_id,
            "shop:type": "cog_shop_case",
        },
        data_set_id=output_data_set_id,
    )

    return {
        "cog_shop_case_file": cog_reader.file_metadata_to_dict(cog_shop_case_file_md),
        "cog_shop_file_list": cog_shop_file_list,
    }
