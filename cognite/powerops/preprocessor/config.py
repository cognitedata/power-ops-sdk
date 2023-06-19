LOG_LEVEL = "DEBUG"


class StatusEventConfig:
    EVENT_EXTERNAL_ID_PREFIX = "POWEROPS_PROCESS"
    EVENT_TYPE_PREFIX = "POWEROPS_PROCESS"
    EVENT_SOURCE = "CogShop"


class RelationshipsConfig:
    LABEL_PREFIX = "relationship_to."
    INCREMENTAL_MAPPING_LABEL = f"{LABEL_PREFIX}incremental_mapping_sequence"
    OBJECTIVE_SEQUENCE_LABEL = f"{LABEL_PREFIX}objective_sequence"
    SHOP_RUN_LOG_FILE_LABEL = f"{LABEL_PREFIX}log_file"
