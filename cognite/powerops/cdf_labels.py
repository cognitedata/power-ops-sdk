from cognite.client.data_classes import LabelDefinition

try:
    from enum import StrEnum
except ImportError:
    from strenum import StrEnum


class CDFLabel(StrEnum):
    @classmethod
    def as_label_definitions(cls) -> list[LabelDefinition]:
        return [
            LabelDefinition(
                external_id=label_external_id.value,
                name=label_external_id.value,
            )
            for label_external_id in cls
        ]


class AssetLabel(CDFLabel):
    BENCHMARKING_CONFIGURATION = "benchmarking_configuration"
    BID_PROCESS_CONFIGURATION = "bid_process_configuration"
    BID_MATRIX_GENERATOR_CONFIG_SEQUENCE = "bid_matrix_generator_config_sequence"
    INCREMENTAL_MAPPING_SEQUENCE = "incremental_mapping_sequence"
    CONNECTION_BYPASS = "connection_bypass"
    CONNECTION_SPILL = "connection_spill"
    CONNECTION_STANDARD = "connection_standard"
    DAYAHEAD_BIDDING_BENCHMARKING_CONFIG = "dayahead_bidding_benchmarking_config"
    GATE = "gate"
    GATE_RESERVOIR = "gate_reservoir"
    GENERATOR = "generator"
    GENERATOR_PLANT = "generator_plant"
    MARKET = "market"
    PLANT = "plant"
    PLANT_RESERVOIR = "plant_reservoir"
    PRICE_AREA = "price_area"
    RESERVE_GROUP = "reserve_group"
    RESERVE_GROUP_GENERATOR = "reserve_group_generator"
    RESERVOIR = "reservoir"
    RESERVOIR_GATE = "reservoir_gate"
    RESERVOIR_PLANT = "reservoir_plant"
    RKOM_BID_CONFIGURATION = "rkom_bid_configuration"
    RKOM_BID_COMBINATION_CONFIGURATION = "rkom_bid_combination_configuration"
    WATERCOURSE = "watercourse"


class RelationshipLabel(CDFLabel):
    BID_MATRIX_GENERATOR_CONFIG_SEQUENCE = "relationship_to.bid_matrix_generator_config_sequence"
    BID_MATRIX_SEQUENCE = "relationship_to.bid_matrix_sequence"
    RKOM_BID_FORM_SEQUENCE = "relationship_to.rkom_bid_form_sequence"
    BID_PROCESS_CONFIGURATION_ASSET = "relationship_to.bid_process_configuration_asset"
    BID_PROCESS_EVENT = "relationship_to.bid_process_event"
    CALCULATE_TOTAL_BID_MATRIX_EVENT = "relationship_to.calculate_total_bid_matrix_event"
    DAYAHEAD_PRICE_TIME_SERIES = "relationship_to.dayahead_price_time_series"
    GATE_DISCHARGE_TIME_SERIES = "relationship_to.gate_discharge_time_series"
    GENERATOR_DISCHARGE_TIME_SERIES = "relationship_to.generator_discharge_time_series"
    GENERATOR_PRODUCTION_TIME_SERIES = "relationship_to.generator_production_time_series"
    INCREMENTAL_MAPPING_SEQUENCE = "relationship_to.incremental_mapping_sequence"
    LOG_FILE = "relationship_to.log_file"
    MARKET_ASSET = "relationship_to.market_asset"
    MARKET_PRICE_TIME_SERIES = "relationship_to.market_price_time_series"
    MARKET_SALES_TIME_SERIES = "relationship_to.market_sales_time_series"
    OBJECTIVE_SEQUENCE = "relationship_to.objective_sequence"
    PARENT_COLLECTION = "relationship_to.parent_collection"
    PARENT_PROCESS = "relationship_to.parent_process"
    PLANT = "relationship_to.plant"
    PLANT_DISCHARGE_TIME_SERIES = "relationship_to.plant_discharge_time_series"
    PLANT_PRODUCTION_TIME_SERIES = "relationship_to.plant_production_time_series"
    PLANT_CONSUMPTION_TIME_SERIES = "relationship_to.plant_consumption_time_series"
    RESERVOIR_WATER_VALUE_TIME_SERIES = "relationship_to.reservoir_water_value_time_series"
    RESERVOIR_ENERGY_CONVERSION_FACTOR_SERIES = "relationship_to.reservoir_energy_conversion_factor_time_series"
    PRODUCTION_OBLIGATION_TIME_SERIES = "relationship_to.production_obligation_time_series"
    PRODUCTION_SCHEDULE = "relationship_to.production_schedule"
    CONSUMPTION_SCHEDULE = "relationship_to.consumption_schedule"
    RESERVOIR_LEVEL_TIME_SERIES = "relationship_to.reservoir_level_time_series"
    RESERVOIR_VOLUME_TIME_SERIES = "relationship_to.reservoir_volume_time_series"
    RKOM_COLLECTION_CONFIG = "relationship_to.rkom_collection_config"
    SHOP_RUN_EVENT = "relationship_to.shop_run_event"
    STATUS_EVENT_STARTED = "relationship_to.status_event_started"
    STATUS_EVENT_FINISHED = "relationship_to.status_event_finished"
    STATUS_EVENT_FAILED = "relationship_to.status_event_failed"
    TRIGGER_EVENT = "relationship_to.trigger_event"
    WATERCOURSE = "relationship_to.watercourse"
    INLET_RESERVOIR = "relationship_to.inlet_reservoir"
    GENERATOR = "relationship_to.generator"
    GENERATOR_EFFICIENCY_CURVE = "relationship_to.generator_efficiency_curve"
    TURBINE_EFFICIENCY_CURVE = "relationship_to.turbine_efficiency_curve"
    P_MIN_TIME_SERIES = "relationship_to.p_min_time_series"
    P_MAX_TIME_SERIES = "relationship_to.p_max_time_series"
    WATER_VALUE_TIME_SERIES = "relationship_to.water_value_time_series"
    FEEDING_FEE_TIME_SERIES = "relationship_to.feeding_fee_time_series"
    INLET_LEVEL_TIME_SERIES = "relationship_to.inlet_level_time_series"
    OUTLET_LEVEL_TIME_SERIES = "relationship_to.outlet_level_time_series"
    HEAD_DIRECT_TIME_SERIES = "relationship_to.head_direct_time_series"
    START_STOP_COST_TIME_SERIES = "relationship_to.start_stop_cost_time_series"
    IS_AVAILABLE_TIME_SERIES = "relationship_to.is_available_time_series"
    CASE_FILE = "relationship_to.case_file"
    CUT_FILE = "relationship_to.cut_file"
    MAPPING_FILE = "relationship_to.mapping_file"
    EXTRA_FILE = "relationship_to.extra_file"
