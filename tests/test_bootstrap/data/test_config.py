from bootstrap.data_classes.shop_file_config import ShopFileConfig
from bootstrap.config import WatercourseConfig

watercourse_configs = [
    WatercourseConfig(
        name="Fornebu",
        directory="fornebu",
        model_raw="model_raw.yaml",
        model_mapping="model_mapping.yaml",
        model_processed="model_processed.yaml",
        market_to_price_area={"1": "NO2"},
        version="1",
        # --- SHOP files
        yaml_raw_path="data/demo/fornebu/model_raw.yaml",
        yaml_mapping_path="data/demo/fornebu/model_mapping.yaml",
        yaml_processed_path="data/demo/fornebu/model_processed.yaml",
        other_shop_files=[
            ShopFileConfig(
                cogshop_file_type="commands", path="data/demo/fornebu/commands.yaml", watercourse_name="fornebu"
            ),
            ShopFileConfig(
                cogshop_file_type="water_value_cut_file",
                path="data/demo/fornebu/water_value_cut_file.dat",
                watercourse_name="fornebu",
            ),
            ShopFileConfig(
                cogshop_file_type="water_value_cut_file_reservoir_mapping",
                path="data/demo/fornebu/water_value_cut_file_reservoir_mapping.txt",
                watercourse_name="fornebu",
            ),
            ShopFileConfig(
                cogshop_file_type="extra_data",
                path="data/demo/fornebu/extra_data_sum_flow_restriction_Suldalsvann.ascii",
                watercourse_name="fornebu",
            ),
            ShopFileConfig(
                cogshop_file_type="extra_data",
                path="data/demo/fornebu/extra_data_sum_restriction_NovleT2.ascii",
                watercourse_name="fornebu",
            ),
        ],
    )
]
