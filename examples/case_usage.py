import logging
import os
import tempfile

from cognite.powerops import Case, PowerOpsClient
from cognite.powerops.logger import configure_debug_logging

# some nice logging:
configure_debug_logging(level=logging.INFO)

# init the SDK:
powerops = PowerOpsClient(
    read_dataset="uc:001:shop:dataset",
    write_dataset="sc:000:sandbox:dataset",
)

# we'll be downloading files, need a dir:
# (using tempfile here, but anything works)
tmp_dir = tempfile.mkdtemp(prefix="power-ops-sdk-usage")

# get a file:
file_meta = powerops.cdf.files.retrieve(external_id="SHOP_Fornebu_model")
powerops.cdf.files.download(
    directory=tmp_dir,
    external_id="SHOP_Fornebu_model",
)
case_file = os.path.join(tmp_dir, file_meta.name)


# load the file:
case = Case.load_yaml(case_file)


# get a value:
print(case["model.creek_intake.Golebiowski_intake.net_head"])

# get a list:
print(case["model.generator.Holen_G1.gen_eff_curve.y"])

# modify a list: a gen_eff_curve, add +10 to Y:
case["model.generator.Holen_G1.gen_eff_curve.y"] = [
    val + 10 for val in case["model.generator.Holen_G1.gen_eff_curve.y"]
]
# TODO ^ Cast lists to pd.Series automatically? Then need to handle setter, too.

# export yaml:
edited_case_file = os.path.join(tmp_dir, "edited_case.yaml")
case.save_yaml(edited_case_file)

# (just for show) load that exported yaml and check the edited curve:
case2 = Case.load_yaml(edited_case_file)
print(case2["model.generator.Holen_G1.gen_eff_curve.y"])
