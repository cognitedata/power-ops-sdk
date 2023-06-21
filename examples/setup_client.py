from pathlib import Path

from cognite.powerops import PowerOpsClient

config = Path(__file__).resolve().parent.parent / "config.yaml"

powerops = PowerOpsClient.from_file(config)

print(powerops.client.iam.token.inspect())
