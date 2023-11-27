"""
Helper script that builds and deploys the frontend interface model to CDF.

It populates the environment variables required by cdf-tk from the settings
files (settings.toml and .secrets.toml), so no need for a local .env file.
"""
import os
import subprocess

from pathlib import Path

from cognite.powerops.utils.cdf import Settings

ROOT = Path(__file__).parent.parent
CWD = ROOT / "cognite/powerops/resync/models/v2/dms/dayAheadFrontendContract"


def main():
    settings = Settings()
    env = os.environ.copy()
    env["CDF_CLUSTER"] = settings.cognite.cdf_cluster
    env["CDF_PROJECT"] = settings.cognite.project
    env["IDP_CLIENT_ID"] = settings.cognite.client_id
    env["IDP_CLIENT_SECRET"] = settings.cognite.client_secret
    env["IDP_TOKEN_URL"] = f"https://login.microsoftonline.com/{settings.cognite.tenant_id}/oauth2/v2.0/token"

    subprocess.check_call(["cdf-tk", "build", ".", "--clean", "--env=local"], env=env, cwd=CWD)
    subprocess.check_call(["cdf-tk", "deploy", "--env=local"], env=env, cwd=CWD)


if __name__ == "__main__":
    main()
