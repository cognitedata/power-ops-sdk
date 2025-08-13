"""
Use this script to update the GitHub secrets for the repo.

This script will update all the required secrets for the given CDF project based on the environment variables set
in the provided dotenv file. Ensure all the below environment variables are set in the dotenv file before running and
you have the required dependencies installed and authenticated.

DEPENDENCIES:
    - GitHub CLI (gh): https://cli.github.com/
    - Run  `gh auth login` to authenticate with your GitHub account.

EXAMPLE DOTENV FILE (.sandbox.env):
    PROJECT=power-ops-sandbox
    CDF_CLUSTER="bluefield"
    CLIENT_ID=xxx
    CLIENT_SECRET=xxx
    WF_TRIGGER_SECRET=xxx
    TENANT_ID=xxx
    TOOLKIT_ENV=sandbox # See `CONTRIBUTING.md` how to set this value

EXAMPLE USAGE:
    python3 scripts/update_gh_env_secrets.py .sandbox.env

If no path to an env file is provided, the script will use the default .env file in the current directory.

"""

import os
import sys
import dotenv
import subprocess
import json
import base64


REPO_NAME = "cognitedata/power-ops-sdk"

# List of required environment variables with its corresponding GitHub secret name
REQUIRED_ENVS = {
    "PROJECT": None,
    "CDF_CLUSTER": "CLUSTER",
    "CLIENT_ID": "CLIENT_ID",
    "CLIENT_SECRET": "CLIENT_SECRET",
    "TENANT_ID": "TENANT_ID",
    "TOOLKIT_ENV": "TOOLKIT_ENV",
}


def base64_encode_dict(secret: dict) -> str:
    json_secret = json.dumps(secret)
    json_secret_bytes = json_secret.encode("utf-8")
    return base64.b64encode(json_secret_bytes).decode("utf-8")


def set_github_secret(cdf_project: str, env_name: str, env_value: str):
    try:
        subprocess.run(
            [
                "gh",
                "secret",
                "set",
                env_name,
                "--repo",
                REPO_NAME,
                "--env",
                cdf_project,
                "--body",
                env_value,
            ],
            check=True,
        )
    except subprocess.CalledProcessError:
        print(f"Failed to set GitHub secret '{env_name}' for project '{cdf_project}'.")
        pass


def check_missing_vars():
    missing_vars = [env for env in REQUIRED_ENVS.keys() if os.getenv(env) is None]

    if missing_vars:
        print("The following required environment variables are not set:")
        for var in missing_vars:
            print(f"  - {var}")
        exit(1)


def set_all_github_secrets_from_name_mapping(cdf_project):
    for env, gh_secret in REQUIRED_ENVS.items():
        value = os.getenv(env)
        if isinstance(gh_secret, list):
            for secret in gh_secret:
                set_github_secret(cdf_project, secret, value)
        elif gh_secret is not None:
            set_github_secret(cdf_project, gh_secret, value)


if __name__ == "__main__":
    # Check if a custom env file path is provided
    env_file_path = ".env"
    if len(sys.argv) == 2:
        env_file_path = sys.argv[1]

    print(f"Using env file: {env_file_path}")

    print()
    # Load environment variables from the specified .env file and check for missing variables
    dotenv.load_dotenv(env_file_path)
    check_missing_vars()

    # Set GitHub secrets
    cdf_project = os.getenv("PROJECT")
    print(f"Setting GitHub secrets in env {cdf_project}...")
    set_all_github_secrets_from_name_mapping(cdf_project)
    print()
