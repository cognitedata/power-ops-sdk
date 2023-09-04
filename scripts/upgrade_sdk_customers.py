"""
Assumes all customer repos are in checked out in a customer folder in the root of this repo.
"""

import subprocess
from tests.utils import chdir
from tests.constants import REPO_ROOT
from cognite.powerops.utils.io_file import read_toml_file, write_toml_file


def main():
    with chdir(REPO_ROOT):
        pyproject_toml = read_toml_file(REPO_ROOT / "pyproject.toml")
        sdk_version = pyproject_toml["tool"]["poetry"]["version"]
        print(f"Current version: {sdk_version}")

        for customer_repo in (REPO_ROOT / "customers").iterdir():
            if not customer_repo.is_dir() or not customer_repo.name.startswith("resync"):
                continue
            with chdir(customer_repo):
                customer_pyproject_toml = customer_repo / "pyproject.toml"
                customer_project_config = read_toml_file(customer_pyproject_toml)
                customer_sdk_version = customer_project_config["tool"]["poetry"]["dependencies"]["cognite-power-ops"]
                if customer_sdk_version.endswith(sdk_version):
                    print(f"Repo {customer_repo.name} has version {customer_sdk_version} == {sdk_version}. Skipping.")
                    continue
                print(f"Upgrading repo: {customer_repo.name} from {customer_sdk_version} to {sdk_version}")

                default_branch = (
                    subprocess.run(
                        "git symbolic-ref refs/remotes/origin/HEAD | sed 's@^refs/remotes/origin/@@'".split(),
                        capture_output=True,
                    )
                    .stdout.decode("utf-8")
                    .strip()
                )
                subprocess.run(["git", "checkout", default_branch])
                subprocess.run(["git", "pull"])
                subprocess.run(["git", "checkout", "-b", f"upgrade-sdk-{sdk_version}"])
                customer_project_config["tool"]["poetry"]["dependencies"]["cognite-power-ops"] = sdk_version
                write_toml_file(customer_pyproject_toml, customer_project_config)
                subprocess.run(["poetry", "run", "pip", "install", f"cognite-power-ops=={sdk_version}"])
                subprocess.run(["poetry", "update"])
                subprocess.run(["git", "commit", "-m", f"Upgrade SDK to {sdk_version}", "-a"])
                subprocess.run(["git", "push", "-u", "origin", f"upgrade-sdk-{sdk_version}"])
                subprocess.run(["git", "checkout", default_branch])


if __name__ == "__main__":
    main()
