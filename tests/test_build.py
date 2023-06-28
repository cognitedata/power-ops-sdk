import re

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # py < 3.11

from cognite.powerops import version
from tests.constants import REPO_ROOT


def test_matching_versions():
    with (REPO_ROOT / "pyproject.toml").open("rb") as fh:
        pyproject_toml = tomllib.load(fh)
    version_in_pyproject_toml = pyproject_toml["tool"]["poetry"]["version"]

    changelog = (REPO_ROOT / "CHANGELOG.md").read_text()
    if not (changelog_version_result := re.search(r"\[(\d+\.\d+\.\d+)\]", changelog)):
        raise ValueError("Failed to obtain changelog version")
    changelog_version = changelog_version_result.groups()[0]

    assert version.__version__ == version_in_pyproject_toml == changelog_version, (
        f"Version in pyproject.toml ({version_in_pyproject_toml}) does not match version in "
        f"cognite/powerops/version.py ({version.__version__}) or in "
        f"the CHANGELOG.md {changelog_version}"
    )
