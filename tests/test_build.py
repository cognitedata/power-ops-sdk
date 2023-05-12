import toml

from cognite.powerops import version
from tests.constants import REPO_ROOT


def test_matching_versions():
    with (REPO_ROOT / "pyproject.toml").open() as fh:
        pyproject_toml = toml.load(fh)

    version_in_pyproject_toml = pyproject_toml["tool"]["poetry"]["version"]

    assert version.__version__ == version_in_pyproject_toml, (
        f"Version in pyproject.toml ({version_in_pyproject_toml}) does not match version in "
        f"cognite/powerops/version.py ({version.__version__})"
    )
