from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

SENSITIVE_TESTS = REPO_ROOT / "sensitive_tests.toml"


class ReSync:
    data = REPO_ROOT / "tests" / "data"
    demo = data / "demo"
    production = demo / "production"
    market = demo / "market"
    cogshop = demo / "cogshop"
