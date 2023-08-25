import contextlib

import os
from pathlib import Path


@contextlib.contextmanager
def chdir(new_dir: Path) -> None:
    current_working_dir = os.getcwd()
    os.chdir(new_dir)

    try:
        yield

    finally:
        os.chdir(current_working_dir)
