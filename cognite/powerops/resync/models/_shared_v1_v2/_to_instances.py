"""
The module contains functions for transforming the ReSync configuration into Cognite Data Model Types which are used
in multiple models. Note the use of the `make_ext_id` function which is used to generate unique external IDs for
all the conversions. This is done to ensure that the same external ID is used for the same object across multiple
transformations.
"""

from __future__ import annotations

import json
from hashlib import md5
from typing import Any


def make_ext_id(arg: Any, class_: type) -> str:
    hash_value = md5()
    if isinstance(arg, (str, int, float, bool)):
        hash_value.update(str(arg).encode())
    elif isinstance(arg, (list, dict, tuple)):
        hash_value.update(json.dumps(arg).encode())
    return f"{class_.__name__.removesuffix('Apply')}__{hash_value.hexdigest()}"
