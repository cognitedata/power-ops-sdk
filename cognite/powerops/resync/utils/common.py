import json
import re
from hashlib import md5


def special_case_handle_gate_number(name: str) -> None:
    """Must handle any gate numbers above 1 if found in other sources than YAML"""
    # TODO: extend to handle special case if needed
    if re.search(pattern=r"L[2-9]", string=name):
        print_warning(f"Potential gate {name} not in YAML!")


def print_warning(s: str) -> None:
    """Adds some nice colors to the printed text :)"""
    print(f"\033[91m[WARNING] {s}\033[0m")


def make_ext_id(*args: str, prefix: str) -> str:
    hash_value = md5()
    for args in args:
        if isinstance(args, (str, int, float, bool)):
            hash_value.update(str(args).encode())
        elif isinstance(args, (list, dict)):
            hash_value.update(json.dumps(args).encode())
    return f"{prefix}__{hash_value.hexdigest()}"
