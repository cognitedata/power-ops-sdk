import doctest
from pprint import pformat
from typing import Union


def format_change_binary(
    deep_diff: dict[str, dict],
) -> list[str]:
    """
    Formats a dict of changes to a list of strings

    >>> deep_diff = {
    ...     "root[0][plants][name]": {
    ...         "old_value": "Old name",
    ...         "new_value": "New name",
    ...     },
    ... }
    >>> format_change_binary(deep_diff)
    [' * [0][plants][name]:\n', '\t- Old name\t', ' -->   ','New name\n', '\n']
    """
    str_builder = []
    for path_, change_dict in deep_diff.items():
        str_builder.extend(
            (
                f" * {path_.replace('root', '') }:\n",
                f'\t- {pformat(change_dict.get("old_value"))}\t',
                "  -->   ",
                f'{pformat(change_dict.get("new_value"))}\n',
                "\n",
            )
        )
    return str_builder


def format_change_unary(
    deep_diff: dict[str, list[Union[str, dict]]],
    is_iterable: bool,
) -> list[str]:
    """
    Formats a dict of changes to a list of strings

    TODO: doctest

    """
    str_builder = []
    for _path, change in deep_diff.items():
        # remove the index/key of which the item was added/removed in path
        _path = _path[: _path.rfind("[")] if is_iterable else _path

        str_builder.extend((f" * {_path[:_path.rfind('[')].replace('root', '')}:\n", f"\t- {pformat(change)}\n"))
    return str_builder

    # @classmethod
    # def _pretty_difference_str_builder(
    #     cls,
    #     resource,
    #     resource_diff: dict[str, dict],
    #     diff_base: list, # a list since ProductionModel only has lists under it
    #     ) -> list[str]:
    #     str_builder = [f'-----------{resource}-----------']
    #     print("base")
    #     print(pformat(base))
    #     for diff_type, diffs in resource_diff.items():
    #         print(f"{diff_type=}")
    #         str_builder.append("The following fields have changed:")
    #         str_builder.append(f"{diff_type=}")

    #         for k, v in diffs.items():
    #             old = v.get('old_value')
    #             new = v.get('new_value')
    #             str_builder.append(f"{k}: \n{pformat(old)} \n->\n{pformat(new)}")
    #             str_builder.append('--')

    #                 # print(f"{v.get('old_value')=}")
    #                 # print(f"{v.get('new_value')=}")
    #             print('--')
    #         # print(f"**{resource}**\n {diff_type}:" f'\n')

    #         str_builder.append('-----------')
    #         # str_builder.extend((f"**{resource}**\n {diff_type}:", f'\n'))

    #     return str_builder


if __name__ == "__main__":
    doctest.testmod()
