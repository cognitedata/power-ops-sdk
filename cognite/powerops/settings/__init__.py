import logging

import tomli_w

from cognite.powerops.settings.data_classes import Settings

__all__ = ["settings"]

settings = Settings()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    # This is just a convenience for development, prints out all settings as TOML.
    # TOML has no support for `None`, filter it out:
    _dump = tomli_w.dumps(
        {
            group_key: {key: val for key, val in group_val.items() if val is not None}
            for group_key, group_val in settings.dict().items()
        }
    )
    print(_dump)
