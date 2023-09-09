from __future__ import annotations

from typing import Protocol


class Echo(Protocol):
    """
    Protocol for the echo function used by resync to communicate with the user.
    """

    def __call__(self, message: str, is_warning: bool = False) -> None:
        """
        Message printer.

        Args:
            message: The message to print.
            is_warning: Whether the message is a warning or not.

        """
        ...
