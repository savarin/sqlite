from typing import Optional
import dataclasses
import enum
import sys


class ExitStatus(enum.Enum):
    """ """

    EXIT_SUCCESS = 0
    EXIT_FAILURE = 1


@dataclasses.dataclass
class InputBuffer:
    """ """

    buffer: Optional[str]
    buffer_length: int


def init_input_buffer() -> InputBuffer:
    """ """
    return InputBuffer(buffer=None, buffer_length=0)


def read_input(buffer: str, input_buffer: InputBuffer) -> InputBuffer:
    """ """
    input_buffer.buffer = buffer
    input_buffer.buffer_length = len(input_buffer.buffer)
    return input_buffer


def close_input_buffer(input_buffer: InputBuffer) -> InputBuffer:
    """ """
    input_buffer.buffer = None
    input_buffer.buffer_length = 0
    return input_buffer


if __name__ == "__main__":
    input_buffer = init_input_buffer()

    while True:
        buffer = input("db > ")

        if len(buffer) == 0:
            print("Error reading input.")
            sys.exit(ExitStatus.EXIT_FAILURE.value)

        input_buffer = read_input(buffer, input_buffer)

        if input_buffer.buffer == ".exit":
            close_input_buffer(input_buffer)
            sys.exit(ExitStatus.EXIT_SUCCESS.value)
        else:
            print(f"Unrecognized command '{input_buffer.buffer}'.")
