from typing import Optional, Tuple
import dataclasses
import enum
import sys


class ExitStatus(enum.Enum):
    """ """

    EXIT_SUCCESS = 0
    EXIT_FAILURE = 1


class MetaCommandResult(enum.Enum):
    """ """

    META_COMMAND_SUCCESS = 0
    META_COMMAND_UNRECOGNIZED_COMMAND = 1


class PrepareResult(enum.Enum):
    """ """

    PREPARE_SUCCESS = 0
    PREPARE_UNRECOGNIZED_STATEMENT = 1


class StatementType(enum.Enum):
    """ """

    STATEMENT_INSERT = "STATEMENT_INSERT"
    STATEMENT_SELECT = "STATEMENT_SELECT"


@dataclasses.dataclass
class InputBuffer:
    """ """

    buffer: Optional[str]
    buffer_length: int


@dataclasses.dataclass
class Statement:
    """ """

    statement_type: StatementType


def read_input(buffer: str) -> InputBuffer:
    """ """
    return InputBuffer(buffer=buffer, buffer_length=len(buffer))


def do_meta_command(input_buffer: InputBuffer) -> MetaCommandResult:
    """ """
    if input_buffer.buffer == ".exit":
        return MetaCommandResult.META_COMMAND_SUCCESS

    return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND


def close_input_buffer(input_buffer: InputBuffer) -> InputBuffer:
    """ """
    input_buffer.buffer = None
    input_buffer.buffer_length = 0
    return input_buffer


def prepare_statement(
    input_buffer: InputBuffer,
) -> Tuple[PrepareResult, Optional[Statement]]:
    """ """
    if input_buffer.buffer == "insert":
        statement = Statement(StatementType.STATEMENT_INSERT)
        return PrepareResult.PREPARE_SUCCESS, statement

    elif input_buffer.buffer == "select":
        statement = Statement(StatementType.STATEMENT_SELECT)
        return PrepareResult.PREPARE_SUCCESS, statement

    return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT, None


def execute_statement(statement: Statement) -> None:
    """ """
    if statement.statement_type == StatementType.STATEMENT_INSERT:
        print("This is where we would do an insert.")

    elif statement.statement_type == StatementType.STATEMENT_SELECT:
        print("This is where we would do an select.")


if __name__ == "__main__":
    while True:
        buffer = input("db > ")

        if len(buffer) == 0:
            print("Error reading input.")
            sys.exit(ExitStatus.EXIT_FAILURE.value)

        input_buffer = read_input(buffer)

        if input_buffer.buffer is not None and input_buffer.buffer[0] == ".":
            meta_command_result = do_meta_command(input_buffer)

            if meta_command_result == MetaCommandResult.META_COMMAND_SUCCESS:
                if input_buffer.buffer == ".exit":
                    close_input_buffer(input_buffer)
                    sys.exit(ExitStatus.EXIT_SUCCESS.value)

            elif (
                meta_command_result
                == MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND
            ):
                print(f"Unrecognized command '{input_buffer.buffer}'.")
                continue

        prepare_result, statement = prepare_statement(input_buffer)

        if prepare_result == PrepareResult.PREPARE_SUCCESS:
            pass

        elif prepare_result == PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
            print(f"Unrecognized keyword at start of '{input_buffer.buffer}'")
            continue

        assert statement is not None
        execute_statement(statement)
        print("Executed.")
