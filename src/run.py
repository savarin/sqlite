from typing import List, Optional, Tuple
import dataclasses
import enum
import sys


ID_SIZE: int = 4
USERNAME_SIZE: int = 32
EMAIL_SIZE: int = 255

ID_OFFSET: int = 0
USERNAME_OFFSET: int = ID_OFFSET + ID_SIZE
EMAIL_OFFSET: int = USERNAME_OFFSET + USERNAME_SIZE

ROW_SIZE: int = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

PAGE_SIZE: int = 4096
TABLE_MAX_PAGES: int = 100
ROWS_PER_PAGE: int = PAGE_SIZE // ROW_SIZE
TABLE_MAX_ROWS: int = ROWS_PER_PAGE * TABLE_MAX_PAGES


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
    PREPARE_SYNTAX_ERROR = 2


class ExecuteResult(enum.Enum):
    """ """

    EXECUTE_SUCCESS = 0
    EXECUTE_TABLE_FULL = 1


class StatementType(enum.Enum):
    """ """

    STATEMENT_INSERT = "STATEMENT_INSERT"
    STATEMENT_SELECT = "STATEMENT_SELECT"


@dataclasses.dataclass
class Table:
    """ """

    num_rows: int
    pages: List[Optional[bytearray]]


@dataclasses.dataclass
class Row:
    """ """

    row_id: int
    username: str
    email: str


@dataclasses.dataclass
class InputBuffer:
    """ """

    buffer: Optional[str]
    buffer_length: int


@dataclasses.dataclass
class Statement:
    """ """

    statement_type: StatementType
    row_to_insert: Optional[Row]


def init_table() -> Table:
    """ """
    return Table(num_rows=0, pages=[None for _ in range(TABLE_MAX_PAGES)])


def free_table(table: Table) -> None:
    """ """
    num_pages = table.num_rows // ROWS_PER_PAGE

    for i in range(num_pages):
        table.pages[i] = None

    table.num_rows = 0


def print_row(row: Row) -> None:
    """ """
    print(f"({str(row.row_id)}, {row.username}, {row.email})")


def serialize_row(row: Row) -> bytes:
    """ """
    return (
        row.row_id.to_bytes(ID_SIZE, byteorder="big")
        + row.username.encode("ascii")
        + (0).to_bytes(USERNAME_SIZE - len(row.username), byteorder="big")
        + row.email.encode("ascii")
        + (0).to_bytes(EMAIL_SIZE - len(row.email), byteorder="big")
    )


def deserialize_row(row_bytes: bytes) -> Row:
    """ """
    return Row(
        row_id=int.from_bytes(row_bytes[:USERNAME_OFFSET], byteorder="big"),
        username=row_bytes[USERNAME_OFFSET:EMAIL_OFFSET].decode("ascii"),
        email=row_bytes[EMAIL_OFFSET:].decode("ascii"),
    )


def read_input(buffer: str) -> InputBuffer:
    """ """
    return InputBuffer(buffer=buffer, buffer_length=len(buffer))


def close_input_buffer(input_buffer: InputBuffer) -> None:
    """ """
    input_buffer.buffer = None
    input_buffer.buffer_length = 0


def do_meta_command(input_buffer: InputBuffer) -> MetaCommandResult:
    """ """
    if input_buffer.buffer == ".exit":
        return MetaCommandResult.META_COMMAND_SUCCESS

    return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND


def prepare_statement(
    input_buffer: InputBuffer,
) -> Tuple[PrepareResult, Optional[Statement]]:
    """ """
    assert input_buffer.buffer is not None

    if input_buffer.buffer.startswith("insert"):
        arguments = input_buffer.buffer.split(" ")

        if len(arguments) < 4:
            return PrepareResult.PREPARE_SYNTAX_ERROR, None

        row = Row(row_id=int(arguments[1]), username=arguments[2], email=arguments[3])
        statement = Statement(
            statement_type=StatementType.STATEMENT_INSERT, row_to_insert=row
        )
        return PrepareResult.PREPARE_SUCCESS, statement

    elif input_buffer.buffer.startswith("select"):
        statement = Statement(
            statement_type=StatementType.STATEMENT_SELECT, row_to_insert=None
        )
        return PrepareResult.PREPARE_SUCCESS, statement

    return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT, None


def row_slot(table: Table, row_num: int) -> Tuple[bytearray, int, int]:
    """ """
    page_num = row_num // ROWS_PER_PAGE
    page = table.pages[page_num]

    if page is None:
        # Allocate memory only when we try to access a page.
        page = bytearray([0 for _ in range(PAGE_SIZE)])

    row_offset = row_num % ROWS_PER_PAGE
    byte_offset = row_offset * ROW_SIZE

    return page, page_num, byte_offset


def insert_row(row_bytes: bytes, table: Table) -> Table:
    """ """
    page, page_num, byte_offset = row_slot(table, table.num_rows)
    page[byte_offset : byte_offset + ROW_SIZE] = row_bytes

    table.pages[page_num] = page
    table.num_rows += 1
    return table


def execute_insert(statement: Statement, table: Table) -> Tuple[ExecuteResult, Table]:
    """ """
    if table.num_rows >= TABLE_MAX_ROWS:
        return ExecuteResult.EXECUTE_TABLE_FULL, table

    row_to_insert = statement.row_to_insert
    assert row_to_insert is not None

    row_bytes = serialize_row(row_to_insert)
    table = insert_row(row_bytes, table)

    return ExecuteResult.EXECUTE_SUCCESS, table


def select_row(table: Table, row_num: int) -> bytes:
    """ """
    page, _, byte_offset = row_slot(table, row_num)
    return page[byte_offset : byte_offset + ROW_SIZE]


def execute_select(statement: Statement, table: Table) -> Tuple[ExecuteResult, Table]:
    """ """
    for i in range(table.num_rows):
        row_bytes = select_row(table, i)
        row = deserialize_row(row_bytes)
        print_row(row)

    return ExecuteResult.EXECUTE_SUCCESS, table


def execute_statement(
    statement: Statement, table: Table
) -> Tuple[ExecuteResult, Table]:
    """ """
    if statement.statement_type == StatementType.STATEMENT_INSERT:
        return execute_insert(statement, table)

    assert statement.statement_type == StatementType.STATEMENT_SELECT
    return execute_select(statement, table)


if __name__ == "__main__":
    table = init_table()

    while True:
        buffer = input("db > ").lower()

        if len(buffer) == 0:
            print("Error reading input.")
            sys.exit(ExitStatus.EXIT_FAILURE.value)

        input_buffer = read_input(buffer)

        if input_buffer.buffer is not None and input_buffer.buffer[0] == ".":
            meta_command_result = do_meta_command(input_buffer)

            if meta_command_result == MetaCommandResult.META_COMMAND_SUCCESS:
                if input_buffer.buffer == ".exit":
                    close_input_buffer(input_buffer)
                    free_table(table)
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

        elif prepare_result == PrepareResult.PREPARE_SYNTAX_ERROR:
            print("Syntax error. Could not parse statement.")
            continue

        elif prepare_result == PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
            print(f"Unrecognized keyword at start of '{input_buffer.buffer}'")
            continue

        assert statement is not None
        execute_result, table = execute_statement(statement, table)

        if execute_result == ExecuteResult.EXECUTE_SUCCESS:
            print("Executed.")

        elif execute_result == ExecuteResult.EXECUTE_TABLE_FULL:
            print("Error: Table full")
