from typing import List, Tuple

import run


def run_commands(
    commands: List[str],
) -> Tuple[List[run.PrepareResult], List[run.ExecuteResult], List[str]]:
    """ """
    prepare_results: List[run.PrepareResult] = []
    execute_results: List[run.ExecuteResult] = []
    results: List[str] = []

    table = run.init_table()

    for i, command in enumerate(commands):
        input_buffer = run.read_input(command)
        prepare_result, statement = run.prepare_statement(input_buffer)

        prepare_results.append(prepare_result)
        if statement is None:
            continue

        execute_result, table, result = run.execute_statement(statement, table)
        execute_results.append(execute_result)
        if result is not None:
            results += result

    return prepare_results, execute_results, results


def test_single_standard_row():
    """ """
    prepare_results, execute_results, results = run_commands(
        ["insert 1 user1 person1@example.com", "select"]
    )

    assert len(prepare_results) == 2
    assert prepare_results[0] == run.PrepareResult.PREPARE_SUCCESS
    assert prepare_results[1] == run.PrepareResult.PREPARE_SUCCESS

    assert len(execute_results) == 2
    assert execute_results[0] == run.ExecuteResult.EXECUTE_SUCCESS
    assert execute_results[1] == run.ExecuteResult.EXECUTE_SUCCESS

    assert len(results) == 1
    assert results[0] == "(1, user1, person1@example.com)"


def test_single_wide_row():
    """ """
    prepare_results, execute_results, results = run_commands(
        [f"insert 1 {'a'*32} {'a'*255}", "select"]
    )

    assert len(prepare_results) == 2
    assert prepare_results[0] == run.PrepareResult.PREPARE_SUCCESS
    assert prepare_results[1] == run.PrepareResult.PREPARE_SUCCESS

    assert len(execute_results) == 2
    assert execute_results[0] == run.ExecuteResult.EXECUTE_SUCCESS
    assert execute_results[1] == run.ExecuteResult.EXECUTE_SUCCESS

    assert len(results) == 1
    assert results[0] == f"(1, {'a'*32}, {'a'*255})"


def test_single_overflow_row():
    """ """
    prepare_results, execute_results, results = run_commands(
        [f"insert 1 {'a'*33} {'a'*256}", "select"]
    )

    assert len(prepare_results) == 2
    assert prepare_results[0] == run.PrepareResult.PREPARE_STRING_TOO_LONG
    assert prepare_results[1] == run.PrepareResult.PREPARE_SUCCESS

    assert len(execute_results) == 1
    assert execute_results[0] == run.ExecuteResult.EXECUTE_SUCCESS

    assert len(results) == 0


def test_max_count_rows():
    """ """
    commands = [
        f"insert {str(i)} user{str(i)} person{str(i)}@example.com" for i in range(1401)
    ]
    commands.append("select")

    prepare_results, execute_results, results = run_commands(commands)

    assert len(prepare_results) == 1402
    assert set(prepare_results) == {run.PrepareResult.PREPARE_SUCCESS}

    assert len(execute_results) == 1402
    assert set(execute_results[:1400]) == {run.ExecuteResult.EXECUTE_SUCCESS}
    assert execute_results[1400] == run.ExecuteResult.EXECUTE_TABLE_FULL
    assert execute_results[1401] == run.ExecuteResult.EXECUTE_SUCCESS

    assert len(results) == 1400
    for i in range(1400):
        assert results[i] == f"({str(i)}, user{str(i)}, person{str(i)}@example.com)"
