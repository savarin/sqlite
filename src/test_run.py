from typing import List, Tuple

import run


def run_commands(commands: List[str]) -> Tuple[List[run.ExecuteResult], List[str]]:
    """ """
    execute_results: List[run.ExecuteResult] = []
    results: List[str] = []

    table = run.init_table()

    for i, command in enumerate(commands):
        input_buffer = run.read_input(command)
        _, statement = run.prepare_statement(input_buffer)
        assert statement is not None

        execute_result, table, result = run.execute_statement(statement, table)
        execute_results.append(execute_result)
        if result is not None:
            results += result

    return execute_results, results


def test_single_row():
    """ """
    execute_results, results = run_commands(
        ["insert 1 user1 person1@example.com", "select",]
    )

    assert len(execute_results) == 2
    assert execute_results[0] == run.ExecuteResult.EXECUTE_SUCCESS
    assert execute_results[1] == run.ExecuteResult.EXECUTE_SUCCESS

    assert len(results) == 1
    assert results[0] == "(1, user1, person1@example.com)"


def test_max_row():
    """ """
    commands = [
        f"insert {str(i)} user{str(i)} person{str(i)}@example.com" for i in range(1401)
    ]
    commands.append("select")

    execute_results, results = run_commands(commands)

    assert len(execute_results) == 1402
    assert set(execute_results[:1400]) == {run.ExecuteResult.EXECUTE_SUCCESS}
    assert execute_results[1400] == run.ExecuteResult.EXECUTE_TABLE_FULL
    assert execute_results[1401] == run.ExecuteResult.EXECUTE_SUCCESS

    assert len(results) == 1400
    for i in range(1400):
        assert results[i] == f"({str(i)}, user{str(i)}, person{str(i)}@example.com)"
