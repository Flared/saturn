from saturn_engine.utils.traceback_data import format_local


def test_format_local() -> None:
    assert format_local("123") == "'123'"
    assert format_local(b"123") == "b'123'"
    assert format_local(123) == "123"
    assert format_local(1.23) == "1.23"
    assert (
        format_local({"a": 123, "b": (123, 4), "c": [1, 2, {"e": "f"}]})
        == "{'a': 123, 'b': <class 'tuple'>[123, 4], 'c': [1, 2, {'e': 'f'}]}"
    )

    assert (
        format_local({"a": "a", "b": "b" * 80, "c": "c"})
        == "{'a': 'a', 'b': '" + "b" * 66 + "<...>"
    )
