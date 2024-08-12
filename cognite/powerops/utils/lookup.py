def attr_lookup(value, attr_path):  # type: ignore[no-untyped-def]
    """
    Retrieve attributes from a nested structure, in a generator.

    Usage
    Some nested containers:
    >>> class A:
    ...     def __init__(self, b):
    ...         self.b = b
    >>> class B:
    ...     def __init__(self, list_of_cs):
    ...         self.cs = list_of_cs
    >>> class C:
    ...     def __init__(self, x):
    ...         self.x = x
    >>> a = A(B([C(1), C(2)]))

    >>> list(attr_lookup(a, ["b", "cs", each, "x"]))
    [1, 2]

    >>> list(attr_lookup(a, []))  # doctest: +ELLIPSIS
    [<powerops.utils.lookup.A object at 0x...>]
    >>> list(attr_lookup(a, ["b"]))  # doctest: +ELLIPSIS
    [<powerops.utils.lookup.B object at 0x...>]
    >>> list(attr_lookup(a, ["b", "cs"]))  # doctest: +ELLIPSIS
    [[<powerops.utils.lookup.C object at 0x...>, <powerops.utils.lookup.C object at 0x...>]]
    >>> list(attr_lookup(a, ["b", "cs", each]))  # doctest: +ELLIPSIS
    [<powerops.utils.lookup.C object at 0x...>, <powerops.utils.lookup.C object at 0x...>]

    Dicts are supported via special getter:
    >>> a.foo = {"bar": 11, "baz": 22}
    >>> list(attr_lookup(a, ["foo", dict_get, "baz"]))
    [22]

    Errors:
    >>> a.foo = [{"bar": 11}, "blah", {"bar": 33}]
    >>> list(attr_lookup(a, ["foo", each, dict_get, "bar"]))
    [11, None, 33]

    Callables can transform results:
    >>> def stringify(value, _attr_path):
    ...     yield f"here be {value}"
    >>> list(attr_lookup(a, ["foo", each, dict_get, "bar", stringify]))
    ['here be 11', None, 'here be 33']

    Callables can affect the traversal:
    >>> def only_dicts(value, attr_path):
    ...     filtered_value = [val for val in value if isinstance(val, dict)]
    ...     yield from attr_lookup(filtered_value, attr_path)
    >>> list(attr_lookup(a, ["foo", only_dicts, each, dict_get, "bar", stringify]))
    ['here be 11', 'here be 33']
    """
    if len(attr_path) == 0 or value is None:
        yield value
    else:
        sub_attr = attr_path[0]
        if callable(sub_attr):
            yield from sub_attr(value, attr_path[1:])
        else:
            yield from attr_lookup(getattr(value, sub_attr, None), attr_path[1:])


def each(items, attr_path):  # type: ignore[no-untyped-def]
    for item in items:
        yield from attr_lookup(item, attr_path)


def dict_get(value, attr_path):  # type: ignore[no-untyped-def]
    if isinstance(value, dict):
        yield from attr_lookup(value.get(attr_path[0]), attr_path[1:])
    else:
        yield None


def dict_values(value, attr_path):  # type: ignore[no-untyped-def]
    yield from attr_lookup(value.values(), attr_path)
