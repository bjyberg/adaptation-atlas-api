"""SQL literal/identifier escaping helpers."""


def quote_literal(s: str) -> str:
    """Return a SQL string literal with single quotes escaped."""
    return "'" + str(s).replace("'", "''") + "'"


def quote_ident(name: str) -> str:
    """Return a SQL identifier wrapped in double quotes."""
    return '"' + str(name).replace('"', '""') + '"'
