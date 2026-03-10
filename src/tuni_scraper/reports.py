from collections.abc import Iterable, Sequence


def _stringify(value: object) -> str:
    if value is None:
        return "-"
    return str(value)


def render_table(title: str, headers: Sequence[str], rows: Iterable[Sequence[object]]) -> str:
    prepared_rows = [[_stringify(value) for value in row] for row in rows]
    widths = [len(header) for header in headers]

    for row in prepared_rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def format_row(values: Sequence[str]) -> str:
        return " | ".join(value.ljust(widths[index]) for index, value in enumerate(values))

    if not prepared_rows:
        return f"{title}\n(no data)"

    separator = "-+-".join("-" * width for width in widths)
    lines = [title, format_row(list(headers)), separator]
    lines.extend(format_row(row) for row in prepared_rows)
    return "\n".join(lines)