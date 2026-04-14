from __future__ import annotations


def russian_plural(count: int, forms: tuple[str, str, str]) -> str:
    value = abs(int(count))
    mod10 = value % 10
    mod100 = value % 100
    if mod10 == 1 and mod100 != 11:
        return forms[0]
    if 2 <= mod10 <= 4 and not 12 <= mod100 <= 14:
        return forms[1]
    return forms[2]


def event_count_label(count: int) -> str:
    return f"{count} {russian_plural(count, ('событие', 'события', 'событий'))}"
