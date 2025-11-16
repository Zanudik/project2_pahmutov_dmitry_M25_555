#!/usr/bin/env python3
import shlex
from typing import Any, Dict, List, Tuple


def split_command(line: str) -> List[str]:
    """
    Разбивает строку команды на токены с учётом кавычек.

    Args:
        line (str): строка команды, введённая пользователем

    Returns:
        List[str]: список токенов команды
    """
    return shlex.split(line)


def parse_columns(columns: List[str]) -> List[Tuple[str, str]]:
    """
    Преобразует список строк вида 'name:type' в список кортежей (name, type).

    Args:
        columns (List[str]): список строк с определением столбцов

    Returns:
        List[Tuple[str, str]]: список кортежей (имя_столбца, тип_данных)

    Raises:
        ValueError: если столбец задан некорректно (без ':')
    """
    result = []
    for col in columns:
        if ":" not in col:
            raise ValueError(f"Некорректное определение столбца: {col}")
        name, typ = col.split(":", 1)
        result.append((name, typ))
    return result


def parse_where(expr_tokens: List[str]) -> Dict[str, Any]:
    """
    Парсит выражение WHERE вида <column> = <value>.

    Args:
        expr_tokens (List[str]): токены выражения WHERE

    Returns:
        Dict[str, Any]: словарь {column: value}

    Raises:
        ValueError: если выражение некорректное
    """
    if len(expr_tokens) < 3 or expr_tokens[1] != "=":
        raise ValueError("Некорректный where. Ожидается: <col> = <value>")
    key = expr_tokens[0]
    val_token = expr_tokens[2]
    return {key: _parse_value_token(val_token)}


def parse_set(expr_tokens: List[str]) -> Dict[str, Any]:
    """
    Парсит выражение SET для команды UPDATE.

    Args:
        expr_tokens (List[str]): токены выражения SET

    Returns:
        Dict[str, Any]: словарь {column: value} для обновления

    Raises:
        ValueError: если выражение некорректное
    """
    joined = " ".join(expr_tokens)
    parts = [p.strip() for p in joined.split(",")]
    res = {}
    for p in parts:
        if "=" not in p:
            raise ValueError("Некорректный set выражение")
        k, v = [s.strip() for s in p.split("=", 1)]
        res[k] = _parse_value_token(v)
    return res


def parse_values(value_token: str) -> List[Any]:
    """
    Парсит строку значений вида ("Alice", 30, true) в список Python-объектов.

    Args:
        value_token (str): строка значений

    Returns:
        List[Any]: список значений (str, int, bool)
    """
    s = value_token.strip()
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
    parts = []
    current = ""
    in_quote = False
    quote_char = None
    for ch in s:
        if ch in ('"', "'"):
            if not in_quote:
                in_quote = True
                quote_char = ch
                current += ch
            elif quote_char == ch:
                in_quote = False
                current += ch
            else:
                current += ch
        elif ch == "," and not in_quote:
            parts.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        parts.append(current.strip())
    return [_parse_value_token(p) for p in parts]


def _parse_value_token(tok: str):
    """
    Преобразует токен значения в Python-тип: str, int или bool.

    Args:
        tok (str): токен значения

    Returns:
        Any: значение в соответствующем типе
    """
    t = tok.strip()
    if (t.startswith('"') and t.endswith('"')) \
        or (t.startswith("'") and t.endswith("'")):
        return t[1:-1]
    if t.lower() in ("true", "false"):
        return t.lower() == "true"
    try:
        return int(t)
    except ValueError:
        pass
    return t
