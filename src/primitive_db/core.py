#!/usr/bin/env python3
from typing import Any, List

from .constants import VALID_TYPES
from .decorators import confirm_action, create_cacher, handle_db_errors, log_time
from .utils import load_table_data, save_table_data

_cache = create_cacher()


@handle_db_errors
def create_table(metadata: dict, table_name: str, columns: List[tuple]) -> dict:
    """
    Создает новую таблицу с указанными столбцами.

    Args:
        metadata (dict): Существующие метаданные таблиц.
        table_name (str): Имя создаваемой таблицы.
        columns (List[tuple]): Список кортежей (имя_столбца, тип_данных).

    Returns:
        dict: Обновленные метаданные с новой таблицей.

    Raises:
        ValueError: Если таблица уже существует или указан недопустимый тип.
    """
    if table_name in metadata:
        raise ValueError(f'Таблица "{table_name}" уже существует.')
    cols = [("ID", "int")] + columns
    for name, typ in cols:
        if typ not in VALID_TYPES:
            raise ValueError(f"Недопустимый тип: {typ}")
    metadata[table_name] = {"columns": cols}
    return metadata


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata: dict, table_name: str) -> dict:
    """
    Удаляет таблицу и очищает её данные.

    Args:
        metadata (dict): Метаданные таблиц.
        table_name (str): Имя таблицы для удаления.

    Returns:
        dict: Обновленные метаданные без удаленной таблицы.

    Raises:
        KeyError: Если таблица не существует.
    """
    if table_name not in metadata:
        raise KeyError(table_name)
    metadata.pop(table_name)
    try:
        save_table_data(table_name, [])  
    except Exception:
        pass
    return metadata


@handle_db_errors
def list_tables(metadata: dict) -> List[str]:
    """
    Возвращает список всех таблиц.

    Args:
        metadata (dict): Метаданные таблиц.

    Returns:
        List[str]: Список имен таблиц.
    """
    return list(metadata.keys())


@handle_db_errors
@log_time
def insert(metadata: dict, table_name: str, values: List[Any]) -> List[dict]:
    """
    Добавляет запись в таблицу с проверкой типов и генерацией ID.

    Args:
        metadata (dict): Метаданные таблиц.
        table_name (str): Имя таблицы.
        values (List[Any]): Список значений для столбцов (кроме ID).

    Returns:
        List[dict]: Все записи таблицы после добавления.

    Raises:
        KeyError: Если таблица не существует.
        ValueError: Если количество значений или типы неверны.
    """
    if table_name not in metadata:
        raise KeyError(table_name)
    cols = metadata[table_name]["columns"]
    expected = len(cols) - 1
    if len(values) != expected:
        raise ValueError(f"Ожидалось {expected} значений, получено {len(values)}")
    row = {}
    data = load_table_data(table_name)
    max_id = max((r.get("ID", 0) for r in data), default=0)
    new_id = max_id + 1
    row["ID"] = new_id
    for (col_name, col_type), val in zip(cols[1:], values):
        if col_type == "int" and not isinstance(val, int):
            raise ValueError(f"Ожидался int для {col_name}")
        if col_type == "str" and not isinstance(val, str):
            raise ValueError(f"Ожидался str для {col_name}")
        if col_type == "bool" and not isinstance(val, bool):
            raise ValueError(f"Ожидался bool для {col_name}")
        row[col_name] = val
    data.append(row)
    save_table_data(table_name, data)
    return data


@handle_db_errors
@log_time
def select(metadata: dict, table_name: str, where_clause: dict = None) -> List[dict]:
    """
    Возвращает записи таблицы с фильтром по where_clause.

    Args:
        metadata (dict): Метаданные таблиц.
        table_name (str): Имя таблицы.
        where_clause (dict, optional): Условие фильтрации {столбец: значение}.

    Returns:
        List[dict]: Список словарей с данными.

    Raises:
        KeyError: Если таблица не существует.
    """
    if table_name not in metadata:
        raise KeyError(table_name)
    data = load_table_data(table_name)
    key = f"{table_name}:{str(where_clause)}"
    def compute():
        if not where_clause:
            return data
        result = []
        for row in data:
            match = True
            for k, v in where_clause.items():
                if row.get(k) != v:
                    match = False
                    break
            if match:
                result.append(row)
        return result
    return _cache(key, compute)


@handle_db_errors
def update(metadata: dict, table_name: str, \
    set_clause: dict, where_clause: dict) -> List[dict]:
    """
    Обновляет записи таблицы по условию where_clause с заданными значениями.

    Args:
        metadata (dict): Метаданные таблиц.
        table_name (str): Имя таблицы.
        set_clause (dict): Словарь обновляемых значений {столбец: новое_значение}.
        where_clause (dict): Словарь условий фильтрации {столбец: значение}.

    Returns:
        List[dict]: Все записи таблицы после обновления.

    Raises:
        KeyError: Если таблица или столбец не существует.
        ValueError: Если тип нового значения не соответствует столбцу.
    """
    if table_name not in metadata:
        raise KeyError(table_name)
    cols = dict(metadata[table_name]["columns"])
    data = load_table_data(table_name)
    updated = 0
    for row in data:
        match = True
        for k, v in where_clause.items():
            if row.get(k) != v:
                match = False
                break
        if match:
            for sk, sv in set_clause.items():
                if sk not in cols:
                    raise KeyError(sk)
                expected_type = cols[sk]
                if expected_type == "int" and not isinstance(sv, int):
                    raise ValueError(f"Ожидался int для {sk}")
                if expected_type == "str" and not isinstance(sv, str):
                    raise ValueError(f"Ожидался str для {sk}")
                if expected_type == "bool" and not isinstance(sv, bool):
                    raise ValueError(f"Ожидался bool для {sk}")
                row[sk] = sv
            updated += 1
    save_table_data(table_name, data)
    return data


@handle_db_errors
@confirm_action("удаление записи")
def delete(metadata: dict, table_name: str, where_clause: dict) -> List[dict]:
    """
    Удаляет записи таблицы по условию where_clause.

    Args:
        metadata (dict): Метаданные таблиц.
        table_name (str): Имя таблицы.
        where_clause (dict): Условие удаления {столбец: значение}.

    Returns:
        List[dict]: Все записи таблицы после удаления.

    Raises:
        KeyError: Если таблица не существует.
    """
    if table_name not in metadata:
        raise KeyError(table_name)
    data = load_table_data(table_name)
    new_data = []
    deleted = 0
    for row in data:
        match = True
        for k, v in where_clause.items():
            if row.get(k) != v:
                match = False
                break
        if match:
            deleted += 1
            continue
        new_data.append(row)
    save_table_data(table_name, new_data)
    return new_data


@handle_db_errors
def info(metadata: dict, table_name: str) -> dict:
    """
    Возвращает информацию о таблице: столбцы и количество записей.

    Args:
        metadata (dict): Метаданные таблиц.
        table_name (str): Имя таблицы.

    Returns:
        dict: {"table": имя_таблицы, "columns": список_столбцов, "count": число_записей}

    Raises:
        KeyError: Если таблица не существует.
    """
    if table_name not in metadata:
        raise KeyError(table_name)
    cols = metadata[table_name]["columns"]
    data = load_table_data(table_name)
    return {"table": table_name, "columns": cols, "count": len(data)}
