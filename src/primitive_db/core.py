#!/usr/bin/env python3
from typing import List, Dict, Any
from .constants import VALID_TYPES
from .decorators import handle_db_errors, log_time, confirm_action, create_cacher
from .utils import save_metadata, load_metadata, load_table_data, save_table_data

_cache = create_cacher()


@handle_db_errors
def create_table(metadata: dict, table_name: str, columns: List[tuple]) -> dict:
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
    return list(metadata.keys())


@handle_db_errors
@log_time
def insert(metadata: dict, table_name: str, values: List[Any]) -> List[dict]:
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
def update(metadata: dict, table_name: str, set_clause: dict, where_clause: dict) -> List[dict]:
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
    if table_name not in metadata:
        raise KeyError(table_name)
    cols = metadata[table_name]["columns"]
    data = load_table_data(table_name)
    return {"table": table_name, "columns": cols, "count": len(data)}
