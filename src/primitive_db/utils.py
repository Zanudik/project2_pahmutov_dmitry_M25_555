#!/usr/bin/env python3
import json
import os
from typing import Any
from .constants import META_FILE, DATA_DIR


def ensure_data_dir():
    if not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)


def load_metadata(filepath: str = META_FILE) -> dict:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str, data: dict) -> None:
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def table_data_path(table_name: str) -> str:
    ensure_data_dir()
    return os.path.join(DATA_DIR, f"{table_name}.json")


def load_table_data(table_name: str) -> list:
    path = table_data_path(table_name)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def save_table_data(table_name: str, data: list) -> None:
    path = table_data_path(table_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
