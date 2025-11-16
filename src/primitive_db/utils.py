#!/usr/bin/env python3
import json
import os

from .constants import DATA_DIR, META_FILE


def ensure_data_dir():
    """
    Проверяет наличие директории для хранения данных и создаёт её при необходимости.

    Эта функция гарантирует, что директория DATA_DIR существует перед
    чтением или записью файлов таблиц.
    """
    if not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)


def load_metadata(filepath: str = META_FILE) -> dict:
    """
    Загружает метаданные базы данных из JSON-файла.

    Args:
        filepath (str, optional): путь к файлу метаданных. По умолчанию META_FILE.

    Returns:
        dict: словарь метаданных таблиц. Пустой словарь, если файл не найден.
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str, data: dict) -> None:
    """
    Сохраняет метаданные базы данных в JSON-файл.

    Args:
        filepath (str): путь к файлу для сохранения
        data (dict): словарь метаданных для записи
    """
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def table_data_path(table_name: str) -> str:
    """
    Формирует путь к файлу с данными конкретной таблицы.

    Args:
        table_name (str): имя таблицы

    Returns:
        str: путь к JSON-файлу таблицы
    """
    ensure_data_dir()
    return os.path.join(DATA_DIR, f"{table_name}.json")


def load_table_data(table_name: str) -> list:
    """
    Загружает данные таблицы из JSON-файла.

    Args:
        table_name (str): имя таблицы

    Returns:
        list: список записей таблицы. Пустой список, если файл не найден.
    """
    path = table_data_path(table_name)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def save_table_data(table_name: str, data: list) -> None:
    """
    Сохраняет данные таблицы в JSON-файл.

    Args:
        table_name (str): имя таблицы
        data (list): список записей для сохранения
    """
    path = table_data_path(table_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
