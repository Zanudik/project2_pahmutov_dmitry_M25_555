#!/usr/bin/env python3
import time
from functools import wraps
from typing import Any, Callable


def handle_db_errors(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print("Ошибка: Файл данных не найден.")
        except KeyError as e:
            print(f"Ошибка: Таблица или столбец {e} не найден.")
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
        return None  
    return wrapper


def confirm_action(action_name: str):
    def deco(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            ans = input(f'Вы уверены, что хотите выполнить \
                "{action_name}"? [y/n]: ').strip().lower()
            if ans != "y":
                print("Операция отменена.")
                return None
            return func(*args, **kwargs)
        return wrapper
    return deco


def log_time(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        return result
    return wrapper


def create_cacher():
    cache = {}

    def cache_result(key: str, value_func: Callable[[], Any]):
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value

    return cache_result
