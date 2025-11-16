#!/usr/bin/env python3
import time
from functools import wraps
from typing import Any, Callable


def handle_db_errors(func: Callable) -> Callable:
    """
    Декоратор для обработки ошибок функций работы с базой данных.

    Перехватывает:
        - FileNotFoundError: если файл данных отсутствует
        - KeyError: если таблица или столбец не найден
        - ValueError: ошибки валидации
        - другие исключения: вывод сообщения об ошибке

    Возвращает None при ошибке.
    """
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
    """
    Декоратор для обработки ошибок функций работы с базой данных.

    Перехватывает:
        - FileNotFoundError: если файл данных отсутствует
        - KeyError: если таблица или столбец не найден
        - ValueError: ошибки валидации
        - другие исключения: вывод сообщения об ошибке

    Возвращает None при ошибке.
    """
    def deco(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            ans = input(f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: ').strip().lower()
            if ans != "y":
                print("Операция отменена.")
                return None
            return func(*args, **kwargs)
        return wrapper
    return deco


def log_time(func: Callable) -> Callable:
    """
    Декоратор для измерения и вывода времени выполнения функции.

    После выполнения функции выводит сообщение:
        Функция <имя_функции> выполнилась за <время> секунд.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        return result
    return wrapper


def create_cacher():
    """
    Создает функцию-кешер для хранения результатов вычислений по ключу.

    Возвращает функцию cache_result(key, value_func), которая:
        - возвращает закешированное значение для ключа, если есть
        - иначе вызывает value_func(), сохраняет результат в кеш и возвращает его

    Пример использования:
        cache = create_cacher()
        result = cache("some_key", lambda: expensive_computation())
    """
    cache = {}

    def cache_result(key: str, value_func: Callable[[], Any]):
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value

    return cache_result
