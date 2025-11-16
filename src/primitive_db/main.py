#!/usr/bin/env python3
from .engine import run


def main():
    """
    Точка входа для запуска базы данных.

    Вызывает функцию run из engine.py, которая обрабатывает команды пользователя.
    """
    run()

if __name__ == "__main__":
    main()
