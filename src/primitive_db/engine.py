#!/usr/bin/env python3
import shlex
from prettytable import PrettyTable
from .utils import load_metadata, save_metadata, load_table_data
from .parser import split_command, parse_columns, parse_where, parse_set, parse_values
from .core import create_table, drop_table, list_tables, insert, select, update, delete, info
from .constants import META_FILE


def print_help():
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("list_tables - показать список всех таблиц")
    print("drop_table <имя_таблицы> - удалить таблицу")
    print("insert into <имя_таблицы> values (v1, v2, ...)")
    print("select from <имя_таблицы> [where col = value]")
    print("update <table> set col = val [, col2 = val2] where col = val")
    print("delete from <table> where col = val")
    print("info <table> - информация о таблице")
    print("exit - выход")
    print("help - показать это сообщение\n")


def pretty_print_rows(cols, rows):
    table = PrettyTable()
    table.field_names = [c for c, _ in cols]
    for r in rows:
        table.add_row([r.get(c) for c, _ in cols])
    print(table)


def run():
    print("DB project is running! Введите help для подсказки.")
    while True:
        try:
            metadata = load_metadata(META_FILE)
            user_input = input("Введите команду: ").strip()
            if not user_input:
                continue
            tokens = split_command(user_input)
            if not tokens:
                continue
            cmd = tokens[0].lower()

            if cmd == "help":
                print_help()
                continue
            if cmd == "exit":
                print("Выход.")
                break

            if cmd == "create_table":
                if len(tokens) < 3:
                    print("Некорректное значение: недостаточно аргументов.")
                    continue
                table_name = tokens[1]
                cols = parse_columns(tokens[2:])
                metadata = create_table(metadata, table_name, cols)
                save_metadata(META_FILE, metadata)
                print(f'Таблица "{table_name}" успешно создана со столбцами: ' +
                      ", ".join([f"{n}:{t}" for n, t in metadata[table_name]["columns"]]))
                continue

            if cmd == "list_tables":
                names = list_tables(metadata)
                if names:
                    for n in names:
                        print("-", n)
                else:
                    print("Таблиц нет.")
                continue

            if cmd == "drop_table":
                if len(tokens) != 2:
                    print("Некорректное значение.")
                    continue
                table_name = tokens[1]
                metadata = drop_table(metadata, table_name)
                save_metadata(META_FILE, metadata)
                print(f'Таблица "{table_name}" успешно удалена.')
                continue

            if cmd == "insert" and len(tokens) >= 4 and tokens[1].lower() == "into":
                table_name = tokens[2]
                rest = user_input[user_input.lower().find("values") + len("values"):].strip()
                values = parse_values(rest)
                insert(metadata, table_name, values)
                print(f"Запись успешно добавлена в таблицу \"{table_name}\".")
                continue


            if cmd == "select" and len(tokens) >= 3 and tokens[1].lower() == "from":
                table_name = tokens[2]
                where_clause = None
                if "where" in [t.lower() for t in tokens]:
                    idx = [i for i, t in enumerate(tokens) if t.lower() == "where"][0]
                    where_clause = parse_where(tokens[idx + 1:])
                rows = select(metadata, table_name, where_clause)
                cols = metadata[table_name]["columns"]
                pretty_print_rows(cols, rows)
                continue

            if cmd == "update" and len(tokens) >= 6:
                table_name = tokens[1]
                try:
                    set_idx = [i for i, t in enumerate(tokens) if t.lower() == "set"][0]
                    where_idx = [i for i, t in enumerate(tokens) if t.lower() == "where"][0]
                except IndexError:
                    print("Некорректная команда update")
                    continue
                set_clause = parse_set(tokens[set_idx + 1:where_idx])
                where_clause = parse_where(tokens[where_idx + 1:])
                update(metadata, table_name, set_clause, where_clause)
                print("Обновление выполнено.")
                continue

            if cmd == "delete" and len(tokens) >= 4 and tokens[1].lower() == "from":
                table_name = tokens[2]
                if "where" not in [t.lower() for t in tokens]:
                    print("Требуется where для delete")
                    continue
                idx = [i for i, t in enumerate(tokens) if t.lower() == "where"][0]
                where_clause = parse_where(tokens[idx + 1:])
                delete(metadata, table_name, where_clause)
                print("Удаление выполнено.")
                continue

            if cmd == "info" and len(tokens) == 2:
                table_name = tokens[1]
                res = info(metadata, table_name)
                print(f"Таблица: {res['table']}")
                print("Столбцы: " + ", ".join([f"{n}:{t}" for n, t in res["columns"]]))
                print("Количество записей:", res["count"])
                continue

            print(f"Функции {cmd} нет. Попробуйте снова.")
        except KeyboardInterrupt:
            print("\nВыход.")
            break
