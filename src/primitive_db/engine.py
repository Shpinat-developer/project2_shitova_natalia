# src/primitive_db/engine.py

import prompt
import shlex  # для аккуратного разбора строки на части
from prettytable import PrettyTable  # для красивого вывода 
from .utils import load_metadata, save_metadata, load_table_data, save_table_data
from .core import create_table, drop_table, insert, select, update, delete
from pathlib import Path


META_FILE = "db_meta.json"


def print_help() -> None:
    """Prints the help message for the current mode."""
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")

    print("\n***Операции с данными***")
    print("Функции:")
    print("<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...) - создать запись.")
    print("<command> select from <имя_таблицы> - прочитать все записи.")
    print("<command> select from <имя_таблицы> where <столбец> = <значение> - прочитать записи по условию.")
    print("<command> update <имя_таблицы> set <столбец1> = <новое_значение1> "
          "where <столбец_условия> = <значение_условия> - обновить запись.")
    print("<command> delete from <имя_таблицы> where <столбец> = <значение> - удалить запись.")
    print("<command> info <имя_таблицы> - вывести информацию о таблице.")
    
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")

# парсер для сложных команд

def parse_condition(condition: str) -> dict:
    """
    Превращает строку вида 'age = 28' или 'name = \"Sergei\"'
    в словарь {'age': 28} или {'name': 'Sergei'}.
    """
    # разбиваем по первому знаку '='
    if "=" not in condition:
        raise ValueError(f"Некорректное условие: {condition!r}")

    left, right = condition.split("=", 1)
    key = left.strip()
    raw_value = right.strip()

    # строка в кавычках -> строка
    if (raw_value.startswith('"') and raw_value.endswith('"')) or \
       (raw_value.startswith("'") and raw_value.endswith("'")):
        value = raw_value[1:-1]
    else:
        # пробуем int
        try:
            value = int(raw_value)
        except ValueError:
            # пробуем bool
            lowered = raw_value.lower()
            if lowered == "true":
                value = True
            elif lowered == "false":
                value = False
            else:
                # на крайний случай — как строку без кавычек
                value = raw_value

    return {key: value}

def run() -> None:
    while True:
        # 1. Загружаем актуальные метаданные из файла
        metadata = load_metadata(META_FILE)

        # 2. Запрашиваем команду
        user_input = prompt.string(">>>Введите команду: ").strip()
        if not user_input:
            continue

        # 3. Разбираем строку на команду и аргументы
        try:
            args = shlex.split(user_input)
        except ValueError:
            # строка некорректно разбита (например, незакрытые кавычки)
            print(f"Некорректное значение: {user_input}. Попробуйте снова.")
            continue

        command = args[0]

        # 4. Обработка команды

        if command == "exit":
            break
            
        elif command == "help":
            print_help()

        elif command == "list_tables":
            if metadata:
                for name in metadata.keys():
                    print(f"- {name}")
            else:
 
                print("Таблиц нет.")

        elif command == "create_table":
            # ожидается: create_table <имя> <столбец1:тип> ...
            if len(args) < 3:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = args[1]
            columns = args[2:]

            old_metadata = metadata.copy()
            metadata = create_table(metadata, table_name, columns)

            # если метаданные изменились — сохраняем
            if metadata != old_metadata:
                save_metadata(META_FILE, metadata)

        elif command == "drop_table":
            # ожидается: drop_table <имя_таблицы>
            if len(args) != 2:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = args[1]
            old_metadata = metadata.copy()
            metadata = drop_table(metadata, table_name)

            if metadata != old_metadata:
                # сохраняем обновлённые метаданные
                save_metadata(META_FILE, metadata)

                # удаляем файл с данными таблицы, если он есть
                data_path = Path("data") / f"{table_name}.json"
                if data_path.exists():
                    data_path.unlink()
                
 # ===== CRUD по данным =====

        elif command == "insert":
            # ожидаем: insert into <имя_таблицы> values (<значение1>, <значение2>, ...)
            if len(args) < 4 or args[1] != "into" or "values" not in args:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = args[2]

            values_index = args.index("values") + 1
            raw_values = args[values_index:]

            # убираем запятые и крайние скобки
            cleaned = []
            for v in raw_values:
                v = v.strip()
                if v.endswith(","):
                    v = v[:-1]
                v = v.lstrip("(").rstrip(")")
                cleaned.append(v)

            table_data = load_table_data(table_name)

            table_data = insert(metadata, table_name, table_data, cleaned)
            if table_data is None:
                # декоратор handle_db_errors уже вывел сообщение
                continue

            save_table_data(table_name, table_data)
            new_id = table_data[-1]["ID"]
            print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')

        elif command == "select":
            # select from <имя_таблицы>
            # select from <имя_таблицы> where <условие>
            if len(args) < 3 or args[1] != "from":
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = args[2]
            table_data = load_table_data(table_name)

            where_clause = None
            if "where" in args:
                where_index = args.index("where") + 1
                condition_str = " ".join(args[where_index:])
                try:
                    where_clause = parse_condition(condition_str)
                except ValueError as e:
                    print(e)
                    continue

            rows = select(table_name, table_data, where_clause)
            if rows is None:
                # декоратор обработал ошибку
                continue

            if not rows:
                print("Записей не найдено.")
            else:
                table = PrettyTable()
                table.field_names = list(rows[0].keys())
                for row in rows:
                    table.add_row([row[col] for col in table.field_names])
                print(table)
                
        elif command == "update":
            # update <имя_таблицы> set <...> where <...>
            if len(args) < 6 or args[2] != "set" or "where" not in args:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = args[1]
            where_pos = args.index("where")

            set_str = " ".join(args[3:where_pos])
            where_str = " ".join(args[where_pos + 1:])

            try:
                set_clause = parse_condition(set_str)
                where_clause = parse_condition(where_str)
            except ValueError as e:
                print(e)
                continue

            table_data = load_table_data(table_name)
            table_data = update(table_data, set_clause, where_clause)
            if table_data is None:
                # декоратор обработал ошибку
                continue

            save_table_data(table_name, table_data)
            print(f'Записи в таблице "{table_name}" успешно обновлены.')

        elif command == "delete":
            # delete from <имя_таблицы> where <...>
            if len(args) < 5 or args[1] != "from" or "where" not in args:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = args[2]
            where_pos = args.index("where")
            where_str = " ".join(args[where_pos + 1:])

            try:
                where_clause = parse_condition(where_str)
            except ValueError as e:
                print(e)
                continue

            table_data = load_table_data(table_name)
            table_data = delete(table_data, where_clause)
            if table_data is None:
                # декоратор обработал ошибку
                continue

            save_table_data(table_name, table_data)
            print(f'Записи в таблице "{table_name}" успешно удалены.')


        elif command == "info":
            # info <имя_таблицы>
            if len(args) != 2:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = args[1]

            if table_name not in metadata:
                print(f'Таблица "{table_name}" не существует.')
                continue

            schema = metadata[table_name]
            table_data = load_table_data(table_name)

            cols_str = ", ".join(f"{name}:{typ}" for name, typ in schema.items())
            print(f"Таблица: {table_name}")
            print(f"Столбцы: {cols_str}")
            print(f"Количество записей: {len(table_data)}")

        else:
            print(f"Функции {command} нет. Попробуйте снова.")




