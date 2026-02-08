# src/primitive_db/engine.py

import prompt
import shlex  # для аккуратного разбора строки на части
from .utils import load_metadata, save_metadata
from .core import create_table, drop_table

META_FILE = "db_meta.json"


HELP_TEXT = """***Процесс работы с таблицей***
Функции:
<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. - создать таблицу
<command> list_tables - показать список всех таблиц
<command> drop_table <имя_таблицы> - удалить таблицу
<command> exit - выход из программы
<command> help - справочная информация
"""

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
            print(HELP_TEXT)

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
                save_metadata(META_FILE, metadata)

        else:
            # некорректная функция
            print(f"Функции {command} нет. Попробуйте снова.")

