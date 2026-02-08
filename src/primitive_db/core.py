# src/primitive_db/core.py

from typing import Any, Callable
from .decorators import handle_db_errors, confirm_action, log_time


ALLOWED_TYPES = {"int", "str", "bool"}

@handle_db_errors
def create_table(metadata: dict, table_name: str, columns: list[str]) -> dict:
    if table_name in metadata:
        print(f'Ошибка: Таблица "{table_name}" уже существует.')
        return metadata

    schema: dict[str, str] = {}

    for col_def in columns:
        if ":" not in col_def:
            print(f"Некорректное значение: {col_def}. Попробуйте снова.")
            return metadata

        col_name, col_type = col_def.split(":", 1)
        col_name = col_name.strip()
        col_type = col_type.strip()

        if col_type not in ALLOWED_TYPES:
            print(f"Некорректное значение: {col_def}. Попробуйте снова.")
            return metadata

        schema[col_name] = col_type

    # Если пользователь НЕ задал ID, добавляем сами
    if "ID" not in schema and "id" not in schema:
        full_schema = {"ID": "int"}
        full_schema.update(schema)
    else:
        full_schema = schema

    metadata[table_name] = full_schema

    cols_str = ", ".join(f"{name}:{typ}" for name, typ in full_schema.items())
    print(f'Таблица "{table_name}" успешно создана со столбцами: {cols_str}')

    return metadata

@handle_db_errors
@confirm_action("удаление таблицы")

def drop_table(metadata: dict, table_name: str) -> dict:
    """
    Удаляет таблицу из метаданных.
    
    """
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return metadata

    del metadata[table_name]
    print(f'Таблица "{table_name}" успешно удалена.')

    return metadata
    
# ===== CRUD по данным таблиц =====

#Ф-цтя приведения типов данных
def _cast_value(value, type_name: str):
    if type_name == "int":
        return int(value)
    if type_name == "str":
        return str(value)
    if type_name == "bool":
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("true", "1", "yes"):
                return True
            if v in ("false", "0", "no"):
                return False
        return bool(value)
    return value

@handle_db_errors
@log_time
def insert(
    metadata: dict, 
    table_name: str, 
    table_data: list[dict], 
    values: list
    ) -> list[dict]:
    """
    Добавляет новую запись в таблицу.
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')

    columns = metadata[table_name]          # dict: имя_столбца -> тип
    column_names = list(columns.keys())

    # считаем, что первый столбец — ID и заполняем его сами
    non_id_columns = column_names[1:]

    if len(values) != len(non_id_columns):
        raise ValueError(
        "Количество значений не совпадает с количеством столбцов (без ID)."
        )

    # генерируем новый ID
    if table_data:
        max_id = max(row["ID"] for row in table_data)
        new_id = max_id + 1
    else:
        new_id = 1

    row: dict = {"ID": new_id}

    for col_name, raw_value in zip(non_id_columns, values):
        type_name = columns[col_name]
        row[col_name] = _cast_value(raw_value, type_name)

    table_data.append(row)
    return table_data

@handle_db_errors
@log_time
def select(table_name: str, table_data: list[dict], where_clause: dict | None = None) -> list[dict]:
    """
    Возвращает все записи или только те, что подходят под where.
    Результаты одинаковых запросов кэшируются.
    """
    if where_clause is None:
        key = (table_name, None)
    else:
        key = (table_name, tuple(sorted(where_clause.items())))

    def compute() -> list[dict]:
        if where_clause is None:
            return table_data

        result: list[dict] = []
        for row in table_data:
            ok = True
            for k, v in where_clause.items():
                if row.get(k) != v:
                    ok = False
                    break
            if ok:
                result.append(row)
        return result

    return select_cache(key, compute)

@handle_db_errors
def update(table_data: list[dict], set_clause: dict, where_clause: dict) -> list[dict]:
    """
    Обновляет записи, подходящие под where полями из set
    """
    for row in table_data:
        match = True
        for key, value in where_clause.items():
            if row.get(key) != value:
                match = False
                break

        if not match:
            continue

        for key, value in set_clause.items():
            row[key] = value

    return table_data

@handle_db_errors
@confirm_action("удаление записей")
def delete(table_data: list[dict], where_clause: dict) -> list[dict]:
    """
    Удаляет записи, подходящие под where
    """
    new_data: list[dict] = []

    for row in table_data:
        match = True
        for key, value in where_clause.items():
            if row.get(key) != value:
                match = False
                break

        if not match:
            new_data.append(row)

    return new_data

def create_cacher():
    """
    Создаёт замыкание cache_result с внутренним кэшем.
    cache_result(key, value_func) -> результат, возможно из кэша.
    """
    cache: dict[Any, Any] = {}

    def cache_result(key: Any, value_func: Callable[[], Any]) -> Any:
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value

    return cache_result 

select_cache = create_cacher()
