# src/primitive_db/core.py

ALLOWED_TYPES = {"int", "str", "bool"}


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

