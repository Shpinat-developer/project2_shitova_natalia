# src/primitive_db/utils.py

import json
from pathlib import Path


def load_metadata(filepath: str) -> dict:
    """
    Загружает данные из JSON-файла.
    Если файл не найден, возвращает пустой словарь {}.
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str, data: dict) -> None:
    """
    Сохраняет переданные данные в JSON-файл.
    """
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
        
DATA_DIR = Path("data")


def load_table_data(table_name: str) -> list[dict]:
    """
    Загружает данные таблицы из файла data/<table_name>.json.
    Если файла нет, возвращает пустой список [].
    """
    DATA_DIR.mkdir(exist_ok=True)

    table_path = DATA_DIR / f"{table_name}.json"
    if not table_path.exists():
        return []

    with table_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_table_data(table_name: str, data: list[dict]) -> None:
    """
    Сохраняет данные таблицы в файл data/<table_name>.json.
    """
    DATA_DIR.mkdir(exist_ok=True)

    table_path = DATA_DIR / f"{table_name}.json"
    with table_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

