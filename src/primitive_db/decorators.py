import time
from functools import wraps


def handle_db_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as e:
            print(f"Ошибка: некорректный ключ/столбец или таблица. Детали: {e}")
        except ValueError as e:
            print(f"Ошибка: некорректное значение. Детали: {e}")
        except FileNotFoundError as e:
            print(f"Ошибка: файл данных не найден. Детали: {e}")
    return wrapper


def confirm_action(action_name: str):
    """
    Декоратор с параметром.
    Перед выполнением функции спрашивает подтверждение действия action_name.
    Если пользователь вводит не 'y' — действие отменяется.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            answer = input(f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            ).strip().lower()
            if answer != "y":
                print("Операция отменена.")
                return None
            return func(*args, **kwargs)
        return wrapper
    return decorator  # фабрика декораторов [web:404][web:410]


def log_time(func):
    """
    Замеряет время выполнения функции и печатает его в консоль.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start  # [web:413][web:415]
        print(f'Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.')
        return result
    return wrapper
