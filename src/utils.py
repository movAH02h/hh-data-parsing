import os
import logging

logger = logging.getLogger(__name__)

def get_user_file_path(default_filename="hh.csv") -> Optional[Tuple[str, str]]:
    """
    Запрашивает путь к .csv файлу.

    Аргументы:
        Нет.

    Вернёт:
        Optional[str]: путь_к_файлу в случае успеха, иначе None.

    Исключения:
        Ошибки ввода-вывода при обращении к файловой системе.
    """
    path = input("Введите путь к исходному файлу: ").strip()

    if not path:
      base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
      path_csv = os.path.join(base_dir, "data", default_filename)

    if not os.path.exists(path):
        logger.error(f"Файл '{path}' не найден")
        return None

    return path
