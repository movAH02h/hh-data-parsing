import logging
import sys
from loaders import FileLoader
from cleaners import MojibakeCorrector


def get_user_file_path() -> Optional[Tuple[str, str]]:
    """
    Запрашивает путь к директории у пользователя и формирует путь к файлу.

    Аргументы:
        Нет.

    Вернёт:
        Optional[Tuple[str, str]]: Кортеж (путь_к_файлу, базовая_директория)
            в случае успеха, иначе None.

    Исключения:
        Ошибки ввода-вывода при обращении к файловой системе.
    """
    prompt_msg = (
        f"Введите директорию с файлом 'hh.csv'. "
        f"Туда сохранятся файлы {x_data_file} и {y_data_file}: "
    )

    base_dir: str = input(prompt_msg)
    path_csv: str = os.path.join(base_dir, "hh.csv")

    if not os.path.exists(path_csv):
        logger.error(f"Файл '{path_csv}' не найден")
        return None

    return path_csv, base_dir


def run_app(csv_path: str, base_dir: str) -> None:
    """
    Запускает полный пайплайн обработки данных из CSV-файла.

    Аргументы:
        csv_path (str): Путь к исходному CSV-файлу.
        base_dir (str): Путь для сохранения результатов.
    """
    logger.info(f"Запуск пайплайна для файла: {csv_path}")
    pipeline: FileLoader = FileLoader()

    (pipeline.set_next(MojibakeCorrector())

    try:
        pipeline.process(csv_path)
        logger.info("Пайплайн успешно завершен")
    except (FileNotFoundError, PermissionError) as e:
        logger.error(f"Ошибка в пайплайне: {e}")
        raise
    except Exception as e:
        logger.exception(f"Непредвиденная ошибка: {e}")
        raise RuntimeError(f"Критический сбой: {e}")

if __name__ == "__main__":
    result = get_user_file_path()
    if result:
        path_csv, base_dir = result
        run_app(path_csv, base_dir)
