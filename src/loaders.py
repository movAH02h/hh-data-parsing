import io
import logging
import pandas as pd
from .base import ProcessingStep

class FileLoader(ProcessingStep):
    """Загружает CSV-файл из указанного пути."""

    def process(self, file_path: str) -> pd.DataFrame:
        """
        Читает CSV-файл в сыром виде и преобразует в DataFrame.

        Аргументы:
            file_path (str): Путь к CSV-файлу для загрузки.

        Вернёт:
            pd.DataFrame: Загруженные данные в виде DataFrame.

        Исключения:
            FileNotFoundError: Если файл не существует.
            UnicodeDecodeError: Если возникают проблемы с декодированием.
            ValueError: При пустом файле или ошибке парсинга.
        """
        logger.info(f"Загрузка: {file_path}")
        try:
            with open(file_path, 'rb') as f:
                raw_content: bytes = f.read()
        except FileNotFoundError:
            logger.error(f"Файл не найден: {file_path}")
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        except PermissionError:
            logger.error(f"Нет прав на чтение файла: {file_path}")
            raise PermissionError(f"Нет прав на чтение файла: {file_path}")
        except OSError as e:
            logger.error(f"Ошибка при чтении файла {file_path}: {e}")
            raise OSError(f"Ошибка при чтении файла {file_path}: {e}")

        try:
            content: str = raw_content.decode('cp1251', errors='ignore')
        except UnicodeDecodeError as e:
            logger.error(f"Ошибка декодирования файла {file_path}: {e}")
            raise UnicodeDecodeError(f"Ошибка декодирования {file_path}: {e}")

        try:
            df: pd.DataFrame = pd.read_csv(
                io.StringIO(content),
                on_bad_lines='skip',
                engine='python',
                index_col=0
            )
        except pd.errors.EmptyDataError:
            logger.error(f"Файл {file_path} пуст или не содержит данных")
            raise ValueError(f"Файл {file_path} пуст или не содержит данных")
        except pd.errors.ParserError as e:
            logger.error(f"Ошибка парсинга CSV файла {file_path}: {e}")
            raise ValueError(f"Ошибка парсинга CSV файла {file_path}: {e}")

        bad_col = 'РС‰РµС‚ СЂР°Р±РѕС‚Сѓ РЅР° РґРѕР»Р¶РЅРѕСЃС‚СЊ:'
        df.rename(
            columns={bad_col: 'Ищет работу на должность'},
            inplace=True
        )
        return super().process(df)
