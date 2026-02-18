import numpy as np
import logging
from base import ProcessingStep

logger = logging.getLogger(__name__)

class NumpyNanny(ProcessingStep):
    """Сохраняет обработанные данные в формате NumPy."""

    def __init__(self, base_dir: str) -> None:
        """
        Аргументы:
            base_dir (str): Директория для сохранения файлов.
        """
        super().__init__()
        self.output_dir: str = base_dir

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Сохраняет x_data.npy и y_data.npy в указанную директорию.

        Аргументы:
            df (pd.DataFrame): Итоговый DataFrame.

        Вернёт:
            pd.DataFrame: Исходный DataFrame.

        Исключения:
            PermissionError: Если нет прав на запись.
            KeyError: Если отсутствует целевая переменная.
        """
        logger.info("- Сохранение в .npy...")
        try:
            y: np.ndarray = df['target_salary'].values
            x: np.ndarray = df.drop(columns=['target_salary']).values

            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
                logger.debug(f"Создана директория: {self.output_dir}")

            x_path: str = os.path.join(self.output_dir, 'x_data.npy')
            y_path: str = os.path.join(self.output_dir, 'y_data.npy')

            np.save(x_path, x)
            np.save(y_path, y)
            logger.info(f"Успех. Директория: {self.output_dir}")

            return df

        except KeyError as e:
            logger.error(f"Отсутствует столбец в DataFrame: {e}")
            raise KeyError(f"Отсутствует столбец в DataFrame: {e}")
        except Exception as e:
            logger.error(f"Ошибка сохранения: {e}")
            raise e
