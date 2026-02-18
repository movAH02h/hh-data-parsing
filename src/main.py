import logging
import os
from utils import get_user_file_path
from loaders import FileLoader
from cleaners import MojibakeCorrector, CsvSaver
from processors import SalaryExtractor, FeatureEncoder
from storage import NumpyNanny

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def main() -> None:
    """
    Запускает полный пайплайн обработки данных из CSV-файла.

    Вернет:
        None
    """
    csv_path = get_user_file_path()
    if csv_path is None:
        return

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(BASE_DIR, "data")
    CLEANED_CSV = os.path.join(DATA_DIR, "hh_cleaned.csv")

    logger.info(f"Запуск пайплайна для файла: {csv_path}")
    pipeline = FileLoader()

    pipeline = FileLoader()
    (pipeline.set_next(MojibakeCorrector())
             .set_next(CsvSaver(CLEANED_CSV))
             .set_next(SalaryExtractor())
             .set_next(FeatureEncoder())
             .set_next(NumpyNanny(DATA_DIR)))

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
    main()
