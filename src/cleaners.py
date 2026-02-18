import logging
import pandas as pd
from typing import Any
from base import ProcessingStep

logger = logging.getLogger(__name__)

class MojibakeCorrector(ProcessingStep):
    """Исправляет проблемы с кодировкой (Mojibake) в текстовых данных."""

    @staticmethod
    def _repair_string(text: Any) -> Any:
        """
        Исправляет строку с проблемами двойной кодировки.

        Аргументы:
            text (Any): Исходная строка.

        Вернёт:
            Any: Исправленная строка или исходное значение.
        """
        if not isinstance(text, str):
            return text
        try:
            return text.encode('cp1251').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Применяет исправление кодировки ко всему DataFrame.

        Аргументы:
            df (pd.DataFrame): DataFrame с проблемами кодировки.

        Вернёт:
            pd.DataFrame: Исправленный DataFrame.
        """
        logger.info("Исправление кодировки (Mojibake)...")
        df.columns = [self._repair_string(col) for col in df.columns]
        df = df.map(self._repair_string)
        return super().process(df)
