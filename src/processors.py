import pandas as pd
import logging
from base import ProcessingStep

logger = logging.getLogger(__name__)

class SalaryExtractor(ProcessingStep):
    """Извлекает и преобразует данные о зарплате в числовой формат."""

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Создает целевую переменную 'target_salary' из столбца зарплаты.

        Аргументы:
            df (pd.DataFrame): Исходный DataFrame.

        Вернет:
            pd.DataFrame: DataFrame с числовой колонкой 'target_salary'.
        """
        logger.info("Извлечение целевой переменной (ЗП)...")

        target_col: Optional[str] = self._find_target_column(df.columns)

        if target_col:
            raw_val = df[target_col].astype(str).str.replace(
                r'[^\d]', '', regex=True
            )
            df['target_salary'] = pd.to_numeric(
                raw_val, errors='coerce'
            ).fillna(0).astype(float)
            df = df.drop(columns=[target_col])
        else:
            logger.warning("Столбец ЗП не найден, установлено 0.0")
            df['target_salary'] = 0.0

        return super().process(df)

    def _find_target_column(self, columns: pd.Index) -> Optional[str]:
        """Находит столбец с зарплатой по названию."""
        for col in columns:
            if 'ЗП' in col.upper():
                return col
        return None


class FeatureEncoder(ProcessingStep):
    """Преобразует категориальные признаки в числовые."""

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Кодирует категориальные столбцы через factorize.

        Аргументы:
            df (pd.DataFrame): DataFrame с категориями.

        Вернет:
            pd.DataFrame: DataFrame с закодированными признаками.
        """
        logger.info("Кодирование признаков...")
        for col in df.columns:
            if col != 'target_salary' and df[col].dtype == 'object':
                df[col] = pd.factorize(df[col])[0]
        return super().process(df)
