"""
QamqorBinSource - Extract BINs from qamqor_tax and qamqor_customs tables
"""

from typing import List, Optional, Set
import psycopg2
from psycopg2.extras import RealDictCursor

from .base import BinSource
from ..core.config import QAMQOR_DB_CONFIG, TARGET_DB_CONFIG
from ..core.logger import logger
from ..utils.validators import validate_bin


class QamqorBinSource(BinSource):
    """
    Extract BINs from qamqor_tax and qamqor_customs tables.
    
    Подключается к двум БД PostgreSQL:
    - QAMQOR_DB_CONFIG: для чтения БИНов из qamqor_tax, qamqor_customs (БД: qamqor)
    - TARGET_DB_CONFIG: для проверки companies таблицы (БД: companies)
    """
    
    def __init__(self):
        self.qamqor_conn = None
        self.target_conn = None
    
    @property
    def name(self) -> str:
        return 'qamqor'
    
    def _get_qamqor_connection(self):
        """Get or create database connection to QAMQOR (source)."""
        if self.qamqor_conn is None or self.qamqor_conn.closed:
            self.qamqor_conn = psycopg2.connect(**QAMQOR_DB_CONFIG)
            logger.debug(f"Connected to qamqor database: {QAMQOR_DB_CONFIG['database']}")
        return self.qamqor_conn
    
    def _get_target_connection(self):
        """Get or create database connection to TARGET (where companies table is)."""
        if self.target_conn is None or self.target_conn.closed:
            self.target_conn = psycopg2.connect(**TARGET_DB_CONFIG)
            logger.debug(f"Connected to target database: {TARGET_DB_CONFIG['database']}")
        return self.target_conn
    
    def _is_iin(self, bin_code: str) -> bool:
        """
        Check if code is IIN (individual) instead of BIN (company).
        
        IIN format: ГГММДД (year, month, day of birth)
        - Positions 3-4: month (01-12 or 13-32 for 1800s)
        
        BIN format: starts with year + 40/41/42
        - Positions 3-4: always 40, 41, or 42
        
        Args:
            bin_code: 12-digit code
            
        Returns:
            True if IIN (individual), False if BIN (company)
        """
        if len(bin_code) != 12:
            return False
        
        # Проверяем 4-ю и 5-ю цифры (позиции 3-4)
        month_code = bin_code[3:5]
        
        # БИН: месяц регистрации = 40, 41, 42
        # ИИН: месяц рождения = 01-12 (или 13-32 для 1800-х годов)
        if month_code in ['40', '41', '42']:
            return False  # Это БИН (компания)
        
        # Дополнительная проверка: месяц должен быть в валидном диапазоне для ИИН
        try:
            month = int(month_code)
            # 01-12 (1900-1999), 13-24 (1800-1899), 25-36 (2000-2099)
            if 1 <= month <= 36:
                return True  # Это ИИН (физлицо)
        except ValueError:
            pass
        
        return False
    
    def _get_existing_bins(self) -> Set[str]:
        """
        Get set of BINs that already exist in companies table.
        
        Returns:
            Set of existing BIN strings (empty set if table doesn't exist or on error)
        """
        try:
            conn = self._get_target_connection()
            cursor = conn.cursor()
            
            # Проверяем существование таблицы companies
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'companies'
                )
            """)
            
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                logger.warning("⚠️  Table 'companies' does not exist yet in target database")
                cursor.close()
                return set()
            
            # Читаем БИНы из таблицы companies
            cursor.execute("SELECT bin FROM companies WHERE bin IS NOT NULL")
            existing_bins = {row[0] for row in cursor.fetchall()}
            
            cursor.close()
            
            logger.debug(f"Found {len(existing_bins)} existing BINs in companies table")
            
            return existing_bins
            
        except Exception as e:
            logger.warning(f"⚠️  Could not fetch existing BINs from companies table: {e}")
            return set()
    
    def get_bins(self, limit: Optional[int] = None) -> List[str]:
        """
        Extract unique BINs from qamqor_tax and qamqor_customs,
        excluding IINs (individuals) and already processed companies.
        
        Args:
            limit: Maximum number of NEW BINs to return
        
        Returns:
            List of unique BIN strings (companies only, not individuals)
        """
        
        # Получаем существующие БИНы из target БД (companies)
        existing_bins = self._get_existing_bins()
        
        if existing_bins:
            logger.info(f"Excluding {len(existing_bins)} existing BINs from companies table")
        else:
            logger.info("No existing BINs to exclude (table is empty or doesn't exist)")
        
        # SQL запрос к qamqor БД с запасом
        # Берём в 20 раз больше, чтобы после фильтрации ИИН осталось достаточно БИНов
        fetch_limit = limit * 20 if limit else None
        
        sql = """
            SELECT DISTINCT subject_bin
            FROM (
                SELECT subject_bin FROM qamqor_tax 
                WHERE subject_bin IS NOT NULL
                UNION
                SELECT subject_bin FROM qamqor_customs 
                WHERE subject_bin IS NOT NULL
            ) combined
            WHERE subject_bin ~ '^[0-9]{12}$'
            ORDER BY subject_bin
        """
        
        if fetch_limit:
            sql += f" LIMIT {fetch_limit}"
        
        try:
            conn = self._get_qamqor_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            
            all_bins = cursor.fetchall()
            
            # Фильтруем: валидация + исключаем ИИН + исключаем существующие БИНы
            new_bins = []
            iin_count = 0
            
            for row in all_bins:
                bin_code = row[0]
                
                # Пропускаем невалидные
                if not validate_bin(bin_code):
                    continue
                
                # Пропускаем ИИН (физлица)
                if self._is_iin(bin_code):
                    iin_count += 1
                    logger.debug(f"Skipping IIN (individual): {bin_code}")
                    continue
                
                # Пропускаем уже существующие
                if bin_code in existing_bins:
                    continue
                
                new_bins.append(bin_code)
                
                # Применяем лимит
                if limit and len(new_bins) >= limit:
                    break
            
            excluded_count = len(all_bins) - len(new_bins)
            
            logger.info(
                f"✅ Extracted {len(new_bins)} NEW company BINs from qamqor tables"
            )
            logger.info(
                f"📊 Stats: fetched {len(all_bins)}, excluded {iin_count} IINs, "
                f"excluded {excluded_count - iin_count} existing/invalid"
            )
            
            cursor.close()
            return new_bins
            
        except Exception as e:
            logger.error(f"❌ Error extracting BINs from qamqor: {e}")
            raise
    
    def __del__(self):
        """Close connections on cleanup."""
        if self.qamqor_conn and not self.qamqor_conn.closed:
            self.qamqor_conn.close()
            logger.debug("Closed qamqor database connection")
        if self.target_conn and not self.target_conn.closed:
            self.target_conn.close()
            logger.debug("Closed target database connection")