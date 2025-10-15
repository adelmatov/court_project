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
        Extract unique 12-digit codes from qamqor_tax and qamqor_customs,
        excluding codes that already exist in companies table.
        
        Args:
            limit: Maximum number of NEW codes to return
        
        Returns:
            List of 12-digit codes (BIN or IIN - doesn't matter)
        """
        
        # Проверяем существование таблицы companies в target БД
        try:
            target_conn = self._get_target_connection()
            target_cursor = target_conn.cursor()
            
            target_cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'companies'
                )
            """)
            
            table_exists = target_cursor.fetchone()[0]
            target_cursor.close()
            
            if not table_exists:
                logger.warning("⚠️  Table 'companies' does not exist yet in target database")
                has_existing = False
            else:
                # Получаем количество существующих кодов
                target_cursor = target_conn.cursor()
                target_cursor.execute("SELECT COUNT(*) FROM companies WHERE bin IS NOT NULL")
                existing_count = target_cursor.fetchone()[0]
                target_cursor.close()
                
                has_existing = existing_count > 0
                if has_existing:
                    logger.info(f"Excluding {existing_count} existing codes from companies table")
                else:
                    logger.info("No existing codes to exclude (table is empty)")
            
        except Exception as e:
            logger.warning(f"⚠️  Could not check companies table: {e}")
            has_existing = False
        
        # Загружаем существующие коды
        if has_existing:
            try:
                target_cursor = target_conn.cursor()
                target_cursor.execute("SELECT bin FROM companies WHERE bin IS NOT NULL")
                existing_codes_list = [row[0] for row in target_cursor.fetchall()]
                target_cursor.close()
                
                logger.debug(f"Loaded {len(existing_codes_list)} existing codes from companies table")
            except Exception as e:
                logger.warning(f"Failed to load existing codes: {e}")
                existing_codes_list = []
        else:
            existing_codes_list = []
        
        # ✅ Простой SQL: только 12 цифр + исключить существующие
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
        """
        
        # Добавляем фильтр NOT IN, если есть существующие коды
        params = []
        if existing_codes_list:
            sql += " AND subject_bin NOT IN %s"
            params.append(tuple(existing_codes_list))
        
        sql += " ORDER BY subject_bin"
        
        # Лимит применяется к УЖЕ отфильтрованным данным
        if limit:
            sql += f" LIMIT {limit}"
        
        try:
            conn = self._get_qamqor_connection()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            all_codes = cursor.fetchall()
            
            # ✅ Минимальная валидация (только формат)
            new_codes = []
            
            for row in all_codes:
                code = row[0]
                
                # Только проверка что это 12 цифр
                if validate_bin(code):
                    new_codes.append(code)
            
            logger.info(
                f"✅ Extracted {len(new_codes)} NEW codes from qamqor tables"
            )
            logger.info(
                f"📊 Stats: fetched {len(all_codes)}"
            )
            
            cursor.close()
            return new_codes
            
        except Exception as e:
            logger.error(f"❌ Error extracting codes from qamqor: {e}")
            raise
    
    def __del__(self):
        """Close connections on cleanup."""
        if self.qamqor_conn and not self.qamqor_conn.closed:
            self.qamqor_conn.close()
            logger.debug("Closed qamqor database connection")
        if self.target_conn and not self.target_conn.closed:
            self.target_conn.close()
            logger.debug("Closed target database connection")