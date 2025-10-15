"""
Reference Manager - Управление справочниками
"""

from typing import Optional, Dict, Any, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor

from .logger import logger


"""
Reference Manager - Управление справочниками
"""

from typing import Optional, Dict, Any, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor

from .logger import logger


class ReferenceManager:
    """
    Кеширование и управление справочными таблицами.
    """
    
    def __init__(self, conn):
        self.conn = conn
        self._cache = {
            'status': {},    # {code: id}
            'krp': {},       # {code: id}
            'kfc': {},       # {code: id}
            'kse': {},       # {code: id}
            'oked': {}       # {code: id}
        }
        self._load_cache()
    
    def _load_cache(self):
        """Загрузить все справочники в память."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Status
            cursor.execute("SELECT id, code FROM ref_status")
            for row in cursor.fetchall():
                self._cache['status'][row['code']] = row['id']
            
            # KRP
            cursor.execute("SELECT id, code FROM ref_krp")
            for row in cursor.fetchall():
                self._cache['krp'][row['code']] = row['id']
            
            # KFC
            cursor.execute("SELECT id, code FROM ref_kfc")
            for row in cursor.fetchall():
                self._cache['kfc'][row['code']] = row['id']
            
            # KSE
            cursor.execute("SELECT id, code FROM ref_kse")
            for row in cursor.fetchall():
                self._cache['kse'][row['code']] = row['id']
            
            # OKED
            cursor.execute("SELECT id, code FROM ref_oked")
            for row in cursor.fetchall():
                self._cache['oked'][row['code']] = row['id']
            
            logger.debug(
                f"Loaded refs: status={len(self._cache['status'])}, "
                f"krp={len(self._cache['krp'])}, "
                f"kfc={len(self._cache['kfc'])}, kse={len(self._cache['kse'])}, "
                f"oked={len(self._cache['oked'])}"
            )
        
        finally:
            cursor.close()
    
    def get_or_create_status(self, code: int, name: str) -> Optional[int]:
        """Получить ID статуса."""
        if code in self._cache['status']:
            return self._cache['status'][code]
        
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO ref_status (code, name)
                VALUES (%s, %s)
                ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (code, name))
            
            ref_id = cursor.fetchone()[0]
            self.conn.commit()
            self._cache['status'][code] = ref_id
            return ref_id
        finally:
            cursor.close()
    
    def get_or_create_krp(self, code: int, name: str) -> Optional[int]:
        """Получить/создать ID KRP."""
        if code in self._cache['krp']:
            return self._cache['krp'][code]
        
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO ref_krp (code, name)
                VALUES (%s, %s)
                ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (code, name))
            
            ref_id = cursor.fetchone()[0]
            self.conn.commit()
            self._cache['krp'][code] = ref_id
            logger.debug(f"Created KRP: {code} - {name}")
            return ref_id
        finally:
            cursor.close()
    
    def get_or_create_kfc(self, code: int, name: str) -> Optional[int]:
        """Получить/создать ID KFC."""
        if code in self._cache['kfc']:
            return self._cache['kfc'][code]
        
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO ref_kfc (code, name)
                VALUES (%s, %s)
                ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (code, name))
            
            ref_id = cursor.fetchone()[0]
            self.conn.commit()
            self._cache['kfc'][code] = ref_id
            logger.debug(f"Created KFC: {code} - {name}")
            return ref_id
        finally:
            cursor.close()
    
    def get_or_create_kse(self, code: int, name: str) -> Optional[int]:
        """Получить/создать ID KSE."""
        if code in self._cache['kse']:
            return self._cache['kse'][code]
        
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO ref_kse (code, name)
                VALUES (%s, %s)
                ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (code, name))
            
            ref_id = cursor.fetchone()[0]
            self.conn.commit()
            self._cache['kse'][code] = ref_id
            logger.debug(f"Created KSE: {code} - {name}")
            return ref_id
        finally:
            cursor.close()
    
    def get_or_create_oked(self, code: str, name: str) -> Optional[int]:
        """Получить/создать ID OKED."""
        if code in self._cache['oked']:
            return self._cache['oked'][code]
        
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO ref_oked (code, name)
                VALUES (%s, %s)
                ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (code, name))
            
            ref_id = cursor.fetchone()[0]
            self.conn.commit()
            self._cache['oked'][code] = ref_id
            logger.debug(f"Created OKED: {code}")
            return ref_id
        finally:
            cursor.close()