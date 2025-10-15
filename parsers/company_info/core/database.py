"""
Database manager for company data
"""

from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor

from .config import DB_CONFIG
from .logger import logger
from .references import ReferenceManager

class DatabaseManager:
    """
    PostgreSQL database manager for company data.
    
    Подключается к БД 'companies' для сохранения данных.
    """
    
    def __init__(self):
        self.conn = None
        self.ref_manager = None
    
    def _get_connection(self):
        """Get or create database connection."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.ref_manager = ReferenceManager(self.conn)
            logger.debug(f"Connected to database: {DB_CONFIG['database']}")
        return self.conn
    
    def close(self):
        """Close database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.debug("Database connection closed")
    
    # ════════════════════════════════════════════════════════════════
    # READ OPERATIONS
    # ════════════════════════════════════════════════════════════════
    
    def get_existing_bins(self, bins: List[str]) -> List[str]:
        """
        Get BINs that already exist in database.
        
        Args:
            bins: List of BINs to check
        
        Returns:
            List of existing BINs
        """
        if not bins:
            return []
        
        sql = "SELECT bin FROM companies WHERE bin = ANY(%s)"
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (bins,))
            
            existing = [row[0] for row in cursor.fetchall()]
            
            logger.debug(f"Found {len(existing)}/{len(bins)} existing BINs")
            
            return existing
            
        except Exception as e:
            logger.error(f"Error checking existing BINs: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def get_company(self, bin_value: str) -> Optional[Dict[str, Any]]:
        """
        Get company by BIN with references.
        
        Args:
            bin_value: Company BIN
        
        Returns:
            Company data as dict or None if not found
        """
        sql = """
            SELECT 
                c.bin, c.name_ru, c.registration_date, c.ceo_name,
                c.is_nds, c.phone,
                c.last_api_check, c.api_check_count,
                c.created_at, c.updated_at,
                
                -- Данные из справочников
                s.code as status_code,
                s.name as status_name,
                krp.code as krp_code,
                krp.name as krp_name,
                kfc.code as kfc_code,
                kfc.name as kfc_name,
                kse.code as kse_code,
                kse.name as kse_name,
                oked.code as oked_code,
                oked.name as oked_name
                
            FROM companies c
            LEFT JOIN ref_status s ON c.status_id = s.id
            LEFT JOIN ref_krp krp ON c.krp_id = krp.id
            LEFT JOIN ref_kfc kfc ON c.kfc_id = kfc.id
            LEFT JOIN ref_kse kse ON c.kse_id = kse.id
            LEFT JOIN ref_oked oked ON c.oked_id = oked.id
            WHERE c.bin = %s
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(sql, (bin_value,))
            
            result = cursor.fetchone()
            
            if not result:
                return None
            
            # Преобразовать в формат как из data_processor
            company = dict(result)
            
            # Собрать справочники в dict
            if company.get('krp_code') is not None:
                company['krp'] = {
                    'code': company.pop('krp_code'),
                    'name': company.pop('krp_name')
                }
            else:
                company.pop('krp_code', None)
                company.pop('krp_name', None)
                company['krp'] = None
            
            if company.get('kfc_code') is not None:
                company['kfc'] = {
                    'code': company.pop('kfc_code'),
                    'name': company.pop('kfc_name')
                }
            else:
                company.pop('kfc_code', None)
                company.pop('kfc_name', None)
                company['kfc'] = None
            
            if company.get('kse_code') is not None:
                company['kse'] = {
                    'code': company.pop('kse_code'),
                    'name': company.pop('kse_name')
                }
            else:
                company.pop('kse_code', None)
                company.pop('kse_name', None)
                company['kse'] = None
            
            if company.get('status_code') is not None:
                company['status'] = {
                    'code': company.pop('status_code'),
                    'name': company.pop('status_name')
                }
            else:
                company.pop('status_code', None)
                company.pop('status_name', None)
                company['status'] = None
            
            if company.get('oked_code') is not None:
                company['oked'] = {
                    'code': company.pop('oked_code'),
                    'name': company.pop('oked_name')
                }
            else:
                company.pop('oked_code', None)
                company.pop('oked_name', None)
                company['oked'] = None
            
            return company
            
        except Exception as e:
            logger.error(f"Error getting company {bin_value}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def exists_company(self, bin_value: str) -> bool:
        """
        Check if company exists.
        
        Args:
            bin_value: Company BIN
        
        Returns:
            True if exists, False otherwise
        """
        sql = "SELECT EXISTS(SELECT 1 FROM companies WHERE bin = %s)"
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (bin_value,))
            
            return cursor.fetchone()[0]
            
        except Exception as e:
            logger.error(f"Error checking company existence {bin_value}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    # ════════════════════════════════════════════════════════════════
    # CREATE OPERATION
    # ════════════════════════════════════════════════════════════════
    
    def create_company(self, data: Dict[str, Any]):
        """Create new company with references."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            import json
            
            # Получить ID из справочников
            status_id = None
            if data.get('status'):
                status_id = self.ref_manager.get_or_create_status(
                    data['status']['code'],
                    data['status']['name']
                )
            
            krp_id = None
            if data.get('krp'):
                krp_id = self.ref_manager.get_or_create_krp(
                    data['krp']['code'],
                    data['krp']['name']
                )
            
            kfc_id = None
            if data.get('kfc'):
                kfc_id = self.ref_manager.get_or_create_kfc(
                    data['kfc']['code'],
                    data['kfc']['name']
                )
            
            kse_id = None
            if data.get('kse'):
                kse_id = self.ref_manager.get_or_create_kse(
                    data['kse']['code'],
                    data['kse']['name']
                )
            
            oked_id = None
            if data.get('oked'):
                oked_id = self.ref_manager.get_or_create_oked(
                    data['oked']['code'],
                    data['oked']['name']
                )
            
            # INSERT компании
            sql = """
                INSERT INTO companies (
                    bin, name_ru, registration_date, ceo_name,
                    is_nds, krp_id, kfc_id, kse_id, oked_id, status_id,
                    phone, last_api_check, api_check_count
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), 1
                )
            """
            
            cursor.execute(sql, (
                data.get('bin'),
                data.get('name_ru'),
                data.get('registration_date'),
                data.get('ceo_name'),
                data.get('is_nds'),
                krp_id,
                kfc_id,
                kse_id,
                oked_id,
                status_id,
                json.dumps(data.get('phone')) if data.get('phone') else None
            ))
            
            # ═══ INSERT NAME HISTORY ═══
            for name_entry in data.get('name_history', []):
                cursor.execute("""
                    INSERT INTO company_names (
                        bin, name_ru, valid_from, valid_to, is_current
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    data.get('bin'),
                    name_entry.get('name_ru'),
                    name_entry.get('valid_from'),
                    name_entry.get('valid_to'),
                    bool(name_entry.get('is_current', False))
                ))
            
            # ═══ INSERT CEO ═══
            if data.get('ceo_name'):
                cursor.execute("""
                    INSERT INTO company_ceos (
                        bin, ceo_name, valid_from, is_current
                    ) VALUES (%s, %s, %s, true)
                """, (
                    data.get('bin'),
                    data.get('ceo_name'),
                    data.get('registration_date')
                ))
            
            
            # ═══ INSERT TAXES ═══
            for tax in data.get('taxes', []):
                cursor.execute("""
                    INSERT INTO company_taxes (
                        bin, year, total_taxes, check_date
                    ) VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (bin, year) DO UPDATE SET
                        total_taxes = EXCLUDED.total_taxes,
                        check_date = NOW()
                """, (
                    data.get('bin'),
                    tax.get('year'),
                    tax.get('total_taxes')
                ))
            
            # ═══ INSERT NDS ═══
            for nds in data.get('nds', []):
                cursor.execute("""
                    INSERT INTO company_nds (
                        bin, year, nds_amount, check_date
                    ) VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (bin, year) DO UPDATE SET
                        nds_amount = EXCLUDED.nds_amount,
                        check_date = NOW()
                """, (
                    data.get('bin'),
                    nds.get('year'),
                    nds.get('nds_amount')
                ))
            
            # ═══ INSERT RELATIONS ═══
            for relation in data.get('relations', []):
                bin_to = relation.get('bin_to')
                if bin_to and self.exists_company(bin_to):
                    cursor.execute("""
                        INSERT INTO company_relations (
                            bin_from, bin_to, relation_type
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        relation.get('bin_from'),
                        bin_to,
                        relation.get('relation_type')
                    ))
            
            conn.commit()
            logger.info(f"✅ Created company: {data.get('bin')} - {data.get('name_ru')}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating company {data.get('bin')}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        finally:
            cursor.close()
    
    # ════════════════════════════════════════════════════════════════
    # UPDATE OPERATION
    # ════════════════════════════════════════════════════════════════
    
    def update_company(
        self,
        bin_value: str,
        data: Dict[str, Any],
        changes: Dict[str, Dict[str, Any]]
    ):
        """
        Update existing company with references.
        
        Args:
            bin_value: Company BIN
            data: New company data
            changes: Detected changes
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            import json
            
            # Получить ID из справочников
            status_id = None
            if data.get('status'):
                status_id = self.ref_manager.get_or_create_status(
                    data['status']['code'],
                    data['status']['name']
                )
            
            krp_id = None
            if data.get('krp'):
                krp_id = self.ref_manager.get_or_create_krp(
                    data['krp']['code'],
                    data['krp']['name']
                )
            
            kfc_id = None
            if data.get('kfc'):
                kfc_id = self.ref_manager.get_or_create_kfc(
                    data['kfc']['code'],
                    data['kfc']['name']
                )
            
            kse_id = None
            if data.get('kse'):
                kse_id = self.ref_manager.get_or_create_kse(
                    data['kse']['code'],
                    data['kse']['name']
                )
            
            oked_id = None
            if data.get('oked'):
                oked_id = self.ref_manager.get_or_create_oked(
                    data['oked']['code'],
                    data['oked']['name']
                )
            
            # UPDATE компании
            sql = """
                UPDATE companies SET
                    name_ru = %s,
                    ceo_name = %s,
                    is_nds = %s,
                    krp_id = %s,
                    kfc_id = %s,
                    kse_id = %s,
                    oked_id = %s,
                    status_id = %s,
                    phone = %s::jsonb,
                    last_api_check = NOW(),
                    api_check_count = api_check_count + 1,
                    updated_at = NOW()
                WHERE bin = %s
            """
            
            cursor.execute(sql, (
                data.get('name_ru'),
                data.get('ceo_name'),
                data.get('is_nds'),
                krp_id,
                kfc_id,
                kse_id,
                oked_id,
                status_id,
                json.dumps(data.get('phone')) if data.get('phone') else None,
                bin_value
            ))
            
            # ═══ HANDLE NAME CHANGE ═══
            if 'name_ru' in changes:
                cursor.execute("""
                    UPDATE company_names
                    SET valid_to = NOW()::DATE, is_current = false
                    WHERE bin = %s AND is_current = true
                """, (bin_value,))
                
                cursor.execute("""
                    INSERT INTO company_names (
                        bin, name_ru, valid_from, is_current
                    ) VALUES (%s, %s, NOW()::DATE, true)
                """, (bin_value, data.get('name_ru')))
            
            # ═══ HANDLE CEO CHANGE ═══
            if 'ceo_name' in changes:
                cursor.execute("""
                    UPDATE company_ceos
                    SET valid_to = NOW()::DATE, is_current = false
                    WHERE bin = %s AND is_current = true
                """, (bin_value,))
                
                cursor.execute("""
                    INSERT INTO company_ceos (
                        bin, ceo_name, valid_from, is_current
                    ) VALUES (%s, %s, NOW()::DATE, true)
                """, (bin_value, data.get('ceo_name')))
            
            # ═══ UPDATE TAXES (UPSERT) ═══
            for tax in data.get('taxes', []):
                cursor.execute("""
                    INSERT INTO company_taxes (
                        bin, year, total_taxes, check_date
                    ) VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (bin, year) DO UPDATE SET
                        total_taxes = EXCLUDED.total_taxes,
                        check_date = NOW()
                """, (
                    bin_value,
                    tax.get('year'),
                    tax.get('total_taxes')
                ))
            
            # ═══ UPDATE NDS (UPSERT) ═══
            for nds in data.get('nds', []):
                cursor.execute("""
                    INSERT INTO company_nds (
                        bin, year, nds_amount, check_date
                    ) VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (bin, year) DO UPDATE SET
                        nds_amount = EXCLUDED.nds_amount,
                        check_date = NOW()
                """, (
                    bin_value,
                    nds.get('year'),
                    nds.get('nds_amount')
                ))
            
            # ═══ UPDATE RELATIONS (APPEND NEW) ═══
            for relation in data.get('relations', []):
                bin_to = relation.get('bin_to')
                if bin_to and self.exists_company(bin_to):
                    cursor.execute("""
                        INSERT INTO company_relations (
                            bin_from, bin_to, relation_type
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        relation.get('bin_from'),
                        bin_to,
                        relation.get('relation_type')
                    ))
            
            conn.commit()
            
            change_fields = ', '.join(changes.keys())
            logger.info(
                f"🔄 Updated company: {bin_value} - "
                f"Changes: {change_fields}"
            )
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating company {bin_value}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        finally:
            cursor.close()
    
    # ════════════════════════════════════════════════════════════════
    # UTILITY OPERATIONS
    # ════════════════════════════════════════════════════════════════
    
    def touch_last_check(self, bin_value: str):
        """
        Update last_api_check timestamp (no changes).
        
        Args:
            bin_value: Company BIN
        """
        sql = """
            UPDATE companies
            SET last_api_check = NOW(),
                api_check_count = api_check_count + 1
            WHERE bin = %s
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (bin_value,))
            conn.commit()
            
            logger.debug(f"⏭️ No changes: {bin_value}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error touching last_check for {bin_value}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def mark_not_found(self, bin_value: str):
        """
        Mark BIN as not found in API.
        
        Args:
            bin_value: Company BIN
        """
        sql = """
            INSERT INTO companies (
                bin, name_ru, api_not_found, last_api_check, api_check_count
            ) VALUES (%s, %s, true, NOW(), 1)
            ON CONFLICT (bin) DO UPDATE SET
                api_not_found = true,
                last_api_check = NOW(),
                api_check_count = companies.api_check_count + 1
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (bin_value, f'NOT_FOUND_{bin_value}'))
            conn.commit()
            
            logger.warning(f"❌ Marked as not found: {bin_value}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error marking not found {bin_value}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def mark_deleted(self, bin_value: str, registration_date: Optional[str] = None):
        """
        Mark BIN as deleted company.
        
        Args:
            bin_value: Company BIN
            registration_date: Registration date if available
        """
        sql = """
            INSERT INTO companies (
                bin, name_ru, registration_date, api_not_found, 
                last_api_check, api_check_count
            ) VALUES (%s, %s, %s, true, NOW(), 1)
            ON CONFLICT (bin) DO UPDATE SET
                api_not_found = true,
                last_api_check = NOW(),
                api_check_count = companies.api_check_count + 1
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (
                bin_value, 
                f'DELETED_{bin_value}',
                registration_date
            ))
            conn.commit()
            
            logger.warning(f"🗑️ Marked as deleted: {bin_value}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error marking deleted {bin_value}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def __del__(self):
        """Cleanup on deletion."""
        self.close()