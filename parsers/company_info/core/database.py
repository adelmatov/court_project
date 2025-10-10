"""
Database manager for company data
"""

from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor

from .config import DB_CONFIG
from .logger import logger


class DatabaseManager:
    """
    PostgreSQL database manager for company data.
    
    Подключается к БД 'companies' для сохранения данных.
    """
    
    def __init__(self):
        self.conn = None
    
    def _get_connection(self):
        """Get or create database connection to COMPANIES database."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**DB_CONFIG)
            logger.debug(f"Connected to companies database: {DB_CONFIG['database']}")
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
        Get company by BIN.
        
        Args:
            bin_value: Company BIN
        
        Returns:
            Company data as dict or None if not found
        """
        sql = """
            SELECT 
                bin, name_ru, registration_date, ceo_name,
                is_nds, degree_of_risk, krp_description,
                kfc_description, kse_description,
                primary_oked_code, primary_oked_name,
                status_description, phone,
                last_api_check, api_check_count,
                created_at, updated_at
            FROM companies
            WHERE bin = %s
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(sql, (bin_value,))
            
            result = cursor.fetchone()
            
            return dict(result) if result else None
            
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
        """
        Create new company with all related data.
        
        Args:
            data: Parsed company data
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # ═══ HELPER FUNCTIONS ═══
            def safe_value(value, default=None):
                """Safely extract value, converting dict to string if needed."""
                if value is None:
                    return default
                
                # Если dict - попробовать извлечь 'value'
                if isinstance(value, dict):
                    if 'value' in value:
                        return safe_value(value['value'], default)
                    return default
                
                return value
            
            def safe_str(value):
                """Convert to string safely (no length limit)."""
                result = safe_value(value)
                return str(result) if result is not None else None
            
            def safe_bool(value):
                """Convert to boolean safely."""
                result = safe_value(value)
                if result is None:
                    return None
                if isinstance(result, bool):
                    return result
                if isinstance(result, str):
                    return result.lower() in ('true', '1', 'yes')
                return bool(result)
            
            def safe_int(value):
                """Convert to int safely."""
                result = safe_value(value)
                if result is None:
                    return None
                try:
                    return int(result)
                except (ValueError, TypeError):
                    return None
            
            def safe_float(value):
                """Convert to float safely."""
                result = safe_value(value)
                if result is None:
                    return 0.0
                try:
                    return float(result)
                except (ValueError, TypeError):
                    return 0.0
            
            # ═══ INSERT MAIN COMPANY RECORD ═══
            sql = """
                INSERT INTO companies (
                    bin, name_ru, registration_date, ceo_name,
                    is_nds, degree_of_risk, krp_description,
                    kfc_description, kse_description,
                    primary_oked_code, primary_oked_name,
                    status_description, phone,
                    last_api_check, api_check_count
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 1
                )
            """
            
            cursor.execute(sql, (
                safe_str(data.get('bin')),
                safe_str(data.get('name_ru')),
                data.get('registration_date'),
                safe_str(data.get('ceo_name')),
                safe_bool(data.get('is_nds')),
                safe_str(data.get('degree_of_risk')),
                safe_str(data.get('krp_description')),
                safe_str(data.get('kfc_description')),
                safe_str(data.get('kse_description')),
                safe_str(data.get('primary_oked_code')),
                safe_str(data.get('primary_oked_name')),
                safe_str(data.get('status_description')),
                safe_str(data.get('phone'))
            ))
            
            # ═══ INSERT NAME HISTORY ═══
            for name_entry in data.get('name_history', []):
                cursor.execute("""
                    INSERT INTO company_names (
                        bin, name_ru, valid_from, valid_to, is_current
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    safe_str(data.get('bin')),
                    safe_str(name_entry.get('name_ru')),
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
                    safe_str(data.get('bin')),
                    safe_str(data.get('ceo_name')),
                    data.get('registration_date')
                ))
            
            # ═══ INSERT OKED ═══
            if data.get('primary_oked_code'):
                cursor.execute("""
                    INSERT INTO company_okeds (
                        bin, oked_code, oked_name, is_primary
                    ) VALUES (%s, %s, %s, true)
                """, (
                    safe_str(data.get('bin')),
                    safe_str(data.get('primary_oked_code')),
                    safe_str(data.get('primary_oked_name'))
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
                    safe_str(data.get('bin')),
                    safe_int(tax.get('year')),
                    safe_float(tax.get('total_taxes'))
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
                    safe_str(data.get('bin')),
                    safe_int(nds.get('year')),
                    safe_float(nds.get('nds_amount'))
                ))
            
            # ═══ INSERT RELATIONS ═══
            for relation in data.get('relations', []):
                bin_to = safe_str(relation.get('bin_to'))
                if bin_to and self.exists_company(bin_to):
                    cursor.execute("""
                        INSERT INTO company_relations (
                            bin_from, bin_to, relation_type
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        safe_str(relation.get('bin_from')),
                        bin_to,
                        safe_str(relation.get('relation_type'))
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
            if cursor:
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
        Update existing company.
        
        Args:
            bin_value: Company BIN
            data: New company data
            changes: Detected changes
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # ═══ HELPER FUNCTIONS ═══
            def safe_value(value, default=None):
                """Safely extract value."""
                if value is None:
                    return default
                if isinstance(value, dict):
                    if 'value' in value:
                        return safe_value(value['value'], default)
                    return default
                return value
            
            def safe_str(value):
                """Convert to string safely."""
                result = safe_value(value)
                return str(result) if result is not None else None
            
            def safe_bool(value):
                """Convert to boolean safely."""
                result = safe_value(value)
                if result is None:
                    return None
                if isinstance(result, bool):
                    return result
                if isinstance(result, str):
                    return result.lower() in ('true', '1', 'yes')
                return bool(result)
            
            def safe_int(value):
                """Convert to int safely."""
                result = safe_value(value)
                if result is None:
                    return None
                try:
                    return int(result)
                except (ValueError, TypeError):
                    return None
            
            def safe_float(value):
                """Convert to float safely."""
                result = safe_value(value)
                if result is None:
                    return 0.0
                try:
                    return float(result)
                except (ValueError, TypeError):
                    return 0.0
            
            # ═══ UPDATE MAIN RECORD ═══
            sql = """
                UPDATE companies SET
                    name_ru = %s,
                    ceo_name = %s,
                    is_nds = %s,
                    degree_of_risk = %s,
                    krp_description = %s,
                    kfc_description = %s,
                    kse_description = %s,
                    primary_oked_code = %s,
                    primary_oked_name = %s,
                    status_description = %s,
                    phone = %s,
                    last_api_check = NOW(),
                    api_check_count = api_check_count + 1,
                    updated_at = NOW()
                WHERE bin = %s
            """
            
            cursor.execute(sql, (
                safe_str(data.get('name_ru')),
                safe_str(data.get('ceo_name')),
                safe_bool(data.get('is_nds')),
                safe_str(data.get('degree_of_risk')),
                safe_str(data.get('krp_description')),
                safe_str(data.get('kfc_description')),
                safe_str(data.get('kse_description')),
                safe_str(data.get('primary_oked_code')),
                safe_str(data.get('primary_oked_name')),
                safe_str(data.get('status_description')),
                safe_str(data.get('phone')),
                safe_str(bin_value)
            ))
            
            # ═══ HANDLE NAME CHANGE ═══
            if 'name_ru' in changes:
                cursor.execute("""
                    UPDATE company_names
                    SET valid_to = NOW()::DATE, is_current = false
                    WHERE bin = %s AND is_current = true
                """, (safe_str(bin_value),))
                
                cursor.execute("""
                    INSERT INTO company_names (
                        bin, name_ru, valid_from, is_current
                    ) VALUES (%s, %s, NOW()::DATE, true)
                """, (safe_str(bin_value), safe_str(data.get('name_ru'))))
            
            # ═══ HANDLE CEO CHANGE ═══
            if 'ceo_name' in changes:
                cursor.execute("""
                    UPDATE company_ceos
                    SET valid_to = NOW()::DATE, is_current = false
                    WHERE bin = %s AND is_current = true
                """, (safe_str(bin_value),))
                
                cursor.execute("""
                    INSERT INTO company_ceos (
                        bin, ceo_name, valid_from, is_current
                    ) VALUES (%s, %s, NOW()::DATE, true)
                """, (safe_str(bin_value), safe_str(data.get('ceo_name'))))
            
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
                    safe_str(bin_value),
                    safe_int(tax.get('year')),
                    safe_float(tax.get('total_taxes'))
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
                    safe_str(bin_value),
                    safe_int(nds.get('year')),
                    safe_float(nds.get('nds_amount'))
                ))
            
            # ═══ UPDATE RELATIONS (APPEND NEW) ═══
            for relation in data.get('relations', []):
                bin_to = safe_str(relation.get('bin_to'))
                if bin_to and self.exists_company(bin_to):
                    cursor.execute("""
                        INSERT INTO company_relations (
                            bin_from, bin_to, relation_type
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        safe_str(relation.get('bin_from')),
                        bin_to,
                        safe_str(relation.get('relation_type'))
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
            if cursor:
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
    
    def __del__(self):
        """Cleanup on deletion."""
        self.close()