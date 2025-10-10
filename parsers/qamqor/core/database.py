"""Менеджер работы с базой данных."""

import asyncio
import logging
import psycopg2
import psycopg2.extras
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .config import Config


class DatabaseManager:
    """Менеджер работы с PostgreSQL."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self._connection_retry_count = 3
        self._connection_retry_delay = 5.0
        
    def _get_connection_params(self) -> dict:
        """Получить параметры подключения к БД."""
        return {
            'host': self.config.DB_HOST,
            'port': self.config.DB_PORT,
            'database': self.config.DB_NAME,
            'user': self.config.DB_USER,
            'password': self.config.DB_PASSWORD,
            'connect_timeout': 10
        }
    
    @asynccontextmanager
    async def get_connection(self):
        """
        Подключение к БД с ретраями при сетевых ошибках.
        
        Yields:
            psycopg2.connection
        """
        conn = None
        
        for attempt in range(1, self._connection_retry_count + 1):
            try:
                conn = psycopg2.connect(**self._get_connection_params())
                conn.autocommit = False
                
                yield conn
                
                # Коммит только если не было исключений
                if conn and not conn.closed:
                    conn.commit()
                break
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                # Сетевые ошибки - retry
                if conn:
                    try:
                        conn.rollback()
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                
                if attempt < self._connection_retry_count:
                    delay = self._connection_retry_delay * attempt
                    self.logger.warning(
                        f"⚠️ БД недоступна (попытка {attempt}/{self._connection_retry_count}). "
                        f"Повтор через {delay}с..."
                    )
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(
                        f"❌ БД недоступна после {self._connection_retry_count} попыток"
                    )
                    raise
                    
            except Exception as e:
                # Другие ошибки - rollback и raise
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                self.logger.error(f"❌ Ошибка БД: {e}", exc_info=True)
                raise
                
            finally:
                # Закрываем соединение
                if conn and not conn.closed:
                    try:
                        conn.close()
                    except Exception as e:
                        self.logger.debug(f"Ошибка закрытия соединения: {e}")
    
    async def initialize_tables(self):
        """Создание таблиц с правильной структурой."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            registration_number TEXT PRIMARY KEY,
            reg_date DATE,
            act_date DATE,
            start_date DATE,
            end_date DATE,
            suspend_date DATE,
            resume_date DATE,
            prolong_start DATE,
            prolong_end DATE,
            revenue_name TEXT,
            kpssu_name TEXT,
            check_type TEXT,
            subject_bin TEXT,
            subject_name TEXT,
            subject_address TEXT,
            status_id TEXT,
            status TEXT,
            audit_theme TEXT,
            audit_theme_1 TEXT,
            theme_check TEXT,
            theme_check_1 TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_{table_name}_reg_date ON {table_name}(reg_date);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_bin ON {table_name}(subject_bin);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_status ON {table_name}(status_id);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_updated ON {table_name}(updated_at);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_region_seq ON {table_name}(
            CAST(SUBSTRING(registration_number, 3, 7) AS INTEGER),
            CAST(SPLIT_PART(registration_number, '/', 2) AS INTEGER)
        );
        
        CREATE OR REPLACE FUNCTION update_{table_name}_timestamp()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        DROP TRIGGER IF EXISTS trigger_update_{table_name}_timestamp ON {table_name};
        CREATE TRIGGER trigger_update_{table_name}_timestamp
        BEFORE UPDATE ON {table_name}
        FOR EACH ROW
        EXECUTE FUNCTION update_{table_name}_timestamp();
        """
        
        create_audit_log_sql = """
        CREATE TABLE IF NOT EXISTS audit_log (
            id SERIAL PRIMARY KEY,
            registration_number TEXT NOT NULL,
            table_name TEXT NOT NULL,
            changed_fields TEXT[],
            old_values JSONB,
            new_values JSONB,
            changed_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_audit_log_reg_num ON audit_log(registration_number);
        CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at ON audit_log(changed_at);
        CREATE INDEX IF NOT EXISTS idx_audit_log_table ON audit_log(table_name);
        """
        
        add_updated_at_sql = """
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name=%s AND column_name='updated_at'
            ) THEN
                EXECUTE 'ALTER TABLE ' || %s || ' ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP';
            END IF;
        END $$;
        """
        
        async with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Создание таблиц
            cursor.execute(create_table_sql.format(table_name="qamqor_tax"))
            cursor.execute(create_table_sql.format(table_name="qamqor_customs"))
            cursor.execute(create_audit_log_sql)
            
            # Добавление updated_at к существующим таблицам
            for table in ['qamqor_tax', 'qamqor_customs']:
                cursor.execute(add_updated_at_sql, (table, table))
            
            self.logger.debug("✅ Таблицы инициализированы")
    
    async def get_region_state(self) -> Dict[int, int]:
        """
        Получение последних обработанных номеров из БД.
        
        Returns:
            Словарь {region_code: next_number}
        """
        region_state = {}
        
        async with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT region_code, COALESCE(MAX(seq_number), 0) + 1 as next_number
                FROM (
                    SELECT 
                        CAST(SUBSTRING(registration_number, 3, 7) AS INTEGER) as region_code,
                        CAST(SPLIT_PART(registration_number, '/', 2) AS INTEGER) as seq_number
                    FROM qamqor_tax 
                    WHERE registration_number ~ '^25[0-9]{7}170101[12]/[0-9]{5}$'
                    UNION ALL
                    SELECT 
                        CAST(SUBSTRING(registration_number, 3, 7) AS INTEGER) as region_code,
                        CAST(SPLIT_PART(registration_number, '/', 2) AS INTEGER) as seq_number
                    FROM qamqor_customs 
                    WHERE registration_number ~ '^25[0-9]{7}170101[12]/[0-9]{5}$'
                ) combined 
                GROUP BY region_code
            """)
            
            for region_code, next_number in cursor.fetchall():
                region_state[region_code] = next_number
        
        # Устанавливаем стартовое значение для регионов без данных
        for region_code in self.config.REGIONS.keys():
            region_state.setdefault(region_code, self.config.START_NUMBER)
        
        return region_state
    
    async def get_region_stats(self) -> Dict[int, Dict]:
        """
        Получение статистики по регионам.
        
        Returns:
            Словарь со статистикой по каждому региону
        """
        stats = {}
        
        async with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for region_code in self.config.REGIONS.keys():
                pattern = f'^25{region_code}170101[12]/[0-9]{{5}}$'
                
                # Подсчет записей
                cursor.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT registration_number FROM qamqor_tax WHERE registration_number ~ %s
                        UNION ALL
                        SELECT registration_number FROM qamqor_customs WHERE registration_number ~ %s
                    ) combined
                """, (pattern, pattern))
                total_count = cursor.fetchone()[0]
                
                # Следующий номер
                cursor.execute("""
                    SELECT COALESCE(MAX(seq_number), 0) + 1
                    FROM (
                        SELECT CAST(SPLIT_PART(registration_number, '/', 2) AS INTEGER) as seq_number
                        FROM qamqor_tax WHERE registration_number ~ %s
                        UNION ALL
                        SELECT CAST(SPLIT_PART(registration_number, '/', 2) AS INTEGER) as seq_number
                        FROM qamqor_customs WHERE registration_number ~ %s
                    ) combined
                """, (pattern, pattern))
                next_number = cursor.fetchone()[0]
                
                # Пропущенные номера
                cursor.execute("""
                    SELECT CAST(SPLIT_PART(registration_number, '/', 2) AS INTEGER) as seq_num
                    FROM (
                        SELECT registration_number FROM qamqor_tax WHERE registration_number ~ %s
                        UNION 
                        SELECT registration_number FROM qamqor_customs WHERE registration_number ~ %s
                    ) combined 
                    ORDER BY seq_num
                """, (pattern, pattern))
                
                existing_numbers = {row[0] for row in cursor.fetchall()}
                missing_count = 0
                
                if existing_numbers:
                    max_num = max(existing_numbers)
                    expected_numbers = set(range(1, max_num + 1))
                    missing_count = len(expected_numbers - existing_numbers)
                
                stats[region_code] = {
                    'total_records': total_count,
                    'next_number': max(next_number, 1),
                    'missing_count': missing_count,
                    'found_new': 0  # Будет обновляться во время парсинга
                }
        
        return stats
    
    async def find_missing_numbers(self) -> Dict[int, List[int]]:
        """
        Поиск пропущенных номеров в БД.
        
        Returns:
            Словарь {region_code: [missing_numbers]}
        """
        missing_numbers = {}
        
        async with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for region_code in self.config.REGIONS.keys():
                pattern = f'^25{region_code}170101[12]/[0-9]{{5}}$'
                
                cursor.execute("""
                    SELECT CAST(SPLIT_PART(registration_number, '/', 2) AS INTEGER) as seq_num
                    FROM (
                        SELECT registration_number FROM qamqor_tax WHERE registration_number ~ %s
                        UNION 
                        SELECT registration_number FROM qamqor_customs WHERE registration_number ~ %s
                    ) combined 
                    ORDER BY seq_num
                """, (pattern, pattern))
                
                existing_numbers = {row[0] for row in cursor.fetchall()}
                
                if existing_numbers:
                    max_num = max(existing_numbers)
                    expected_numbers = set(range(1, max_num + 1))
                    region_missing = sorted(list(expected_numbers - existing_numbers))
                    
                    # Исключаем номера из конфигурации
                    if region_code in self.config.EXCLUDED_MISSING_NUMBERS:
                        excluded = set(self.config.EXCLUDED_MISSING_NUMBERS[region_code])
                        region_missing = [num for num in region_missing if num not in excluded]
                    
                    if region_missing:
                        missing_numbers[region_code] = region_missing
        
        return missing_numbers
    
    async def bulk_insert_data(
        self, 
        data_batch: List[Dict], 
        silent: bool = False
    ) -> Tuple[int, int]:
        """Массовая вставка с оптимизацией."""
        if not data_batch:
            return 0, 0
        
        # ✅ Предобработка данных ВНЕ транзакции
        tax_data = []
        customs_data = []
        
        for item in data_batch:
            reg_num = item.get('registration_number', '')
            if not reg_num or '/' not in reg_num:
                continue
            
            try:
                prefix, _ = reg_num.split('/')
                check_type = int(prefix[-1])
                values = self._prepare_db_values(item)
                
                if check_type == 1:
                    tax_data.append(values)
                elif check_type == 2:
                    customs_data.append(values)
            except (ValueError, IndexError):
                continue
        
        if not tax_data and not customs_data:
            return 0, 0
        
        # ✅ Retry с экспоненциальной задержкой
        for attempt in range(1, 4):
            try:
                async with self.get_connection() as conn:
                    cursor = conn.cursor()
                    tax_inserted = customs_inserted = 0
                    
                    # ✅ Используем COPY для больших батчей
                    if len(tax_data) > 1000:
                        tax_inserted = await self._bulk_copy(cursor, "qamqor_tax", tax_data)
                    elif tax_data:
                        tax_inserted = await self._bulk_execute_values(cursor, "qamqor_tax", tax_data)
                    
                    if len(customs_data) > 1000:
                        customs_inserted = await self._bulk_copy(cursor, "qamqor_customs", customs_data)
                    elif customs_data:
                        customs_inserted = await self._bulk_execute_values(cursor, "qamqor_customs", customs_data)
                    
                    if not silent and (tax_inserted > 0 or customs_inserted > 0):
                        self.logger.info(f"💾 TAX: +{tax_inserted}, CUSTOMS: +{customs_inserted}")
                    
                    return tax_inserted, customs_inserted
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                if attempt < 3:
                    delay = 2 ** attempt  # Экспоненциальная задержка
                    self.logger.warning(f"⚠️ DB retry {attempt}/3 через {delay}s")
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(f"❌ DB недоступна после 3 попыток")
                    raise
            
            except Exception as e:
                self.logger.error(f"❌ Ошибка вставки: {e}", exc_info=True)
                raise
        
        return 0, 0

    async def _bulk_execute_values(self, cursor, table_name: str, data: List[Tuple]) -> int:
        """Вставка через execute_values."""
        insert_sql = f"""
            INSERT INTO {table_name} (
                registration_number, reg_date, act_date, start_date, end_date,
                suspend_date, resume_date, prolong_start, prolong_end,
                revenue_name, kpssu_name, check_type, subject_bin, subject_name,
                subject_address, status_id, status, audit_theme, audit_theme_1,
                theme_check, theme_check_1
            ) VALUES %s 
            ON CONFLICT (registration_number) DO NOTHING
        """
        
        psycopg2.extras.execute_values(cursor, insert_sql, data, page_size=500)
        return cursor.rowcount
    
    def _prepare_db_values(self, item: Dict) -> Tuple:
        """Подготовка данных для вставки."""
        return (
            item.get('registration_number'),
            self._parse_date(item.get('reg_date')),
            self._parse_date(item.get('act_date')),
            self._parse_date(item.get('start_date')),
            self._parse_date(item.get('end_date')),
            self._parse_date(item.get('suspend_date')),
            self._parse_date(item.get('resume_date')),
            self._parse_date(item.get('prolong_start')),
            self._parse_date(item.get('prolong_end')),
            item.get('revenue_name'),
            item.get('kpssu_name'),
            item.get('check_type'),
            item.get('subject_bin'),
            item.get('subject_name'),
            item.get('subject_address'),
            item.get('status_id'),
            item.get('status'),
            item.get('audit_theme'),
            item.get('audit_theme_1'),
            item.get('theme_check'),
            item.get('theme_check_1')
        )
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Парсинг даты в формат PostgreSQL."""
        if not date_str:
            return None
        
        # Проверяем, что уже в формате YYYY-MM-DD
        if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
                return date_str
            except ValueError:
                pass
        
        # Парсим другие форматы
        for fmt in ("%d/%m/%Y", "%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        return None
    
    async def get_summary_stats(self) -> Dict:
        """Получение сводной статистики из БД."""
        async with self.get_connection() as conn:
            cursor = conn.cursor()
            stats = {}
            
            for table in ['qamqor_tax', 'qamqor_customs']:
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total,
                        MIN(reg_date) as earliest,
                        MAX(reg_date) as latest
                    FROM {table}
                """)
                row = cursor.fetchone()
                stats[table] = {
                    'total': row[0],
                    'earliest': row[1],
                    'latest': row[2]
                }
            
            return stats
    
    async def get_records_to_update(
        self,
        statuses: Optional[List[str]] = None,
        min_age_days: Optional[int] = None,
        cooldown_days: Optional[int] = None,
        force: bool = False
    ) -> Dict[str, List[str]]:
        """
        Получить registration_numbers для обновления.
        
        Args:
            statuses: Список статусов для фильтрации
            min_age_days: Минимальный возраст записи от reg_date
            cooldown_days: Минимальный интервал между обновлениями
            force: Игнорировать фильтры по возрасту
            
        Returns:
            {"tax": [...], "customs": [...]}
        """
        if statuses is None:
            statuses = self.config.UPDATE_STATUSES
        
        if min_age_days is None:
            min_age_days = self.config.UPDATE_MIN_AGE_DAYS
        
        if cooldown_days is None:
            cooldown_days = self.config.UPDATE_COOLDOWN_DAYS
        
        result = {"tax": [], "customs": []}
        
        async with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for table_name, key in [("qamqor_tax", "tax"), ("qamqor_customs", "customs")]:
                sql = f"""
                    SELECT registration_number 
                    FROM {table_name}
                    WHERE status_id = ANY(%s)
                    AND reg_date IS NOT NULL
                """
                
                params = [statuses]
                
                if not force:
                    # Фильтр по возрасту от reg_date
                    sql += f" AND reg_date < CURRENT_DATE - INTERVAL '{min_age_days} days'"
                    
                    # Не обновлялись дольше cooldown_days
                    sql += f"""
                        AND (
                            updated_at IS NULL 
                            OR updated_at < NOW() - INTERVAL '{cooldown_days} days'
                            OR updated_at = created_at
                        )
                    """
                
                sql += " ORDER BY reg_date ASC"
                
                cursor.execute(sql, params)
                result[key] = [row[0] for row in cursor.fetchall()]
        
        return result

    async def bulk_update_data(
        self, 
        data_batch: List[Dict], 
        silent: bool = False
    ) -> Tuple[int, int, List[Dict]]:
        """
        Массовое обновление записей с отслеживанием изменений.
        
        Args:
            data_batch: Список записей для обновления
            silent: Не логировать успешные обновления
            
        Returns:
            (tax_updated, customs_updated, changes_log)
        """
        if not data_batch:
            return 0, 0, []
        
        # Разделение на tax и customs
        tax_data = []
        customs_data = []
        
        for item in data_batch:
            reg_num = item.get('registration_number', '')
            if not reg_num or '/' not in reg_num:
                continue
            
            try:
                prefix = reg_num.split('/')[0]
                check_type = int(prefix[-1])
                values = self._prepare_db_values(item)
                
                if check_type == 1:
                    tax_data.append(values)
                elif check_type == 2:
                    customs_data.append(values)
            except (ValueError, IndexError):
                continue
        
        changes_log = []
        
        # Retry-логика
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                async with self.get_connection() as conn:
                    cursor = conn.cursor()
                    tax_updated = customs_updated = 0
                    
                    # UPDATE SQL с отслеживанием изменений
                    update_sql = """
                        WITH old_data AS (
                            SELECT status_id, status, end_date, registration_number
                            FROM {table_name} 
                            WHERE registration_number = %s
                        )
                        UPDATE {table_name} SET
                            reg_date = %s,
                            act_date = %s,
                            start_date = %s,
                            end_date = %s,
                            suspend_date = %s,
                            resume_date = %s,
                            prolong_start = %s,
                            prolong_end = %s,
                            revenue_name = %s,
                            kpssu_name = %s,
                            check_type = %s,
                            subject_bin = %s,
                            subject_name = %s,
                            subject_address = %s,
                            status_id = %s,
                            status = %s,
                            audit_theme = %s,
                            audit_theme_1 = %s,
                            theme_check = %s,
                            theme_check_1 = %s
                        WHERE registration_number = %s
                        RETURNING 
                            (SELECT status_id FROM old_data) as old_status_id,
                            (SELECT status FROM old_data) as old_status,
                            (SELECT end_date FROM old_data) as old_end_date,
                            status_id as new_status_id,
                            status as new_status,
                            end_date as new_end_date,
                            registration_number
                    """
                    
                    if tax_data:
                        for values in tax_data:
                            reg_num = values[0]
                            params = (reg_num,) + values[1:] + (reg_num,)
                            
                            cursor.execute(
                                update_sql.format(table_name="qamqor_tax"),
                                params
                            )
                            
                            result = cursor.fetchone()
                            if result:
                                tax_updated += 1
                                
                                if self.config.UPDATE_TRACK_CHANGES:
                                    changes = self._detect_changes(result)
                                    if changes:
                                        changes_log.append({
                                            'table': 'qamqor_tax',
                                            'registration_number': result[6],
                                            'changes': changes
                                        })
                        
                        if tax_updated > 0 and not silent:
                            self.logger.info(f"🔄 TAX: ~{tax_updated}")
                    
                    if customs_data:
                        for values in customs_data:
                            reg_num = values[0]
                            params = (reg_num,) + values[1:] + (reg_num,)
                            
                            cursor.execute(
                                update_sql.format(table_name="qamqor_customs"),
                                params
                            )
                            
                            result = cursor.fetchone()
                            if result:
                                customs_updated += 1
                                
                                if self.config.UPDATE_TRACK_CHANGES:
                                    changes = self._detect_changes(result)
                                    if changes:
                                        changes_log.append({
                                            'table': 'qamqor_customs',
                                            'registration_number': result[6],
                                            'changes': changes
                                        })
                        
                        if customs_updated > 0 and not silent:
                            self.logger.info(f"🔄 CUSTOMS: ~{customs_updated}")
                    
                    # Записать изменения в audit_log
                    if changes_log:
                        await self._log_changes(cursor, changes_log)
                    
                    return tax_updated, customs_updated, changes_log
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                if attempt < max_attempts:
                    self.logger.warning(
                        f"⚠️ Ошибка обновления (попытка {attempt}/{max_attempts}): {e}"
                    )
                    await asyncio.sleep(2 * attempt)
                else:
                    self.logger.error(f"❌ Не удалось обновить данные после {max_attempts} попыток")
                    raise
            
            except Exception as e:
                self.logger.error(f"❌ Ошибка обновления: {e}", exc_info=True)
                raise
        
        return 0, 0, []

    def _detect_changes(self, result: Tuple) -> Optional[Dict]:
        """Определить изменения между старыми и новыми данными."""
        (old_status_id, old_status, old_end_date, 
         new_status_id, new_status, new_end_date, reg_num) = result
        
        changes = {}
        
        if old_status_id != new_status_id:
            changes['status_id'] = {'old': old_status_id, 'new': new_status_id}
        
        if old_status != new_status:
            changes['status'] = {'old': old_status, 'new': new_status}
        
        if old_end_date != new_end_date:
            changes['end_date'] = {
                'old': str(old_end_date) if old_end_date else None,
                'new': str(new_end_date) if new_end_date else None
            }
        
        return changes if changes else None

    async def _log_changes(self, cursor, changes_log: List[Dict]):
        """Записать изменения в таблицу audit_log."""
        if not changes_log:
            return
        
        insert_sql = """
            INSERT INTO audit_log (
                registration_number, 
                table_name, 
                changed_fields, 
                old_values, 
                new_values
            ) VALUES %s
        """
        
        values = []
        for change in changes_log:
            changed_fields = list(change['changes'].keys())
            old_values = {k: v['old'] for k, v in change['changes'].items()}
            new_values = {k: v['new'] for k, v in change['changes'].items()}
            
            values.append((
                change['registration_number'],
                change['table'],
                changed_fields,
                psycopg2.extras.Json(old_values),
                psycopg2.extras.Json(new_values)
            ))
        
        psycopg2.extras.execute_values(cursor, insert_sql, values)

    async def get_update_summary(self, since: datetime) -> Dict:
        """Получить сводку обновлений за период."""
        async with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    table_name,
                    COUNT(*) as total_changes,
                    COUNT(DISTINCT registration_number) as unique_records,
                    array_agg(DISTINCT unnest(changed_fields)) as fields_changed
                FROM audit_log
                WHERE changed_at >= %s
                GROUP BY table_name
            """, (since,))
            
            results = cursor.fetchall()
            
            summary = {}
            for table_name, total, unique, fields in results:
                summary[table_name] = {
                    'total_changes': total,
                    'unique_records': unique,
                    'fields_changed': [f for f in fields if f is not None]
                }
            
            return summary