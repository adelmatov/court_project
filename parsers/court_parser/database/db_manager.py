"""
Менеджер базы данных
"""
import re
from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timedelta
import asyncpg

from database.models import CaseData, EventData
from utils.text_processor import TextProcessor
from utils.validators import DataValidator
from utils.logger import get_logger
from utils.constants import PartyRole


class DatabaseManager:
    """Менеджер базы данных"""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.pool: Optional[asyncpg.Pool] = None
        self.text_processor = TextProcessor()
        self.validator = DataValidator()
        self.logger = get_logger('db_manager')
        
        # Кеши для ID сущностей
        self.judges_cache: Dict[str, int] = {}
        self.parties_cache: Dict[str, int] = {}
        self.event_types_cache: Dict[str, int] = {}
    
    async def connect(self):
        """Подключение к БД"""
        import traceback
        
        try:
            self.pool = await asyncpg.create_pool(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                min_size=1,
                max_size=10
            )
            
            # Загрузка кешей
            await self._load_caches()
            
            self.logger.info("✅ Подключение к БД установлено")
            
        except Exception as e:
            self.logger.critical(f"❌ Ошибка подключения к БД: {e}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            raise
    
    async def disconnect(self):
        """Отключение от БД"""
        if self.pool:
            await self.pool.close()
            self.logger.info("Подключение к БД закрыто")
    
    async def save_case(self, case_data: CaseData) -> Dict[str, Any]:
        """
        Сохранение дела в БД
        
        Возвращает: {'status': 'saved'|'updated'|'error', 'case_id': int}
        """
        try:
            # Валидация данных
            self.validator.validate_case_data(case_data.to_dict())
            
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Сохранение дела
                    case_id = await self._save_case_record(conn, case_data)
                    
                    if not case_id:
                        return {'status': 'error', 'case_id': None}
                    
                    # 2. Сохранение сторон
                    await self._save_parties(conn, case_id, case_data)
                    
                    # 3. Сохранение событий
                    await self._save_events(conn, case_id, case_data.events)
                    
                    self.logger.info(f"✅ Дело сохранено: {case_data.case_number}")
                    return {'status': 'saved', 'case_id': case_id}
        
        except asyncpg.UniqueViolationError:
            self.logger.debug(f"Дело уже существует: {case_data.case_number}")
            return {'status': 'updated', 'case_id': None}
        
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения дела {case_data.case_number}: {e}")
            return {'status': 'error', 'case_id': None}
    
    async def _save_case_record(self, conn: asyncpg.Connection, 
                               case_data: CaseData) -> Optional[int]:
        """Сохранение записи дела"""
        # Получение/создание судьи
        judge_id = None
        if case_data.judge:
            judge_id = await self._get_or_create_judge(conn, case_data.judge)
        
        # Вставка дела
        query = """
            INSERT INTO cases (case_number, case_date, judge_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (case_number) DO UPDATE 
            SET case_date = EXCLUDED.case_date,
                judge_id = COALESCE(EXCLUDED.judge_id, cases.judge_id),
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
        """
        
        case_id = await conn.fetchval(
            query,
            case_data.case_number,
            case_data.case_date,
            judge_id
        )
        
        return case_id
    
    async def _save_parties(self, conn: asyncpg.Connection, 
                          case_id: int, case_data: CaseData):
        """Сохранение сторон дела"""
        # Истцы
        for plaintiff in case_data.plaintiffs:
            if self.validator.validate_party_name(plaintiff):
                party_id = await self._get_or_create_party(conn, plaintiff)
                await self._link_party_to_case(conn, case_id, party_id, PartyRole.PLAINTIFF)
        
        # Ответчики
        for defendant in case_data.defendants:
            if self.validator.validate_party_name(defendant):
                party_id = await self._get_or_create_party(conn, defendant)
                await self._link_party_to_case(conn, case_id, party_id, PartyRole.DEFENDANT)
    
    async def _save_events(self, conn: asyncpg.Connection, 
                         case_id: int, events: List[EventData]):
        """Сохранение событий дела"""
        for event in events:
            if self.validator.validate_event(event.to_dict()):
                event_type_id = await self._get_or_create_event_type(
                    conn, event.event_type
                )
                
                await conn.execute(
                    """
                    INSERT INTO case_events (case_id, event_type_id, event_date)
                    VALUES ($1, $2, $3)
                    ON CONFLICT DO NOTHING
                    """,
                    case_id, event_type_id, event.event_date
                )
    
    async def _get_or_create_judge(self, conn: asyncpg.Connection, 
                                  judge_name: str) -> int:
        """Получение или создание судьи"""
        judge_name = self.text_processor.clean(judge_name)
        
        # Проверка кеша
        if judge_name in self.judges_cache:
            return self.judges_cache[judge_name]
        
        # Создание/получение из БД
        judge_id = await conn.fetchval(
            """
            INSERT INTO judges (full_name)
            VALUES ($1)
            ON CONFLICT (full_name) DO UPDATE SET full_name = EXCLUDED.full_name
            RETURNING id
            """,
            judge_name
        )
        
        self.judges_cache[judge_name] = judge_id
        return judge_id
    
    async def _get_or_create_party(self, conn: asyncpg.Connection, 
                                  party_name: str) -> int:
        """Получение или создание стороны"""
        party_name = self.text_processor.clean(party_name)
        
        if party_name in self.parties_cache:
            return self.parties_cache[party_name]
        
        party_id = await conn.fetchval(
            """
            INSERT INTO parties (name)
            VALUES ($1)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            party_name
        )
        
        self.parties_cache[party_name] = party_id
        return party_id
    
    async def _get_or_create_event_type(self, conn: asyncpg.Connection, 
                                       event_type: str) -> int:
        """Получение или создание типа события"""
        event_type = self.text_processor.clean(event_type)
        
        if event_type in self.event_types_cache:
            return self.event_types_cache[event_type]
        
        event_type_id = await conn.fetchval(
            """
            INSERT INTO event_types (name)
            VALUES ($1)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            event_type
        )
        
        self.event_types_cache[event_type] = event_type_id
        return event_type_id
    
    async def _link_party_to_case(self, conn: asyncpg.Connection,
                                 case_id: int, party_id: int, role: str):
        """Связывание стороны с делом"""
        await conn.execute(
            """
            INSERT INTO case_parties (case_id, party_id, party_role)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            """,
            case_id, party_id, role
        )
    
    async def _load_caches(self):
        """Загрузка кешей из БД"""
        async with self.pool.acquire() as conn:
            # Судьи
            judges = await conn.fetch("SELECT id, full_name FROM judges")
            for row in judges:
                self.judges_cache[row['full_name']] = row['id']
            
            # Стороны
            parties = await conn.fetch("SELECT id, name FROM parties")
            for row in parties:
                self.parties_cache[row['name']] = row['id']
            
            # Типы событий
            events = await conn.fetch("SELECT id, name FROM event_types")
            for row in events:
                self.event_types_cache[row['name']] = row['id']
        
        self.logger.debug(f"Кеши загружены: {len(self.judges_cache)} судей, "
                         f"{len(self.parties_cache)} сторон, "
                         f"{len(self.event_types_cache)} типов событий")
        
    def _extract_sequence_number(self, case_number: str) -> Optional[int]:
        """
        Извлечь порядковый номер из номера дела
        
        Обрабатывает суффиксы дубликатов (2), (3) и т.д.
        
        Примеры:
            7101-25-00-6/123     → 123
            7101-25-00-6/123(2)  → 123
            6001-25-00-6ап/1736  → 1736
            6001-25-00-6ап/1736(2) → 1736
        
        Returns:
            int или None если формат некорректный
        """
        
        if '/' not in case_number:
            return None
        
        # Берём часть после последнего /
        seq_part = case_number.split('/')[-1]
        
        # Убираем суффикс (N) если есть: "1736(2)" → "1736"
        seq_clean = re.sub(r'\(\d+\)$', '', seq_part)
        
        try:
            return int(seq_clean)
        except ValueError:
            self.logger.warning(f"Не удалось извлечь sequence из: {case_number}")
            return None
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
    
    async def get_cases_for_update(self, filters: Dict) -> List[str]:
        """
        Получить номера дел для обновления
        """
        defendant_keywords = filters.get('defendant_keywords', [])
        exclude_events = filters.get('exclude_event_types', [])
        interval_days = filters.get('update_interval_days', 2)
        
        # Построение SQL запроса
        query = """
            SELECT DISTINCT c.case_number, c.case_date
            FROM cases c
        """
        
        conditions = []
        params = []
        param_counter = 1
        
        # ФИЛЬТР 1: По ответчику (если указан)
        if defendant_keywords:
            query += """
                JOIN case_parties cp ON c.id = cp.case_id AND cp.party_role = 'defendant'
                JOIN parties p ON cp.party_id = p.id
            """
            
            keyword_conditions = []
            for keyword in defendant_keywords:
                keyword_conditions.append(f"p.name ILIKE ${param_counter}")
                params.append(f'%{keyword}%')
                param_counter += 1
            
            conditions.append(f"({' OR '.join(keyword_conditions)})")
        
        # ФИЛЬТР 2: Исключить дела с определёнными событиями
        if exclude_events:
            placeholders = ', '.join([f'${i}' for i in range(param_counter, param_counter + len(exclude_events))])
            
            conditions.append(f"""
                NOT EXISTS (
                    SELECT 1 
                    FROM case_events ce
                    JOIN event_types et ON ce.event_type_id = et.id
                    WHERE ce.case_id = c.id
                    AND et.name IN ({placeholders})
                )
            """)
            
            params.extend(exclude_events)
            param_counter += len(exclude_events)
        
        # ФИЛЬТР 3: Не проверялись последние N дней
        conditions.append(f"""
            (
                c.last_updated_at IS NULL 
                OR c.last_updated_at < NOW() - INTERVAL '{interval_days} days'
            )
        """)
        
        # Собираем WHERE
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Сортировка: старые дела первыми
        query += " ORDER BY c.case_date ASC"
        
        # Выполнение запроса
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        # Извлекаем только номера дел
        case_numbers = [row['case_number'] for row in rows]
        
        self.logger.info(f"Найдено дел для обновления: {len(case_numbers)}")
        return case_numbers

    async def mark_case_as_updated(self, case_number: str):
        """
        Пометить дело как успешно обновлённое
        
        Вызывается ТОЛЬКО если весь цикл обновления прошёл успешно:
        - Дело найдено на сайте
        - События распарсены
        - Данные сохранены в БД
        - Без ошибок
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE cases 
                SET last_updated_at = CURRENT_TIMESTAMP 
                WHERE case_number = $1
            """, case_number)
        
        self.logger.debug(f"Дело помечено как обновлённое: {case_number}")

    async def get_existing_case_numbers(
        self, 
        region_key: str, 
        court_key: str, 
        year: str,
        settings
    ) -> Set[int]:
        """
        Получить множество существующих порядковых номеров дел для региона/суда/года
        
        Args:
            region_key: ключ региона ('astana', 'almaty', ...)
            court_key: ключ суда ('smas', 'appellate')
            year: год ('2025')
            settings: экземпляр Settings для получения конфигурации
        
        Returns:
            {1, 2, 5, 10, 15, 23, 45, 67, 89, 100, ...}
        """
        # Получаем конфигурацию для формирования префикса номера
        region_config = settings.get_region(region_key)
        court_config = settings.get_court(region_key, court_key)
        
        # Формируем префикс номера дела
        kato = region_config['kato_code']
        instance = court_config['instance_code']
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        
        prefix = f"{kato}{instance}-{year_short}-00-{case_type}/"
        
        # SQL запрос
        query = """
            SELECT case_number
            FROM cases
            WHERE case_number LIKE $1
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, f"{prefix}%")
        
        # Извлекаем порядковые номера
        sequence_numbers = set()
        
        for row in rows:
            seq_num = self._extract_sequence_number(row['case_number'])
            if seq_num is not None:
                sequence_numbers.add(seq_num)
        
        self.logger.info(
            f"Загружено существующих номеров для {region_key}/{court_key}/{year}: {len(sequence_numbers)}"
        )
        
        return sequence_numbers
    
    async def get_last_sequence_number(
        self, 
        region_key: str, 
        court_key: str, 
        year: str,
        settings
    ) -> int:
        """
        Получить последний (максимальный) порядковый номер дела для региона/суда/года
        
        Args:
            region_key: ключ региона ('astana')
            court_key: ключ суда ('smas', 'appellate')
            year: год ('2025')
            settings: экземпляр Settings
        
        Returns:
            Максимальный порядковый номер или 0 если дел нет
        """
        region_config = settings.get_region(region_key)
        court_config = settings.get_court(region_key, court_key)
        
        # Формируем префикс номера дела
        kato = region_config['kato_code']
        instance = court_config['instance_code']
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        
        prefix = f"{kato}{instance}-{year_short}-00-{case_type}/"
        
        query = """
            SELECT case_number
            FROM cases
            WHERE case_number LIKE $1
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, f"{prefix}%")
        
        if not rows:
            self.logger.info(f"Дел для {region_key}/{court_key}/{year} не найдено, начинаем с 1")
            return 0
        
        # Извлекаем максимальный порядковый номер
        max_sequence = 0
        
        for row in rows:
            seq_num = self._extract_sequence_number(row['case_number'])
            if seq_num is not None and seq_num > max_sequence:
                max_sequence = seq_num
        
        self.logger.info(
            f"Последний номер для {region_key}/{court_key}/{year}: {max_sequence}"
        )
        
        return max_sequence
    
    def get_smas_instance_codes(self, settings) -> Set[str]:
        """
        Собрать все instance_code для судов СМАС из конфигурации
        
        Returns:
            {'94', '93', ...}
        """
        smas_codes = set()
        
        for region_key, region_config in settings.regions.items():
            courts = region_config.get('courts', {})
            smas_court = courts.get('smas')
            
            if smas_court and smas_court.get('instance_code'):
                smas_codes.add(smas_court['instance_code'])
        
        self.logger.debug(f"СМАС instance_codes: {smas_codes}")
        return smas_codes
    
    async def get_smas_cases_without_judge(
        self, 
        settings,
        interval_days: int = 2
    ) -> List[str]:
        """
        Получить номера дел СМАС без назначенного судьи
        
        Args:
            settings: экземпляр Settings для получения instance_codes
            interval_days: интервал проверки (общий для update mode)
        
        Returns:
            ['7194-25-00-4/123', '7594-25-00-4/456', ...]
        """
        # Получаем все instance_code для СМАС
        smas_codes = self.get_smas_instance_codes(settings)
        
        if not smas_codes:
            self.logger.warning("Не найдены instance_codes для СМАС в конфиге")
            return []
        
        # Формируем условия для SQL
        # Номер дела: "7194-25-00-4/123" → court_code = "7194" → instance = "94"
        # Паттерн: символы 3-4 в court_code (индексы 2-3 в номере до первого дефиса)
        # SQL: SUBSTRING(case_number FROM 3 FOR 2)
        
        # Строим условие для проверки instance_code
        code_conditions = []
        params = []
        param_counter = 1
        
        for code in smas_codes:
            code_conditions.append(f"SUBSTRING(case_number FROM 3 FOR 2) = ${param_counter}")
            params.append(code)
            param_counter += 1
        
        codes_where = f"({' OR '.join(code_conditions)})"
        
        query = f"""
            SELECT case_number
            FROM cases
            WHERE 
                judge_id IS NULL
                AND {codes_where}
                AND (
                    last_updated_at IS NULL
                    OR last_updated_at < NOW() - INTERVAL '{interval_days} days'
                )
            ORDER BY case_date DESC
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        case_numbers = [row['case_number'] for row in rows]
        
        self.logger.info(
            f"Найдено дел СМАС без судьи: {len(case_numbers)}"
        )
        
        return case_numbers
    
    # Docs processing:

    async def get_document_keys(self, case_id: int) -> set:
        """Получить ключи уже скачанных документов"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT doc_date, doc_name FROM case_documents WHERE case_id = $1",
                case_id
            )
        return {f"{r['doc_date'].isoformat()}|{ r['doc_name']}" for r in rows}

    async def save_documents(self, case_id: int, documents: List[Dict]) -> int:
        """Сохранить информацию о документах"""
        if not documents:
            return 0
        saved = 0
        async with self.pool.acquire() as conn:
            for doc in documents:
                try:
                    await conn.execute("""
                        INSERT INTO case_documents (case_id, doc_date, doc_name, file_path, file_size, downloaded_at)
                        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                        ON CONFLICT (case_id, doc_date, doc_name) DO UPDATE
                        SET file_path = EXCLUDED.file_path, 
                            file_size = EXCLUDED.file_size,
                            downloaded_at = CURRENT_TIMESTAMP
                    """, case_id, doc['doc_date'], doc['doc_name'], doc['file_path'], doc.get('file_size'))
                    saved += 1
                except Exception as e:
                    self.logger.error(f"Ошибка сохранения документа: {e}")
        return saved

    async def get_cases_pending_documents(self, limit: int = None) -> List[Dict]:
        """Получить дела для скачивания документов"""
        query = """
            SELECT id, case_number FROM cases
            WHERE documents_pending = TRUE
            ORDER BY case_date DESC
        """
        if limit:
            query += f" LIMIT {limit}"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
        return [{'id': r['id'], 'case_number': r['case_number']} for r in rows]

    async def mark_documents_downloaded(self, case_id: int):
        """Пометить что документы скачаны"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE cases SET documents_pending = FALSE, documents_checked_at = CURRENT_TIMESTAMP
                WHERE id = $1
            """, case_id)

    async def mark_case_for_documents(self, case_id: int):
        """Пометить дело для скачивания документов"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE cases SET documents_pending = TRUE WHERE id = $1", case_id
            )

    async def get_cases_for_documents(
        self, 
        filters: Dict, 
        limit: int = None,
        kato_code: str = None,
        instance_code: str = None
    ) -> List[Dict]:
        """
        Получить дела для скачивания документов с гибкой фильтрацией
        
        Args:
            filters: словарь фильтров
            limit: лимит записей
            kato_code: КАТО-код региона (для max_per_court)
            instance_code: код инстанции суда (для max_per_court)
        """
        mode = filters.get('mode', 'any')
        interval_days = filters.get('check_interval_days', 7)
        
        filter_conditions = []
        params = []
        param_counter = 1
        
        # === ФИЛЬТР: missing_parties ===
        if filters.get('missing_parties'):
            filter_conditions.append("""
                NOT EXISTS (
                    SELECT 1 FROM case_parties cp WHERE cp.case_id = c.id
                )
            """)
        
        # === ФИЛЬТР: missing_judge ===
        if filters.get('missing_judge'):
            filter_conditions.append("c.judge_id IS NULL")
        
        # === ФИЛЬТР: party_keywords ===
        party_keywords = filters.get('party_keywords', [])
        party_role = filters.get('party_role')
        
        if party_keywords:
            role_condition = ""
            
            if party_role:
                if isinstance(party_role, str):
                    party_role = [party_role]
                
                role_placeholders = [f"${param_counter + i}" for i in range(len(party_role))]
                role_condition = f"AND cp.party_role IN ({', '.join(role_placeholders)})"
                params.extend(party_role)
                param_counter += len(party_role)
            
            keyword_placeholders = []
            for keyword in party_keywords:
                keyword_placeholders.append(f"p.name ILIKE ${param_counter}")
                params.append(f'%{keyword}%')
                param_counter += 1
            
            keywords_sql = ' OR '.join(keyword_placeholders)
            
            filter_conditions.append(f"""
                EXISTS (
                    SELECT 1 FROM case_parties cp
                    JOIN parties p ON cp.party_id = p.id
                    WHERE cp.case_id = c.id
                    {role_condition}
                    AND ({keywords_sql})
                )
            """)
        
        if not filter_conditions:
            if filters.get('mode') == 'none':
                # Режим "none" — все дела без дополнительных фильтров
                filter_conditions.append("1=1")
            else:
                self.logger.warning("Не указано ни одного фильтра для документов")
                return []
        
        if mode == 'all':
            combined_filters = ' AND '.join(f'({cond})' for cond in filter_conditions)
        else:
            combined_filters = ' OR '.join(f'({cond})' for cond in filter_conditions)
        
        # === ОБЩИЕ УСЛОВИЯ ===
        common_conditions = []
        
        common_conditions.append(f"""
            (
                c.documents_checked_at IS NULL 
                OR c.documents_checked_at < NOW() - INTERVAL '{interval_days} days'
            )
        """)
        
        # Фильтр по конкретному суду (для max_per_court)
        if kato_code and instance_code:
            common_conditions.append(f"SUBSTRING(c.case_number FROM 1 FOR 2) = ${param_counter}")
            params.append(kato_code)
            param_counter += 1
            
            common_conditions.append(f"SUBSTRING(c.case_number FROM 3 FOR 2) = ${param_counter}")
            params.append(instance_code)
            param_counter += 1
        else:
            # Фильтр по списку регионов
            regions = filters.get('regions')
            if regions:
                region_codes = self._get_region_kato_codes(regions)
                if region_codes:
                    region_conditions = []
                    for code in region_codes:
                        region_conditions.append(f"SUBSTRING(c.case_number FROM 1 FOR 2) = ${param_counter}")
                        params.append(code)
                        param_counter += 1
                    common_conditions.append(f"({' OR '.join(region_conditions)})")
            
            # Фильтр по типам судов
            court_types = filters.get('court_types')
            if court_types:
                court_codes = self._get_court_instance_codes(court_types)
                if court_codes:
                    code_conditions = []
                    for code in court_codes:
                        code_conditions.append(f"SUBSTRING(c.case_number FROM 3 FOR 2) = ${param_counter}")
                        params.append(code)
                        param_counter += 1
                    common_conditions.append(f"({' OR '.join(code_conditions)})")
        
        # Фильтр по году
        year = filters.get('year')
        if year:
            year_short = year[-2:]
            common_conditions.append(f"SUBSTRING(c.case_number FROM 6 FOR 2) = ${param_counter}")
            params.append(year_short)
            param_counter += 1
        
        # === СБОРКА ЗАПРОСА ===
        query = """
            SELECT DISTINCT c.id, c.case_number, c.case_date, c.documents_checked_at
            FROM cases c
            WHERE 
        """
        
        all_conditions = [f"({combined_filters})"] + common_conditions
        query += ' AND '.join(all_conditions)
        
        order = filters.get('order', 'newest')
        if order == 'oldest':
            query += " ORDER BY c.case_date ASC, c.documents_checked_at NULLS FIRST"
        else:
            query += " ORDER BY c.case_date DESC, c.documents_checked_at NULLS FIRST"
        
        if limit:
            query += f" LIMIT {limit}"
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        return [{'id': r['id'], 'case_number': r['case_number']} for r in rows]
    
    def _get_court_instance_codes(self, court_types: List[str]) -> List[str]:
        """
        Получить instance_codes для указанных типов судов
        """
        court_code_map = {
            'smas': ['94', '93'],
            'appellate': ['99', '00'],
            'cassation': ['03'],
            'supreme': ['01'],
        }
        
        codes = set()
        for court_type in court_types:
            if court_type in court_code_map:
                codes.update(court_code_map[court_type])
        
        return list(codes)

    def _get_region_kato_codes(self, regions: List[str]) -> List[str]:
        """
        Получить КАТО-коды для указанных регионов
        """
        region_kato_map = {
            'republic': '60',
            'astana': '71',
            'almaty': '75',
            'shymkent': '52',
            'akmola': '11',
            'aktobe': '15',
            'almaty_region': '19',
            'atyrau': '23',
            'vko': '63',
            'zhambyl': '31',
            'zko': '27',
            'karaganda': '35',
            'kostanay': '39',
            'kyzylorda': '43',
            'mangystau': '47',
            'pavlodar': '55',
            'sko': '59',
            'turkestan': '51',
            'ulytau': '62',
            'abay': '10',
            'zhetysu': '33',
        }
        
        codes = []
        for region in regions:
            if region in region_kato_map:
                codes.append(region_kato_map[region])
        
        return codes
    
    async def get_case_id(self, case_number: str) -> Optional[int]:
        """Получить ID дела по номеру"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM cases WHERE case_number = $1",
                case_number
            )
            return row['id'] if row else None
    
    async def get_gaps_check_date(
        self, 
        region_key: str, 
        court_key: str, 
        year: str
    ) -> Optional[datetime]:
        """
        Получить дату последней проверки пропусков
        
        Returns:
            datetime или None если никогда не проверялось
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT gaps_checked_at 
                FROM parsing_metadata
                WHERE region_key = $1 AND court_key = $2 AND year = $3
            """, region_key, court_key, year)
            
            return row['gaps_checked_at'] if row else None

    async def update_gaps_check_date(
        self, 
        region_key: str, 
        court_key: str, 
        year: str,
        last_sequence: int = 0
    ):
        """
        Обновить дату проверки пропусков
        
        Вызывается после завершения проверки gaps для региона/суда/года
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO parsing_metadata (region_key, court_key, year, gaps_checked_at, last_sequence_checked)
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP, $4)
                ON CONFLICT (region_key, court_key, year) 
                DO UPDATE SET 
                    gaps_checked_at = CURRENT_TIMESTAMP,
                    last_sequence_checked = $4,
                    updated_at = CURRENT_TIMESTAMP
            """, region_key, court_key, year, last_sequence)
            
            self.logger.debug(
                f"Обновлена дата проверки пропусков: {region_key}/{court_key}/{year}"
            )

    async def should_check_gaps(
        self, 
        region_key: str, 
        court_key: str, 
        year: str,
        interval_days: int = 30
    ) -> bool:
        """
        Проверить, нужно ли проверять пропуски
        
        Returns:
            True если прошло больше interval_days с последней проверки
        """
        last_check = await self.get_gaps_check_date(region_key, court_key, year)
        
        if last_check is None:
            self.logger.debug(f"Пропуски {region_key}/{court_key}/{year}: никогда не проверялись")
            return True
        
        days_since_check = (datetime.now() - last_check).days
        should_check = days_since_check >= interval_days
        
        self.logger.debug(
            f"Пропуски {region_key}/{court_key}/{year}: "
            f"проверялись {days_since_check} дней назад, "
            f"{'нужна проверка' if should_check else 'пропускаем'}"
        )
        
        return should_check
        
    async def update_case(self, case_data: CaseData) -> Dict[str, Any]:
        """
        Обновление дела (события, судья)
        
        Returns:
            {'case_id': int, 'events_added': int}
        """
        try:
            async with self.pool.acquire() as conn:
                # 1. Получаем case_id
                case_id = await conn.fetchval(
                    "SELECT id FROM cases WHERE case_number = $1",
                    case_data.case_number
                )
                
                if not case_id:
                    # Дело не найдено — создаём новое
                    result = await self.save_case(case_data)
                    return {
                        'case_id': result.get('case_id'),
                        'events_added': len(case_data.events)
                    }
                
                # 2. Обновляем судью (если появился)
                if case_data.judge:
                    judge_id = await self._get_or_create_judge(conn, case_data.judge)
                    await conn.execute(
                        "UPDATE cases SET judge_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
                        judge_id, case_id
                    )
                
                # 3. Получаем существующие события
                existing_events = await conn.fetch("""
                    SELECT et.name, ce.event_date 
                    FROM case_events ce
                    JOIN event_types et ON ce.event_type_id = et.id
                    WHERE ce.case_id = $1
                """, case_id)
                
                existing_keys = {
                    f"{row['name']}|{row['event_date'].isoformat()}" 
                    for row in existing_events
                }
                
                # 4. Добавляем новые события
                events_added = 0
                for event in case_data.events:
                    event_key = f"{event.event_type}|{event.event_date.isoformat()}"
                    if event_key not in existing_keys:
                        event_type_id = await self._get_or_create_event_type(conn, event.event_type)
                        await conn.execute("""
                            INSERT INTO case_events (case_id, event_type_id, event_date)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """, case_id, event_type_id, event.event_date)
                        events_added += 1
                
                # 5. Обновляем метку времени
                await conn.execute(
                    "UPDATE cases SET last_updated_at = CURRENT_TIMESTAMP WHERE id = $1",
                    case_id
                )
                
                return {'case_id': case_id, 'events_added': events_added}
        
        except Exception as e:
            self.logger.error(f"Ошибка обновления дела {case_data.case_number}: {e}")
            return {'case_id': None, 'events_added': 0}