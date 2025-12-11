"""
Скачивание документов дел
"""
from typing import Dict, Any, Optional, List, Set
import asyncio

from core.updaters.base_updater import BaseUpdater
from core.region_worker import RegionWorker
from search.document_handler import DocumentHandler


class DocsUpdater(BaseUpdater):
    """
    Updater для скачивания документов
    
    Команда: --mode update docs
    
    Логика:
    1. Находит дела по фильтру (ключевые слова в ответчике)
    2. Исключает дела с финальными событиями
    3. Для каждого дела:
       - Ищет на сайте
       - Открывает карточку дела
       - Скачивает новые документы
    """
    MODE = 'docs'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Инициализация обработчика документов
        docs_config = self.get_config()
        self.doc_handler = DocumentHandler(
            base_url=self.settings.base_url,
            storage_dir=docs_config.get('storage_dir', './documents'),
            regions_config=self.settings.regions
        )
        
        self.download_delay = docs_config.get('download_delay', 2.0)
        self.max_per_session = docs_config.get('max_per_session')
        self.downloaded_count = 0
    
    def get_config(self) -> Dict[str, Any]:
        """Получить конфигурацию для docs updater"""
        return self.settings.config.get('update_settings', {}).get('docs', {})
    
    async def get_cases_to_process(self) -> List[str]:
        """Получить дела для скачивания документов"""
        config = self.get_config()
        
        if not config.get('enabled', True):
            self.logger.info("DocsUpdater отключен в конфиге")
            return []
        
        filters_config = config.get('filters', {})
        
        filters = {
            'mode': filters_config.get('mode', 'any'),
            'missing_parties': filters_config.get('missing_parties', False),
            'missing_judge': filters_config.get('missing_judge', False),
            'party_keywords': filters_config.get('party_keywords', []),
            'party_role': filters_config.get('party_role'),
            'court_types': filters_config.get('court_types'),
            'regions': filters_config.get('regions'),
            'year': filters_config.get('year'),
            'check_interval_days': config.get('check_interval_days', 7),
            'order': filters_config.get('order', 'newest'),
        }
        
        max_per_session = config.get('max_per_session')
        max_per_court = config.get('max_per_court')
        
        # Если есть лимит на суд — собираем по судам
        if max_per_court:
            all_cases = []
            
            # Получаем список регионов
            target_regions = filters_config.get('regions')
            if not target_regions:
                target_regions = list(self.settings.regions.keys())
            
            # Получаем список типов судов
            target_courts = filters_config.get('court_types')
            if not target_courts:
                target_courts = ['smas', 'appellate', 'cassation', 'supreme']
            
            for region_key in target_regions:
                region_config = self.settings.regions.get(region_key)
                if not region_config:
                    continue
                
                kato_code = region_config.get('kato_code')
                if not kato_code:
                    continue
                
                courts = region_config.get('courts', {})
                
                for court_key in target_courts:
                    court_config = courts.get(court_key)
                    if not court_config:
                        continue
                    
                    instance_code = court_config.get('instance_code')
                    if not instance_code:
                        continue
                    
                    # Запрос для конкретного суда
                    court_cases = await self.db_manager.get_cases_for_documents(
                        filters=filters,
                        limit=max_per_court,
                        kato_code=kato_code,
                        instance_code=instance_code
                    )
                    
                    all_cases.extend(court_cases)
                    
                    if court_cases:
                        self.logger.info(
                            f"{region_key}/{court_key}: {len(court_cases)} дел"
                        )
            
            cases = all_cases
        else:
            # Старая логика — общий лимит
            cases = await self.db_manager.get_cases_for_documents(filters, max_per_session)
        
        # Применяем max_per_session если указан
        if max_per_session and len(cases) > max_per_session:
            cases = cases[:max_per_session]
        
        # Логирование
        active_filters = []
        if filters['missing_parties']:
            active_filters.append('missing_parties')
        if filters['missing_judge']:
            active_filters.append('missing_judge')
        if filters['party_keywords']:
            active_filters.append(f"keywords={filters['party_keywords']}")
        
        self.logger.info(
            f"Всего дел: {len(cases)}, фильтры: {', '.join(active_filters)}"
        )
        
        return [c['case_number'] for c in cases]
    
    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        """Обработать одно дело — скачать новые документы"""
        result = {
            'case_number': case_number,
            'success': False,
            'documents_downloaded': 0
        }
        
        # Проверка лимита на сессию
        if self.max_per_session and self.downloaded_count >= self.max_per_session:
            self.logger.info(f"Достигнут лимит документов: {self.max_per_session}")
            result['skipped'] = True
            return result
        
        try:
            # Поиск дела на сайте
            results_html, cases = await worker.search_case_by_number(case_number)
            
            if not results_html or not cases:
                self.logger.debug(f"Дело не найдено на сайте: {case_number}")
                return result
            
            # Ищем целевое дело
            target = next(
                (c for c in cases 
                 if self.text_processor.is_matching_case_number(c.case_number, case_number)),
                None
            )
            
            if not target or target.result_index is None:
                self.logger.debug(f"Индекс дела не определён: {case_number}")
                return result
            
            # Получаем ID дела в БД
            case_id = await self.db_manager.get_case_id(case_number)
            if not case_id:
                self.logger.warning(f"Дело не найдено в БД: {case_number}")
                return result
            
            # Получаем уже скачанные документы
            existing_keys = await self.db_manager.get_document_keys(case_id)
            
            # Скачиваем новые документы
            downloaded = await self.doc_handler.fetch_all_documents(
                session=worker.session,
                results_html=results_html,
                case_number=case_number,
                case_index=target.result_index,
                existing_keys=existing_keys,
                delay=self.download_delay
            )
            
            if downloaded:
                # Сохраняем информацию о документах в БД
                await self.db_manager.save_documents(case_id, downloaded)
                self.downloaded_count += len(downloaded)
                
                result['documents_downloaded'] = len(downloaded)
                self.logger.info(
                    f"✅ Скачано документов: {len(downloaded)} для {case_number}"
                )
            
            # Помечаем дело как проверенное
            await self.db_manager.mark_documents_downloaded(case_id)
            result['success'] = True
        
        except Exception as e:
            self.logger.error(f"Ошибка обработки {case_number}: {e}")
            result['error'] = str(e)
        
        return result