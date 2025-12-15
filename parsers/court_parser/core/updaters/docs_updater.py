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
    2. Для активных дел (без финальных событий) — проверяет по check_interval_days
    3. Для завершённых дел — проверяет в течение final_check_period_days после финала
    4. Для каждого дела:
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
        
        # Базовые фильтры
        filters = {
            'party_keywords': filters_config.get('party_keywords', []),
            'party_role': filters_config.get('party_role'),
            'court_types': filters_config.get('court_types'),
            'regions': filters_config.get('regions'),
            'year': filters_config.get('year'),
            'check_interval_days': config.get('check_interval_days', 7),
            'order': filters_config.get('order', 'oldest'),
        }
        
        # Параметры финальных событий и возраста
        final_event_types = config.get('final_event_types', [])
        final_check_period_days = config.get('final_check_period_days', 30)
        max_active_age_days = config.get('max_active_age_days')
        
        # Получаем дела
        max_per_session = config.get('max_per_session')
        
        cases = await self.db_manager.get_cases_for_documents(
            filters=filters,
            limit=max_per_session,
            final_event_types=final_event_types,
            final_check_period_days=final_check_period_days,
            max_active_age_days=max_active_age_days
        )
        
        # Логирование
        active_filters = []
        if filters['party_keywords']:
            active_filters.append(f"keywords={filters['party_keywords']}")
        if filters['party_role']:
            active_filters.append(f"role={filters['party_role']}")
        if final_event_types:
            active_filters.append(f"final_window={final_check_period_days}д")
        if max_active_age_days:
            active_filters.append(f"макс. возраст={max_active_age_days}д")
        
        self.logger.info(
            f"Дел для обработки: {len(cases)}, фильтры: {', '.join(active_filters) or 'none'}"
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