"""
Скачивание документов дел
"""
from typing import Dict, List, Any

from core.updaters.base_updater import BaseUpdater
from core.region_worker import RegionWorker
from search.document_handler import DocumentHandler
from utils.terminal_ui import Mode


class DocsUpdater(BaseUpdater):
    """Updater для скачивания документов"""
    
    MODE = Mode.DOCS

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        docs_config = self.get_config()
        self.doc_handler = DocumentHandler(
            base_url=self.settings.base_url,
            storage_dir=docs_config.get('storage_dir', './documents'),
            regions_config=self.settings.regions
        )
        self.download_delay = docs_config.get('download_delay', 2.0)

    def get_config(self) -> Dict[str, Any]:
        return self.settings.config.get('update_settings', {}).get('docs', {})
    
    async def get_cases_to_process(self) -> List[str]:
        config = self.get_config()
        
        if not config.get('enabled', True):
            self.logger.info("DocsUpdater отключен в конфиге")
            return []
        
        filters = config.get('filters', {})
        
        cases = await self.db_manager.get_cases_for_documents(
            filters={
                'party_keywords': filters.get('party_keywords', []),
                'party_role': filters.get('party_role'),
                'court_types': filters.get('court_types'),
                'regions': filters.get('regions'),
                'year': filters.get('year'),
                'check_interval_days': config.get('check_interval_days', 7),
                'order': filters.get('order', 'oldest'),
            },
            limit=config.get('max_per_session'),
            final_event_types=config.get('final_event_types', []),
            final_check_period_days=config.get('final_check_period_days', 30),
            max_active_age_days=config.get('max_active_age_days')
        )
        
        self.logger.info(f"Дел для скачивания документов: {len(cases)}")
        return [c['case_number'] for c in cases]
    
    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        result = {
            'case_number': case_number,
            'success': False,
            'documents_downloaded': 0
        }
        
        try:
            results_html, cases = await worker.search_case_by_number(case_number)
            
            if not results_html or not cases:
                return result
            
            target = next(
                (c for c in cases 
                 if self.text_processor.is_matching_case_number(c.case_number, case_number)),
                None
            )
            
            if not target or target.result_index is None:
                return result
            
            case_id = await self.db_manager.get_case_id(case_number)
            if not case_id:
                return result
            
            existing_keys = await self.db_manager.get_document_keys(case_id)
            
            downloaded = await self.doc_handler.fetch_all_documents(
                session=worker.session,
                results_html=results_html,
                case_number=case_number,
                case_index=target.result_index,
                existing_keys=existing_keys,
                delay=self.download_delay
            )
            
            if downloaded:
                await self.db_manager.save_documents(case_id, downloaded)
                result['documents_downloaded'] = len(downloaded)
                self.logger.info(f"Скачано: {len(downloaded)} документов для {case_number}")
            
            await self.db_manager.mark_documents_downloaded(case_id)
            result['success'] = True
        
        except Exception as e:
            self.logger.error(f"Ошибка: {case_number}: {e}")
            result['error'] = str(e)
        
        return result