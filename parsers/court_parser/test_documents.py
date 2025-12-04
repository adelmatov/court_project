"""
Тестирование загрузки документов для одного дела
"""
import asyncio
import sys
sys.path.insert(0, 'parsers/court_parser')

from core.parser import CourtParser
from search.document_handler import DocumentHandler
from config.settings import Settings
from utils.logger import setup_logger


async def test_single_case_documents():
    """Тест загрузки документов для одного конкретного дела"""
    logger = setup_logger('test_docs', level='DEBUG')
    
    # Укажите номер дела для теста
    TEST_CASE_NUMBER = "7594-25-00-4/5021"
    
    logger.info("=" * 70)
    logger.info("ТЕСТ ЗАГРУЗКИ ДОКУМЕНТОВ")
    logger.info(f"Дело: {TEST_CASE_NUMBER}")
    logger.info("=" * 70)
    
    settings = Settings()
    
    async with CourtParser() as parser:
        doc_handler = DocumentHandler(
            base_url=settings.base_url,
            storage_dir="./test_documents",
            regions_config=settings.regions
        )
        
        # 1. Ищем дело
        logger.info("\n📋 Шаг 1: Поиск дела...")
        results_html, cases = await parser.search_case_by_number(TEST_CASE_NUMBER)
        
        if not results_html:
            logger.error("❌ Не удалось выполнить поиск")
            return
        
        if not cases:
            logger.error("❌ Дело не найдено в результатах")
            return
        
        logger.info(f"✅ Найдено дел: {len(cases)}")
        for case in cases:
            logger.info(f"   - {case.case_number} (index={case.result_index})")
        
        # 2. Находим нужное дело
        target_case = None
        for case in cases:
            if case.case_number == TEST_CASE_NUMBER:
                target_case = case
                break
        
        if target_case is None:
            logger.error(f"❌ Целевое дело {TEST_CASE_NUMBER} не найдено в списке")
            return
        
        if target_case.result_index is None:
            logger.error("❌ result_index не извлечён")
            return
        
        logger.info(f"\n✅ Целевое дело найдено: index={target_case.result_index}")
        
        # 3. Получаем сесс сию
        session = await parser.session_manager.get_session()
        
        # 4. Открываем карточку дела с правильным индексом
        logger.info(f"\n📋 Шаг 2: Открытие карточки дела (index={target_case.result_index})...")
        opened = await doc_handler.open_case_card(
            session, results_html, case_index=target_case.result_index
        )
        
        if not opened:
            logger.error("❌ Не удалось открыть карточку дела")
            return
        
        logger.info("✅ Карточка дела открыта")
        await asyncio.sleep(1)
        
        # 5. Получаем список документов
        logger.info("\n📋 Шаг 3: Получение списка документов...")
        documents, form_data = await doc_handler.get_document_list(session)
        
        if not documents:
            logger.warning("⚠️ Документы не найдены")
            return
        
        logger.info(f"✅ Найдено документов: {len(documents)}")
        for doc in documents:
            logger.info(f"   [index={doc.index}] {doc.doc_date} - {doc.doc_name}")
        
        if not form_data:
            logger.error("❌ Форма для скачивания не найдена")
            return
        
        # 6. Скачиваем первый документ
        if documents:
            logger.info("\n📋 Шаг 4: Скачивание первого документа...")
            
            first_doc = documents[0]
            logger.info(f"Документ: {first_doc.doc_name} (index={first_doc.index})")
            
            opened = await doc_handler.open_document(session, form_data, first_doc.index)
            if not opened:
                logger.error("❌ Не удалось открыть документ")
                return
            
            await asyncio.sleep(1)
            
            doc_html = await doc_handler.get_document_page(session)
            if not doc_html:
                logger.error("❌ Не удалось получить страницу документа")
                return
            
            pdf_url = doc_handler.parser.extract_pdf_url(doc_html)
            if not pdf_url:
                logger.error("❌ URL PDF не найден")
                with open("doc_page_debug.html", "w", encoding="utf-8") as f:
                    f.write(doc_html)
                return
            
            logger.info(f"PDF URL: {pdf_url}")
            
            file_path = await doc_handler.download_pdf(
                session, pdf_url, TEST_CASE_NUMBER, first_doc
            )
            
            if file_path:
                logger.info(f"✅ Документ сохранён: {file_path}")
            else:
                logger.error("❌ Ошибка сохранения")
    
    logger.info("\n" + "=" * 70)
    logger.info("ТЕСТ ЗАВЕРШЁН")
    logger.info("=" * 70)


async def test_full_download():
    """Тест полной загрузки всех документов для дела"""
    logger = setup_logger('test_docs', level='INFO')
    
    TEST_CASE_NUMBER = "7594-25-00-4/5021"
    
    logger.info("=" * 70)
    logger.info("ТЕСТ ПОЛНОЙ ЗАГРУЗКИ ДОКУМЕНТОВ")
    logger.info(f"Дело: {TEST_CASE_NUMBER}")
    logger.info("=" * 70)
    
    settings = Settings()
    
    async with CourtParser() as parser:
        doc_handler = DocumentHandler(
            base_url=settings.base_url,
            storage_dir="./test_documents",
            regions_config=settings.regions
        )
        
        # 1. Поиск дела
        logger.info("Поиск дела...")
        results_html, cases = await parser.search_case_by_number(TEST_CASE_NUMBER)
        
        if not results_html or not cases:
            logger.error("Дело не найдено")
            return
        
        # 2. Находим нужное дело
        target_case = next(
            (c for c in cases if c.case_number == TEST_CASE_NUMBER), 
            None
        )
        
        if not target_case or target_case.result_index is None:
            logger.error(f"Целевое дело не найдено. Результаты: {[c.case_number for c in cases]}")
            return
        
        logger.info(f"Найдено: {target_case.case_number} (index={target_case.result_index})")
        
        # 3. Скачиваем все документы
        session = await parser.session_manager.get_session()
        
        downloaded = await doc_handler.fetch_all_documents(
            session=session,
            results_html=results_html,
            case_number=TEST_CASE_NUMBER,
            case_index=target_case.result_index,  # ← Правильный индекс
            existing_keys=set(),
            delay=2.0
        )
        
        logger.info(f"\n✅ Скачано документов: {len(downloaded)}")
        for doc in downloaded:
            logger.info(f"   - {doc['file_path']}")


if __name__ == "__main__":
    print("\nВыберите тест:")
    print("1 - Пошаговый тест (с подробным выводом)")
    print("2 - Полная загрузка всех документов")
    
    choice = input("\nВведите номер (1 или 2): ").strip()
    
    if choice == "1":
        asyncio.run(test_single_case_documents())
    elif choice == "2":
        asyncio.run(test_full_download())
    else:
        print("Неверный выбор")