"""
Тестовый скрипт для проверки авторизации и смены языка
"""
import asyncio
import ssl
import aiohttp
from pathlib import Path

# Добавляем путь к модулям
import sys
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import Settings
from auth.authenticator import Authenticator
from utils.logger import setup_logger


async def test_authentication():
    """Тест авторизации с выводом результатов"""
    
    # Настройка логгера с выводом в консоль
    logger = setup_logger('test_auth', log_dir='logs', level='DEBUG', console_output=True)
    
    logger.info("=" * 60)
    logger.info("ТЕСТ АВТОРИЗАЦИИ И СМЕНЫ ЯЗЫКА")
    logger.info("=" * 60)
    
    # Загрузка конфигурации
    settings = Settings()
    
    # Создание сессии
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=5)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        
        # Создаём простой session_manager для Authenticator
        class SimpleSessionManager:
            def __init__(self, session):
                self._session = session
            
            async def get_session(self):
                return self._session
            
            async def create_session(self):
                return self._session
        
        session_manager = SimpleSessionManager(session)
        
        # Создаём аутентификатор
        authenticator = Authenticator(
            base_url=settings.base_url,
            auth_config=settings.auth,
            retry_config=settings.retry_settings
        )
        
        try:
            # === ЭТАП 1: Загрузка страницы ===
            logger.info("\n" + "=" * 60)
            logger.info("ЭТАП 1: Загрузка главной страницы")
            logger.info("=" * 60)
            
            html, viewstate = await authenticator._load_page(session)
            
            logger.info(f"✅ Страница загружена")
            logger.info(f"   ViewState: {viewstate[:50]}...")
            logger.info(f"   HTML размер: {len(html)} символов")
            
            # Сохраняем HTML до смены языка
            with open("test_1_before_language.html", "w", encoding="utf-8") as f:
                f.write(html)
            logger.info("   Сохранено: test_1_before_language.html")
            
            # Проверяем текущий язык
            is_russian = authenticator._is_russian_interface(html)
            logger.info(f"   Текущий язык: {'РУССКИЙ' if is_russian else 'КАЗАХСКИЙ'}")
            
            await asyncio.sleep(1)
            
            # === ЭТАП 2: Смена языка ===
            logger.info("\n" + "=" * 60)
            logger.info("ЭТАП 2: Установка русского языка")
            logger.info("=" * 60)
            
            html, viewstate = await authenticator._ensure_russian_language(session, html, viewstate)
            
            logger.info(f"✅ Язык проверен/установлен")
            logger.info(f"   Новый ViewState: {viewstate[:50]}...")
            
            # Сохраняем HTML после смены языка
            with open("test_2_after_language.html", "w", encoding="utf-8") as f:
                f.write(html)
            logger.info("   Сохранено: test_2_after_language.html")
            
            # Проверяем язык ещё раз
            is_russian = authenticator._is_russian_interface(html)
            logger.info(f"   Язык после смены: {'РУССКИЙ ✅' if is_russian else 'КАЗАХСКИЙ ❌'}")
            
            if not is_russian:
                logger.error("❌ Язык не сменился! Проверьте test_2_after_language.html")
                return False
            
            await asyncio.sleep(1)
            
            # === ЭТАП 3: Извлечение формы авторизации ===
            logger.info("\n" + "=" * 60)
            logger.info("ЭТАП 3: Извлечение формы авторизации")
            logger.info("=" * 60)
            
            form_ids = authenticator._extract_auth_form_ids(html)
            
            logger.info(f"   form_base: {form_ids.get('form_base', 'НЕ НАЙДЕН')}")
            logger.info(f"   submit_button: {form_ids.get('submit_button', 'НЕ НАЙДЕН')}")
            logger.info(f"   xin_field: {form_ids.get('xin_field', 'НЕ НАЙДЕН')}")
            
            if not form_ids.get('form_base') or not form_ids.get('submit_button'):
                logger.error("❌ Форма авторизации не найдена!")
                return False
            
            logger.info("✅ Форма авторизации найдена")
            
            await asyncio.sleep(1)
            
            # === ЭТАП 4: Авторизация ===
            logger.info("\n" + "=" * 60)
            logger.info("ЭТАП 4: Отправка логина/пароля")
            logger.info("=" * 60)
            
            await authenticator._perform_login(session, viewstate, form_ids)
            
            logger.info("✅ Логин отправлен")
            
            await asyncio.sleep(1)
            
            # === ЭТАП 5: Проверка авторизации ===
            logger.info("\n" + "=" * 60)
            logger.info("ЭТАП 5: Проверка авторизации")
            logger.info("=" * 60)
            
            is_authenticated = await authenticator._verify_authentication(session)
            
            if is_authenticated:
                logger.info("✅ АВТОРИЗАЦИЯ УСПЕШНА!")
            else:
                logger.error("❌ Авторизация не удалась")
                return False
            
            await asyncio.sleep(1)
            
            # === ЭТАП 6: Загрузка страницы после авторизации ===
            logger.info("\n" + "=" * 60)
            logger.info("ЭТАП 6: Загрузка страницы после авторизации")
            logger.info("=" * 60)
            
            # Загружаем главную страницу после авторизации
            url = f"{settings.base_url}/form/proceedings/services.xhtml"
            async with session.get(url) as response:
                final_html = await response.text()
            
            # Сохраняем финальный HTML
            with open("test_3_after_auth.html", "w", encoding="utf-8") as f:
                f.write(final_html)
            logger.info("   Сохранено: test_3_after_auth.html")
            logger.info(f"   HTML размер: {len(final_html)} символов")
            
            # Проверяем признаки русского интерфейса
            russian_words = ['Выйти', 'Профиль', 'Услуги', 'Поиск', 'Судебное']
            found_words = [w for w in russian_words if w in final_html]
            
            logger.info(f"   Русские слова найдены: {found_words}")
            
            # Проверяем признаки казахского
            kazakh_words = ['Шығу', 'Профиль', 'Қызметтер', 'Іздеу', 'Сот']
            found_kazakh = [w for w in kazakh_words if w in final_html]
            
            if found_kazakh:
                logger.warning(f"   ⚠️ Казахские слова найдены: {found_kazakh}")
            
            # === ИТОГ ===
            logger.info("\n" + "=" * 60)
            logger.info("ИТОГ ТЕСТИРОВАНИЯ")
            logger.info("=" * 60)
            logger.info("✅ Все этапы пройдены успешно!")
            logger.info("")
            logger.info("Созданные файлы:")
            logger.info("  1. test_1_before_language.html - до смены языка")
            logger.info("  2. test_2_after_language.html  - после смены языка")
            logger.info("  3. test_3_after_auth.html      - после авторизации")
            logger.info("")
            logger.info("Откройте эти файлы в браузере для проверки интерфейса")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False


if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("Запуск теста авторизации...")
    print("=" * 60 + "\n")
    
    result = asyncio.run(test_authentication())
    
    print("\n" + "=" * 60)
    if result:
        print("✅ ТЕСТ ПРОЙДЕН УСПЕШНО")
    else:
        print("❌ ТЕСТ НЕ ПРОЙДЕН")
    print("=" * 60 + "\n")