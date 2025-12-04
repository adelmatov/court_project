# parsers/court_parser/auth/authenticator.py
"""
Авторизация на сайте office.sud.kz
"""

from typing import Dict, Optional
import asyncio
import aiohttp
from selectolax.parser import HTMLParser

from utils.logger import get_logger
from utils.retry import RetryStrategy, RetryConfig, NonRetriableError, RetryableError
from utils.http_utils import HttpHeaders, ViewStateExtractor


class AuthenticationError(Exception):
    """Ошибка авторизации"""
    pass


class Authenticator:
    """Класс авторизации с retry"""
    
    def __init__(self, base_url: str, auth_config: Dict[str, str], 
                 retry_config: Optional[Dict] = None):
        self.base_url = base_url
        self.login = auth_config['login']
        self.password = auth_config['password']
        self.user_name = auth_config['user_name']
        self.logger = get_logger('authenticator')
        self.retry_config = retry_config or {}
    
    async def authenticate(self, session_manager) -> bool:
        """Полный процесс авторизации с retry"""
        auth_retry_config = self.retry_config.get('authentication', {})
        
        if not auth_retry_config:
            return await self._do_authenticate(session_manager)
        
        retry_cfg = RetryConfig(auth_retry_config)
        strategy = RetryStrategy(retry_cfg)
        create_new_session = auth_retry_config.get('create_new_session', True)
        
        async def _auth_with_session_reset():
            try:
                return await self._do_authenticate(session_manager)
            except RetryableError:
                if create_new_session:
                    self.logger.debug("Создаю новую сессию перед retry...")
                    await session_manager.create_session()
                raise
        
        try:
            return await strategy.execute_with_retry(
                _auth_with_session_reset,
                error_context="Авторизация"
            )
        except Exception as e:
            self.logger.error(f"❌ Авторизация не удалась: {e}")
            raise AuthenticationError(f"Не удалось авторизоваться: {e}") from e
    
    async def _do_authenticate(self, session_manager) -> bool:
        """Один цикл авторизации"""
        session = await session_manager.get_session()
        
        self.logger.info("Начинаю авторизацию...")
        
        # Этап 1: Загрузка страницы и извлечение данных формы
        viewstate, form_ids = await self._load_login_page(session)
        await asyncio.sleep(0.5)
        
        # Этап 2: Отправка логина
        await self._perform_login(session, viewstate, form_ids)
        await asyncio.sleep(0.5)
        
        # Этап 3: Проверка
        if await self._verify_authentication(session):
            self.logger.info("✅ Авторизация успешна")
            return True
        
        raise RetryableError("Проверка авторизации не пройдена")
    
    async def _load_login_page(self, session: aiohttp.ClientSession) -> tuple:
        """Загрузка страницы логина и динамическое извлечение данных формы"""
        url = f"{self.base_url}/index.xhtml?lang=ru"
        headers = self._get_base_headers()
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status in [500, 502, 503, 504]:
                    raise RetryableError(f"HTTP {response.status}")
                if response.status != 200:
                    raise RetryableError(f"HTTP {response.status}")
                
                html = await response.text()
                
                # Динамическое извлечение ViewState
                viewstate = ViewStateExtractor.extract(html)
                if not viewstate:
                    raise RetryableError("ViewState не найден")
                
                # Динамическое извлечение ID формы
                form_ids = self._extract_auth_form_ids(html)
                
                if not form_ids.get('form_base') or not form_ids.get('submit_button'):
                    # Сохраняем для отладки
                    with open("auth_form_not_found.html", "w", encoding="utf-8") as f:
                        f.write(html)
                    raise RetryableError(
                        f"Форма авторизации не найдена. Извлечено: {form_ids}"
                    )
                
                self.logger.info(f"📋 Форма: {form_ids['form_base']}, кнопка: {form_ids['submit_button']}")
                return viewstate, form_ids
                
        except aiohttp.ClientError as e:
            raise RetryableError(f"Сетевая ошибка: {e}")
    
    async def _perform_login(self, session: aiohttp.ClientSession, 
                            viewstate: str, form_ids: Dict[str, str]):
        """Отправка логина и пароля"""
        url = f"{self.base_url}/index.xhtml"
        
        form_base = form_ids['form_base']
        submit_button = form_ids['submit_button']
        
        data = {
            form_base: form_base,
            f'{form_base}:xin': self.login,
            f'{form_base}:password': self.password,
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': submit_button,
            'javax.faces.partial.event': 'click',
            'javax.faces.partial.execute': f'{submit_button} @component',
            'javax.faces.partial.render': '@component',
            'org.richfaces.ajax.component': submit_button,
            submit_button: submit_button,
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
        
        headers = self._get_ajax_headers()
        headers['Referer'] = url
        
        try:
            async with session.post(url, data=data, headers=headers) as response:
                if response.status in [400, 500, 502, 503, 504]:
                    text = await response.text()
                    self.logger.warning(f"HTTP {response.status} при логине: {text[:200]}")
                    raise RetryableError(f"HTTP {response.status} при логине")
                
                if response.status != 200:
                    raise RetryableError(f"HTTP {response.status}")
                
                self.logger.debug("Логин отправлен")
                
        except aiohttp.ClientError as e:
            raise RetryableError(f"Сетевая ошибка: {e}")
    
    async def _verify_authentication(self, session: aiohttp.ClientSession) -> bool:
        """Проверка успешности авторизации"""
        url = f"{self.base_url}/form/proceedings/services.xhtml"
        
        try:
            async with session.get(url, headers=self._get_base_headers(), 
                                   allow_redirects=False) as response:
                
                if response.status in [301, 302, 303, 307, 308]:
                    raise RetryableError("Редирект - не авторизован")
                
                if response.status in [401, 403]:
                    raise NonRetriableError(f"HTTP {response.status}: Доступ запрещён")
                
                if response.status in [500, 502, 503, 504]:
                    raise RetryableError(f"HTTP {response.status}")
                
                if response.status != 200:
                    raise RetryableError(f"HTTP {response.status}")
                
                html = await response.text()
                
                # Признаки успешной авторизации
                checks = [
                    'profile-context-menu' in html,
                    'Выйти' in html,
                    'logout()' in html,
                    'userInfo.xhtml' in html,
                ]
                
                passed = sum(checks)
                
                if passed >= 2:
                    return True
                
                # Если форма логина - точно не авторизован
                if 'password' in html.lower() and 'xin' in html.lower():
                    raise RetryableError("Обнаружена форма логина")
                
                # Хотя бы 1 признак - принимаем
                if passed >= 1:
                    self.logger.warning(f"⚠️ Только {passed}/4 признаков, но принимаем")
                    return True
                
                raise RetryableError(f"Нет признаков авторизации ({passed}/4)")
                
        except (RetryableError, NonRetriableError):
            raise
        except aiohttp.ClientError as e:
            raise RetryableError(f"Сетевая ошибка: {e}")
    
    def _extract_auth_form_ids(self, html: str) -> Dict[str, str]:
        """Динамическое извлечение ID элементов формы авторизации"""
        parser = HTMLParser(html)
        ids = {}
        
        # 1. Ищем поле email (ИИН)
        email_input = parser.css_first('input[type="email"]')
        if email_input and email_input.attributes:
            xin_name = email_input.attributes.get('name', '')
            if ':' in xin_name:
                # "j_idt72:auth:xin" → "j_idt72:auth"
                ids['form_base'] = ':'.join(xin_name.split(':')[:-1])
                ids['xin_field'] = xin_name
        
        # 2. Ищем кнопку "Войти" в форме авторизации
        if ids.get('form_base'):
            for btn in parser.css('input[type="submit"]'):
                if not btn.attributes:
                    continue
                
                btn_name = btn.attributes.get('name', '')
                btn_value = btn.attributes.get('value', '')
                btn_style = btn.attributes.get('style', '')
                
                # Пропускаем скрытые
                if 'display: none' in btn_style or 'display:none' in btn_style:
                    continue
                
                # Кнопка должна принадлежать форме авторизации
                if ids['form_base'] in btn_name:
                    # Предпочитаем кнопку "Войти"
                    if btn_value == 'Войти':
                        ids['submit_button'] = btn_name
                        break
                    # Или любую видимую кнопку формы
                    elif 'submit_button' not in ids:
                        ids['submit_button'] = btn_name
        
        self.logger.debug(f"Извлечённые ID: {ids}")
        return ids
    
    def _get_base_headers(self) -> Dict[str, str]:
        return HttpHeaders.get_base()

    def _get_ajax_headers(self) -> Dict[str, str]:
        return HttpHeaders.get_ajax()