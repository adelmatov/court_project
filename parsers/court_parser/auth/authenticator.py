# parsers/court_parser/auth/authenticator.py
"""
Авторизация на сайте office.sud.kz
"""

import asyncio
import aiohttp
from typing import Dict, Optional

from selectolax.parser import HTMLParser

from utils.logger import get_logger
from utils.retry import RetryStrategy, RetryConfig, NonRetriableError


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
        """
        Полный процесс авторизации с retry
        
        Args:
            session_manager: SessionManager для создания новых сессий
        """
        auth_retry_config = self.retry_config.get('authentication', {})
        
        if not auth_retry_config:
            # Без retry
            return await self._do_authenticate(session_manager)
        
        # С retry
        retry_cfg = RetryConfig(auth_retry_config)
        strategy = RetryStrategy(retry_cfg)
        
        create_new_session = auth_retry_config.get('create_new_session', True)
        
        async def _auth_with_session_reset():
            try:
                return await self._do_authenticate(session_manager)
            except Exception as e:
                # При ошибке создаем новую сессию (если настроено)
                if create_new_session:
                    self.logger.debug("Создаю новую сессию перед retry...")
                    await session_manager.create_session()
                raise
        
        try:
            result = await strategy.execute_with_retry(
                _auth_with_session_reset,
                error_context="Авторизация"
            )
            return result
        
        except Exception as e:
            self.logger.error(f"❌ Авторизация не удалась: {e}")
            raise AuthenticationError(f"Не удалось авторизоваться: {e}") from e
    
    async def _do_authenticate(self, session_manager) -> bool:
        """Один цикл авторизации"""
        session = await session_manager.get_session()
        
        self.logger.info("Начинаю авторизацию...")
        
        # Этап 1: Главная страница
        viewstate = await self._load_main_page(session)
        await asyncio.sleep(1)
        
        # Этап 2: Смена языка
        await self._switch_to_russian(session, viewstate)
        await asyncio.sleep(0.5)
        
        # Этап 3: Логин
        await self._perform_login(session, viewstate)
        await asyncio.sleep(0.5)
        
        # Этап 4: Проверка
        is_authenticated = await self._verify_authentication(session)
        
        if is_authenticated:
            self.logger.info("✅ Авторизация успешна")
            return True
        else:
            # Проверка не прошла - это может быть как временная, так и постоянная ошибка
            retriable_on_fail = self.retry_config.get('authentication', {}).get(
                'retriable_on_auth_check_fail', True
            )
            
            if retriable_on_fail:
                raise AuthenticationError("Проверка авторизации не пройдена")
            else:
                raise NonRetriableError("Проверка авторизации не пройдена (неверные учетные данные?)")
    
    async def _load_main_page(self, session: aiohttp.ClientSession) -> Optional[str]:
        """Загрузка главной страницы и извлечение ViewState"""
        url = f"{self.base_url}/"
        headers = self._get_base_headers()
        
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                raise AuthenticationError(f"HTTP {response.status} при загрузке главной")
            
            html = await response.text()
            viewstate = self._extract_viewstate(html)
            
            self.logger.debug("Главная страница загружена")
            return viewstate
    
    async def _switch_to_russian(self, session: aiohttp.ClientSession, 
                                 viewstate: Optional[str]):
        """Смена языка на русский"""
        url = f"{self.base_url}/index.xhtml"
        
        # Получаем текущую страницу для ID элементов
        async with session.get(url, headers=self._get_base_headers()) as response:
            html = await response.text()
            current_viewstate = self._extract_viewstate(html)
        
        # Формируем данные для смены языка
        data = {
            'f_l_temp': 'f_l_temp',
            'javax.faces.ViewState': current_viewstate or viewstate,
            'javax.faces.source': 'f_l_temp:js_temp_1',
            'javax.faces.partial.execute': 'f_l_temp:js_temp_1 @component',
            'javax.faces.partial.render': '@component',
            'param1': f'{self.base_url}/',
            'org.richfaces.ajax.component': 'f_l_temp:js_temp_1',
            'f_l_temp:js_temp_1': 'f_l_temp:js_temp_1',
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
        
        headers = self._get_ajax_headers()
        headers['Referer'] = f'{self.base_url}/'
        
        async with session.post(url, data=data, headers=headers) as response:
            if response.status != 200:
                raise AuthenticationError(f"HTTP {response.status} при смене языка")
            
            self.logger.debug("Язык переключен на русский")
    
    async def _perform_login(self, session: aiohttp.ClientSession, 
                            viewstate: Optional[str]):
        """Отправка логина и пароля"""
        url = f"{self.base_url}/index.xhtml"
        
        # Получаем форму авторизации
        async with session.get(url, headers=self._get_base_headers()) as response:
            html = await response.text()
            auth_ids = self._extract_auth_form_ids(html)
            current_viewstate = self._extract_viewstate(html)
        
        # Извлекаем ID элементов
        form_base = auth_ids.get('form_base', 'j_idt82:auth')
        submit_button = auth_ids.get('submit_button')
        
        # ВАЖНО: Если кнопка не найдена - используем дефолт
        if not submit_button:
            submit_button = f'{form_base}:j_idt89'
            self.logger.warning(f"ID кнопки не найден, используется дефолт: {submit_button}")
        else:
            self.logger.debug(f"Используется ID кнопки: {submit_button}")
        
        # Формируем данные для логина
        data = {
            form_base: form_base,
            f'{form_base}:xin': self.login,
            f'{form_base}:password': self.password,
            'javax.faces.ViewState': current_viewstate or viewstate,
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
        headers['Referer'] = f'{self.base_url}/index.xhtml'
        
        async with session.post(url, data=data, headers=headers) as response:
            if response.status != 200:
                raise AuthenticationError(f"HTTP {response.status} при отправке логина")
            
            self.logger.debug("Логин и пароль отправлены")
    
    async def _verify_authentication(self, session: aiohttp.ClientSession) -> bool:
        """
        Проверка успешности авторизации
        
        Raises:
            aiohttp.ClientError: при HTTP 502, 503, 504 (retriable ошибки)
            NonRetriableError: при HTTP 401, 403 (постоянные ошибки)
        
        Returns:
            True если авторизация успешна
            False если не удалось определить (будет обработано в _do_authenticate)
        """
        url = f"{self.base_url}/form/proceedings/services.xhtml"
        
        try:
            async with session.get(url, headers=self._get_base_headers()) as response:
                # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: обработка HTTP ошибок
                
                # Постоянные ошибки (не авторизован)
                if response.status in [401, 403]:
                    self.logger.error(f"HTTP {response.status}: Неверные учетные данные")
                    raise NonRetriableError(f"HTTP {response.status}: Авторизация отклонена сервером")
                
                # Временные ошибки сервера (retry)
                if response.status in [500, 502, 503, 504]:
                    self.logger.warning(f"HTTP {response.status}: Временная ошибка сервера")
                    raise aiohttp.ClientError(f"HTTP {response.status}: Сервер недоступен")
                
                # Успешный ответ
                if response.status != 200:
                    self.logger.error(f"HTTP {response.status} при проверке авторизации")
                    return False
                
                html = await response.text()
                
                # Проверяем наличие элементов авторизованной страницы
                checks = {
                    'profile-context-menu': 'profile-context-menu' in html,
                    'Выйти': 'Выйти' in html,
                    'logout()': 'logout()' in html,
                    'userInfo.xhtml': 'userInfo.xhtml' in html
                }
                
                passed = sum(checks.values())
                
                if passed >= 3:  # Минимум 3 признака из 4
                    self.logger.info(f"✅ Авторизация подтверждена ({passed}/4 проверок)")
                    return True
                
                self.logger.error(f"❌ Авторизация не подтверждена ({passed}/4 проверок)")
                
                # Сохраняем HTML для отладки
                try:
                    with open('failed_auth_debug.html', 'w', encoding='utf-8') as f:
                        f.write(html)
                    self.logger.info("HTML сохранён в failed_auth_debug.html")
                except:
                    pass
                
                return False
        
        except (aiohttp.ClientError, NonRetriableError):
            # Пробрасываем исключения дальше (для retry логики)
            raise
        
        except Exception as e:
            # Неожиданная ошибка
            self.logger.error(f"Неожиданная ошибка при проверке авторизации: {e}")
            raise aiohttp.ClientError(f"Ошибка проверки авторизации: {e}")
    
    def _extract_viewstate(self, html: str) -> Optional[str]:
        """Извлечение ViewState из HTML"""
        parser = HTMLParser(html)
        viewstate_input = parser.css_first('input[name="javax.faces.ViewState"]')
        
        if viewstate_input and viewstate_input.attributes:
            return viewstate_input.attributes.get('value')
        return None
    
    def _extract_auth_form_ids(self, html: str) -> Dict[str, str]:
        """
        Извлечение ID элементов формы авторизации
        
        Возвращает:
            {
                'form_base': 'j_idt82:auth',
                'xin_field': 'j_idt82:auth:xin',
                'password_field': 'j_idt82:auth:password',
                'submit_button': 'j_idt82:auth:j_idt89'
            }
        """
        parser = HTMLParser(html)
        ids = {}
        
        # 1. Поиск поля ИИН (input[type="email"])
        xin_input = parser.css_first('input[type="email"]')
        if xin_input and xin_input.attributes:
            xin_name = xin_input.attributes.get('name', '') or ''
            xin_id = xin_input.attributes.get('id', '') or ''
            
            ids['xin_field'] = xin_name or xin_id
            
            # Извлекаем базовый ID формы из имени поля
            # Например: 'j_idt82:auth:xin' → 'j_idt82:auth'
            if ':' in ids['xin_field']:
                parts = ids['xin_field'].split(':')
                ids['form_base'] = ':'.join(parts[:-1])
        
        # 2. Поиск поля пароля (input[type="password"])
        password_input = parser.css_first('input[type="password"]')
        if password_input and password_input.attributes:
            password_name = password_input.attributes.get('name', '') or ''
            password_id = password_input.attributes.get('id', '') or ''
            ids['password_field'] = password_name or password_id
        
        # 3. Поиск кнопки "Войти"
        # Метод 1: По value="Войти" и type="submit"
        submit_buttons = parser.css('input[type="submit"]')
        
        for button in submit_buttons:
            if not button.attributes:
                continue
                
            button_value = button.attributes.get('value', '')
            if button_value is None:
                button_value = ''
            button_value = button_value.strip()
            
            button_id = button.attributes.get('id', '') or ''
            button_name = button.attributes.get('name', '') or ''
            
            # Проверяем текст кнопки
            if button_value.lower() in ['войти', 'login', 'кіру']:
                ids['submit_button'] = button_name or button_id
                self.logger.debug(f"Найдена кнопка входа: {ids['submit_button']}")
                break
        
        # Метод 2: По классу button-primary (если первый не сработал)
        if 'submit_button' not in ids:
            primary_buttons = parser.css('.button-primary[type="submit"]')
            if primary_buttons:
                button = primary_buttons[0]
                if button.attributes:
                    button_id = button.attributes.get('id', '') or ''
                    button_name = button.attributes.get('name', '') or ''
                    ids['submit_button'] = button_name or button_id
                    self.logger.debug(f"Найдена кнопка (по классу): {ids['submit_button']}")
        
        # Метод 3: По onclick с RichFaces.ajax (запасной)
        if 'submit_button' not in ids:
            ajax_elements = parser.css('[onclick*="RichFaces.ajax"]')
            for elem in ajax_elements:
                if not elem.attributes:
                    continue
                    
                elem_id = elem.attributes.get('id', '') or ''
                elem_name = elem.attributes.get('name', '') or ''
                elem_type = elem.attributes.get('type', '') or ''
                
                # Проверяем что это submit кнопка формы авторизации
                if 'auth' in (elem_id or elem_name) and elem_type == 'submit':
                    ids['submit_button'] = elem_name or elem_id
                    self.logger.debug(f"Найдена кнопка (через onclick): {ids['submit_button']}")
                    break
        
        return ids
    
    def _get_base_headers(self) -> Dict[str, str]:
        """Базовые HTTP заголовки"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru,en;q=0.9',
            'Cache-Control': 'no-cache'
        }
    
    def _get_ajax_headers(self) -> Dict[str, str]:
        """AJAX заголовки"""
        headers = self._get_base_headers()
        headers.update({
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'Faces-Request': 'partial/ajax',
            'X-Requested-With': 'XMLHttpRequest'
        })
        return headers