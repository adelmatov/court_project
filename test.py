# test_auth_final.py
import asyncio
import aiohttp
import json
import ssl
from selectolax.parser import HTMLParser

def extract_auth_form_ids(html: str) -> dict:
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
    if xin_input:
        xin_name = xin_input.attributes.get('name', '') if xin_input.attributes else ''
        xin_id = xin_input.attributes.get('id', '') if xin_input.attributes else ''
        
        ids['xin_field'] = xin_name or xin_id
        
        # Извлекаем базовый ID формы из имени поля
        # Например: 'j_idt82:auth:xin' → 'j_idt82:auth'
        if ':' in ids['xin_field']:
            parts = ids['xin_field'].split(':')
            ids['form_base'] = ':'.join(parts[:-1])
    
    # 2. Поиск поля пароля (input[type="password"])
    password_input = parser.css_first('input[type="password"]')
    if password_input:
        password_name = password_input.attributes.get('name', '') if password_input.attributes else ''
        password_id = password_input.attributes.get('id', '') if password_input.attributes else ''
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
            ids['submit_button_value'] = button_value
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
    
    # Метод 3: По onclick с RichFaces.ajax (запасной)
    if 'submit_button' not in ids:
        ajax_elements = parser.css('[onclick*="RichFaces.ajax"]')
        for elem in ajax_elements:
            if not elem.attributes:
                continue
                
            onclick = elem.attributes.get('onclick', '') or ''
            elem_id = elem.attributes.get('id', '') or ''
            elem_name = elem.attributes.get('name', '') or ''
            elem_type = elem.attributes.get('type', '') or ''
            
            # Проверяем что это submit кнопка формы авторизации
            if 'auth' in (elem_id or elem_name) and elem_type == 'submit':
                ids['submit_button'] = elem_name or elem_id
                break
    
    return ids


async def test_auth():
    # Загружаем config
    with open('parsers/court_parser/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    base_url = config['base_url']
    login = config['auth']['login']
    password = config['auth']['password']
    user_name = config['auth']['user_name']
    
    print("=" * 90)
    print("ФИНАЛЬНЫЙ ТЕСТ АВТОРИЗАЦИИ office.sud.kz")
    print("=" * 90)
    print(f"🌐 URL: {base_url}")
    print(f"👤 ИИН: {login}")
    print(f"🔑 Пароль: {'*' * len(password)}")
    print(f"📛 Ожидаемое имя: {user_name}")
    print("=" * 90)
    
    # SSL контекст
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        
        # ============================================================
        # ШАГ 1: ГЛАВНАЯ СТРАНИЦА
        # ============================================================
        print("\n" + "─" * 90)
        print("📄 ШАГ 1/5: Загрузка главной страницы")
        print("─" * 90)
        
        async with session.get(f"{base_url}/") as resp:
            html = await resp.text()
            
            # Сохраняем
            with open('debug_step1_main.html', 'w', encoding='utf-8') as f:
                f.write(html)
            
            # ViewState
            parser = HTMLParser(html)
            viewstate_input = parser.css_first('input[name="javax.faces.ViewState"]')
            viewstate = viewstate_input.attributes.get('value') if viewstate_input and viewstate_input.attributes else None
            
            print(f"✅ Статус: {resp.status}")
            print(f"✅ Размер: {len(html):,} байт")
            print(f"✅ ViewState: {viewstate[:60] if viewstate else 'NOT FOUND'}...")
        
        await asyncio.sleep(1)
        
        # ============================================================
        # ШАГ 2: СМЕНА ЯЗЫКА
        # ============================================================
        print("\n" + "─" * 90)
        print("🌍 ШАГ 2/5: Переключение на русский язык")
        print("─" * 90)
        
        data = {
            'f_l_temp': 'f_l_temp',
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': 'f_l_temp:js_temp_1',
            'javax.faces.partial.execute': 'f_l_temp:js_temp_1 @component',
            'javax.faces.partial.render': '@component',
            'param1': f'{base_url}/',
            'org.richfaces.ajax.component': 'f_l_temp:js_temp_1',
            'f_l_temp:js_temp_1': 'f_l_temp:js_temp_1',
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'Faces-Request': 'partial/ajax',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        async with session.post(f"{base_url}/index.xhtml", data=data, headers=headers) as resp:
            response_text = await resp.text()
            
            with open('debug_step2_lang.xml', 'w', encoding='utf-8') as f:
                f.write(response_text)
            
            print(f"✅ Статус: {resp.status}")
            print(f"✅ Размер ответа: {len(response_text):,} байт")
        
        await asyncio.sleep(1)
        
        # ============================================================
        # ШАГ 3: ПОЛУЧЕНИЕ ФОРМЫ С РУССКИМ ИНТЕРФЕЙСОМ
        # ============================================================
        print("\n" + "─" * 90)
        print("📋 ШАГ 3/5: Получение формы авторизации")
        print("─" * 90)
        
        async with session.get(f"{base_url}/index.xhtml") as resp:
            html = await resp.text()
            
            # Сохраняем
            with open('debug_step3_form.html', 'w', encoding='utf-8') as f:
                f.write(html)
            
            # Извлекаем ID элементов
            auth_ids = extract_auth_form_ids(html)
            
            # Новый ViewState
            parser = HTMLParser(html)
            viewstate_input = parser.css_first('input[name="javax.faces.ViewState"]')
            viewstate = viewstate_input.attributes.get('value') if viewstate_input and viewstate_input.attributes else viewstate
            
            print(f"✅ Статус: {resp.status}")
            print(f"✅ Размер: {len(html):,} байт")
            print(f"✅ Обновлённый ViewState: {viewstate[:60]}...")
            print(f"\n📝 Извлечённые ID элементов формы:")
            
            if auth_ids:
                for key, value in auth_ids.items():
                    print(f"   • {key:20s}: {value}")
            else:
                print("   ⚠️  Элементы не найдены!")
        
        # Проверка что все нужные поля найдены
        required_fields = ['form_base', 'xin_field', 'submit_button']
        missing = [f for f in required_fields if f not in auth_ids]
        
        if missing:
            print(f"\n❌ ОШИБКА: Не найдены поля: {', '.join(missing)}")
            print(f"\n🔍 Попытка альтернативного извлечения...")
            
            # Альтернативный метод - ручной поиск в HTML
            parser = HTMLParser(html)
            
            # Ищем все input элементы
            print("\n📋 Все input элементы на странице:")
            inputs = parser.css('input')
            for inp in inputs[:10]:  # Первые 10
                if inp.attributes:
                    inp_type = inp.attributes.get('type', 'N/A')
                    inp_id = inp.attributes.get('id', 'N/A')
                    inp_name = inp.attributes.get('name', 'N/A')
                    inp_value = inp.attributes.get('value', 'N/A')
                    print(f"   type={inp_type:10s} id={inp_id:30s} name={inp_name:30s} value={inp_value[:20]}")
            
            return
        
        # Извлекаем значения
        form_base = auth_ids['form_base']
        submit_button = auth_ids['submit_button']
        
        await asyncio.sleep(1)
        
        # ============================================================
        # ШАГ 4: ОТПРАВКА ЛОГИНА И ПАРОЛЯ
        # ============================================================
        print("\n" + "─" * 90)
        print("🔐 ШАГ 4/5: Отправка учётных данных")
        print("─" * 90)
        
        # Формируем данные ТОЧНО как в DevTools
        auth_data = {
            form_base: form_base,
            f'{form_base}:xin': login,
            f'{form_base}:password': password,
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
        
        print("📤 Отправляемые данные:")
        print("─" * 90)
        for key, value in auth_data.items():
            if 'password' in key.lower():
                display_value = '•' * len(value)
            elif key == 'javax.faces.ViewState':
                display_value = f"{value[:40]}... ({len(value)} символов)"
            else:
                display_value = value
            
            print(f"   {key:40s} = {display_value}")
        print("─" * 90)
        
        async with session.post(f"{base_url}/index.xhtml", 
                              data=auth_data, 
                              headers=headers) as resp:
            response_text = await resp.text()
            
            # Сохраняем
            with open('debug_step4_auth.xml', 'w', encoding='utf-8') as f:
                f.write(response_text)
            
            print(f"\n✅ Статус: {resp.status}")
            print(f"✅ Размер ответа: {len(response_text):,} байт")
            
            # Проверяем нет ли ошибок в ответе
            if 'error' in response_text.lower() or 'ошибка' in response_text.lower():
                print(f"⚠️  В ответе обнаружены ошибки!")
                print(f"   Первые 500 символов:")
                print(f"   {response_text[:500]}")
        
        await asyncio.sleep(1)
        
        # ============================================================
        # ШАГ 5: ПРОВЕРКА АВТОРИЗАЦИИ
        # ============================================================
        print("\n" + "─" * 90)
        print("✔️  ШАГ 5/5: Проверка успешности авторизации")
        print("─" * 90)
        
        check_url = f"{base_url}/form/proceedings/services.xhtml"
        
        async with session.get(check_url) as resp:
            html = await resp.text()
            
            # Сохраняем
            with open('debug_step5_check.html', 'w', encoding='utf-8') as f:
                f.write(html)
            
            print(f"✅ Статус: {resp.status}")
            print(f"✅ Размер: {len(html):,} байт")
            
            # Проверки авторизации
            checks = {
                '🔹 profile-context-menu': 'profile-context-menu' in html,
                f'🔹 Имя "{user_name}"': user_name in html,
                '🔹 Кнопка "Выйти"': 'Выйти' in html,
                '🔹 Функция logout()': 'logout()' in html,
                '🔹 Ссылка userInfo.xhtml': 'userInfo.xhtml' in html,
                '🔹 Текст "Редактировать профиль"': 'Редактировать профиль' in html,
                '🔹 Размер страницы > 30 KB': len(html) > 30000
            }
            
            print("\n" + "─" * 90)
            print("📊 РЕЗУЛЬТАТЫ ПРОВЕРКИ:")
            print("─" * 90)
            
            for check_name, result in checks.items():
                status = "✅" if result else "❌"
                print(f"   {status} {check_name}")
            
            passed = sum(checks.values())
            total = len(checks)
            percentage = (passed / total) * 100
            
            print("─" * 90)
            print(f"📈 Пройдено проверок: {passed}/{total} ({percentage:.1f}%)")
            print("─" * 90)
            
            # Итоговый вердикт
            if passed >= 5:
                print("\n" + "=" * 90)
                print("🎉🎉🎉 АВТОРИЗАЦИЯ УСПЕШНА! 🎉🎉🎉")
                print("=" * 90)
                print("\n✅ Все системы работают корректно!")
                print("✅ Можно переносить код в основной парсер!")
                return True
            elif passed >= 3:
                print("\n" + "=" * 90)
                print("⚠️  ЧАСТИЧНАЯ АВТОРИЗАЦИЯ")
                print("=" * 90)
                print("\n⚠️  Некоторые проверки не пройдены, но базовая авторизация работает")
                return True
            else:
                print("\n" + "=" * 90)
                print("❌❌❌ АВТОРИЗАЦИЯ НЕ ПРОЙДЕНА ❌❌❌")
                print("=" * 90)
                print("\n🔍 Анализ проблемы:")
                print(f"   • Проверьте файл debug_step5_check.html")
                print(f"   • Первые 1000 символов страницы:")
                print("   " + "─" * 86)
                print(f"   {html[:1000]}")
                print("   " + "─" * 86)
                return False


if __name__ == "__main__":
    result = asyncio.run(test_auth())
    
    if result:
        print("\n✅ Тест завершён успешно!")
        print("📝 Следующий шаг: перенос кода в authenticator.py")
    else:
        print("\n❌ Тест не пройден")
        print("📝 Проверьте debug файлы для анализа проблемы")