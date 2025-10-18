# collect_parser_code.py - Сборщик всех скриптов парсера

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для сборки всех модулей парсера в один файл
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import hashlib

# Структура проекта с порядком импортов
PROJECT_STRUCTURE = [
    # Базовые утилиты и конфигурация
    ('parsers/court_parser/utils', ['__init__.py', 'logger.py', 'text_processor.py', 'validators.py']),
    ('parsers/court_parser/config', ['__init__.py', 'settings.py']),
    
    # Аутентификация
    ('parsers/court_parser/auth', ['__init__.py', 'authenticator.py']),
    
    # База данных
    ('parsers/court_parser/database', ['__init__.py', 'models.py', 'db_manager.py']),
    
    # Парсинг
    ('parsers/court_parser/parsing', ['__init__.py', 'html_parser.py', 'data_extractor.py']),
    
    # Поиск
    ('parsers/court_parser/search', ['__init__.py', 'form_handler.py', 'search_engine.py']),
    
    # Ядро
    ('parsers/court_parser/core', ['__init__.py', 'session.py', 'parser.py']),
    
    # Главный модуль
    ('parsers/court_parser', ['main.py']),
]

HEADER = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Объединенный файл парсера суда
Дата сборки: {build_date}
Автор: Court Parser Team

Этот файл содержит все модули проекта, объединенные в один файл.
Для запуска: python court_parser_unified.py

Структура:
- Утилиты (логирование, валидация, обработка текста)
- Конфигурация
- Аутентификация
- База данных
- Парсинг HTML
- Поисковые функции
- Основная логика
"""

# ============================================================================
# СТАНДАРТНЫЕ БИБЛИОТЕКИ
# ============================================================================
import os
import sys
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any

'''

MODULE_SEPARATOR = '''

# ============================================================================
# МОДУЛЬ: {module_name}
# ============================================================================
'''

FOOTER = '''

# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\\n⚠️  Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
'''


def read_file_content(filepath: Path) -> str:
    """Читает содержимое файла"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"⚠️  Файл не найден: {filepath}")
        return ""
    except Exception as e:
        print(f"⚠️  Ошибка чтения файла {filepath}: {e}")
        return ""


def clean_content(content: str, filename: str) -> str:
    """Очищает содержимое файла от ненужных элементов"""
    lines = content.split('\n')
    cleaned_lines = []
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        
        # Пропускаем shebang и encoding (оставляем только в начале итогового файла)
        if i < 3 and (stripped.startswith('#!') or stripped.startswith('# -*- coding:')):
            continue
        
        # Пропускаем относительные импорты внутри проекта
        if any(x in line for x in [
            'from .', 'from ..', 
            'from auth import', 'from auth.', 
            'from config import', 'from config.',
            'from core import', 'from core.',
            'from database import', 'from database.',
            'from parsing import', 'from parsing.',
            'from search import', 'from search.',
            'from utils import', 'from utils.'
        ]):
            # Заменяем на комментарий для отладки
            cleaned_lines.append(f"# REMOVED IMPORT: {line}")
            continue
        
        # Для __init__.py оставляем только важный код
        if filename == '__init__.py' and not stripped:
            continue
            
        cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)


def extract_imports(content: str) -> tuple:
    """Извлекает импорты из содержимого"""
    lines = content.split('\n')
    imports = []
    code = []
    in_docstring = False
    docstring_char = None
    skip_until_code = True
    
    for line in lines:
        stripped = line.strip()
        
        # Обработка docstring
        if '"""' in line or "'''" in line:
            skip_until_code = False
            if not in_docstring:
                in_docstring = True
                docstring_char = '"""' if '"""' in line else "'''"
                code.append(line)
                if line.count(docstring_char) >= 2:
                    in_docstring = False
            elif docstring_char in line:
                in_docstring = False
                code.append(line)
            else:
                code.append(line)
            continue
            
        if in_docstring:
            code.append(line)
            continue
        
        # Пропускаем комментарии в начале файла до первого кода
        if skip_until_code and (not stripped or stripped.startswith('#')):
            continue
            
        # Собираем внешние импорты
        if stripped.startswith('import ') or stripped.startswith('from '):
            skip_until_code = False
            # Только внешние библиотеки
            if not any(x in stripped for x in [
                'from .', 'from ..', 
                'from auth', 'from config', 'from core', 
                'from database', 'from parsing', 'from search', 'from utils'
            ]):
                imports.append(line)
        else:
            skip_until_code = False
            code.append(line)
    
    return '\n'.join(imports), '\n'.join(code)


def build_unified_file(output_file: str = 'court_parser_unified.py'):
    """Собирает все модули в один файл"""
    
    print("=" * 70)
    print("🔨 СБОРКА ПРОЕКТА COURT PARSER")
    print("=" * 70)
    print(f"📁 Рабочая директория: {os.getcwd()}\n")
    
    base_path = Path('.')
    all_imports = set()
    all_code = []
    files_processed = 0
    files_not_found = 0
    
    # Собираем все файлы
    for module_dir, files in PROJECT_STRUCTURE:
        module_path = base_path / module_dir
        
        print(f"📂 Обработка модуля: {module_dir}")
        
        for filename in files:
            filepath = module_path / filename
            
            if not filepath.exists():
                print(f"   ⚠️  Не найден: {filename}")
                files_not_found += 1
                continue
            
            print(f"   ✓ {filename}")
            files_processed += 1
            
            # Читаем содержимое
            content = read_file_content(filepath)
            if not content:
                continue
            
            # Очищаем содержимое
            content = clean_content(content, filename)
            
            # Извлекаем импорты и код
            imports, code = extract_imports(content)
            
            # Сохраняем импорты
            if imports:
                for imp in imports.split('\n'):
                    imp = imp.strip()
                    if imp and not imp.startswith('#'):
                        all_imports.add(imp)
            
            # Добавляем код с разделителем (кроме пустых __init__.py)
            code_stripped = code.strip()
            if code_stripped and not (filename == '__init__.py' and len(code_stripped) < 50):
                module_name = f"{module_dir}/{filename}"
                all_code.append(MODULE_SEPARATOR.format(module_name=module_name))
                all_code.append(code_stripped)
    
    # Формируем итоговый файл
    print(f"\n✍️  Запись в файл: {output_file}")
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Заголовок
            f.write(HEADER.format(build_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            # Все внешние импорты
            if all_imports:
                f.write('\n# Внешние библиотеки\n')
                f.write('\n'.join(sorted(all_imports)))
                f.write('\n')
            
            # Весь код
            f.write('\n'.join(all_code))
            
            # Футер
            f.write(FOOTER)
        
        # Статистика
        file_size = os.path.getsize(output_file)
        with open(output_file, 'r', encoding='utf-8') as f:
            lines_count = len(f.readlines())
        
        # Вычисляем хеш
        with open(output_file, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()
        
        print(f"\n{'=' * 70}")
        print("✅ СБОРКА ЗАВЕРШЕНА УСПЕШНО!")
        print(f"{'=' * 70}")
        print(f"📊 СТАТИСТИКА:")
        print(f"   📄 Обработано файлов: {files_processed}")
        print(f"   ⚠️  Не найдено файлов: {files_not_found}")
        print(f"   📝 Строк кода: {lines_count:,}")
        print(f"   📦 Уникальных импортов: {len(all_imports)}")
        print(f"   💾 Размер файла: {file_size:,} байт ({file_size/1024:.2f} KB)")
        print(f"   🔐 SHA-256: {file_hash[:32]}...")
        print(f"{'=' * 70}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка при записи файла: {e}")
        return False


def create_requirements():
    """Создает файл requirements.txt"""
    print("\n📦 Создание requirements.txt...")
    
    requirements = [
        "# Court Parser Dependencies",
        "# Generated: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "",
        "# Core dependencies",
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.0",
        "lxml>=4.9.0",
        "",
        "# Database",
        "sqlalchemy>=2.0.0",
        "",
        "# Optional",
        "selenium>=4.15.0  # For JavaScript-heavy pages",
        "pandas>=2.0.0  # For data export",
    ]
    
    try:
        with open('requirements.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(requirements))
        print("✅ Создан requirements.txt")
        return True
    except Exception as e:
        print(f"⚠️  Ошибка создания requirements.txt: {e}")
        return False


def main():
    """Главная функция"""
    output_file = 'court_parser_unified.py'
    
    # Сборка проекта
    success = build_unified_file(output_file)
    
    if success:
        # Создание requirements
        create_requirements()
        
        print(f"\n🎉 Готово! Используйте файл: {output_file}")
        print(f"💡 Запуск: python {output_file}")
    else:
        print("\n❌ Сборка завершилась с ошибками")
        return 1
    
    return 0


if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)