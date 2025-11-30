#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для сборки всех модулей парсера в один файл
Автоматически собирает все .py файлы и ресурсы из папки court_parser
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import hashlib
import re
import json
import base64

# Базовая директория парсера
PARSER_DIR = 'parsers/court_parser'

# Порядок модулей (важно для правильных зависимостей)
MODULE_ORDER = [
    'utils',
    'config',
    'auth',
    'database',
    'parsing',
    'search',
    'core',
    '',  # Корневая папка (main.py)
]

# Исключения
EXCLUDE_PATTERNS = [
    r'^test.*\.py$',
    r'^.*_test\.py$',
    r'^debug.*\.py$',
    r'__pycache__',
    r'\.pyc$',
]

EXCLUDE_DIRS = [
    'logs',
    '__pycache__',
    '.git',
    'venv',
    '.venv',
]

# Расширения для исключения (убрали .html если нужны шаблоны)
EXCLUDE_EXTENSIONS = [
    '.log',
    '.pyc',
    '.pyo',
    '.html'
]

# Расширения ресурсов для встраивания
RESOURCE_EXTENSIONS = [
    '.xml',
    '.txt',
    '.css',
    '.json'
]

# Бинарные ресурсы (будут в base64)
BINARY_EXTENSIONS = [
    '.png',
    '.jpg',
    '.ico',
]

HEADER = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Объединенный файл парсера суда
Дата сборки: {build_date}

Этот файл содержит все модули проекта, объединенные в один файл.
Для запуска: python court_parser_unified.py
"""

# ============================================================================
# СТАНДАРТНЫЕ БИБЛИОТЕКИ
# ============================================================================
import os
import sys
import json
import time
import logging
import base64
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any

'''

RESOURCES_SECTION = '''
# ============================================================================
# ВСТРОЕННЫЕ РЕСУРСЫ
# ============================================================================

_EMBEDDED_RESOURCES = {resources_dict}

def get_embedded_resource(name: str) -> str:
    """Получить встроенный ресурс по имени"""
    return _EMBEDDED_RESOURCES.get(name, "")

def get_embedded_json(name: str) -> dict:
    """Получить встроенный JSON как словарь"""
    content = _EMBEDDED_RESOURCES.get(name, "{{}}")
    return json.loads(content)

def get_embedded_binary(name: str) -> bytes:
    """Получить бинарный ресурс (декодирует из base64)"""
    content = _EMBEDDED_RESOURCES.get(name, "")
    return base64.b64decode(content) if content else b""

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


def should_exclude_file(filename: str) -> bool:
    """Проверяет, нужно ли исключить файл"""
    for pattern in EXCLUDE_PATTERNS:
        if re.match(pattern, filename, re.IGNORECASE):
            return True
    
    for ext in EXCLUDE_EXTENSIONS:
        if filename.endswith(ext):
            return True
    
    return False


def should_exclude_dir(dirname: str) -> bool:
    """Проверяет, нужно ли исключить директорию"""
    return dirname in EXCLUDE_DIRS or dirname.startswith('.')


def is_resource_file(filename: str) -> bool:
    """Проверяет, является ли файл ресурсом для встраивания"""
    return any(filename.endswith(ext) for ext in RESOURCE_EXTENSIONS)


def is_binary_resource(filename: str) -> bool:
    """Проверяет, является ли файл бинарным ресурсом"""
    return any(filename.endswith(ext) for ext in BINARY_EXTENSIONS)


def discover_files(base_path: Path, module_dir: str) -> tuple:
    """
    Автоматически обнаруживает все файлы в директории
    
    Returns:
        Кортеж (python_files, resource_files)
    """
    if module_dir:
        dir_path = base_path / module_dir
    else:
        dir_path = base_path
    
    if not dir_path.exists():
        return [], []
    
    py_files = []
    resources = []
    init_file = None
    
    for item in dir_path.iterdir():
        if item.is_file():
            filename = item.name
            
            if should_exclude_file(filename):
                continue
            
            if item.suffix == '.py':
                if filename == '__init__.py':
                    init_file = filename
                else:
                    py_files.append(filename)
            elif is_resource_file(filename) or is_binary_resource(filename):
                resources.append(filename)
    
    py_files.sort()
    resources.sort()
    
    if init_file:
        py_files.insert(0, init_file)
    
    return py_files, resources


def discover_all_resources(base_path: Path) -> dict:
    """Рекурсивно находит все ресурсные файлы"""
    resources = {}
    
    for root, dirs, files in os.walk(base_path):
        # Исключаем ненужные директории
        dirs[:] = [d for d in dirs if not should_exclude_dir(d)]
        
        for filename in files:
            if should_exclude_file(filename):
                continue
            
            filepath = Path(root) / filename
            rel_path = filepath.relative_to(base_path)
            
            if is_resource_file(filename):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        resources[str(rel_path)] = f.read()
                except Exception as e:
                    print(f"⚠️  Ошибка чтения ресурса {rel_path}: {e}")
                    
            elif is_binary_resource(filename):
                try:
                    with open(filepath, 'rb') as f:
                        resources[str(rel_path)] = base64.b64encode(f.read()).decode('ascii')
                except Exception as e:
                    print(f"⚠️  Ошибка чтения бинарного ресурса {rel_path}: {e}")
    
    return resources


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
            cleaned_lines.append(f"# REMOVED IMPORT: {line}")
            continue
        
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
        
        if skip_until_code and (not stripped or stripped.startswith('#')):
            continue
            
        if stripped.startswith('import ') or stripped.startswith('from '):
            skip_until_code = False
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
    print(f"📁 Рабочая директория: {os.getcwd()}")
    print(f"📂 Директория парсера: {PARSER_DIR}\n")
    
    base_path = Path('.') / PARSER_DIR
    
    if not base_path.exists():
        print(f"❌ Директория не найдена: {base_path}")
        return False
    
    # ============================================
    # СБОР РЕСУРСОВ
    # ============================================
    print("📦 Сбор ресурсов...")
    resources = discover_all_resources(base_path)
    
    if resources:
        print(f"   Найдено ресурсов: {len(resources)}")
        for res_name in sorted(resources.keys()):
            size = len(resources[res_name])
            print(f"   ✓ {res_name} ({size} символов)")
    else:
        print("   Ресурсы не найдены")
    
    print()
    
    # ============================================
    # СБОР PYTHON МОДУЛЕЙ
    # ============================================
    all_imports = set()
    all_code = []
    files_processed = 0
    
    for module_name in MODULE_ORDER:
        if module_name:
            module_path = base_path / module_name
            display_name = f"{PARSER_DIR}/{module_name}"
        else:
            module_path = base_path
            display_name = PARSER_DIR
        
        if not module_path.exists():
            print(f"⚠️  Модуль не найден: {display_name}")
            continue
        
        py_files, _ = discover_files(base_path, module_name)
        
        if not py_files:
            continue
        
        print(f"📂 Модуль: {display_name}")
        
        for filename in py_files:
            filepath = module_path / filename
            
            if not filepath.exists():
                continue
            
            print(f"   ✓ {filename}")
            files_processed += 1
            
            content = read_file_content(filepath)
            if not content:
                continue
            
            content = clean_content(content, filename)
            imports, code = extract_imports(content)
            
            if imports:
                for imp in imports.split('\n'):
                    imp = imp.strip()
                    if imp and not imp.startswith('#'):
                        all_imports.add(imp)
            
            code_stripped = code.strip()
            if code_stripped and not (filename == '__init__.py' and len(code_stripped) < 50):
                if module_name:
                    full_module_name = f"{PARSER_DIR}/{module_name}/{filename}"
                else:
                    full_module_name = f"{PARSER_DIR}/{filename}"
                
                all_code.append(MODULE_SEPARATOR.format(module_name=full_module_name))
                all_code.append(code_stripped)
    
    # ============================================
    # ЗАПИСЬ ИТОГОВОГО ФАЙЛА
    # ============================================
    print(f"\n✍️  Запись в файл: {output_file}")

    print(f"DEBUG: resources = {resources}")
    print(f"DEBUG: len(resources) = {len(resources)}")
    print(f"DEBUG: bool(resources) = {bool(resources)}")
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Заголовок
            f.write(HEADER.format(build_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            # Внешние импорты
            if all_imports:
                f.write('\n# Внешние библиотеки\n')
                f.write('\n'.join(sorted(all_imports)))
                f.write('\n')
            
            # Встроенные ресурсы
            if resources:
                print("DEBUG: Вхожу в блок записи ресурсов")
                # Экранируем для безопасной вставки
                resources_repr = json.dumps(resources, ensure_ascii=False, indent=2)
                print(f"DEBUG: resources_repr[:200] = {resources_repr[:200]}")
                f.write(RESOURCES_SECTION.format(resources_dict=resources_repr))
                print("DEBUG: Ресурсы записаны")
            else:
                print("DEBUG: resources ПУСТОЙ, пропускаю")
            
            # Код модулей
            f.write('\n'.join(all_code))
            
            # Футер
            f.write(FOOTER)
        
        # Статистика
        file_size = os.path.getsize(output_file)
        with open(output_file, 'r', encoding='utf-8') as f:
            lines_count = len(f.readlines())
        
        with open(output_file, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()
        
        print(f"\n{'=' * 70}")
        print("✅ СБОРКА ЗАВЕРШЕНА УСПЕШНО!")
        print(f"{'=' * 70}")
        print(f"📊 СТАТИСТИКА:")
        print(f"   📄 Обработано Python файлов: {files_processed}")
        print(f"   📦 Встроено ресурсов: {len(resources)}")
        print(f"   📝 Строк кода: {lines_count:,}")
        print(f"   📦 Уникальных импортов: {len(all_imports)}")
        print(f"   💾 Размер файла: {file_size:,} байт ({file_size/1024:.2f} KB)")
        print(f"   🔐 SHA-256: {file_hash[:32]}...")
        print(f"{'=' * 70}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка при записи файла: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Главная функция"""
    output_file = 'court_parser_unified.py'
    success = build_unified_file(output_file)
    
    if success:
        print(f"\n🎉 Готово! Используйте файл: {output_file}")
        print(f"💡 Запуск: python {output_file}")
        return 0
    else:
        print("\n❌ Сборка завершилась с ошибками")
        return 1


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