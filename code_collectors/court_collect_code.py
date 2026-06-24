#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для сборки всех модулей парсера в один текстовый файл
Автоматически собирает все .py файлы и ресурсы из папки court_parser

ВАЖНО: Результат сборки — это НЕ исполняемый файл парсера,
а текстовый файл для анализа ИИ, содержащий весь код проекта.
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

# Расширения для исключения
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


HEADER = '''################################################################################
#
#                    СБОРКА КОДА ПАРСЕРА СУДЕБНЫХ ДЕЛ
#
################################################################################
#
# Дата сборки: {build_date}
# 
# ⚠️  ВАЖНО: ЭТО НЕ ИСПОЛНЯЕМЫЙ ФАЙЛ!
#
# Это текстовый файл, содержащий весь исходный код проекта court_parser,
# собранный из отдельных модулей в один файл для удобства анализа.
#
# Назначение:
#   - Предоставить ИИ полную картину кодовой базы
#   - Упростить code review и анализ архитектуры
#   - Документировать текущее состояние проекта
#
# Структура проекта:
#   parsers/court_parser/
#   ├── utils/          - Утилиты (логирование, retry, валидация)
#   ├── config/         - Конфигурация и настройки
#   ├── auth/           - Авторизация на сайте
#   ├── database/       - Работа с PostgreSQL
#   ├── parsing/        - Парсинг HTML
#   ├── search/         - Поисковые запросы
#   ├── core/           - Главный класс парсера
#   └── main.py         - Точка входа
#
# Для запуска парсера используйте оригинальные файлы:
#   python parsers/court_parser/main.py
#
################################################################################


'''

RESOURCES_HEADER = '''
################################################################################
# ВСТРОЕННЫЕ РЕСУРСЫ (config.json и др.)
################################################################################

'''

MODULE_SEPARATOR = '''

################################################################################
# ФАЙЛ: {module_name}
################################################################################

'''

FOOTER = '''

################################################################################
#                           КОНЕЦ СБОРКИ
################################################################################
#
# Всего файлов: {files_count}
# Всего строк: {lines_count}
# Размер: {file_size}
#
################################################################################
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


def discover_files_recursive(dir_path: Path, base_path: Path) -> list:
    """
    ИСПРАВЛЕНО: Рекурсивно обнаруживает все Python файлы в директории и поддиректориях
    
    Args:
        dir_path: Путь к директории для сканирования
        base_path: Базовый путь проекта (для формирования относительных путей)
    
    Returns:
        Список кортежей (relative_path, absolute_path) для каждого .py файла
    """
    if not dir_path.exists():
        return []
    
    py_files = []
    init_files = []  # __init__.py файлы обрабатываем первыми
    
    # Используем os.walk для рекурсивного обхода
    for root, dirs, files in os.walk(dir_path):
        # Исключаем ненужные директории (модифицируем список in-place)
        dirs[:] = [d for d in dirs if not should_exclude_dir(d)]
        
        root_path = Path(root)
        
        for filename in files:
            if not filename.endswith('.py'):
                continue
                
            if should_exclude_file(filename):
                continue
            
            filepath = root_path / filename
            rel_path = filepath.relative_to(base_path)
            
            if filename == '__init__.py':
                init_files.append((str(rel_path), filepath))
            else:
                py_files.append((str(rel_path), filepath))
    
    # Сортируем для предсказуемого порядка
    init_files.sort(key=lambda x: x[0])
    py_files.sort(key=lambda x: x[0])
    
    # __init__.py файлы идут первыми в каждой директории
    # Группируем по директориям
    result = []
    all_files = init_files + py_files
    
    # Сортируем так, чтобы __init__.py шёл перед другими файлами той же директории
    def sort_key(item):
        rel_path = item[0]
        dir_part = str(Path(rel_path).parent)
        filename = Path(rel_path).name
        # __init__.py получает приоритет (0), остальные файлы (1)
        priority = 0 if filename == '__init__.py' else 1
        return (dir_part, priority, filename)
    
    all_files.sort(key=sort_key)
    
    return all_files


def discover_root_files(base_path: Path) -> list:
    """
    Обнаруживает Python файлы только в корневой директории (без рекурсии)
    Используется для корневых файлов типа main.py
    """
    if not base_path.exists():
        return []
    
    py_files = []
    init_file = None
    
    for item in base_path.iterdir():
        if item.is_file() and item.suffix == '.py':
            filename = item.name
            
            if should_exclude_file(filename):
                continue
            
            rel_path = item.relative_to(base_path.parent.parent)  # Относительно PARSER_DIR parent
            
            if filename == '__init__.py':
                init_file = (str(item.relative_to(base_path.parent.parent)), item)
            else:
                py_files.append((str(item.relative_to(base_path.parent.parent)), item))
    
    py_files.sort(key=lambda x: x[0])
    
    if init_file:
        py_files.insert(0, init_file)
    
    return py_files


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


def format_resource_content(resources: dict) -> str:
    """Форматирует ресурсы для читаемого вывода"""
    if not resources:
        return ""
    
    output = []
    output.append(RESOURCES_HEADER)
    
    for name, content in sorted(resources.items()):
        output.append(f"# --- {name} ---")
        output.append("")
        
        # Для JSON форматируем красиво
        if name.endswith('.json'):
            try:
                parsed = json.loads(content)
                formatted = json.dumps(parsed, ensure_ascii=False, indent=2)
                # Добавляем комментарий перед каждой строкой для читаемости
                for line in formatted.split('\n'):
                    output.append(f"# {line}")
            except json.JSONDecodeError:
                output.append(f"# {content}")
        else:
            # Для остальных файлов просто добавляем как комментарии
            for line in content.split('\n'):
                output.append(f"# {line}")
        
        output.append("")
        output.append("")
    
    return '\n'.join(output)


def build_unified_file(output_file: str = 'court_parser_full_code.txt'):
    """Собирает все модули в один текстовый файл"""
    
    print("=" * 70)
    print("🔨 СБОРКА КОДА ПРОЕКТА COURT PARSER")
    print("=" * 70)
    print(f"📁 Рабочая директория: {os.getcwd()}")
    print(f"📂 Директория парсера: {PARSER_DIR}")
    print(f"📄 Выходной файл: {output_file}")
    print()
    
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
    # СБОР PYTHON МОДУЛЕЙ (ИСПРАВЛЕНО - РЕКУРСИВНО)
    # ============================================
    all_code = []
    files_processed = 0
    
    for module_name in MODULE_ORDER:
        if module_name:
            # Для модулей (utils, config, core и т.д.) - рекурсивный обход
            module_path = base_path / module_name
            display_name = f"{PARSER_DIR}/{module_name}"
            
            if not module_path.exists():
                print(f"⚠️  Модуль не найден: {display_name}")
                continue
            
            # ИСПРАВЛЕНО: используем рекурсивную функцию
            py_files = discover_files_recursive(module_path, base_path)
            
        else:
            # Для корневой папки - только файлы первого уровня (main.py и т.д.)
            display_name = PARSER_DIR
            py_files = []
            
            for item in base_path.iterdir():
                if item.is_file() and item.suffix == '.py':
                    if not should_exclude_file(item.name):
                        rel_path = str(item.relative_to(base_path))
                        py_files.append((rel_path, item))
            
            py_files.sort(key=lambda x: (x[0] != '__init__.py', x[0]))
        
        if not py_files:
            continue
        
        print(f"📂 Модуль: {display_name}")
        
        for rel_path, filepath in py_files:
            if not filepath.exists():
                continue
            
            # Формируем путь для отображения
            if module_name:
                display_path = f"{module_name}/{rel_path}"
            else: display_path = rel_path
            
            print(f"   ✓ {display_path}")
            files_processed += 1
            
            content = read_file_content(filepath)
            if not content:
                continue
            
            # Формируем полный путь для заголовка
            full_module_name = f"{PARSER_DIR}/{display_path}"
            
            all_code.append(MODULE_SEPARATOR.format(module_name=full_module_name))
            all_code.append(content.rstrip())
    
    # ============================================
    # ЗАПИСЬ ИТОГОВОГО ФАЙЛА
    # ============================================
    print(f"\n✍️  Запись в файл: {output_file}")
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Заголовок
            f.write(HEADER.format(build_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            # Встроенные ресурсы (как комментарии для читаемости)
            if resources:
                f.write(format_resource_content(resources))
            
            # Код модулей
            f.write('\n'.join(all_code))
            
            # Подсчёт статистики для футера
            f.flush()
        
        # Статистика
        file_size = os.path.getsize(output_file)
        with open(output_file, 'r', encoding='utf-8') as f:
            lines_count = len(f.readlines())
        
        # Дописываем футер с актуальной статистикой
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(FOOTER.format(
                files_count=files_processed,
                lines_count=f"{lines_count:,}",
                file_size=f"{file_size:,} байт ({file_size/1024:.1f} KB)"
            ))
        
        # Пересчитываем после добавления футера
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
        print(f"   💾 Размер файла: {file_size:,} байт ({file_size/1024:.1f} KB)")
        print(f"   🔐 SHA-256: {file_hash[:32]}...")
        print(f"{'=' * 70}")
        print()
        print("💡 Этот файл предназначен для анализа ИИ, а не для запуска!")
        print(f"   Для запуска парсера используйте: python {PARSER_DIR}/main.py")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка при записи файла: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Главная функция"""
    # Выходной файл — текстовый, не .py
    output_file = 'court_parser_full_code.txt'
    
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        output_file = sys.argv[1]
    
    success = build_unified_file(output_file)
    
    if success:
        print(f"\n🎉 Готово! Файл для анализа: {output_file}")
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