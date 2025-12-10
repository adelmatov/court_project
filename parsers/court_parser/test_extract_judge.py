#!/usr/bin/env python3
"""
Скрипт для извлечения председательствующего судьи из PDF судебных актов.
Ищет "Председательствующий" + ФИО в разных форматах
"""

import re
import sys
from pathlib import Path
from typing import Optional

try:
    import fitz  # PyMuPDF
except ImportError:
    print("❌ Установите PyMuPDF: pip install pymupdf")
    sys.exit(1)


# ============================================================
# 👇 ВСТАВЬТЕ ПУТЬ К PDF ФАЙЛУ ЗДЕСЬ 👇
# ============================================================

INPUT_FILE = r"C:\Users\adelmatov001\court_project\docs\ТОО+«Алматыинжстрой»+к+ГУ+«Управление+финансов+и+государственных+активов++Карагандинской+области»+закупки.pdf"

# ============================================================


def extract_text(pdf_path: Path) -> str:
    """Извлечение текста из PDF"""
    doc = fitz.open(pdf_path)
    text_parts = []
    
    for page in doc:
        text_parts.append(page.get_text("text", sort=True))
    
    doc.close()
    return "\n".join(text_parts)


def normalize_judge_name(surname: str, initials: str) -> str:
    """Нормализация ФИО судьи в формат: Фамилия И.О."""
    
    surname = surname.strip()
    initials = initials.strip()
    
    # Убираем лишние пробелы и точки из инициалов
    initials = re.sub(r'\s+', '', initials)
    
    # Добавляем точки после каждой заглавной буквы если их нет
    normalized_initials = ""
    for char in initials:
        if char.isupper():
            normalized_initials += char + "."
        elif char != ".":
            normalized_initials += char
    
    # Убираем двойные точки
    normalized_initials = re.sub(r'\.+', '.', normalized_initials)
    
    return f"{surname} {normalized_initials}".strip()


def extract_presiding_judge(text: str) -> Optional[str]:
    """
    Извлечение председательствующего судьи.
    
    Поддерживаемые форматы:
    - Председательствующий Е.И.Идиров
    - Председательствующий Е.Абдильдин
    - Председательствующий судья: С.И. Таусаров
    - Председательствующий Николаева И.В.
    - Председательствующий Касимов Т.Т
    - Председательствующий С. Ж. Габдулин
    """
    
    # Нормализуем пробелы
    text_normalized = re.sub(r'\s+', ' ', text)
    
    # Паттерн для поиска "Председательствующий" с возможным "судья:"
    base_pattern = r'Председательствующ(?:ий|ая)(?:\s+судья\s*:?)?\s*'
    
    # Казахские и русские буквы
    letters = r'А-ЯЁІҮӨҚҰҒҢӘҺа-яёіүөқұғңәһA-Za-z'
    
    patterns = [
        # Формат 1: И.О.Фамилия или И.О. Фамилия (Е.И.Идиров, Е.Абдильдин)
        base_pattern + rf'([{letters}])\s*\.?\s*([{letters}])?\s*\.?\s*([{letters}][{letters.lower()}]+)',
        
        # Формат 2: Фамилия И.О. или Фамилия И.О (Николаева И.В., Касимов Т.Т)
        base_pattern + rf'([{letters}][{letters.lower()}]+)\s+([{letters}])\s*\.?\s*([{letters}])?\s*\.?',
    ]
    
    for i, pattern in enumerate(patterns):
        match = re.search(pattern, text_normalized)
        
        if match:
            groups = match.groups()
            
            if i == 0:
                # Формат: И.О.Фамилия
                initial1 = groups[0]
                initial2 = groups[1] if groups[1] else ""
                surname = groups[2]
                initials = initial1 + ("." if initial1 else "") + initial2 + ("." if initial2 else "")
            else:
                # Формат: Фамилия И.О.
                surname = groups[0]
                initial1 = groups[1]
                initial2 = groups[2] if groups[2] else ""
                initials = initial1 + ("." if initial1 else "") + initial2 + ("." if initial2 else "")
            
            return normalize_judge_name(surname, initials)
    
    return None


def main():
    """Главная функция"""
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        if not INPUT_FILE:
            print("❌ Укажите путь к файлу в INPUT_FILE")
            sys.exit(1)
        input_file = INPUT_FILE
    
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"❌ Файл не найден: {input_path}")
        sys.exit(1)
    
    print(f"\n📂 Файл: {input_path.name}")
    print("-" * 40)
    
    # Извлекаем текст
    text = extract_text(input_path)
    
    # Извлекаем судью
    judge = extract_presiding_judge(text)
    
    if judge:
        print(f"👨‍⚖️ Председательствующий: {judge}")
    else:
        print("❌ Председательствующий не найден")
    
    print("-" * 40)


# === ТЕСТЫ ===
def test_patterns():
    """Тестирование на примерах"""
    test_cases = [
        "Председательствующий     Е.И.Идиров",
        "Председательствующий                                     Е.Абдильдин",
        "Председательствующий судья:                              С.И. Таусаров",
        "Председательствующий судья:                              О.М. Мамытбеков",
        "Председательствующий                                                         Николаева И.В.",
        "Председательствующий          Касимов Т.Т",
        "Председательствующий         С. Ж. Габдулин",
    ]
    
    print("\n" + "=" * 50)
    print("🧪 ТЕСТИРОВАНИЕ")
    print("=" * 50)
    
    for test in test_cases:
        result = extract_presiding_judge(test)
        status = "✅" if result else "❌"
        print(f"{status} {result or 'НЕ НАЙДЕНО'}")
        print(f"   Исходник: {test.strip()[:50]}...")
        print()


if __name__ == "__main__":
    # Раскомментируйте для тестирования:
    # test_patterns()
    
    main()