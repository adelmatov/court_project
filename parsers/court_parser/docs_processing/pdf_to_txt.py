#!/usr/bin/env python3
"""
Скрипт для извлечения текста из PDF судебных актов.
Подготовка текста для NER/LLM обработки.
"""

import re
import sys
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    print("❌ Установите PyMuPDF: pip install pymupdf")
    sys.exit(1)


PREPOSITIONS = {
    'в', 'на', 'по', 'с', 'со', 'к', 'ко', 'о', 'об', 'от', 'до', 'из', 'за',
    'для', 'при', 'без', 'под', 'над', 'между', 'через', 'перед', 'после',
    'и', 'а', 'но', 'или', 'либо', 'что', 'как', 'так', 'чем',
    'который', 'которая', 'которое', 'которые', 'которого', 'которой', 'которым',
}

NUMERALS = {
    'один', 'одна', 'одно', 'одного', 'одной', 'одному', 'одним', 'одном',
    'два', 'две', 'двух', 'двум', 'двумя',
    'три', 'трёх', 'трех', 'трём', 'трем', 'тремя',
    'четыре', 'четырёх', 'четырех', 'четырём', 'четырем', 'четырьмя',
    'пять', 'шесть', 'семь', 'восемь', 'девять', 'десять',
    'пяти', 'шести', 'семи', 'восьми', 'девяти', 'десяти',
    'одиннадцать', 'двенадцать', 'тринадцать', 'четырнадцать', 'пятнадцать',
    'несколько', 'нескольких', 'нескольким', 'несколькими',
    'много', 'многих', 'многим', 'многими',
}

ABBREVIATIONS = {
    'РК', 'РФ', 'КЗ', 'СССР', 'СНГ',
    'ГК', 'ГПК', 'УК', 'УПК', 'КоАП', 'НК', 'ТК', 'ЗК', 'ЖК', 'СК', 'АПК',
    'АППК', 'АО', 'ТОО', 'ИП', 'ОАО', 'ЗАО', 'ООО', 'НАО', 'ПАО',
    'РГУ', 'ГУ', 'КГУ', 'РГКП', 'РГП', 'ГКП',
    'МРП', 'МЗП', 'БИН', 'ИИН', 'НДС', 'КПН',
    'УГД', 'ДГД', 'КГД',
}

# Города Казахстана для определения конца заголовка
CITIES = {
    'Алматы', 'Астана', 'Шымкент', 'Караганда', 'Актобе', 'Тараз', 'Павлодар',
    'Усть-Каменогорск', 'Семей', 'Костанай', 'Петропавловск', 'Кызылорда',
    'Атырау', 'Актау', 'Уральск', 'Темиртау', 'Туркестан', 'Кокшетау',
    'Талдыкорган', 'Экибастуз', 'Рудный', 'Жезказган', 'Жанаозен', 'Балхаш',
    'Кентау', 'Сатпаев', 'Каскелен', 'Конаев',
}


def extract_text(pdf_path: Path) -> tuple[str, int]:
    """Извлечение сырого текста из PDF"""
    doc = fitz.open(pdf_path)
    pages_text = []
    
    for page in doc:
        text = page.get_text("text", sort=True)
        pages_text.append(text)
    
    doc.close()
    return "\n".join(pages_text), len(pages_text)


def remove_page_numbers(text: str) -> str:
    """Удаление номеров страниц"""
    text = re.sub(r'^\s*\d{1,3}\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^\s*[-—]\s*\d{1,3}\s*[-—]\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^\s*(стр\.?|страница)\s*\d{1,3}\s*$', '', text, flags=re.MULTILINE | re.IGNORECASE)
    text = re.sub(r'^\s*\d{1,3}\s*(из|/)\s*\d{1,3}\s*$', '', text, flags=re.MULTILINE)
    return text


def fix_word_hyphenation(text: str) -> str:
    """Склейка слов с переносом"""
    text = re.sub(r'([а-яёА-ЯЁәғқңөұүһіӘҒҚҢӨҰҮҺІa-zA-Z])-\n([а-яёәғқңөұүһіa-z])', r'\1\2', text)
    return text


def split_header_from_body(text: str) -> str:
    """Отделение заголовочной части (дата + город) от основного текста"""
    
    # Паттерн: дата + номер дела + город + начало текста
    # "14 августа 2025 года №6200-25-00-4а/49 город Жезказган Судебная коллегия"
    cities_pattern = '|'.join(CITIES)
    
    pattern = rf'(\d{{1,2}}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+\d{{4}}\s+года?\s*(?:№[^\s]+\s+)?(?:г\.\s*|город\s+)?(?:{cities_pattern}))\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яё])'
    
    def replacer(match):
        header = match.group(1)
        next_char = match.group(2)
        return f"{header}\n\n{next_char}"
    
    text = re.sub(pattern, replacer, text, flags=re.IGNORECASE)
    
    return text


def split_signatures(text: str) -> str:
    """Разделение подписей судей на отдельные строки"""
    
    # Паттерн: "Председательствующий И.О.Фамилия Судьи И.О.Фамилия И.О.Фамилия"
    # Разбиваем перед "Судьи", "Судья", "Председательствующий"
    
    # Разделяем перед "Судьи" или "Судья" (если не в начале строки)
    text = re.sub(r'(\S)\s+(Судь[яи])\s+', r'\1\n\n\2 ', text)
    
    # Разделяем перед "Председательствующий" (если не в начале строки)
    text = re.sub(r'(\S)\s+(Председательствующ\w*)\s+', r'\1\n\n\2 ', text)
    
    # Разделяем между инициалами разных судей: "И.О.Фамилия И.О.Фамилия"
    # После фамилии (маленькие буквы) перед инициалами (Заглавная.Заглавная.)
    text = re.sub(
        r'([а-яёұүғқңәөһі]\s*)([А-ЯЁҰҮҒҚҢӘӨҺІ]\.[А-ЯЁҰҮҒҚҢӘӨҺІ]?\.\s*[А-ЯЁҰҮҒҚҢӘӨҺІа-яёұүғқңәөһі]+)',
        r'\1\n\2',
        text
    )
    
    return text


def is_simple_parenthetical(line: str) -> bool:
    """Проверка простого подзаголовка в скобках"""
    line_stripped = line.strip()
    
    if not re.match(r'^\([^)]+\)\s*$', line_stripped):
        return False
    
    content = line_stripped[1:-1].strip()
    special_chars = ['–', '-', '—', ',', '«', '»', '"', '"', ':', ';']
    for char in special_chars:
        if char in content:
            return False
    
    if re.match(r'^[а-яёА-ЯЁәғқңөұүһіӘҒҚҢӨҰҮҺІa-zA-Z\s]+$', content):
        return True
    
    return False


def is_header_line(line: str) -> bool:
    """Проверка заголовка"""
    line_stripped = line.strip()
    
    if not line_stripped:
        return True
    
    headers = [
        'РЕШЕНИЕ', 'ОПРЕДЕЛЕНИЕ', 'ПОСТАНОВЛЕНИЕ', 'ПРИГОВОР',
        'УСТАНОВИЛ', 'УСТАНОВИЛА', 'УСТАНОВИЛО',
        'ПОСТАНОВИЛ', 'ПОСТАНОВИЛА', 'ПОСТАНОВИЛО',
        'РЕШИЛ', 'РЕШИЛА', 'РЕШИЛО',
        'ОПРЕДЕЛИЛ', 'ОПРЕДЕЛИЛА', 'ОПРЕДЕЛИЛО',
        'РЕЗОЛЮТИВНАЯ ЧАСТЬ', 'МОТИВИРОВОЧНАЯ ЧАСТЬ',
    ]
    
    if line_stripped.upper() in headers:
        return True
    
    if re.match(r'^[А-ЯЁ](\s+[А-ЯЁ]){3,}[:\s]*$', line_stripped):
        return True
    
    if is_simple_parenthetical(line_stripped):
        return True
    
    return False


def is_abbreviation(line: str) -> bool:
    """Проверка, начинается ли строка с известной аббревиатуры"""
    line_stripped = line.strip()
    
    for abbr in ABBREVIATIONS:
        if line_stripped.startswith(abbr + ' ') or line_stripped.startswith(abbr + '.') or line_stripped.startswith(abbr + ',') or line_stripped == abbr:
            return True
    
    if re.match(r'^[А-ЯЁA-Z]{2,6}[.\s,)]', line_stripped):
        return True
    
    return False


def is_name_with_initials(line: str) -> bool:
    """Проверка, является ли строка фамилией с инициалами"""
    line_stripped = line.strip()
    
    if re.match(r'^[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі][а-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?\s*', line_stripped):
        return True
    
    if re.match(r'^[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.\s*[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+', line_stripped):
        return True
    
    return False


def is_parenthetical_explanation(line: str) -> bool:
    """Проверка пояснения в скобках"""
    line_stripped = line.strip()
    
    if re.match(r'^\(далее\s*[-–—]', line_stripped, re.IGNORECASE):
        return True
    
    if re.match(r'^\([^)]+\)\s*$', line_stripped):
        content = line_stripped[1:-1]
        special_chars = ['–', '-', '—', ',', '«', '»', '"', '"', ':', ';']
        for char in special_chars:
            if char in content:
                return True
    
    return False


def is_list_item(line: str) -> bool:
    """Проверка элемента списка"""
    line_stripped = line.strip()
    
    if re.match(r'^\d+[.\)]\s', line_stripped):
        return True
    
    if re.match(r'^[а-яa-z][.\)]\s', line_stripped):
        return True
    
    if re.match(r'^[-•*]\s', line_stripped):
        return True
    
    return False


def is_date_line(line: str) -> bool:
    """Проверка, является ли строка датой"""
    line_stripped = line.strip()
    
    if re.match(r'^\d{1,2}\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+\d{4}', line_stripped, re.IGNORECASE ):
        return True
    
    return False


def is_standalone_metadata_line(line: str) -> bool:
    """Проверка самостоятельной строки метаданных"""
    line_stripped = line.strip()
    
    cities_pattern = '|'.join(CITIES)
    if re.match(rf'^\d{{1,2}}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+\d{{4}}\s+(?:года?\s+)?(?:№[^\s]+\s+)?(?:г\.\s*|город\s+)?(?:{cities_pattern})\s*$', line_stripped, re.IGNORECASE):
        return True
    
    return False


def is_signature_line(line: str) -> bool:
    """Проверка подписи"""
    line_stripped = line.strip()
    
    if re.match(r'^(Председательствующ|Судь[яиеёю]|Секретарь)', line_stripped, re.IGNORECASE):
        return True
    
    if re.match(r'^[А-ЯЁ]\.[А-ЯЁ]\.\s+[А-ЯЁ][а-яё]+\s*$', line_stripped):
        return True
    
    if re.match(r'^[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.[А-ЯЁ]\.\s*$', line_stripped):
        return True
    
    return False


def is_initials(line: str) -> bool:
    """Проверка инициалов в начале строки"""
    line_stripped = line.strip()
    if re.match(r'^[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?', line_stripped):
        return True
    return False


def starts_with_adjective_or_participle(line: str) -> bool:
    """Проверка прилагательного/причастия в начале строки"""
    line_stripped = line.strip()
    
    endings = (
        'ого', 'его', 'ой', 'ей', 'ых', 'их', 'ому', 'ему',
        'ым', 'им', 'ую', 'юю', 'ое', 'ее', 'ая', 'яя',
        'ыми', 'ими', 'ном', 'нем', 'ной', 'ней',
        'щего', 'вшего', 'нного', 'того', 'мого',
        'щей', 'вшей', 'нной', 'щих', 'вших', 'нных',
        'ными', 'ским', 'ской', 'ское', 'ских', 'ская',
        'ённый', 'енный', 'анный', 'янный',
        'ованный', 'ёванный', 'еванный',
    )
    
    pattern = r'^[А-ЯЁа-яёҚҒӘҢӨҰҮҺІқғәңөұүһі][а-яёқғәңөұүһі]+(' + '|'.join(endings) + r')\b'
    if re.match(pattern, line_stripped):
        return True
    
    return False


def starts_with_lowercase(line: str) -> bool:
    """Проверка, начинается ли строка с маленькой буквы"""
    line_stripped = line.strip()
    if line_stripped and re.match(r'^[а-яёәғқңөұүһіa-z]', line_stripped):
        return True
    return False


def starts_with_digit(line: str) -> bool:
    """Проверка, начинается ли строка с цифры"""
    line_stripped = line.strip()
    return bool(re.match(r'^\d', line_stripped))


def ends_with_sentence_end(line: str) -> bool:
    """Проверка, заканчивается ли строка концом предложения"""
    line_stripped = line.strip()
    return bool(re.search(r'[.!?]\s*$', line_stripped))


def ends_with_colon(line: str) -> bool:
    """Проверка, заканчивается ли строка двоеточием"""
    line_stripped = line.strip()
    return line_stripped.endswith(':')


def ends_with_preposition(line: str) -> bool:
    """Проверка окончания на предлог/союз"""
    line_stripped = line.strip().lower()
    words = line_stripped.split()
    if not words:
        return False
    last_word = re.sub(r'[^\w]', '', words[-1])
    return last_word in PREPOSITIONS


def ends_with_numeral(line: str) -> bool:
    """Проверка окончания на числительное"""
    line_stripped = line.strip().lower()
    words = line_stripped.split()
    if not words:
        return False
    last_word = re.sub(r'[^\w]', '', words[-1])
    return last_word in NUMERALS


def ends_with_digit(line: str) -> bool:
    """Проверка окончания на цифру"""
    line_stripped = line.strip()
    return bool(re.search(r'\d\s*$', line_stripped))


def ends_with_number_dash(line: str) -> bool:
    """Проверка окончания на цифру с дефисом (диапазон): 148-"""
    line_stripped = line.strip()
    return bool(re.search(r'\d[-–—]\s*$', line_stripped))


def ends_with_open_quote(line: str) -> bool:
    """Проверка незакрытой кавычки"""
    line_stripped = line.strip()
    open_quotes = line_stripped.count('«') + line_stripped.count('"')
    close_quotes = line_stripped.count('»') + line_stripped.count('"')
    return open_quotes > close_quotes


def ends_with_open_paren(line: str) -> bool:
    """Проверка незакрытой скобки"""
    line_stripped = line.strip()
    open_parens = line_stripped.count('(')
    close_parens = line_stripped.count(')')
    return open_parens > close_parens


def ends_with_number_paren(line: str) -> bool:
    """Проверка окончания на цифру со скобкой"""
    line_stripped = line.strip()
    return bool(re.search(r'\d\)\s*$', line_stripped))


def ends_with_dash(line: str) -> bool:
    """Проверка окончания на тире"""
    line_stripped = line.strip()
    return bool(re.search(r'\s[-—]\s*$', line_stripped))


def ends_with_word(line: str) -> bool:
    """Проверка, заканчивается ли строка на слово"""
    line_stripped = line.strip()
    if not line_stripped:
        return False
    return bool(re.search(r'[а-яёА-ЯЁәғқңөұүһіӘҒҚҢӨҰҮҺІa-zA-Z]\s*$', line_stripped))


def should_force_merge(current_line: str, next_line: str) -> bool:
    """Условия принудительной склейки"""
    current = current_line.strip()
    next_stripped = next_line.strip()
    
    if not current or not next_stripped:
        return False
    
    if starts_with_lowercase(next_stripped):
        return True
    
    if ends_with_number_dash(current):
        return True
    
    if ends_with_preposition(current):
        return True
    
    if is_abbreviation(next_stripped):
        if not ends_with_sentence_end(current):
            return True
    
    if is_name_with_initials(next_stripped):
        if not ends_with_sentence_end(current):
            return True
    
    if is_parenthetical_explanation(next_stripped):
        return True
    
    if next_stripped.startswith('(') and not is_simple_parenthetical(next_stripped):
        return True
    
    if is_initials(next_stripped):
        return True
    
    if starts_with_adjective_or_participle(next_stripped):
        return True
    
    if ends_with_numeral(current):
        return True
    
    if ends_with_digit(current) and not ends_with_sentence_end(current):
        return True
    
    if ends_with_open_quote(current):
        return True
    
    if ends_with_open_paren(current):
        return True
    
    if ends_with_number_paren(current):
        return True
    
    if ends_with_dash(current):
        return True
    
    if current.endswith(','):
        if not is_list_item(next_stripped) and not is_signature_line(next_stripped):
            return True
    
    if ends_with_word(current) and not ends_with_sentence_end(current) and not ends_with_colon(current):
        if not is_header_line(next_stripped) and not is_list_item(next_stripped) and not is_signature_line(next_stripped):
            new_sentence_starters = [
                'В соответствии', 'На основании', 'По результатам', 'При этом',
                'Согласно ', 'Руководствуясь ', 'Заслушав ', 'Рассмотрев ',
                'Истец ', 'Ответчик ', 'Суд ', 'Судебная ', 'Судья ',
                'Административный ', 'Гражданский ', 'Настоящее ',
                'Вышеуказанное ', 'Указанное ', 'Решением ',
            ]
            is_new_sentence = any(next_stripped.startswith(s) for s in new_sentence_starters)
            
            if not is_new_sentence:
                return True
    
    return False


def should_not_merge(current_line: str, next_line: str) -> bool:
    """Условия запрета склейки"""
    current = current_line.strip()
    next_stripped = next_line.strip()
    
    if not current or not next_stripped:
        return True
    
    if is_header_line(current):
        return True
    
    if is_header_line(next_stripped):
        return True
    
    if is_signature_line(current) and is_signature_line(next_stripped):
        return True
    
    if is_list_item(next_stripped):
        return True
    
    if is_standalone_metadata_line(next_stripped):
        return True
    
    return False


def should_merge(current_line: str, next_line: str) -> bool:
    """Основная проверка склейки"""
    current = current_line.strip()
    next_stripped = next_line.strip()
    
    if should_not_merge(current_line, next_line):
        return False
    
    if should_force_merge(current_line, next_line):
        return True
    
    if re.search(r'[а-яёa-z0-9,;]$', current):
        if re.match(r'^[а-яёa-z0-9]', next_stripped):
            return True
    
    if re.search(r'[а-яёА-ЯЁa-zA-Z]$', current):
        if re.match(r'^\d', next_stripped):
            return True
    
    if re.search(r'\d$', current):
        if re.match(r'^[а-яёa-z]', next_stripped):
            return True
    
    if re.match(r'^[)\]»"]', next_stripped):
        return True
    
    return False


def get_next_non_empty_line(lines: list, start_index: int) -> tuple[int, str]:
    """Получить следующую непустую строку и её индекс"""
    i = start_index + 1
    while i < len(lines):
        if lines[i].strip():
            return i, lines[i]
        i += 1
    return -1, ""


def merge_lines(text: str) -> str:
    """Склейка строк с пропуском пустых"""
    lines = text.split('\n')
    result = []
    i = 0
    
    while i < len(lines):
        current_line = lines[i]
        current_stripped = current_line.strip()
        
        if not current_stripped:
            result.append(current_line)
            i += 1
            continue
        
        next_idx, next_line = get_next_non_empty_line(lines, i)
        
        while next_idx != -1 and should_merge(current_line, next_line):
            next_stripped = next_line.strip()
            current_line = current_line.rstrip() + ' ' + next_stripped
            i = next_idx
            next_idx, next_line = get_next_non_empty_line(lines, i)
        
        result.append(current_line)
        i += 1
    
    return '\n'.join(result)


def normalize_whitespace(text: str) -> str:
    """Нормализация пробелов"""
    text = re.sub(r'[ \t]+', ' ', text)
    text = '\n'.join(line.strip() for line in text.split('\n'))
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = text.strip()
    return text


def process_text(raw_text: str) -> str:
    """Полная обработка"""
    text = raw_text
    text = remove_page_numbers(text)
    text = fix_word_hyphenation(text)
    text = merge_lines(text)
    text = split_header_from_body(text)
    text = split_signatures(text)
    text = normalize_whitespace(text)
    return text


def process_pdf(input_file: str, output_file: str = None):
    """Обработка PDF"""
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"❌ Файл не найден: {input_path}")
        sys.exit(1)
    
    if output_file is None:
        output_file = input_path.stem + ".txt"
    
    output_path = Path(output_file)
    
    print(f"📄 Входной файл: {input_path.name}")
    print("-" * 50)
    
    raw_text, pages = extract_text(input_path)
    clean_text = process_text(raw_text)
    
    raw_lines = len(raw_text.split('\n'))
    clean_lines = len(clean_text.split('\n'))
    
    print(f"   Страниц: {pages}")
    print(f"   Строк до: {raw_lines}")
    print(f"   Строк после: {clean_lines}")
    print(f"   Символов: {len(clean_text):,}")
    
    output_path.write_text(clean_text, encoding='utf-8')
    
    print("-" * 50)
    print(f"✅ Сохранено: {output_path}")


# ============================================================
INPUT_FILE = r"C:\Users\adelmatov001\court_project\docs\ТОО+«Алматыинжстрой»+к+ГУ+«Управление+финансов+и+государственных+активов++Карагандинской+области»+закупки.pdf"
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
    else:
        input_file = INPUT_FILE
        output_file = None
    
    process_pdf(input_file, output_file)