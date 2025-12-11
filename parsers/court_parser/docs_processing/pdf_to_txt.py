#!/usr/bin/env python3
"""
Скрипт для извлечения текста из PDF судебных актов и извлечения сторон дела.
Подготовка текста для NER/LLM обработки.
"""

import re
import sys
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

try:
    import fitz  # PyMuPDF
except ImportError:
    print("❌ Установите PyMuPDF: pip install pymupdf")
    sys.exit(1)


# ============================================================
# КОНСТАНТЫ
# ============================================================

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

CITIES = {
    'Алматы', 'Астана', 'Шымкент', 'Караганда', 'Актобе', 'Тараз', 'Павлодар',
    'Усть-Каменогорск', 'Семей', 'Костанай', 'Петропавловск', 'Кызылорда',
    'Атырау', 'Актау', 'Уральск', 'Темиртау', 'Туркестан', 'Кокшетау',
    'Талдыкорган', 'Экибастуз', 'Рудный', 'Жезказган', 'Жанаозен', 'Балхаш',
    'Кентау', 'Сатпаев', 'Каскелен', 'Конаев',
}


# ============================================================
# DATACLASS ДЛЯ РЕЗУЛЬТАТОВ
# ============================================================

@dataclass
class CourtCase:
    """Структура данных судебного дела"""
    case_number: Optional[str] = None
    document_type: Optional[str] = None
    date: Optional[str] = None
    city: Optional[str] = None
    court: Optional[str] = None
    presiding_judge: Optional[str] = None
    judges: List[str] = None
    plaintiffs: List[str] = None
    defendants: List[str] = None
    secretary: Optional[str] = None
    
    def __post_init__(self):
        if self.judges is None:
            self.judges = []
        if self.plaintiffs is None:
            self.plaintiffs = []
        if self.defendants is None:
            self.defendants = []
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)


# ============================================================
# ФУНКЦИИ ИЗВЛЕЧЕНИЯ ТЕКСТА ИЗ PDF
# ============================================================

def extract_text(pdf_path: Path) -> Tuple[str, int]:
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
    text = re.sub(r'(\S)\s+(Судь[яи])\s+', r'\1\n\n\2 ', text)
    text = re.sub(r'(\S)\s+(Председательствующ\w*)\s+', r'\1\n\n\2 ', text)
    text = re.sub(
        r'([а-яёұүғқңәөһі]\s*)([А-ЯЁҰҮҒҚҢӘӨҺІ]\.[А-ЯЁҰҮҒҚҢӘӨҺІ]?\.\s*[А-ЯЁҰҮҒҚҢӘӨҺІа-яёұүғқңәөһі]+)',
        r'\1\n\2',
        text
    )
    return text


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ОБРАБОТКИ ТЕКСТА
# ============================================================

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
    
    if re.match(r'^\d{1,2}\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+\d{4}', line_stripped, re.IGNORECASE):
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
    
    if re.match(r'^( Председательствующ|Судь[яиеёю]|Секретарь)', line_stripped, re.IGNORECASE):
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


def get_next_non_empty_line(lines: list, start_index: int) -> Tuple[int, str]:
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
    """Полная обработка текста"""
    text = raw_text
    text = remove_page_numbers(text)
    text = fix_word_hyphenation(text)
    text = merge_lines(text)
    text = split_header_from_body(text)
    text = split_signatures(text)
    text = normalize_whitespace(text)
    return text


# ============================================================
# ФУНКЦИИ ИЗВЛЕЧЕНИЯ ДАННЫХ (NER)
# ============================================================

def normalize_name(name: str) -> str:
    """Нормализация имени (удаление лишних пробелов, переносов строк)"""
    name = re.sub(r'\s+', ' ', name)
    name = name.strip()
    # Убираем точку в конце если есть
    name = re.sub(r'\.$', '', name)
    return name


def extract_case_number(text: str) -> Optional[str]:
    """Извлечение номера дела"""
    patterns = [
        r'№\s*(\d{4}-\d{2}-\d{2}-\d[а-яa-z]*/\d+)',
        r'дело\s*№?\s*(\d{4}-\d{2}-\d{2}-\d[а-яa-z]*/\d+)',
        r'№\s*([^\s]+/\d+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extract_document_type(text: str) -> Optional[str]:
    """Извлечение типа документа"""
    patterns = [
        (r'\bО\s*П\s*Р\s*Е\s*Д\s*Е\s*Л\s*Е\s*Н\s*И\s*Е\b', 'ОПРЕДЕЛЕНИЕ'),
        (r'\bП\s*О\s*С\s*Т\s*А\s*Н\s*О\s*В\s*Л\s*Е\s*Н\s*И\s*Е\b', 'ПОСТАНОВЛЕНИЕ'),
        (r'\bР\s*Е\s*Ш\s*Е\s*Н\s*И\s*Е\b', 'РЕШЕНИЕ'),
        (r'\bП\s*Р\s*И\s*Г\s*О\s*В\s*О\s*Р\b', 'ПРИГОВОР'), (r'\bОПРЕДЕЛЕНИЕ\b', 'ОПРЕДЕЛЕНИЕ'),
        (r'\bПОСТАНОВЛЕНИЕ\b', 'ПОСТАНОВЛЕНИЕ'),
        (r'\bРЕШЕНИЕ\b', 'РЕШЕНИЕ'),
        (r'\bПРИГОВОР\b', 'ПРИГОВОР'),
    ]
    
    for pattern, doc_type in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return doc_type
    return None


def extract_date(text: str) -> Optional[str]:
    """Извлечение даты документа"""
    pattern = r'(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})\s+года?'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2)} {match.group(3)} года"
    return None


def extract_city(text: str) -> Optional[str]:
    """Извлечение города"""
    cities_pattern = '|'.join(CITIES)
    pattern = rf'(?:г\.\s*|город\s+)({cities_pattern})'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Прямой поиск города
    for city in CITIES:
        if city in text:
            return city
    return None


def extract_court(text: str) -> Optional[str]:
    """Извлечение наименования суда"""
    patterns = [
        r'(Судебная\s+коллегия\s+по\s+(?:административным|гражданским|уголовным)\s+делам\s+(?:суда\s+)?(?:области\s+)?[А-ЯЁа-яё]+)',
        r'((?:специализированн\w+\s+)?(?:межрайонн\w+\s+)?(?:административн\w+\s+)?суд[а]?\s+(?:области\s+)?[А-ЯЁа-яё]+)',
        r'(Верховн\w+\s+Суд\w*\s+Республики\s+Казахстан)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return normalize_name(match.group(1))
    return None


def extract_presiding_judge(text: str) -> Optional[str]:
    """Извлечение председательствующего судьи"""
    patterns = [
        # "председательствующего судьи Фамилия И.О."
        r'председательствующ(?:его|ей|ий)\s+судь[ияе]\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
        # "Председательствующий судья: Фамилия И.О."
        r'Председательствующий\s+судья:?\s*([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s*[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.\s*[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
        # "Председательствующий: Фамилия И.О."
        r'Председательствующ\w*:?\s*([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s*[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.\s*[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return normalize_name(match.group(1))
    return None


def extract_judges(text: str) -> List[str]:
    """Извлечение списка судей (кроме председательствующего)"""
    judges = []
    
    # Паттерн для судей в составе коллегии
    patterns = [
        # "судей Фамилия И.О., Фамилия И.О."
        r'судей\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)(?:,\s*([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?))?',
        # Судьи в подписи
        r'Судь[яи]:?\s*([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s*[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.\s*[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if isinstance(match, tuple):
                for m in match:
                    if m:
                        judges.append(normalize_name(m))
            else:
                judges.append(normalize_name(match))
    
    # Убираем дубликаты
    return list(dict.fromkeys(judges))


def extract_secretary(text: str) -> Optional[str]:
    """Извлечение секретаря судебного заседания"""
    patterns = [
        r'(?:при\s+)?секретар[еёя]\s+(?:судебного\s+заседания\s+)?([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
        r'секретар[ьея]\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return normalize_name(match.group(1))
    return None


def extract_plaintiffs(text: str) -> List[str]:
    """Извлечение истцов"""
    plaintiffs = []
    
    # Нормализация текста
    text_normalized = re.sub(r'\s+', ' ', text)
    
    patterns = [
        # "по иску Фамилия Имя Отчество"
        r'по\s+иску\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+)',
        # "по иску Фамилия И.О."
        r'по\s+иску\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
        # "истец Фамилия И.О."
        r'истец\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
        # "истца Фамилия И.О."
        r'истца\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
        # "истцом Фамилия И.О."
        r'истцом\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
        # "Истец Фамилия Имя Отчество"
        r'[Ии]стец\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+(?:\s+[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+)?)',
        # Для юридических лиц: "по иску ТОО «Название»"
        r'по\s+иску\s+((?:ТОО|АО|ООО|ИП)\s*[«"][^»"]+[»"])',
        # Для организаций без кавычек
        r'по\s+иску\s+([А-ЯЁ][А-ЯЁа-яё\s]+(?:ТОО|АО|ООО|ИП|РГУ|ГУ))',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text_normalized, re.IGNORECASE)
        for match in matches:
            name = normalize_name(match)
            if name and name not in plaintiffs and len(name) > 3:
                plaintiffs.append(name)
    
    return plaintiffs


def extract_defendants(text: str) -> List[str]:
    """Извлечение ответчиков"""
    defendants = []
    
    # Нормализация текста
    text_normalized = re.sub(r'\s+', ' ', text)
    
    patterns = [
        # ЧСИ с полным именем
        r'к\s+частному\s+судебному\s+исполнителю\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+)',
        # ЧСИ с И.О.
        r'к\s+частному\s+судебному\s+исполнителю\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
        # ЧСИ сокращенно
        r'(?:к\s+)?ЧСИ\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
        r'ЧСИ\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s*[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]*)',
        # Учреждение
        r'к\s+((?:республиканскому\s+)?государственному\s+учреждению\s+[«"][^»"]+[»"])',
        r'к\s+(РГУ\s+[«"][^»"]+[»"])',
        r'к\s+(ГУ\s+[«"][^»"]+[»"])',
        # Юридические лица
        r'к\s+((?:ТОО|АО|ООО|ИП)\s*[«"][^»"]+[»"])',
        # Ответчик физ. лицо
        r'к\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+(?:у|ой)?)\s+(?:об\s+оспаривании|о\s+|далее)',
        # Ответчик - общий паттерн
        r'ответчик[а]?\s+([А-ЯЁҚҒӘҢӨҰҮҺІа-яёқғәңөұүһі]+\s+[А-ЯЁҚҒӘҢӨҰҮҺІ]\.[А-ЯЁҚҒӘҢӨҰҮҺІ]?\.?)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text_normalized, re.IGNORECASE)
        for match in matches:
            name = normalize_name(match)
            if name and name not in defendants and len(name) > 3:
                defendants.append(name)
    
    return defendants


def extract_case_data(text: str) -> CourtCase:
    """Извлечение всех данных из текста судебного акта"""
    case = CourtCase()
    
    case.case_number = extract_case_number(text)
    case.document_type = extract_document_type(text)
    case.date = extract_date(text)
    case.city = extract_city(text)
    case.court = extract_court(text)
    case.presiding_judge = extract_presiding_judge(text)
    case.judges = extract_judges(text)
    case.secretary = extract_secretary(text)
    case.plaintiffs = extract_plaintiffs(text)
    case.defendants = extract_defendants(text)
    
    return case


# ============================================================
# ОСНОВНЫЕ ФУНКЦИИ ОБРАБОТКИ
# ============================================================

def process_pdf(input_file: str, output_dir: str = None, save_txt: bool = True, save_json: bool = True) -> CourtCase:
    """
    Обработка PDF файла:
    1. Извлечение и форматирование текста
    2. Сохранение в TXT (опционально)
    3. Извлечение данных (NER)
    4. Сохранение в JSON (опционально)
    
    Args:
        input_file: путь к PDF файлу
        output_dir: директория для сохранения результатов (по умолчанию - рядом с PDF)
        save_txt: сохранять ли текст в TXT
        save_json: сохранять ли данные в JSON
    
    Returns:
        CourtCase: извлеченные данные
    """
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"❌ Файл не найден: {input_path}")
        sys.exit(1)
    
    # Определяем директорию для сохранения
    if output_dir:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        out_dir = input_path.parent
    
    print("=" * 60)
    print(f"📄 Обработка: {input_path.name}")
    print("=" * 60)
    
    # 1. Извлечение текста из PDF
    print("\n🔍 Извлечение текста из PDF...")
    raw_text, pages = extract_text(input_path)
    
    # 2. Обработка текста
    print("📝 Форматирование текста...")
    clean_text = process_text(raw_text)
    
    raw_lines = len(raw_text.split('\n'))
    clean_lines = len(clean_text.split('\n'))
    
    print(f"   • Страниц: {pages}")
    print(f"   • Строк до обработки: {raw_lines}")
    print(f"   • Строк после обработки: {clean_lines}")
    print(f"   • Символов: {len(clean_text):,}")
    
    # 3. Сохранение TXT
    if save_txt:
        txt_path = out_dir / (input_path.stem + ".txt")
        txt_path.write_text(clean_text, encoding='utf-8')
        print(f"\n✅ Текст сохранен: {txt_path}")
    
    # 4. Извлечение данных (NER)
    print("\n🔎 Извлечение данных...")
    case_data = extract_case_data(clean_text)
    
    # 5. Вывод результатов
    print("\n" + "─" * 40)
    print("📋 ИЗВЛЕЧЕННЫЕ ДАННЫЕ:")
    print("─" * 40)
    
    print(f"   Номер дела: {case_data.case_number or 'не найден'}")
    print(f"   Тип документа: {case_data.document_type or 'не определен'}")
    print(f"   Дата: {case_data.date or 'не найдена'}")
    print(f"   Город: {case_data.city or 'не найден'}")
    print(f"   Суд: {case_data.court or 'не найден'}")
    print(f"   Председательствующий: {case_data.presiding_judge or 'не найден'}")
    
    if case_data.judges:
        print(f"   Судьи: {', '.join(case_data.judges)}")
    
    print(f"   Секретарь: {case_data.secretary or 'не найден'}")
    
    if case_data.plaintiffs:
        print(f"\n   📌 ИСТЦЫ ({len(case_data.plaintiffs)}):")
        for i, p in enumerate(case_data.plaintiffs, 1):
            print(f"      {i}. {p}")
    else:
        print("\n   📌 Истцы: не найдены")
    
    if case_data.defendants:
        print(f"\n   📌 ОТВЕТЧИКИ ({len(case_data.defendants)}):")
        for i, d in enumerate(case_data.defendants, 1):
            print(f"      {i}. {d}")
    else:
        print("\n   📌 Ответчики: не найдены")
    
    # 6. Сохранение JSON
    if save_json:
        json_path = out_dir / (input_path.stem + "_data.json")
        json_path.write_text(case_data.to_json(), encoding='utf-8')
        print(f"\n✅ Данные сохранены: {json_path}")
    
    print("\n" + "=" * 60)
    
    return case_data


def process_directory(input_dir: str, output_dir: str = None) -> List[CourtCase]:
    """
    Обработка всех PDF файлов в директории
    
    Args:
        input_dir: путь к директории с PDF файлами
        output_dir: директория для сохранения результатов
    
    Returns:
        List[CourtCase]: список извлеченных данных
    """
    input_path = Path(input_dir)
    
    if not input_path.exists():
        print(f"❌ Директория не найдена: {input_path}")
        sys.exit(1)
    
    pdf_files = list(input_path.glob("*.pdf"))
    
    if not pdf_files:
        print(f"❌ PDF файлы не найдены в: {input_path}")
        sys.exit(1)
    
    print(f"\n📁 Найдено {len(pdf_files)} PDF файлов")
    
    results = []
    for pdf_file in pdf_files:
        try:
            case_data = process_pdf(str(pdf_file), output_dir)
            results.append(case_data)
        except Exception as e:
            print(f"❌ Ошибка при обработке {pdf_file.name}: {e}")
    
    # Сводная статистика
    print("\n" + "=" * 60)
    print("📊 СВОДНАЯ СТАТИСТИКА")
    print("=" * 60)
    print(f"   Обработано файлов: {len(results)}/{len(pdf_files)}")
    print(f"   Найдено истцов: {sum(len(c.plaintiffs) for c in results)}")
    print(f"   Найдено ответчиков: {sum(len(c.defendants) for c in results)}")
    print(f"   Найдено судей: {sum(1 for c in results if c.presiding_judge)}")
    
    # Сохранение общего JSON
    if output_dir:
        out_dir = Path(output_dir)
    else:
        out_dir = input_path
    
    summary_path = out_dir / "all_cases_summary.json"
    summary_data = [case.to_dict() for case in results]
    summary_path.write_text(json.dumps(summary_data, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"\n✅ Сводка сохранена: {summary_path}")
    
    return results


# ============================================================
# НАСТРОЙКИ - УКАЖИТЕ ПУТЬ К ФАЙЛУ ЗДЕСЬ
# ============================================================

INPUT_PATH = r"C:\Users\adelmatov001\court_project\docs\republic\supreme\2025\6001-25-00-6ап_21\2025-05-23_21_ТОО_SANDALYECI-_проект.pdf"

# Директория для сохранения результатов (None = рядом с исходным файлом)
OUTPUT_DIR = None

# Сохранять ли файлы
SAVE_TXT = True
SAVE_JSON = True

# ============================================================
# ТОЧКА ВХОДА
# ============================================================

def main():
    """Главная функция"""
    
    # Определяем путь: из командной строки или из переменной INPUT_PATH
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        input_path = INPUT_PATH
    
    # Парсинг аргументов командной строки
    save_txt = SAVE_TXT if "--no-txt" not in sys.argv else False
    save_json = SAVE_JSON if "--no-json" not in sys.argv else False
    
    output_dir = OUTPUT_DIR
    if "--output" in sys.argv:
        idx = sys.argv.index("--output")
        if idx + 1 < len(sys.argv):
            output_dir = sys.argv[idx + 1]
    
    path = Path(input_path)
    
    if not path.exists():
        print(f"❌ Путь не найден: {input_path}")
        print("\n💡 Укажите путь к PDF файлу:")
        print("   1. В переменной INPUT_PATH в коде")
        print("   2. Или через командную строку: python script.py path/to/file.pdf")
        sys.exit(1)
    
    if path.is_file() and path.suffix.lower() == '.pdf':
        # Обработка одного файла
        process_pdf(input_path, output_dir, save_txt, save_json)
    elif path.is_dir():
        # Обработка директории
        process_directory(input_path, output_dir)
    else:
        print(f"❌ Неверный путь: {input_path}")
        print("   Укажите PDF файл или директорию с PDF файлами")
        sys.exit(1)


if __name__ == "__main__":
    main()