"""
Обработка и очистка текста
"""
import re
from datetime import datetime  # ← оставляем на уровне модуля
from typing import List, Optional, Dict


class TextProcessor:
    """Обработчик текста"""
    
    @staticmethod
    def clean(text: str) -> str:
        """Очистка текста от лишних пробелов"""
        if not text:
            return ""
        return ' '.join(text.split()).strip()
    
    @staticmethod
    def parse_date(date_str: str, format_str: str = '%d.%m.%Y') -> Optional[datetime]:
        """
        Парсинг даты с автоматическим исправлением года
        
        Примеры:
        '15.01.2025' → 2025-01-15
        '15.01.25' → 2025-01-15 (автоисправление)
        '15.01.1925' → None (некорректный год)
        """
        try:
            # Попытка парсинга
            parsed = datetime.strptime(date_str.strip(), format_str)
            
            # ИСПРАВЛЕНИЕ ГОДА
            year = parsed.year
            
            # Если год двузначный (0-99), добавляем 2000
            if year < 100:
                year = 2000 + year
                parsed = parsed.replace(year=year)
            
            # Если год в диапазоне 1900-1999, пытаемся исправить
            elif 1900 <= year < 2000:
                # Берем последние 2 цифры и добавляем 2000
                year_last_two = year % 100
                year = 2000 + year_last_two
                parsed = parsed.replace(year=year)
            
            # ВАЛИДАЦИЯ: год должен быть в разумном диапазоне
            current_year = datetime.now().year
            if not (2000 <= parsed.year <= current_year + 2):
                return None
            
            return parsed
            
        except (ValueError, AttributeError):
            return None
    
    @staticmethod
    def split_parties(text: str) -> List[str]:
        """
        Улучшенное разделение сторон
        
        Правила:
        1. Разделение по запятой (основное)
        2. Разделение после закрывающей кавычки + пробел + заглавная буква
        
        Примеры:
        'ТОО "Компания", Иванов' → ['ТОО "Компания"', 'Иванов']
        'ТОО "Компания" ИВАНОВ' → ['ТОО "Компания"', 'ИВАНОВ']
        'Петров, Сидоров' → ['Петров', 'Сидоров']
        """
        import re
        
        if not text.strip():
            return []
        
        # ШАГ 1: Добавляем запятые после кавычек перед заглавными буквами
        # Паттерн: кавычка + пробелы + заглавная буква (начало ФИО или организации)
        # Примеры: '" ИВАНОВ', '» ТОО', '" Государственное'
        
        text = re.sub(
            r'(["\»"„])\s+([А-ЯЁ][А-ЯЁа-яё\s]+)',  # После кавычки + пробел + заглавная
            r'\1, \2',  # Вставляем запятую
            text
        )
        
        # ШАГ 2: Разделяем по запятым с учетом кавычек
        parts = []
        current = ""
        in_quotes = False
        quote_chars = {'"', '»', '"', '„', '«'}
        
        for i, char in enumerate(text):
            if char in quote_chars:
                in_quotes = not in_quotes
            
            if char == ',' and not in_quotes:
                # Запятая вне кавычек - разделяем
                part = current.strip(' .,;-')
                if part and len(part) >= 5:  # Минимум 5 символов
                    parts.append(part)
                current = ""
            else:
                current += char
        
        # Последняя часть
        part = current.strip(' .,;-')
        if part and len(part) >= 5:
            parts.append(part)
        
        return parts
    
    @staticmethod
    def parse_case_number(case_number: str) -> Optional[Dict[str, str]]:
        """
        Парсинг номера дела
        
        Пример: "6294-25-00-4/215" или "6003-25-00-4к/1454(2)" →
        {
            'court_code': '6294',
            'year': '25',
            'middle': '00',
            'case_type': '4',
            'sequence': '215' или '1454(2)'
        }
        """
        pattern = r'^(\d+)-(\d+)-(\d+)-([0-9а-яА-Я]+)/(\d+(?:\(\d+\))?)$'
        match = re.match(pattern, case_number)
        
        if not match:
            return None
        
        return {
            'court_code': match.group(1),
            'year': match.group(2),
            'middle': match.group(3),
            'case_type': match.group(4),
            'sequence': match.group(5)
        }
    
    @staticmethod
    def generate_case_number(region_config: Dict, court_config: Dict, 
                           year: str, sequence: int) -> str:
        """
        Генерация номера дела
        
        Формат: КАТО+instance-год-00-тип/порядковый
        Пример: 6294-25-00-4/215
        """
        kato = region_config['kato_code']
        instance = court_config['instance_code']
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        
        return f"{kato}{instance}-{year_short}-00-{case_type}/{sequence}"
    
    @staticmethod
    def parse_full_case_number(case_number: str) -> Optional[Dict]:
        """
        Распарсить полный номер дела
        
        Вход: "6294-25-00-4/215" или "6003-25-00-4к/1454(2)"
        Выход: {
            'court_code': '6294',
            'year_short': '25',
            'case_type': '4',
            'sequence': '215' или '1454(2)'
        }
        """
        pattern = r'^(\d+)-(\d+)-(\d+)-([0-9а-яА-Я]+)/(\d+(?:\(\d+\))?)$'
        match = re.match(pattern, case_number)
        
        if not match:
            return None
        
        return {
            'court_code': match.group(1),
            'year_short': match.group(2),
            'middle': match.group(3),
            'case_type': match.group(4),
            'sequence': match.group(5)
        }

    @staticmethod
    def find_region_and_court_by_case_number(case_number: str, regions_config: Dict) -> Optional[Dict]:
        """
        Определить region_key и court_key по номеру дела
        
        Args:
            case_number: полный номер дела "6294-25-00-4/215"
            regions_config: конфигурация регионов из settings
        
        Returns:
            {
                'region_key': 'astana',
                'court_key': 'smas',
                'year': '2025',
                'sequence': '215'
            }
        """
        parsed = TextProcessor.parse_full_case_number(case_number)
        if not parsed:
            return None
        
        # Извлекаем код суда (КАТО + инстанция)
        court_code = parsed['court_code']
        case_type = parsed['case_type']
        
        # Ищем регион и суд по коду
        for region_key, region_config in regions_config.items():
            kato = region_config['kato_code']
            
            for court_key, court_config in region_config['courts'].items():
                instance = court_config['instance_code']
                full_code = f"{kato}{instance}"
                
                if court_code == full_code and court_config['case_type_code'] == case_type:
                    # Восстанавливаем полный год из короткого
                    year_short = int(parsed['year_short'])
                    year = f"20{year_short:02d}"
                    
                    return {
                        'region_key': region_key,
                        'court_key': court_key,
                        'year': year,
                        'sequence': parsed['sequence']
                    }
        
        return None
    
    @staticmethod
    def is_matching_case_number(case_number: str, target: str) -> bool:
        """
        Проверка соответствия номера дела целевому
        
        Правила:
        - Точное совпадение: "991" == "991" ✓
        - С суффиксом: "991(1)" matches "991" ✓
        - С суффиксом: "991(2)" matches "991" ✓
        - Другой номер: "992" != "991" ✗
        - Не суффикс: "9910" != "991" ✗
        
        Args:
            case_number: номер дела из ответа сервера
            target: целевой номер дела (без суффикса)
        
        Returns:
            True если номер соответствует целевому
        
        Examples:
            >>> is_matching_case_number("6003-25-00-4к/991", "6003-25-00-4к/991")
            True
            >>> is_matching_case_number("6003-25-00-4к/991(1)", "6003-25-00-4к/991")
            True
            >>> is_matching_case_number("6003-25-00-4к/992", "6003-25-00-4к/991")
            False
        """
        # Точное совпадение
        if case_number == target:
            return True
        
        # Проверка суффикса вида (1), (2), (10) и т.д.
        # Паттерн: целевой номер + скобки с числом
        pattern = f"^{re.escape(target)}\\(\\d+\\)$"
        return bool(re.match(pattern, case_number))

    @staticmethod
    def extract_base_case_number(case_number: str) -> str:
        """
        Извлечь базовый номер дела без суффикса
        
        Args:
            case_number: номер дела (возможно с суффиксом)
        
        Returns:
            Базовый номер без суффикса
        
        Examples:
            >>> extract_base_case_number("6003-25-00-4к/991(1)")
            "6003-25-00-4к/991"
            >>> extract_base_case_number("6003-25-00-4к/991")
            "6003-25-00-4к/991"
        """
        # Удаляем суффикс (N) в конце
        match = re.match(r'^(.+?)(\(\d+\))?$', case_number)
        if match:
            return match.group(1)
        return case_number