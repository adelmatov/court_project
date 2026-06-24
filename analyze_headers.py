import csv
import os
import re
import multiprocessing
from pathlib import Path
from typing import Iterator, Optional, Tuple, List

import pdfplumber
from tqdm import tqdm

# --- КОНФИГУРАЦИЯ ---
INPUT_DIR = "docs"
OUTPUT_CSV = "classified_documents_robust.csv"
HEADER_CROP_RATIO = 0.25

# --- СПИСОК ПРАВИЛ ДЛЯ КЛАССИФИКАЦИИ ---
# Этот блок должен быть доступен для дочерних процессов, поэтому он на верхнем уровне
def get_classification_rules() -> List[Tuple[str, str]]:
    """Возвращает приоритезированный список правил для классификации."""
    def normalize_keyword(text: str) -> str:
        return re.sub(r'[^А-ЯЁІЇЄҐҰҚӨҺӘІҢҒҮҰҚӨҺA-Z]', '', text.upper())
    
    # Расширенный список, включающий все найденные типы
    rules = [
        # Резолютивные/дополнительные (высший приоритет)
        ("РЕЗОЛЮТИВНАЯЧАСТЬДОПОЛНИТЕЛЬНОГОРЕШЕНИЯ", "РЕЗОЛЮТИВНАЯ ЧАСТЬ ДОПОЛНИТЕЛЬНОГО РЕШЕНИЯ"),
        ("ПОСТАНОВЛЕНИЕОБУТВЕРЖДЕНИИСОГЛАШЕНИЯОПРИМИРЕНИИ", "ПОСТАНОВЛЕНИЕ об утверждении соглашения о примирении"),
        ("ДОПОЛНИТЕЛЬНОЕПОСТАНОВЛЕНИЕ", "ДОПОЛНИТЕЛЬНОЕ ПОСТАНОВЛЕНИЕ"),
        ("ПОСТАНОВЛЕНИЕРЕЗОЛЮТИВНАЯЧАСТЬ", "ПОСТАНОВЛЕНИЕ (резолютивная часть)"),
        ("РЕШЕНИЕИМЕНЕМРЕСПУБЛИКИКАЗАХСТАНРЕЗОЛЮТИВНАЯЧАСТЬ", "РЕШЕНИЕ (резолютивная часть)"),
        
        # Протоколы
        ("КРАТКИЙПРОТОКОЛПРИМИРИТЕЛЬНОЙПРОЦЕДУРЫ", "КРАТКИЙ ПРОТОКОЛ ПРИМИРИТЕЛЬНОЙ ПРОЦЕДУРЫ"),
        ("КРАТКИЙПРОТОКОЛПРЕДВАРИТЕЛЬНОГОСУДЕБНОГОЗАСЕДАНИЯ", "КРАТКИЙ ПРОТОКОЛ ПРЕДВАРИТЕЛЬНОГО СУДЕБНОГО ЗАСЕДАНИЯ"),
        ("КРАТКИЙПРОТОКОЛПРЕДВАРИТЕЛЬНОГОСЛУШАНИЯ", "КРАТКИЙ ПРОТОКОЛ ПРЕДВАРИТЕЛЬНОГО СЛУШАНИЯ"),
        ("ПРОТОКОЛПРЕДВАРИТЕЛЬНОГОСУДЕБНОГОСЛУШАНИЯ", "ПРОТОКОЛ предварительного судебного слушания"),
        ("ПРОТОКОЛВЫЕЗДНОГОЗАСЕДАНИЯ", "ПРОТОКОЛ ВЫЕЗДНОГО ЗАСЕДАНИЯ"),
        ("ПРОТОКОЛСУДЕБНОГОРАЗБИРАТЕЛЬСТВА", "ПРОТОКОЛ СУДЕБНОГО РАЗБИРАТЕЛЬСТВА"),
        ("КРАТКИЙПРОТОКОЛСУДЕБНОГОРАЗБИРАТЕЛЬСТВА", "КРАТКИЙ ПРОТОКОЛ СУДЕБНОГО РАЗБИРАТЕЛЬСТВА"),
        ("КРАТКИЙПРОТОКОЛСУДЕБНОГОЗАСЕДАНИЯ", "КРАТКИЙ ПРОТОКОЛ СУДЕБНОГО ЗАСЕДАНИЯ"),
        
        # Казахские протоколы
        ("АЛДЫНАЛАТЫҢДАЛЫМНЫҢҚЫСҚАШАХАТТАМАСЫ", "АЛДЫН АЛА ТЫҢДАЛЫМНЫҢ ҚЫСҚАША ХАТТАМАСЫ"),
        ("АЛДЫНАЛАТЫҢДАУДЫҢҚЫСҚАШАХАТТАМАСЫ", "АЛДЫН АЛА ТЫҢДАУДЫҢ ҚЫСҚАША ХАТТАМАСЫ"),
        ("АЛДЫНАЛАСОТОТЫРЫСЫНЫҢҚЫСҚАШАХАТТАМАСЫ", "АЛДЫН-АЛА СОТ ОТЫРЫСЫНЫҢ ҚЫСҚАША ХАТТАМАСЫ"),
        ("АШЫҚСОТОТЫРСЫНЫҢХАТТАМАСЫ", "АШЫҚ СОТ ОТЫРСЫНЫҢ ХАТТАМАСЫ"),
        ("СОТОТЫРЫСЫНЫҢҚЫСҚАШАХАТТАМАСЫ", "СОТ ОТЫРЫСЫНЫҢ ҚЫСҚАША ХАТТАМАСЫ"),
        ("СОТОТЫРЫСЫНЫҢХАТТАМАСЫ", "СОТ ОТЫРЫСЫНЫҢ ХАТТАМАСЫ"),
        
        # Основные типы документов
        ("ПОСТАНОВЛЕНИЕ", "ПОСТАНОВЛЕНИЕ"),
        ("РЕШЕНИЕ", "РЕШЕНИЕ"),
        ("ОПРЕДЕЛЕНИЕ", "ОПРЕДЕЛЕНИЕ"),
        ("ПРОТОКОЛ", "ПРОТОКОЛ"),
        ("АНЫҚТАМА", "АНЫҚТАМА"),
        ("ҚАУЛЫ", "ҚАУЛЫ"),
        ("KAULY", "ҚАУЛЫ"),
        ("ХОДАТАЙСТВО", "Ходатайство"),
        
        # Остальные правила из предыдущего анализа
        ("РЕЗОЛЮТИВНАЯЧАСТЬ", "РЕЗОЛЮТИВНАЯ ЧАСТЬ"),
        ("ОПРЕДЕЛЕНИЕОДЕЙСТВИЯХСУДА", "ОПРЕДЕЛЕНИЕ о действиях суда..."),
        ("ОПРЕДЕЛЕНИЕОПОДГОТОВКЕДЕЛА", "ОПРЕДЕЛЕНИЕ о подготовке дела..."),
        ("ДОПОЛНИТЕЛЬНОЕОПРЕДЕЛЕНИЕ", "ДОПОЛНИТЕЛЬНОЕ ОПРЕДЕЛЕНИЕ"),
        ("ЧАСТНОЕОПРЕДЕЛЕНИЕ", "ЧАСТНОЕ ОПРЕДЕЛЕНИЕ"),
    ]
    return [(normalize_keyword(keyword), doc_type) for keyword, doc_type in rules]

CLASSIFICATION_RULES = get_classification_rules()

def classify_header(header_text: str) -> str:
    if not isinstance(header_text, str) or not header_text.strip():
        return "Пустая шапка"
    
    normalized_text = re.sub(r'[^А-ЯЁІЇЄҐҰҚӨҺӘІҢҒҮҰҚӨҺA-Z]', '', header_text.upper())
    
    for keyword, doc_type in CLASSIFICATION_RULES:
        if keyword in normalized_text:
            return doc_type
            
    return "Тип не определен"


def process_file(pdf_path: Path) -> Tuple[Path, str, str]:
    """
    Извлекает шапку, классифицирует и возвращает результат.
    """
    header_text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if pdf.pages:
                first_page = pdf.pages[0]
                crop_box = (0, 0, first_page.width, first_page.height * HEADER_CROP_RATIO)
                header_area = first_page.crop(crop_box)
                extracted = header_area.extract_text(x_tolerance=3, y_tolerance=3)
                if extracted:
                    header_text = " ".join(extracted.split())
    except Exception as e:
        header_text = f"ОШИБКА ОБРАБОТКИ: {type(e).__name__}"

    doc_type = classify_header(header_text)
    return (pdf_path, doc_type, header_text)

# --- ИЗМЕНЕНИЕ: Функции для поиска файлов ---
# Эти функции не должны быть внутри main(), чтобы быть доступными в глобальной области видимости
def find_pdf_files_recursively(directory: Path) -> List[Path]:
    """Рекурсивно находит все PDF-файлы и возвращает список."""
    if not directory.is_dir():
        print(f"Ошибка: Директория '{directory}' не найдена.")
        return []
    print(f"Рекурсивный поиск PDF файлов в '{directory}'...")
    return list(directory.rglob("*.pdf"))

# --- ОСНОВНАЯ ЛОГИКА ---
# Эта часть будет выполняться только в главном процессе
def run_main_process():
    script_dir = Path().resolve()
    input_path = script_dir / INPUT_DIR

    pdf_files = find_pdf_files_recursively(input_path)
    total_files = len(pdf_files)

    if not pdf_files:
        print("PDF файлы не найдены. Завершение работы.")
        return

    print(f"Найдено {total_files} PDF файлов.")
    # Используем os.cpu_count() для определения количества процессов
    num_processes = os.cpu_count()
    print(f"Запускаю обработку и классификацию в {num_processes} параллельных процессах...")

    output_file_path = script_dir / OUTPUT_CSV
    with open(output_file_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["full_path", "document_type", "header_text"])

        # ИЗМЕНЕНИЕ: Используем pool.map, который более стабилен в Windows
        with multiprocessing.Pool(processes=num_processes) as pool:
            # tqdm здесь оборачивает pool.imap, чтобы отслеживать прогресс
            # imap является хорошим компромиссом между map и imap_unordered
            results = list(tqdm(pool.imap(process_file, pdf_files, chunksize=10), total=total_files, desc="Классификация PDF"))
        
        # Записываем результаты в файл после завершения всех процессов
        print("Запись результатов в CSV...")
        for pdf_path, doc_type, header in results:
            writer.writerow([str(pdf_path), doc_type, header])

    print("\nГотово!")
    print(f"Результаты сохранены в файл: {output_file_path}")

# --- ТОЧКА ВХОДА ---
if __name__ == "__main__":
    # Эта строка КРИТИЧЕСКИ важна для Windows, чтобы избежать бесконечного
    # создания дочерних процессов.
    multiprocessing.freeze_support()
    run_main_process()