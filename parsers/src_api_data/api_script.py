import requests
import json
import pandas as pd
from pathlib import Path
import time
import os

# Получаем директорию, где лежит скрипт
SCRIPT_DIR = Path(__file__).parent.resolve()

# Настройки
TOKEN = '4dfbf650-ef3b-412a-8f4c-5dfc33bcea89'
API_URL = 'https://portal.kgd.gov.kz/services/isnaportalsync/public/taxpayer-data'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    'X-Portal-Token': TOKEN
}

DEFAULT_TAXPAYER_TYPE = 'UL'
REQUEST_DELAY = 0


def get_tax_2025(taxpayer_code: str, taxpayer_type: str = DEFAULT_TAXPAYER_TYPE) -> float | None:
    """
    Получает сумму налогов firstType за 2025 год для указанного БИН
    """
    params = {
        'taxpayerCode': str(taxpayer_code).strip(),
        'taxpayerType': taxpayer_type,
    }
    
    try:
        response = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            answer_dto = data.get('answerDto')
            if answer_dto:
                first_type = answer_dto.get('firstType')
                if first_type:
                    return first_type.get('2025')
            
            return None
        else:
            print(f"❌ HTTP {response.status_code}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"❌ Таймаут")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка: {e}")
        return None
    except json.JSONDecodeError:
        print(f"❌ Ошибка JSON")
        return None


import signal
import sys

# Глобальная переменная для хранения данных
current_df = None
current_file = None


def save_and_exit(signum=None, frame=None):
    """Сохраняет данные при экстренном прерывании"""
    global current_df, current_file
    
    if current_df is not None and current_file is not None:
        print(f"\n\n⚠️ Прерывание! Сохраняю данные...")
        current_df.to_excel(current_file, index=False)
        success_count = current_df['Tax 2025'].notna().sum()
        print(f"💾 Сохранено в: {current_file.name}")
        print(f"   Записей с данными: {success_count}")
    
    sys.exit(0)


# Регистрируем обработчики сигналов
signal.signal(signal.SIGINT, save_and_exit)   # Ctrl+C
signal.signal(signal.SIGTERM, save_and_exit)  # Завершение процесса


def process_excel(input_filename: str, output_filename: str = None):
    """
    Обрабатывает Excel файл с БИН и добавляет колонку Tax 2025
    При повторном запуске пропускает уже заполненные строки
    Сохраняет данные при прерывании
    """
    global current_df, current_file
    
    # Строим полный путь относительно директории скрипта
    input_file = SCRIPT_DIR / input_filename
    current_file = input_file
    
    print(f"📁 Директория скрипта: {SCRIPT_DIR}")
    print(f"📄 Ищем файл: {input_file}")
    
    if not input_file.exists():
        print(f"\n❌ Файл не найден: {input_file}")
        return
    
    print(f"\n📖 Чтение файла: {input_file.name}")
    df = pd.read_excel(input_file)
    current_df = df  # Сохраняем ссылку для экстренного сохранения
    
    bin_column = df.columns[0]
    print(f"   Колонка БИН: '{bin_column}'")
    print(f"   Всего записей: {len(df)}")
    
    # Создаём колонку если её нет
    if 'Tax 2025' not in df.columns:
        df['Tax 2025'] = None
    
    # Считаем сколько уже заполнено
    already_filled = df['Tax 2025'].notna().sum()
    to_process = df['Tax 2025'].isna().sum()
    
    print(f"   ✅ Уже заполнено: {already_filled}")
    print(f"   🔄 Осталось обработать: {to_process}")
    print(f"\n   💡 Для прерывания нажмите Ctrl+C (данные сохранятся)")
    
    if to_process == 0:
        print("\n✅ Все записи уже обработаны!")
        return
    
    print("\n🔄 Обработка пустых записей...\n")
    
    processed = 0
    try:
        for index, row in df.iterrows():
            # Пропускаем если Tax 2025 уже заполнен
            if pd.notna(row.get('Tax 2025')):
                continue
            
            bin_code = row[bin_column]
            
            if pd.isna(bin_code):
                print(f"  ⏭️ Строка {index + 2}: пропущена (пустой БИН)")
                continue
            
            bin_code = str(int(bin_code)) if isinstance(bin_code, float) else str(bin_code)
            bin_code = bin_code.zfill(12)
            
            processed += 1
            print(f"  [{processed}/{to_process}] БИН: {bin_code} ", end="")
            
            tax_2025 = get_tax_2025(bin_code)
            
            if tax_2025 is not None:
                df.at[index, 'Tax 2025'] = tax_2025
                print(f"→ {tax_2025:,.2f} ₸")
            else:
                print("→ Нет данных")
            
            # Сохраняем каждые 10 записей
            if processed % 10 == 0:
                df.to_excel(input_file, index=False)
                print(f"  💾 Автосохранение ({processed} записей)")
            
            time.sleep(REQUEST_DELAY)
    
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        print("💾 Сохраняю данные...")
        df.to_excel(input_file, index=False)
        raise
    
    # Финальное сохранение
    df.to_excel(input_file, index=False)
    
    success_count = df['Tax 2025'].notna().sum()
    print(f"\n{'='*50}")
    print(f"✅ Результат сохранён: {input_file.name}")
    print(f"\n📊 Статистика:")
    print(f"   Всего: {len(df)}")
    print(f"   С данными: {success_count}")
    print(f"   Без данных: {len(df) - success_count}")
    
    # Очищаем глобальные переменные
    current_df = None
    current_file = None


if __name__ == "__main__":
    # Укажите ТОЛЬКО имя файла (без пути)
    INPUT_FILE = "bin_list_2026_1st_half.xlsx"
    
    process_excel(INPUT_FILE)