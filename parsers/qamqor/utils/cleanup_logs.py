"""Очистка старых лог-файлов."""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path


def cleanup_logs(logs_dir: str = "logs", max_age_days: int = 3) -> None:
    """
    Удаление лог-файлов старше указанного количества дней.
    
    Args:
        logs_dir: Путь к директории с логами
        max_age_days: Максимальный возраст файлов в днях
    """
    logs_path = Path(logs_dir)
    
    if not logs_path.exists():
        print(f"[CLEANUP] Директория {logs_dir} не существует")
        return
    
    cutoff_time = datetime.now() - timedelta(days=max_age_days)
    deleted_count = 0
    total_size = 0
    
    print(f"[CLEANUP] Очистка логов старше {max_age_days} дней...")
    
    for log_file in logs_path.glob("*.log"):
        try:
            file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
            
            if file_mtime < cutoff_time:
                file_size = log_file.stat().st_size
                log_file.unlink()
                deleted_count += 1
                total_size += file_size
                print(f"[CLEANUP] Удалён: {log_file.name}")
                
        except Exception as e:
            print(f"[CLEANUP] Ошибка удаления {log_file.name}: {e}")
    
    if deleted_count > 0:
        size_mb = total_size / (1024 * 1024)
        print(f"[CLEANUP] Удалено файлов: {deleted_count} ({size_mb:.2f} MB)")
    else:
        print(f"[CLEANUP] Старых логов не найдено")


if __name__ == "__main__":
    # Параметры из командной строки или значения по умолчанию
    logs_dir = sys.argv[1] if len(sys.argv) > 1 else "logs"
    max_age = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    
    cleanup_logs(logs_dir, max_age)