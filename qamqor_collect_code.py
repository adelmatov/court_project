"""
Collect all Python files from qamqor parser
"""

from pathlib import Path
from datetime import datetime

source_dir = Path("parsers/qamqor")
output_file = "qamqor_full_code.txt"

with open(output_file, "w", encoding="utf-8") as out:
    # Header
    out.write("=" * 80 + "\n")
    out.write("QAMQOR PARSER - COMPLETE CODE\n")
    out.write("=" * 80 + "\n")
    out.write(f"Project: court_project\n")
    out.write(f"Module: parsers/qamqor\n")
    out.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    out.write("=" * 80 + "\n\n")
    
    # Structure overview
    out.write("STRUCTURE:\n")
    out.write("parsers/qamqor/\n")
    out.write("├── qamqor_parser.py\n")
    out.write("├── qamqor_updater.py\n")
    out.write("└── core/\n")
    out.write("    ├── __init__.py\n")
    out.write("    ├── api_validator.py\n")
    out.write("    ├── config.py\n")
    out.write("    ├── database.py\n")
    out.write("    ├── data_processor.py\n")
    out.write("    ├── log_manager.py\n")
    out.write("    ├── tab_manager.py\n")
    out.write("    └── web_client.py\n")
    out.write("=" * 80 + "\n\n")
    
    # Collect all .py files (excluding __pycache__)
    py_files = sorted([f for f in source_dir.rglob("*.py") if "__pycache__" not in str(f)])
    
    for py_file in py_files:
        relative_path = py_file.relative_to(source_dir)
        
        out.write("\n" + "=" * 80 + "\n")
        out.write(f"FILE: {relative_path}\n")
        out.write("=" * 80 + "\n\n")
        
        try:
            with open(py_file, "r", encoding="utf-8") as f:
                out.write(f.read())
        except Exception as e:
            out.write(f"ERROR reading file: {e}\n")
        
        out.write("\n")
    
    # Footer
    out.write("\n" + "=" * 80 + "\n")
    out.write("END OF CODE\n")
    out.write("=" * 80 + "\n")

print(f"✅ Файл создан: {output_file}")
print(f"📄 Собрано файлов: {len(py_files)}")
print("\nСписок файлов:")
for i, f in enumerate(py_files, 1):
    print(f"  {i}. {f.relative_to(source_dir)}")