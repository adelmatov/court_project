"""
Collect all Python files from company_info parser
"""

from pathlib import Path
from datetime import datetime

source_dir = Path("parsers/company_info")
output_file = "company_info_full_code.txt"

with open(output_file, "w", encoding="utf-8") as out:
    # Header
    out.write("=" * 80 + "\n")
    out.write("COMPANY_INFO PARSER - COMPLETE CODE\n")
    out.write("=" * 80 + "\n")
    out.write(f"Project: court_project\n")
    out.write(f"Module: parsers/company_info\n")
    out.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    out.write("=" * 80 + "\n\n")
    
    # Collect all .py files
    py_files = sorted(source_dir.rglob("*.py"))
    
    for py_file in py_files:
        relative_path = py_file.relative_to(source_dir)
        
        out.write("\n" + "=" * 80 + "\n")
        out.write(f"FILE: {relative_path}\n")
        out.write("=" * 80 + "\n\n")
        
        with open(py_file, "r", encoding="utf-8") as f:
            out.write(f.read())
        
        out.write("\n")
    
    # Footer
    out.write("\n" + "=" * 80 + "\n")
    out.write("END OF CODE\n")
    out.write("=" * 80 + "\n")

print(f"✅ Файл создан: {output_file}")
print(f"📄 Собрано файлов: {len(py_files)}")