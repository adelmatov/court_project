"""
pdf_to_txt.py — конвертирует все PDF из папки ./declarations в .txt-файлы в ./txt.

Зависимости:
    pip install pdfplumber

Запуск:
    python pdf_to_txt.py            # обычный текст
    python pdf_to_txt.py --coords   # с координатами слов (для отладки)
"""

import os
import sys
import glob
import pdfplumber


def pdf_to_text(pdf_path: str, out_path: str, with_coords: bool = False) -> None:
    with pdfplumber.open(pdf_path) as pdf, open(out_path, "w", encoding="utf-8") as f:
        for pno, page in enumerate(pdf.pages, start=1):
            f.write(f"\n===== PAGE {pno} / {len(pdf.pages)} =====\n")
            if with_coords:
                f.write(f"{'x0':>8} {'top':>8} {'x1':>8} {'bot':>8}  text\n")
                f.write("-" * 60 + "\n")
                words = page.extract_words(x_tolerance=1.5, y_tolerance=2)
                words.sort(key=lambda w: (round(w["top"], 1), w["x0"]))
                for w in words:
                    f.write(f"{w['x0']:8.2f} {w['top']:8.2f} "
                            f"{w['x1']:8.2f} {w['bottom']:8.2f}  {w['text']}\n")
            else:
                f.write((page.extract_text(x_tolerance=1.5, y_tolerance=2) or "") + "\n")


def main():
    with_coords = "--coords" in sys.argv
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pdf_dir = os.path.join(script_dir, "declarations")
    out_dir = os.path.join(script_dir, "txt")
    os.makedirs(out_dir, exist_ok=True)

    pdf_files = sorted(glob.glob(os.path.join(pdf_dir, "*.pdf")))
    if not pdf_files:
        print(f"[!] В {pdf_dir} нет PDF-файлов.")
        return

    print(f"[i] Файлов: {len(pdf_files)}; режим: {'координаты' if with_coords else 'текст'}")
    for pdf in pdf_files:
        base = os.path.splitext(os.path.basename(pdf))[0]
        out  = os.path.join(out_dir, base + ".txt")
        try:
            pdf_to_text(pdf, out, with_coords)
            print(f"  ✓ {os.path.basename(pdf)} → txt/{base}.txt")
        except Exception as e:
            print(f"  [!] {os.path.basename(pdf)}: {e}")
    print("[✓] Готово.")


if __name__ == "__main__":
    main()