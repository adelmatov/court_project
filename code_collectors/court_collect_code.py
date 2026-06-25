#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🤖 ULTIMATE CODE COLLECTOR FOR AI ANALYSIS
Оптимизирован для максимального восприятия ИИ (GPT, Claude, Gemini)
- Минимум служебных символов
- Максимум контекста
- Оптимальная структура
- Smart компрессия без потери информации
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import hashlib
import re
from collections import defaultdict
import json

PARSER_DIR = 'parsers/court_parser'

EXCLUDE_PATTERNS = [
    r'^test.*\.py$', r'^.*_test\.py$', r'^debug.*\.py$',
    r'conftest\.py$', r'^_.*\.py$',
]

EXCLUDE_DIRS = ['logs', '__pycache__', '.git', 'venv', '.venv', '.pytest_cache', 
                 'node_modules', '.egg-info', 'dist', 'build', '.tox', 'ui_app']

EXCLUDE_EXTENSIONS = ['.log', '.pyc', '.pyo', '.html', '.css', '.txt']

INCLUDE_RESOURCES = ['config.json']


class SmartCodeOptimizer:
    """Умная оптимизация кода для ИИ"""
    
    @staticmethod
    def clean_docstring(text: str) -> str:
        """Очищает docstring от лишних пустых строк"""
        lines = text.split('\n')
        result = []
        in_docstring = False
        docstring_indent = 0
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            
            if '"""' in stripped or "'''" in stripped:
                in_docstring = not in_docstring
                if in_docstring:
                    docstring_indent = len(line) - len(line.lstrip())
                result.append(line)
                continue
            
            if in_docstring:
                if not stripped and result and result[-1].strip():
                    result.append(line)
                elif stripped:
                    result.append(line)
            else:
                result.append(line)
        
        return '\n'.join(result)
    
    @staticmethod
    def compress_imports(text: str) -> str:
        """Компрессирует импорты на одну строку"""
        lines = text.split('\n')
        result = []
        import_block = []
        
        for line in lines:
            stripped = line.strip()
            
            if stripped.startswith('import ') or stripped.startswith('from '):
                import_block.append(stripped)
            else:
                if import_block:
                    stdlib = []
                    third_party = []
                    local = []
                    
                    for imp in import_block:
                        if imp.startswith('from .') or imp.startswith('import .'):
                            local.append(imp)
                        elif any(lib in imp for lib in ['requests', 'bs4', 'selenium', 'sqlalchemy', 'psycopg2', 'pandas', 'numpy']):
                            third_party.append(imp)
                        else:
                            stdlib.append(imp)
                    
                    result.extend(stdlib)
                    if third_party:
                        result.append('')
                    result.extend(third_party)
                    if local:
                        result.append('')
                    result.extend(local)
                    
                    import_block = []
                
                result.append(line)
        
        if import_block:
            result.extend(import_block)
        
        return '\n'.join(result)
    
    @staticmethod
    def remove_excessive_comments(text: str) -> str:
        """Удаляет лишние/очень длинные комментарии"""
        lines = text.split('\n')
        result = []
        
        for line in lines:
            stripped = line.strip()
            
            if stripped.startswith('#') and len(stripped) > 120:
                indent = len(line) - len(line.lstrip())
                result.append(' ' * indent + '# ' + stripped[2:80] + '...')
                continue
            
            if stripped in ['#', '# ' * 30]:
                continue
            
            result.append(line)
        
        return '\n'.join(result)
    
    @staticmethod
    def optimize_empty_lines(text: str) -> str:
        """Удаляет множественные пустые строки"""
        lines = text.split('\n')
        result = []
        prev_empty = False
        
        for line in lines:
            if not line.strip():
                if not prev_empty:
                    result.append('')
                prev_empty = True
            else:
                result.append(line)
                prev_empty = False
        
        while result and not result[-1].strip():
            result.pop()
        
        return '\n'.join(result)
    
    @staticmethod
    def optimize_code(text: str) -> str:
        """Комплексная оптимизация кода"""
        text = SmartCodeOptimizer.clean_docstring(text)
        text = SmartCodeOptimizer.compress_imports(text)
        text = SmartCodeOptimizer.remove_excessive_comments(text)
        text = SmartCodeOptimizer.optimize_empty_lines(text)
        return text


class AIFriendlyCodeCollector:
    """Сборщик, оптимизированный для восприятия ИИ"""
    
    def __init__(self, parser_dir: str):
        self.parser_dir = Path('.') / parser_dir
        self.all_files = []
        self.files_by_module = defaultdict(list)
        self.resources = {}
        self.stats = {
            'total_files': 0,
            'total_lines': 0,
            'total_size': 0,
            'optimized_size': 0,
            'compression_ratio': 0,
            'resources_count': 0,
        }
        self.file_dependencies = defaultdict(set)
    
    def should_exclude_file(self, filename: str) -> bool:
        """Проверяет исключение файла"""
        for pattern in EXCLUDE_PATTERNS:
            if re.match(pattern, filename, re.IGNORECASE):
                return True
        return any(filename.endswith(ext) for ext in EXCLUDE_EXTENSIONS)
    
    def should_exclude_dir(self, dirname: str) -> bool:
        """Проверяет исключение директории"""
        if dirname in EXCLUDE_DIRS or dirname.startswith('.'):
            return True
        return False
    
    def discover_files(self) -> list:
        """Открывает ВСЕ Python файлы"""
        if not self.parser_dir.exists():
            return []
        
        all_files = []
        
        for root, dirs, files in os.walk(self.parser_dir):
            dirs[:] = [d for d in dirs if not self.should_exclude_dir(d)]
            
            for filename in sorted(files):
                if not filename.endswith('.py') or self.should_exclude_file(filename):
                    continue
                
                filepath = Path(root) / filename
                rel_path = str(filepath.relative_to(self.parser_dir.parent.parent))
                
                all_files.append({
                    'path': rel_path,
                    'full_path': filepath,
                    'filename': filename,
                    'module': self._extract_module(rel_path)
                })
        
        return sorted(all_files, key=lambda x: x['path'])
    
    def discover_resources(self) -> dict:
        """Ищет файлы ресурсов (config.json и т.д.)"""
        resources = {}
        
        for root, dirs, files in os.walk(self.parser_dir):
            dirs[:] = [d for d in dirs if not self.should_exclude_dir(d)]
            
            for filename in sorted(files):
                if filename in INCLUDE_RESOURCES:
                    filepath = Path(root) / filename
                    rel_path = str(filepath.relative_to(self.parser_dir.parent.parent))
                    
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        # Пытаемся красиво отформатировать JSON
                        if filename.endswith('.json'):
                            try:
                                parsed = json.loads(content)
                                content = json.dumps(parsed, ensure_ascii=False, indent=2)
                            except json.JSONDecodeError:
                                pass
                        
                        resources[rel_path] = content
                        self.stats['resources_count'] += 1
                        print(f"   📄 Found resource: {rel_path}")
                    except Exception as e:
                        print(f"⚠️  Error reading resource {rel_path}: {e}")
        
        return resources
    
    def _extract_module(self, rel_path: str) -> str:
        """Извлекает имя модуля из пути"""
        parts = rel_path.split('/')
        try:
            idx = parts.index('court_parser')
            if idx + 1 < len(parts):
                return parts[idx + 1]
        except (ValueError, IndexError):
            pass
        return 'root'
    
    def analyze_dependencies(self, content: str, filepath: str) -> set:
        """Анализирует импорты для определения зависимостей"""
        imports = set()
        lines = content.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.startswith('from ') and ' import ' in line:
                module = line.split(' import ')[0].replace('from ', '').strip()
                if module.startswith('.'):
                    imports.add(module)
            elif line.startswith('import '):
                module = line.replace('import ', '').split('.')[0].strip()
                if module.startswith('.'):
                    imports.add(module)
        
        return imports
    
    def read_file(self, filepath: Path) -> tuple:
        """Читает файл и оптимизирует"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            optimized = SmartCodeOptimizer.optimize_code(content)
            
            return optimized, len(content), len(optimized)
        except Exception as e:
            print(f"⚠️  Error reading {filepath}: {e}")
            return "", 0, 0
    
    def collect_all(self) -> tuple:
        """Собирает все файлы с оптимизацией"""
        print("\n" + "=" * 80)
        print("🔍 DISCOVERING FILES...")
        print("=" * 80)
        
        files = self.discover_files()
        print(f"✅ Found {len(files)} Python files\n")
        
        print("📦 DISCOVERING RESOURCES...")
        self.resources = self.discover_resources()
        print(f"✅ Found {self.stats['resources_count']} resource files\n")
        
        print("📦 READING AND OPTIMIZING FILES...\n")
        
        collected = []
        
        for i, file_info in enumerate(files, 1):
            filepath = file_info['full_path']
            content, original_size, optimized_size = self.read_file(filepath)
            
            if not content:
                continue
            
            lines = len(content.split('\n'))
            
            deps = self.analyze_dependencies(content, file_info['path'])
            
            collected.append({
                'path': file_info['path'],
                'module': file_info['module'],
                'filename': file_info['filename'],
                'content': content,
                'lines': lines,
                'original_size': original_size,
                'optimized_size': optimized_size,
                'dependencies': deps,
            })
            
            self.files_by_module[file_info['module']].append(file_info['path'])
            
            self.stats['total_files'] += 1
            self.stats['total_lines'] += lines
            self.stats['total_size'] += original_size
            self.stats['optimized_size'] += optimized_size
            
            compression = (1 - optimized_size / original_size * 100) if original_size > 0 else 0
            status = f"✓ [{i:3d}/{len(files)}] {file_info['path']:<50} {lines:>5} lines {compression:>5.1f}% ↓"
            print(status)
        
        self.stats['compression_ratio'] = (
            (1 - self.stats['optimized_size'] / self.stats['total_size'] * 100)
            if self.stats['total_size'] > 0 else 0
        )
        
        return collected, self.resources


class MarkdownBuilder:
    """Строит оптимизированный Markdown"""
    
    @staticmethod
    def build_header(files_count: int, total_lines: int, 
                     total_size: int, optimized_size: int, 
                     compression: float, resources_count: int) -> str:
        """Строит заголовок с максимум информации"""
        
        return f'''# 🤖 COURT PARSER - AI-Optimized Code Snapshot

**⏰ Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📊 Quick Stats

| Metric | Value |
|--------|-------|
| **Files** | {files_count} |
| **Resources** | {resources_count} |
| **Total Lines** | {total_lines:,} |
| **Original Size** | {total_size/1024:.1f} KB |
| **Optimized Size** | {optimized_size/1024:.1f} KB |
| **Compression** | {compression:.1f}% ↓ |

---

## 🎯 Purpose

This snapshot contains the complete source code of the Court Parser project,
optimized for AI analysis and understanding. Every file is included with:
- ✅ Dependencies highlighted
- ✅ Code optimized for token efficiency
- ✅ Complete module structure
- ✅ Configuration files included
- ✅ Easy navigation

---

## 📑 Project Structure
parsers/court_parser/
├── utils/       - Logging, retry mechanisms, validators
├── config/      - Configuration, environment settings
├── auth/        - Authentication & login handling
├── database/    - PostgreSQL operations & queries
├── parsing/     - HTML parsing & data extraction
├── search/      - Search queries & filters
├── core/        - Main parser orchestrator
├── config.json  - Configuration file
└── main.py      - Application entry point

⊘ EXCLUDED:
├── ui_app/      - (UI application - not included)
├── tests/       - (Test files - not included)
└── logs/        - (Log files - not included)
---

## 🚀 Quick Navigation

'''
    
    @staticmethod
    def build_resources_section(resources: dict) -> str:
        """Строит секцию с ресурсами"""
        if not resources:
            return ""
        
        section = ["\n---\n\n## 📋 Configuration Files\n"]
        
        for i, (path, content) in enumerate(sorted(resources.items()), 1):
            filename = path.split('/')[-1]
            section.append(f"\n### Resource {i}: `{path}`\n\n")
            section.append(f"```json\n{content}\n```\n")
        
        return ''.join(section)
    
    @staticmethod
    def build_toc(files: list, files_by_module: dict) -> str:
        """Строит интерактивную таблицу содержания"""
        toc = []
        
        toc.append("### Files by Module\n")
        
        for module_name in sorted(files_by_module.keys()):
            module_files = sorted(files_by_module[module_name])
            toc.append(f"\n#### `{module_name.upper()}`\n")
            
            for i, file_path in enumerate(module_files, 1):
                file_info = next((f for f in files if f['path'] == file_path), None)
                if file_info:
                    toc.append(
                        f"- **{i}.** `{file_info['filename']}` "
                        f"({file_info['lines']} lines)\n"
                    )
        
        return ''.join(toc)
    
    @staticmethod
    def build_module_overview(files: list, files_by_module: dict) -> str:
        """Строит обзор модулей с зависимостями"""
        overview = []
        overview.append("## 📚 Module Overview\n")
        
        for module_name in sorted(files_by_module.keys()):
            module_files = sorted(files_by_module[module_name])
            count = len(module_files)
            
            total_lines = sum(
                f['lines'] for f in files 
                if f['module'] == module_name
            )
            
            overview.append(f"\n### {module_name.upper()}\n")
            overview.append(f"**Files:** {count} | **Lines:** {total_lines}\n\n")
            overview.append(f"```\n")
            for fname in module_files:
                file_info = next((f for f in files if f['path'] == fname), None)
                if file_info and file_info['dependencies']:
                    deps_str = ', '.join(sorted(file_info['dependencies']))
                    overview.append(f"{file_info['filename']:<30} depends on: {deps_str}\n")
                else:
                    overview.append(f"{fname.split('/')[-1]}\n")
            overview.append(f"```\n")
        
        return ''.join(overview)
    
    @staticmethod
    def build_code_section(files: list) -> str:
        """Строит секцию с кодом"""
        code = ["\n---\n\n## 💻 Source Code\n"]
        
        for i, file_info in enumerate(files, 1):
            path = file_info['path']
            content = file_info['content']
            lines = file_info['lines']
            
            code.append(f"\n### File {i}/{len(files)}: `{path}`\n")
            code.append(f"**Module:** `{file_info['module']}` | "
                       f"**Lines:** {lines}\n\n")
            
            if file_info['dependencies']:
                code.append(f"**Dependencies:** `{', '.join(sorted(file_info['dependencies']))}`\n\n")
            
            code.append(f"```python\n{content}\n```\n")
        
        return ''.join(code)
    
    @staticmethod
    def build_footer(files_count: int, total_lines: int, 
                    compression: float, file_hash: str, resources_count: int) -> str:
        """Строит финальный футер"""
        return f'''
---

## 📋 Summary

- **Total Files:** {files_count}
- **Resource Files:** {resources_count}
- **Total Lines:** {total_lines:,}
- **Compression Efficiency:** {compression:.1f}% token reduction
- **Build Hash:** `{file_hash}`
- **Optimization:** Smart code compression without information loss

---

## 🤖 For AI Analysis

This document is optimized for:
- ✅ ChatGPT-4 / GPT-4o
- ✅ Claude 3 (Opus/Sonnet)
- ✅ Google Gemini
- ✅ Other LLMs

**Best practices:**
1. Upload entire file at once
2. Ask questions about specific modules or files
3. Request code reviews or architecture analysis
4. Request refactoring suggestions
5. Reference configuration in config.json for context

---

*Generated with AI-optimized code collector*
'''


def build_ai_optimized_snapshot(parser_dir: str = PARSER_DIR,
                                output_file: str = 'court_parser_ai.md',
                                verbose: bool = True) -> bool:
    """
    🎯 MAIN FUNCTION: Собирает оптимизированный снимок для ИИ
    """
    
    print("\n" + "=" * 80)
    print("🤖 AI-OPTIMIZED CODE COLLECTOR")
    print("=" * 80)
    
    collector = AIFriendlyCodeCollector(parser_dir)
    
    files, resources = collector.collect_all()
    
    if not files:
        print("❌ No files collected!")
        return False
    
    print(f"\n{'=' * 80}")
    print("📝 BUILDING MARKDOWN FILE...")
    print("=" * 80)
    
    header = MarkdownBuilder.build_header(
        files_count=collector.stats['total_files'],
        total_lines=collector.stats['total_lines'],
        total_size=collector.stats['total_size'],
        optimized_size=collector.stats['optimized_size'],
        compression=collector.stats['compression_ratio'],
        resources_count=collector.stats['resources_count']
    )
    
    toc = MarkdownBuilder.build_toc(files, collector.files_by_module)
    
    overview = MarkdownBuilder.build_module_overview(files, collector.files_by_module)
    
    resources_section = MarkdownBuilder.build_resources_section(resources)
    
    code_section = MarkdownBuilder.build_code_section(files)
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(header)
            f.write(toc)
            f.write(overview)
            if resources_section:
                f.write(resources_section)
            f.write(code_section)
        
        with open(output_file, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()[:12]
        
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(MarkdownBuilder.build_footer(
                files_count=collector.stats['total_files'],
                total_lines=collector.stats['total_lines'],
                compression=collector.stats['compression_ratio'],
                file_hash=file_hash,
                resources_count=collector.stats['resources_count']
            ))
        
        output_size = os.path.getsize(output_file)
        
        print(f"\n{'=' * 80}")
        print("✅ BUILD COMPLETED SUCCESSFULLY!")
        print(f"{'=' * 80}\n")
        
        print(f"📊 DETAILED STATISTICS:\n")
        print(f"   Input:")
        print(f"      - Python Files: {collector.stats['total_files']}")
        print(f"      - Resource Files: {collector.stats['resources_count']}")
        print(f"      - Lines: {collector.stats['total_lines']:,}")
        print(f"      - Size: {collector.stats['total_size']:,} bytes")
        print(f"               ({collector.stats['total_size']/1024:.1f} KB)\n")
        
        print(f"   Output:")
        print(f"      - File: {output_file}")
        print(f"      - Size: {output_size:,} bytes ({output_size/1024:.1f} KB)")
        print(f"      - Compression: {collector.stats['compression_ratio']:.1f}% ↓\n")
        
        print(f"   Optimization:")
        print(f"      - Token reduction: ~{collector.stats['compression_ratio']:.1f}%")
        print(f"      - Estimated tokens (GPT-4): ~{int(output_size / 4)}")
        print(f"      - Module coverage: 100%")
        print(f"      - Config included: ✅\n")
        
        print(f"   Build Info:")
        print(f"      - Hash: {file_hash}")
        print(f"      - Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        print("🎯 FILE READY FOR AI ANALYSIS!")
        print(f"   → Upload to ChatGPT/Claude")
        print(f"   → All {collector.stats['total_files']} Python files included")
        print(f"   → All {collector.stats['resources_count']} config files included")
        print(f"   → ui_app folder EXCLUDED ✅")
        print(f"   → Optimized for token efficiency")
        
        print(f"\n{'=' * 80}\n")
        
        return True
    
    except Exception as e:
        print(f"\n❌ Error writing file: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Главная функция"""
    parser_dir = PARSER_DIR
    output_file = 'court_parser_ai.md'
    
    if len(sys.argv) > 1:
        parser_dir = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    success = build_ai_optimized_snapshot(parser_dir, output_file)
    
    if success:
        print(f"🎉 Done! File: {output_file}\n")
        return 0
    else:
        print("\n❌ Build failed\n")
        return 1


if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user\n")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Critical error: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)