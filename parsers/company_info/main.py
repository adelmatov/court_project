"""
Company Parser - Main entry point
"""

# ==================== ИСПРАВЛЕНИЕ RuntimeWarning ====================
import sys
import os

# 1. Установить UTF-8 для Windows (безопасная версия)
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"
    
    try:
        import codecs
        # Проверить, не обёрнут ли уже stdout
        if hasattr(sys.stdout, 'buffer') and not isinstance(sys.stdout, codecs.StreamWriter):
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'replace')
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'replace')
        elif hasattr(sys.stdout, 'reconfigure'):
            # Python 3.7+ с поддержкой reconfigure
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
    except (AttributeError, OSError):
        # Если ничего не сработало, просто переменная окружения
        pass

# 2. Очистить кеш модуля при запуске через python -m
if __name__ == "__main__":
    keys_to_remove = [k for k in sys.modules.keys() if 'company_info.main' in k]
    for key in keys_to_remove:
        sys.modules.pop(key, None)
# ====================================================================

import sys
import argparse
import logging
from pathlib import Path
from typing import List, Optional
from tqdm import tqdm

from .core.logger import logger, setup_logger
from .core.api_client import APIClient, APIError, CompanyNotFoundError
from .core.data_processor import DataProcessor
from .core.database import DatabaseManager
from .core.change_detector import ChangeDetector
from .sources.registry import registry
from .utils.validators import validate_bin, normalize_bin


class CompanyParser:
    """
    Main parser class for company data.
    """
    
    def __init__(
        self,
        verify: bool = False,
        dry_run: bool = False
    ):
        """
        Initialize parser.
        
        Args:
            verify: Enable BIN verification via search API
            dry_run: Run without saving to database
        """
        self.api_client = APIClient()
        self.db = DatabaseManager()
        self.data_processor = DataProcessor()
        self.change_detector = ChangeDetector()
        
        self.verify = verify
        self.dry_run = dry_run
        
        # Statistics
        self.stats = {
            'total': 0,
            'created': 0,
            'updated': 0,
            'no_changes': 0,
            'not_found': 0,
            'errors': 0
        }
    
    def parse_bins(
        self,
        bins: List[str],
        force: bool = False
    ):
        """
        Parse list of BINs.
        
        Args:
            bins: List of BINs to parse
            force: Force re-parse even if exists
        """
        
        if not bins:
            logger.warning("No BINs to parse")
            return
        
        # Validate and normalize
        bins = [normalize_bin(b) for b in bins if validate_bin(b)]
        
        if not bins:
            logger.error("No valid BINs found")
            return
        
        logger.info(f"Total BINs to process: {len(bins)}")
        
        # Filter existing (if not force)
        if not force and not self.dry_run:
            existing = self.db.get_existing_bins(bins)
            bins = [b for b in bins if b not in existing]
            logger.info(
                f"Filtered {len(existing)} existing BINs "
                f"({len(bins)} remaining)"
            )
        
        if not bins:
            logger.info("All BINs already exist in database")
            return
        
        # Process BINs
        self.stats['total'] = len(bins)
        
        # ✅ Зелёный прогресс-бар с ETA
        try:
            from tqdm import tqdm
            
            # ✅ Временно отключить console handler
            console_handler = None
            for handler in logger.handlers:
                if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
                    console_handler = handler
                    logger.removeHandler(handler)
                    break
            
            progress_bar = tqdm(
                bins,
                desc="🚀 Processing",
                unit=" BIN",
                colour="green",
                bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [⏱️ {elapsed} < {remaining}]'
            )
            use_progress = True
        except ImportError:
            logger.warning("tqdm not installed, progress bar disabled")
            progress_bar = bins
            use_progress = False
            console_handler = None
        
        for i, bin_value in enumerate(progress_bar, 1):
            try:
                self._process_bin(bin_value)
                
            except KeyboardInterrupt:
                logger.warning("Interrupted by user")
                if use_progress:
                    progress_bar.close()
                break
            
            except Exception as e:
                self.stats['errors'] += 1
                logger.exception(f"Unexpected error for {bin_value}: {e}")
        
        # ✅ Закрыть прогресс-бар
        if use_progress:
            progress_bar.close()
            
            # ✅ Вернуть console handler
            if console_handler:
                logger.addHandler(console_handler)
        
        # Print summary
        self._print_summary()
    
    def _process_bin(self, bin_value: str):
        """
        Process single BIN.
        
        Args:
            bin_value: BIN to process
        """
        
        # 1. Verify existence (if enabled)
        if self.verify:
            try:
                exists = self.api_client.check_company_exists(bin_value)
                
                if not exists:
                    self.stats['not_found'] += 1
                    if not self.dry_run:
                        self.db.mark_not_found(bin_value)
                    return
                
            except APIError as e:
                logger.error(f"Verification failed for {bin_value}: {e}")
                self.stats['errors'] += 1
                return
        
        # 2. Get full company info
        try:
            response = self.api_client.get_company_info(bin_value)
            
        except CompanyNotFoundError:
            self.stats['not_found'] += 1
            if not self.dry_run:
                self.db.mark_not_found(bin_value)
            return
        
        except APIError as e:
            logger.error(f"Failed to fetch info for {bin_value}: {e}")
            self.stats['errors'] += 1
            return
        
        # 3. Parse JSON
        try:
            parsed = self.data_processor.parse_company(response)
            
            # ✅ Проверка на deleted компанию
            if parsed and parsed.get('is_deleted'):
                self.stats['not_found'] += 1
                if not self.dry_run:
                    self.db.mark_deleted(
                        bin_value,
                        parsed.get('registration_date')
                    )
                return
            
        except Exception as e:
            logger.error(f"Failed to parse data for {bin_value}: {e}")
            self.stats['errors'] += 1
            return
        
        # 4. Dry-run mode
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would process: {parsed['name_ru']}")
            return
        
        # 5. Check if exists
        existing = self.db.get_company(bin_value)
        
        if not existing:
            # Create new
            try:
                self.db.create_company(parsed)
                self.stats['created'] += 1
                
            except Exception as e:
                logger.error(f"Failed to create {bin_value}: {e}")
                self.stats['errors'] += 1
        
        else:
            # Update existing
            changes = self.change_detector.detect_changes(existing, parsed)
            
            if changes:
                try:
                    self.db.update_company(bin_value, parsed, changes)
                    self.stats['updated'] += 1
                    
                except Exception as e:
                    logger.error(f"Failed to update {bin_value}: {e}")
                    self.stats['errors'] += 1
            else:
                self.db.touch_last_check(bin_value)
                self.stats['no_changes'] += 1
    
    def _print_summary(self):
        """Print parsing statistics."""
        logger.info("=" * 60)
        logger.info("PARSING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total processed:  {self.stats['total']}")
        logger.info(f"✅ Created:       {self.stats['created']}")
        logger.info(f"🔄 Updated:       {self.stats['updated']}")
        logger.info(f"⏭️  No changes:    {self.stats['no_changes']}")
        logger.info(f"❌ Not found:     {self.stats['not_found']}")
        logger.info(f"⚠️  Errors:        {self.stats['errors']}")
        logger.info("=" * 60)
    
    def cleanup(self):
        """Cleanup resources."""
        if self.db:
            self.db.close()


def parse_file(filepath: str) -> List[str]:
    """
    Parse BINs from file.
    
    Args:
        filepath: Path to file with BINs (one per line)
    
    Returns:
        List of BINs
    """
    path = Path(filepath)
    
    if not path.exists():
        logger.error(f"File not found: {filepath}")
        return []
    
    bins = []
    
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            bin_value = line.strip()
            if bin_value and validate_bin(bin_value):
                bins.append(bin_value)
    
    logger.info(f"Loaded {len(bins)} BINs from file: {filepath}")
    
    return bins


def main():
    """Main CLI entry point."""
    
    parser = argparse.ArgumentParser(
        description='Company Info Parser',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse from qamqor
  python -m parsers.company_info.main --source qamqor
  
  # Parse specific BINs
  python -m parsers.company_info.main --bins "060840008420,060540010332"
  
  # Parse from file
  python -m parsers.company_info.main --file bins.txt
  
  # Parse with verification
  python -m parsers.company_info.main --source qamqor --verify
  
  # Dry-run mode
  python -m parsers.company_info.main --source qamqor --limit 10 --dry-run
        """
    )
    
    # Source selection
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        '--source',
        choices=registry.list_sources(),
        help='BIN source (e.g., qamqor)'
    )
    source_group.add_argument(
        '--bins',
        help='Comma-separated list of BINs'
    )
    source_group.add_argument(
        '--file',
        help='File with BINs (one per line)'
    )
    
    # Options
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of BINs to process'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-parse even if exists'
    )
    
    parser.add_argument(
        '--verify',
        action='store_true',
        help='Verify BIN existence via search API before parsing'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without saving to database'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Log level'
    )
    
    args = parser.parse_args()
    
    # Setup logger
    setup_logger(
        'company_parser',
        log_file='logs/company_parser.log',
        level=args.log_level
    )
    
    # Get BINs
    bins = []
    
    if args.source:
        try:
            source = registry.get(args.source)
            bins = source.get_bins(limit=args.limit)
            
        except Exception as e:
            logger.error(f"Failed to get BINs from source '{args.source}': {e}")
            sys.exit(1)
    
    elif args.bins:
        bins = [b.strip() for b in args.bins.split(',')]
    
    elif args.file:
        bins = parse_file(args.file)
    
    else:
        parser.print_help()
        sys.exit(1)
    
    if not bins:
        logger.error("No BINs to process")
        sys.exit(1)
    
    # Apply limit
    if args.limit and len(bins) > args.limit:
        bins = bins[:args.limit]
        logger.info(f"Limited to {args.limit} BINs")
    
    # Run parser
    company_parser = CompanyParser(
        verify=args.verify,
        dry_run=args.dry_run
    )
    
    try:
        company_parser.parse_bins(bins, force=args.force)
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
    
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)
    
    finally:
        company_parser.cleanup()


if __name__ == '__main__':
    main()