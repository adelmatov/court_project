"""
Модули обновления данных
"""
from core.updaters.base_updater import BaseUpdater
from core.updaters.judge_updater import JudgeUpdater
from core.updaters.events_updater import EventsUpdater
from core.updaters.docs_updater import DocsUpdater
from core.updaters.gaps_updater import GapsUpdater

__all__ = ['BaseUpdater', 'JudgeUpdater', 'EventsUpdater', 'DocsUpdater', 'GapsUpdater']