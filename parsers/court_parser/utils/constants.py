"""
Константы приложения
"""


class PartyRole:
    """Роли сторон в деле"""
    PLAINTIFF = 'plaintiff'
    DEFENDANT = 'defendant'


class CaseStatus:
    """Статусы результата операций с делом"""
    SAVED = 'saved'
    UPDATED = 'updated'
    ERROR = 'error'
    NO_RESULTS = 'no_results'
    TARGET_NOT_FOUND = 'target_not_found'
    REGION_NOT_FOUND = 'region_not_found'
    SAVE_FAILED = 'save_failed'


class HttpStatus:
    """HTTP статусы для обработки"""
    RETRIABLE = frozenset({500, 502, 503, 504})
    NON_RETRIABLE = frozenset({400, 401, 403, 404})