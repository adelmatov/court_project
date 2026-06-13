"""Сервис чтения данных из PostgreSQL (через psycopg2).

Только ЧТЕНИЕ. Парсер пишет в ту же БД через asyncpg — это безопасно.
Параметры подключения берутся из config.json (секция database).
"""
from __future__ import annotations

from typing import Any

import psycopg2
import psycopg2.extras

from app.models.domain import Case, DashboardStats


class DbError(Exception):
    """Ошибка работы с БД."""


class DbService:
    def __init__(self, db_config: dict[str, Any]) -> None:
        # config.json использует ключ 'dbname', psycopg2 ждёт 'dbname' тоже
        self._params = {
            "host": db_config.get("host", "localhost"),
            "port": int(db_config.get("port", 5432)),
            "dbname": db_config.get("dbname", ""),
            "user": db_config.get("user", ""),
            "password": db_config.get("password", ""),
        }

    def _connect(self):
        try:
            return psycopg2.connect(**self._params, connect_timeout=5)
        except psycopg2.Error as exc:
            raise DbError(f"Не удалось подключиться к БД: {exc}") from exc

    def test_connection(self) -> bool:
        """Проверка доступности БД."""
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return True
        except DbError:
            return False

    def fetch_stats(self) -> DashboardStats:
        """Собирает агрегированную статистику для дашборда."""
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM cases")
                    total_cases = cur.fetchone()[0]

                    cur.execute("SELECT COUNT(*) FROM case_documents")
                    total_documents = cur.fetchone()[0]

                    cur.execute(
                        "SELECT COUNT(*) FROM cases "
                        "WHERE documents_complete = FALSE OR documents_complete IS NULL"
                    )
                    cases_without_docs = cur.fetchone()[0]

                    cur.execute("SELECT COUNT(*) FROM cases WHERE judge_id IS NULL")
                    cases_without_judge = cur.fetchone()[0]

            return DashboardStats(
                total_cases=total_cases,
                total_documents=total_documents,
                cases_without_docs=cases_without_docs,
                cases_without_judge=cases_without_judge,
            )
        except psycopg2.Error as exc:
            raise DbError(f"Ошибка запроса статистики: {exc}") from exc

    def fetch_cases(self, search: str = "", limit: int = 200) -> list[Case]:
        """
        Возвращает список дел. Если задан search — фильтрует по номеру дела
        или имени судьи.
        """
        query = """
            SELECT
                c.id,
                c.case_number,
                c.case_date,
                j.full_name AS judge,
                COALESCE(d.cnt, 0) AS documents_count,
                COALESCE(c.documents_complete, FALSE) AS documents_complete,
                c.updated_at
            FROM cases c
            LEFT JOIN judges j ON c.judge_id = j.id
            LEFT JOIN (
                SELECT case_id, COUNT(*) AS cnt
                FROM case_documents
                GROUP BY case_id
            ) d ON d.case_id = c.id
        """
        params: list[Any] = []
        if search.strip():
            query += " WHERE c.case_number ILIKE %s OR j.full_name ILIKE %s"
            term = f"%{search.strip()}%"
            params.extend([term, term])

        query += " ORDER BY c.case_date DESC NULLS LAST LIMIT %s"
        params.append(limit)

        try:
            with self._connect() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
        except psycopg2.Error as exc:
            raise DbError(f"Ошибка запроса дел: {exc}") from exc

        result: list[Case] = []
        for r in rows:
            result.append(
                Case(
                    id=r["id"],
                    case_number=r["case_number"],
                    case_date=str(r["case_date"]) if r["case_date"] else None,
                    judge=r["judge"],
                    documents_count=r["documents_count"],
                    documents_complete=bool(r["documents_complete"]),
                    updated_at=str(r["updated_at"]) if r["updated_at"] else None,
                )
            )
        return result