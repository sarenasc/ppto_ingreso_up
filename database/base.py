"""
database/base.py — Interfaz común para todos los adaptadores de BD.

Cada motor (SQL Server, PostgreSQL, MySQL) implementa esta clase.
El resto del sistema solo habla con BaseAdapter y no sabe qué motor corre.
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime


class BaseAdapter(ABC):

    # ── Placeholder de parámetros ────────────────────────────────────
    # pyodbc  → '?'
    # psycopg2 / PyMySQL → '%s'
    @property
    @abstractmethod
    def placeholder(self) -> str: ...

    # ── Conexión ─────────────────────────────────────────────────────
    @abstractmethod
    def _new_connection(self):
        """Devuelve una conexión DB-API 2.0 sin autocommit."""

    @contextmanager
    def get_conn(self):
        """Context manager: commit al salir, rollback si hay excepción."""
        conn = self._new_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── Utilidades comunes ───────────────────────────────────────────
    def fetchall_dicts(self, cursor) -> list[dict]:
        """Convierte filas de cursor a lista de dicts."""
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def norm(self, sql: str) -> str:
        """
        Normaliza placeholders: reemplaza '?' por '%s' para motores
        que lo necesiten (psycopg2, PyMySQL).
        """
        if self.placeholder == '%s':
            return sql.replace('?', '%s')
        return sql

    # ── Upserts por motor ────────────────────────────────────────────
    # Cada adaptador implementa estas operaciones con la sintaxis propia:
    #   SQL Server  → MERGE ... USING ... WHEN MATCHED / NOT MATCHED
    #   PostgreSQL  → INSERT ... ON CONFLICT DO UPDATE
    #   MySQL       → INSERT ... ON DUPLICATE KEY UPDATE

    @abstractmethod
    def upsert_unitario(self, cur, schema: str, especie: str,
                        packing: float, frio: float, now: datetime) -> None:
        """Inserta o actualiza un registro en ppto_unitarios."""

    @abstractmethod
    def upsert_exportable(self, cur, schema: str, exportadora: str, especie: str,
                          porcentaje: float, now: datetime) -> None:
        """Inserta o actualiza un registro en ppto_exportable_pct."""

    @abstractmethod
    def ensure_unitario_exists(self, cur, schema: str, especie: str,
                               now: datetime) -> None:
        """Inserta en ppto_unitarios solo si no existe (para el upload)."""

    @abstractmethod
    def ensure_exportable_exists(self, cur, schema: str, exportadora: str,
                                 especie: str, now: datetime) -> None:
        """Inserta en ppto_exportable_pct solo si no existe (para el upload)."""

    # ── Diagnóstico ──────────────────────────────────────────────────
    @abstractmethod
    def test_connection(self) -> tuple[bool, str]:
        """
        Intenta conectar y devuelve (éxito: bool, mensaje: str).
        Nunca lanza excepción.
        """
