"""
database/sqlserver.py — Adaptador para Microsoft SQL Server via pyodbc.

Requiere: pip install pyodbc
Driver:   ODBC Driver 17 for SQL Server (o 18)
"""

from datetime import datetime
from .base import BaseAdapter


class SqlServerAdapter(BaseAdapter):

    def __init__(self, cfg: dict):
        port   = cfg.get('DB_PORT') or '1433'
        parts  = [
            f"DRIVER={{{cfg['DB_DRIVER']}}}",
            f"SERVER={cfg['DB_SERVER']},{port}",
            f"DATABASE={cfg['DB_DATABASE']}",
        ]
        if cfg.get('DB_WINDOWS_AUTH'):
            parts.append("Trusted_Connection=yes")
        else:
            parts += [f"UID={cfg['DB_USER']}", f"PWD={cfg['DB_PASSWORD']}"]
        self._conn_str = ";".join(parts)
        self._schema   = cfg.get('DB_SCHEMA', 'dbo')

    # ── Interface ────────────────────────────────────────────────────
    @property
    def placeholder(self) -> str:
        return '?'

    def _new_connection(self):
        import pyodbc
        return pyodbc.connect(self._conn_str, autocommit=False)

    # ── Upserts (MERGE syntax) ───────────────────────────────────────
    def upsert_unitario(self, cur, schema, especie, packing, frio, now):
        cur.execute(f"""
            MERGE {schema}.ppto_unitarios AS t
            USING (SELECT ? AS especie) AS s ON t.especie = s.especie
            WHEN MATCHED THEN
                UPDATE SET precio_usd_packing=?, precio_usd_frio=?, actualizado_en=?
            WHEN NOT MATCHED THEN
                INSERT (especie, precio_usd_packing, precio_usd_frio, actualizado_en)
                VALUES (?, ?, ?, ?);
        """, especie, packing, frio, now, especie, packing, frio, now)

    def upsert_exportable(self, cur, schema, exportadora, especie, porcentaje, now):
        cur.execute(f"""
            MERGE {schema}.ppto_exportable_pct AS t
            USING (SELECT ? AS exportadora, ? AS especie) AS s
              ON t.exportadora=s.exportadora AND t.especie=s.especie
            WHEN MATCHED THEN
                UPDATE SET porcentaje=?, actualizado_en=?
            WHEN NOT MATCHED THEN
                INSERT (exportadora, especie, porcentaje, actualizado_en)
                VALUES (?, ?, ?, ?);
        """, exportadora, especie, porcentaje, now, exportadora, especie, porcentaje, now)

    def ensure_unitario_exists(self, cur, schema, especie, now):
        cur.execute(f"""
            IF NOT EXISTS (SELECT 1 FROM {schema}.ppto_unitarios WHERE especie=?)
                INSERT INTO {schema}.ppto_unitarios
                    (especie, precio_usd_packing, precio_usd_frio, actualizado_en)
                VALUES (?, 0, 0, ?)
        """, especie, especie, now)

    def ensure_exportable_exists(self, cur, schema, exportadora, especie, now):
        cur.execute(f"""
            IF NOT EXISTS (SELECT 1 FROM {schema}.ppto_exportable_pct
                           WHERE exportadora=? AND especie=?)
                INSERT INTO {schema}.ppto_exportable_pct
                    (exportadora, especie, porcentaje, actualizado_en)
                VALUES (?, ?, 0.8, ?)
        """, exportadora, especie, exportadora, especie, now)

    # ── Diagnóstico ──────────────────────────────────────────────────
    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._new_connection()
            conn.close()
            return True, "Conexión exitosa a SQL Server."
        except Exception as e:
            return False, str(e)
