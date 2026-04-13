"""
database/postgresql.py — Adaptador para PostgreSQL via psycopg2.

Requiere: pip install psycopg2-binary
Schema:   por defecto 'public' (configurable en .env DB_SCHEMA)
"""

from datetime import datetime
from .base import BaseAdapter


class PostgreSQLAdapter(BaseAdapter):

    def __init__(self, cfg: dict):
        self._dsn = {
            'host':     cfg['DB_SERVER'],
            'port':     int(cfg.get('DB_PORT') or 5432),
            'dbname':   cfg['DB_DATABASE'],
            'user':     cfg['DB_USER'],
            'password': cfg['DB_PASSWORD'],
        }
        self._schema = cfg.get('DB_SCHEMA', 'public')

    # ── Interface ────────────────────────────────────────────────────
    @property
    def placeholder(self) -> str:
        return '%s'

    def _new_connection(self):
        import psycopg2
        conn = psycopg2.connect(**self._dsn)
        conn.autocommit = False
        return conn

    # ── Upserts (ON CONFLICT syntax) ─────────────────────────────────
    def upsert_unitario(self, cur, schema, especie, packing, frio, now):
        cur.execute(self.norm(f"""
            INSERT INTO {schema}.ppto_unitarios
                (especie, precio_usd_packing, precio_usd_frio, actualizado_en)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (especie) DO UPDATE SET
                precio_usd_packing = EXCLUDED.precio_usd_packing,
                precio_usd_frio    = EXCLUDED.precio_usd_frio,
                actualizado_en     = EXCLUDED.actualizado_en
        """), (especie, packing, frio, now))

    def upsert_exportable(self, cur, schema, exportadora, especie, porcentaje, now):
        cur.execute(self.norm(f"""
            INSERT INTO {schema}.ppto_exportable_pct
                (exportadora, especie, porcentaje, actualizado_en)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (exportadora, especie) DO UPDATE SET
                porcentaje     = EXCLUDED.porcentaje,
                actualizado_en = EXCLUDED.actualizado_en
        """), (exportadora, especie, porcentaje, now))

    def ensure_unitario_exists(self, cur, schema, especie, now):
        cur.execute(self.norm(f"""
            INSERT INTO {schema}.ppto_unitarios
                (especie, precio_usd_packing, precio_usd_frio, actualizado_en)
            VALUES (?, 0, 0, ?)
            ON CONFLICT (especie) DO NOTHING
        """), (especie, now))

    def ensure_exportable_exists(self, cur, schema, exportadora, especie, now):
        cur.execute(self.norm(f"""
            INSERT INTO {schema}.ppto_exportable_pct
                (exportadora, especie, porcentaje, actualizado_en)
            VALUES (?, ?, 0.8, ?)
            ON CONFLICT (exportadora, especie) DO NOTHING
        """), (exportadora, especie, now))

    # ── Diagnóstico ──────────────────────────────────────────────────
    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._new_connection()
            conn.close()
            return True, "Conexión exitosa a PostgreSQL."
        except Exception as e:
            return False, str(e)
