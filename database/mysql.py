"""
database/mysql.py — Adaptador para MySQL / MariaDB via PyMySQL.

Requiere: pip install PyMySQL
Schema:   en MySQL el "schema" = el nombre de la base de datos.
          Configura DB_SCHEMA = DB_DATABASE en el .env.
"""

from datetime import datetime
from .base import BaseAdapter


class MySQLAdapter(BaseAdapter):

    def __init__(self, cfg: dict):
        self._params = {
            'host':     cfg['DB_SERVER'],
            'port':     int(cfg.get('DB_PORT') or 3306),
            'db':       cfg['DB_DATABASE'],
            'user':     cfg['DB_USER'],
            'password': cfg['DB_PASSWORD'],
            'charset':  'utf8mb4',
        }
        # En MySQL, schema = database. Si DB_SCHEMA no está definido, usa DB_DATABASE.
        self._schema = cfg.get('DB_SCHEMA') or cfg['DB_DATABASE']

    # ── Interface ────────────────────────────────────────────────────
    @property
    def placeholder(self) -> str:
        return '%s'

    def _new_connection(self):
        import pymysql
        return pymysql.connect(**self._params, autocommit=False)

    # ── Upserts (ON DUPLICATE KEY syntax) ────────────────────────────
    def upsert_unitario(self, cur, schema, especie, packing, frio, now):
        cur.execute(self.norm(f"""
            INSERT INTO {schema}.ppto_unitarios
                (especie, precio_usd_packing, precio_usd_frio, actualizado_en)
            VALUES (?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                precio_usd_packing = VALUES(precio_usd_packing),
                precio_usd_frio    = VALUES(precio_usd_frio),
                actualizado_en     = VALUES(actualizado_en)
        """), (especie, packing, frio, now))

    def upsert_exportable(self, cur, schema, exportadora, especie, porcentaje, now):
        cur.execute(self.norm(f"""
            INSERT INTO {schema}.ppto_exportable_pct
                (exportadora, especie, porcentaje, actualizado_en)
            VALUES (?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                porcentaje     = VALUES(porcentaje),
                actualizado_en = VALUES(actualizado_en)
        """), (exportadora, especie, porcentaje, now))

    def ensure_unitario_exists(self, cur, schema, especie, now):
        cur.execute(self.norm(f"""
            INSERT IGNORE INTO {schema}.ppto_unitarios
                (especie, precio_usd_packing, precio_usd_frio, actualizado_en)
            VALUES (?, 0, 0, ?)
        """), (especie, now))

    def ensure_exportable_exists(self, cur, schema, exportadora, especie, now):
        cur.execute(self.norm(f"""
            INSERT IGNORE INTO {schema}.ppto_exportable_pct
                (exportadora, especie, porcentaje, actualizado_en)
            VALUES (?, ?, 0.8, ?)
        """), (exportadora, especie, now))

    # ── Diagnóstico ──────────────────────────────────────────────────
    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._new_connection()
            conn.close()
            return True, "Conexión exitosa a MySQL/MariaDB."
        except Exception as e:
            return False, str(e)
