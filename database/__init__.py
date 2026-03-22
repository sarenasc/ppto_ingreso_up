"""
database/__init__.py — Fábrica de adaptadores.

Uso:
    from database import get_db
    db = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(db.norm("SELECT * FROM tabla WHERE id=?"), (1,))
        rows = db.fetchall_dicts(cur)

Para reinicializar tras cambio de config:
    from database import reset_db
    reset_db()
"""

from .sqlserver  import SqlServerAdapter
from .postgresql import PostgreSQLAdapter
from .mysql      import MySQLAdapter
from .base       import BaseAdapter

_ADAPTERS: dict[str, type] = {
    'sqlserver':  SqlServerAdapter,
    'postgresql': PostgreSQLAdapter,
    'mysql':      MySQLAdapter,
}

_instance: BaseAdapter | None = None


def get_db() -> BaseAdapter:
    """
    Devuelve el adaptador singleton.
    Se crea la primera vez usando CFG['DB_ENGINE'].
    """
    global _instance
    if _instance is None:
        from config import CFG
        engine = CFG['DB_ENGINE'].strip().lower()
        cls = _ADAPTERS.get(engine)
        if cls is None:
            raise ValueError(
                f"Motor '{engine}' no soportado. "
                f"Opciones válidas: {list(_ADAPTERS.keys())}"
            )
        _instance = cls(CFG)
    return _instance


def reset_db() -> None:
    """
    Elimina el singleton para que get_db() cree uno nuevo
    con la configuración actualizada. Llamar tras save_config().
    """
    global _instance
    _instance = None


__all__ = ['get_db', 'reset_db', 'BaseAdapter']
