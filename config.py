"""
config.py — Carga y gestión de configuración desde .env
Todas las variables de entorno del sistema pasan por aquí.
"""

import os
from pathlib import Path

BASE_DIR  = Path(__file__).parent
ENV_FILE  = BASE_DIR / '.env'
UPLOADS   = BASE_DIR / 'uploads'
UPLOADS.mkdir(exist_ok=True)

try:
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE, override=True)
except ImportError:
    pass


def load_config() -> dict:
    """Lee variables de entorno y devuelve dict de configuración."""
    return {
        # ── Motor de base de datos ──────────────────────────────────
        # Valores válidos: sqlserver | postgresql | mysql
        'DB_ENGINE':       os.getenv('DB_ENGINE',       'sqlserver'),

        # ── Conexión ────────────────────────────────────────────────
        'DB_SERVER':       os.getenv('DB_SERVER',       'localhost'),
        'DB_PORT':         os.getenv('DB_PORT',         ''),
        'DB_DATABASE':     os.getenv('DB_DATABASE',     'PptoBD'),
        'DB_USER':         os.getenv('DB_USER',         ''),
        'DB_PASSWORD':     os.getenv('DB_PASSWORD',     ''),
        'DB_SCHEMA':       os.getenv('DB_SCHEMA',       'dbo'),

        # ── SQL Server específico ────────────────────────────────────
        'DB_DRIVER':       os.getenv('DB_DRIVER',       'ODBC Driver 17 for SQL Server'),
        'DB_WINDOWS_AUTH': os.getenv('DB_WINDOWS_AUTH', 'False').lower() == 'true',

        # ── Flask ────────────────────────────────────────────────────
        'FLASK_PORT':      int(os.getenv('FLASK_PORT',  '5050')),
        'APP_SECRET_KEY':   os.getenv('APP_SECRET_KEY',  ''),

        # ── Rutas ───────────────────────────────────────────────────
        'UPLOAD_FOLDER':   str(UPLOADS),
        'ULTIMO_EXCEL':    str(UPLOADS / 'ultima_estimacion.xlsx'),
    }


def save_config(updates: dict) -> None:
    """
    Actualiza o agrega claves en el archivo .env sin borrar el resto.
    Luego recarga las variables de entorno.
    """
    lines: list[str] = []
    if ENV_FILE.exists():
        lines = ENV_FILE.read_text(encoding='utf-8').splitlines()

    written = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('#') and '=' in stripped:
            key = stripped.split('=', 1)[0].strip()
            if key in updates:
                new_lines.append(f"{key}={updates[key]}")
                written.add(key)
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)

    for key, val in updates.items():
        if key not in written:
            new_lines.append(f"{key}={val}")

    ENV_FILE.write_text('\n'.join(new_lines) + '\n', encoding='utf-8')

    # Recargar en el proceso actual
    try:
        from dotenv import load_dotenv
        load_dotenv(ENV_FILE, override=True)
    except ImportError:
        pass


# Instancia global — se importa como `from config import CFG`
CFG: dict = load_config()
