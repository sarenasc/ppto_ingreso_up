"""
routes/configuracion.py — Endpoints de configuración de base de datos.

  GET  /api/config/db          → config actual (sin contraseña)
  POST /api/config/db          → guarda nueva config en .env y reinicia adaptador
  POST /api/config/db-test     → prueba conexión con credenciales dadas
  GET  /api/config/engines     → lista de motores soportados
"""

from flask import Blueprint, request, jsonify

bp = Blueprint('configuracion', __name__, url_prefix='/api/config')

# Motores disponibles con metadatos para el UI
ENGINES = {
    'sqlserver': {
        'label':        'SQL Server',
        'default_port': '1433',
        'schema_label': 'Schema (ej: dbo)',
        'has_windows_auth': True,
        'driver_field': True,
        'icon':         '🗄️',
    },
    'postgresql': {
        'label':        'PostgreSQL',
        'default_port': '5432',
        'schema_label': 'Schema (ej: public)',
        'has_windows_auth': False,
        'driver_field': False,
        'icon':         '🐘',
    },
    'mysql': {
        'label':        'MySQL / MariaDB',
        'default_port': '3306',
        'schema_label': 'Database (= schema en MySQL)',
        'has_windows_auth': False,
        'driver_field': False,
        'icon':         '🐬',
    },
}


@login_required
@bp.route('/engines')
def get_engines():
    return jsonify(ENGINES)


@login_required
@bp.route('/db', methods=['GET'])
def get_db_config():
    """Devuelve la configuración actual sin exponer la contraseña."""
    from config import CFG
from routes.auth import login_required
    return jsonify({
        'DB_ENGINE':       CFG['DB_ENGINE'],
        'DB_SERVER':       CFG['DB_SERVER'],
        'DB_PORT':         CFG['DB_PORT'],
        'DB_DATABASE':     CFG['DB_DATABASE'],
        'DB_USER':         CFG['DB_USER'],
        'DB_PASSWORD':     '***' if CFG['DB_PASSWORD'] else '',
        'DB_SCHEMA':       CFG['DB_SCHEMA'],
        'DB_DRIVER':       CFG['DB_DRIVER'],
        'DB_WINDOWS_AUTH': CFG['DB_WINDOWS_AUTH'],
        'FLASK_PORT':      CFG['FLASK_PORT'],
    })


@login_required
@bp.route('/db', methods=['POST'])
def save_db_config():
    """
    Guarda la nueva configuración en .env, recarga CFG y reinicia el adaptador.
    Si se envía DB_PASSWORD vacío o '***', conserva la contraseña anterior.
    """
    import config as cfg_module
    from database import reset_db
    from services.ddl import init_db

    data = request.json or {}

    # Construir dict de actualizaciones
    updates: dict = {}
    fields = [
        'DB_ENGINE', 'DB_SERVER', 'DB_PORT', 'DB_DATABASE',
        'DB_USER', 'DB_SCHEMA', 'DB_DRIVER', 'DB_WINDOWS_AUTH', 'FLASK_PORT',
    ]
    for f in fields:
        if f in data:
            updates[f] = str(data[f])

    # Contraseña: solo actualizar si se envía un valor real
    pwd = data.get('DB_PASSWORD', '')
    if pwd and pwd != '***':
        updates['DB_PASSWORD'] = pwd

    try:
        cfg_module.save_config(updates)
        cfg_module.CFG = cfg_module.load_config()
        reset_db()  # fuerza recreación del adaptador con nueva config

        # Intentar inicializar tablas con el nuevo motor
        engine = cfg_module.CFG['DB_ENGINE']
        schema = cfg_module.CFG['DB_SCHEMA']
        try:
            init_db(engine, schema)
            ddl_msg = 'Tablas verificadas correctamente.'
        except Exception as e:
            ddl_msg = f'Config guardada pero DDL falló: {e}'

        return jsonify({'ok': True, 'message': f'Configuración guardada. {ddl_msg}'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@login_required
@bp.route('/db-test', methods=['POST'])
def test_db():
    """
    Prueba la conexión con las credenciales enviadas en el body
    SIN guardarlas en .env.
    """
    data   = request.json or {}
    engine = (data.get('DB_ENGINE') or 'sqlserver').lower()

    # Construir un adaptador temporal con los datos del request
    temp_cfg = {
        'DB_ENGINE':       engine,
        'DB_SERVER':       data.get('DB_SERVER',   'localhost'),
        'DB_PORT':         data.get('DB_PORT',     ''),
        'DB_DATABASE':     data.get('DB_DATABASE', ''),
        'DB_USER':         data.get('DB_USER',     ''),
        'DB_PASSWORD':     data.get('DB_PASSWORD', ''),
        'DB_SCHEMA':       data.get('DB_SCHEMA',   'dbo'),
        'DB_DRIVER':       data.get('DB_DRIVER',   'ODBC Driver 17 for SQL Server'),
        'DB_WINDOWS_AUTH': str(data.get('DB_WINDOWS_AUTH', False)).lower() == 'true',
    }

    try:
        from database.sqlserver  import SqlServerAdapter
        from database.postgresql import PostgreSQLAdapter
        from database.mysql      import MySQLAdapter

        adapters = {
            'sqlserver':  SqlServerAdapter,
            'postgresql': PostgreSQLAdapter,
            'mysql':      MySQLAdapter,
        }
        cls = adapters.get(engine)
        if cls is None:
            return jsonify({'ok': False, 'message': f"Motor '{engine}' no reconocido."})

        adapter = cls(temp_cfg)
        ok, msg = adapter.test_connection()
        return jsonify({'ok': ok, 'message': msg})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)})
