"""
app.py — Punto de entrada del Sistema de Presupuesto de Ingresos.

Este archivo es intencionalmente pequeño: solo crea la app Flask,
registra los blueprints y arranca el servidor. Toda la lógica
vive en routes/, services/ y database/.

Estructura del proyecto:
  app.py                    ← estás aquí
  config.py                 ← carga .env, expone CFG
  database/
    __init__.py             ← fábrica get_db() / reset_db()
    base.py                 ← interfaz común BaseAdapter
    sqlserver.py            ← adaptador SQL Server (pyodbc)
    postgresql.py           ← adaptador PostgreSQL (psycopg2)
    mysql.py                ← adaptador MySQL/MariaDB (PyMySQL)
  routes/
    __init__.py             ← register_blueprints()
    estimacion.py           ← /api/estimacion/*
    tasas.py                ← /api/tasas/*
    unitarios.py            ← /api/unitarios/*
    exportable.py           ← /api/exportable/*
    presupuesto.py          ← /api/ppto/*
    configuracion.py        ← /api/config/*
  services/
    calculos.py             ← calcular_fila(), get_tasa_for_row()
    exportar.py             ← build_excel()
    ddl.py                  ← init_db() por motor
  templates/
    index.html              ← SPA frontend
  uploads/
    ultima_estimacion.xlsx  ← última estimación subida (auto)
"""

from flask import Flask, render_template
from config import CFG


def create_app() -> Flask:
    app = Flask(__name__)

    # ── Página principal ─────────────────────────────────────────────
    @app.route('/')
    def index():
        return render_template('index.html')

    # ── Registrar todos los Blueprints ───────────────────────────────
    from routes import register_blueprints
    register_blueprints(app)

    return app


if __name__ == '__main__':
    from config          import CFG
    from database        import get_db
    from services.ddl    import init_db

    # Inicializar / verificar tablas al arrancar
    try:
        init_db(CFG['DB_ENGINE'], CFG['DB_SCHEMA'])
    except Exception as e:
        print(f'\n[ERROR] No se pudo conectar a la base de datos:\n  {e}')
        print('  → Verifica el archivo .env o usa la pantalla de Configuración BD\n')

    app = create_app()
    app.run(
        debug=True,
        port=CFG['FLASK_PORT'],
        host='0.0.0.0',
    )
