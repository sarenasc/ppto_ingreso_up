"""
app.py — Punto de entrada del Sistema de Presupuesto de Ingresos.

Estructura del proyecto:
  app.py                    <- estás aquí
  config.py                 <- carga .env, expone CFG
  database/                 <- adaptadores SQL Server / PostgreSQL / MySQL
  routes/
    auth.py                 <- /login, /logout, /api/usuarios/*
    estimacion.py           <- /api/estimacion/*
    tasas.py                <- /api/tasas/*
    unitarios.py            <- /api/unitarios/*
    exportable.py           <- /api/exportable/*
    presupuesto.py          <- /api/ppto/*
    configuracion.py        <- /api/config/*
  services/
    calculos.py             <- calcular_fila(), get_tasa_for_row()
    exportar.py             <- build_excel()
    ddl.py                  <- init_db() por motor
  templates/
    login.html              <- pantalla de inicio de sesion
    index.html              <- SPA principal (protegida)
  uploads/
    ultima_estimacion.xlsx  <- ultima estimacion subida (auto)
"""

import secrets
from datetime import timedelta
from flask import Flask, render_template, redirect, url_for, session
from config import CFG


def create_app() -> Flask:
    app = Flask(__name__)

    # Clave secreta para sesiones Flask
    # Define APP_SECRET_KEY en .env para que las sesiones sobrevivan reinicios
    app.secret_key = CFG.get('APP_SECRET_KEY') or secrets.token_hex(32)
    app.permanent_session_lifetime = timedelta(hours=8)

    @app.route('/')
    def index():
        if not session.get('user_id'):
            return redirect(url_for('auth.login'))
        return render_template('index.html')

    from routes import register_blueprints
    register_blueprints(app)

    return app


if __name__ == '__main__':
    from services.ddl import init_db
    from routes.auth  import ensure_default_user

    # 1. Crear / verificar tablas
    try:
        init_db(CFG['DB_ENGINE'], CFG['DB_SCHEMA'])
    except Exception as e:
        print(f'\n[ERROR] No se pudo conectar a la BD:\n  {e}')
        print('  -> Verifica .env o usa la pantalla Configuracion BD\n')

    # 2. Migrar unitarios y exportable a esquema exportadora+especie
    try:
        from services.ddl import migrar_unitarios_por_exportadora
        migrar_unitarios_por_exportadora(CFG['DB_ENGINE'], CFG['DB_SCHEMA'])
    except Exception as e:
        print(f'[WARN] Migracion unitarios: {e}')
    try:
        from services.ddl import migrar_exportable_por_exportadora
        migrar_exportable_por_exportadora(CFG['DB_ENGINE'], CFG['DB_SCHEMA'])
    except Exception as e:
        print(f'[WARN] Migracion exportable: {e}')

    # 3. Crear usuario admin por defecto si no hay ninguno
    try:
        ensure_default_user()
    except Exception as e:
        print(f'[WARN] No se pudo verificar usuario inicial: {e}')

    # 3. Arrancar
    app = create_app()
    app.run(debug=True, port=CFG['FLASK_PORT'], host='0.0.0.0')
