"""
routes/__init__.py — Registro de todos los Blueprints en la app Flask.

Uso en app.py:
    from routes import register_blueprints
    register_blueprints(app)
"""

from flask import Flask


def register_blueprints(app: Flask) -> None:
    from routes.estimacion   import bp as est_bp
    from routes.tasas        import bp as tas_bp
    from routes.unitarios    import bp as uni_bp
    from routes.exportable   import bp as exp_bp
    from routes.presupuesto  import bp as ppto_bp
    from routes.configuracion import bp as cfg_bp

    app.register_blueprint(est_bp)
    app.register_blueprint(tas_bp)
    app.register_blueprint(uni_bp)
    app.register_blueprint(exp_bp)
    app.register_blueprint(ppto_bp)
    app.register_blueprint(cfg_bp)
