"""
routes/tasas.py — Endpoints de Tasas de Cambio.

  GET    /api/tasas            → lista todas las tasas
  POST   /api/tasas            → agrega una tasa
  DELETE /api/tasas/<id>       → elimina una tasa
  GET    /api/tasas/monedas    → monedas únicas configuradas
"""

from datetime import datetime
from flask import Blueprint, request, jsonify

from config   import CFG
from routes.auth import login_required
from database import get_db

bp = Blueprint('tasas', __name__, url_prefix='/api/tasas')


@login_required
@bp.route('', methods=['GET'])
def get_tasas():
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, nombre, valor, tipo, anio, mes, semana,
                   CAST(fecha_inicio AS VARCHAR(10)) AS fecha_inicio,
                   CAST(fecha_fin    AS VARCHAR(10)) AS fecha_fin,
                   activo,
                   CAST(creado_en AS VARCHAR(19)) AS creado_en,
                   moneda
            FROM {schema}.ppto_tasas_cambio
            ORDER BY moneda, id DESC
        """)
        rows = db.fetchall_dicts(cur)
    # Normalizar campos de fecha a string
    for r in rows:
        for k in ('fecha_inicio', 'fecha_fin', 'creado_en'):
            if r.get(k) and not isinstance(r[k], str):
                r[k] = str(r[k])[:19]
    return jsonify(rows)


@login_required
@bp.route('', methods=['POST'])
def add_tasa():
    data = request.json or {}
    if not data.get('valor'):
        return jsonify({'error': 'Campo requerido: valor'}), 400
    if not data.get('tipo'):
        return jsonify({'error': 'Campo requerido: tipo'}), 400

    moneda = (data.get('moneda') or 'CLP').strip().upper()
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    now    = datetime.now()

    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(db.norm(f"""
            INSERT INTO {schema}.ppto_tasas_cambio
                (nombre, valor, tipo, anio, mes, semana,
                 fecha_inicio, fecha_fin, activo, creado_en, moneda)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """), (
            data.get('nombre'),
            float(data['valor']),
            data['tipo'],
            data.get('anio')         or None,
            data.get('mes')          or None,
            data.get('semana')       or None,
            data.get('fecha_inicio') or None,
            data.get('fecha_fin')    or None,
            now,
            moneda,
        ))
    return jsonify({'ok': True, 'moneda': moneda})


@login_required
@bp.route('/<int:tid>', methods=['DELETE'])
def delete_tasa(tid):
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(db.norm(
            f'DELETE FROM {schema}.ppto_tasas_cambio WHERE id=?'
        ), (tid,))
    return jsonify({'ok': True})


@login_required
@bp.route('/monedas')
def get_monedas():
    """Devuelve las monedas únicas que tienen al menos una tasa activa."""
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(db.norm(f"""
            SELECT DISTINCT moneda
            FROM {schema}.ppto_tasas_cambio
            WHERE activo = ?
            ORDER BY moneda
        """), (1,))
        monedas = [row[0] for row in cur.fetchall()]
    if 'CLP' not in monedas:
        monedas.insert(0, 'CLP')
    return jsonify(monedas)
