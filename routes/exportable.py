"""
routes/exportable.py — Endpoints de % Exportable por especie.

  GET  /api/exportable → lista especies con su porcentaje
  POST /api/exportable → guarda porcentajes (array de {especie, porcentaje})
"""

from datetime import datetime
from flask import Blueprint, request, jsonify

from config   import CFG
from database import get_db

bp = Blueprint('exportable', __name__, url_prefix='/api/exportable')


@bp.route('', methods=['GET'])
def get_exportable():
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, especie, porcentaje,
                   CAST(actualizado_en AS VARCHAR(10)) AS actualizado_en
            FROM {schema}.ppto_exportable_pct
            ORDER BY especie
        """)
        rows = db.fetchall_dicts(cur)
    for r in rows:
        if r.get('actualizado_en') and not isinstance(r['actualizado_en'], str):
            r['actualizado_en'] = str(r['actualizado_en'])[:10]
    return jsonify(rows)


@bp.route('', methods=['POST'])
def save_exportable():
    items  = request.json or []
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    now    = datetime.now()
    with db.get_conn() as conn:
        cur = conn.cursor()
        for item in items:
            db.upsert_exportable(
                cur, schema,
                item['especie'],
                float(item.get('porcentaje', 0)),
                now,
            )
    return jsonify({'ok': True})
