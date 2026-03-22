"""
routes/presupuesto.py — Endpoints de Presupuesto calculado.

  GET /api/ppto/resumen?agrupar=mes|semana|especie&moneda=CLP
  GET /api/ppto/exportar?moneda=CLP
"""

from datetime import datetime
from flask import Blueprint, request, jsonify, send_file

from config             import CFG
from services.calculos  import load_calc_data, calcular_fila, MESES_NOMBRE
from services.exportar  import build_excel
from database           import get_db

from routes.auth import login_required

bp = Blueprint('presupuesto', __name__, url_prefix='/api/ppto')


@login_required
@bp.route('/resumen')
def resumen():
    agrupar = request.args.get('agrupar', 'mes')
    moneda  = (request.args.get('moneda') or 'CLP').strip().upper()

    rows, tasas, unitarios, exportable = load_calc_data()

    resultado: dict = {}
    for row in rows:
        esp    = row.get('especie') or 'Sin Especie'
        mes    = row.get('mes')
        semana = row.get('semana')
        c      = calcular_fila(row, unitarios, exportable, tasas, moneda)

        if agrupar == 'semana':
            key = f'Semana {semana}'
        elif agrupar == 'especie':
            key = esp
        else:
            key = MESES_NOMBRE.get(mes, f'Mes {mes}')

        if key not in resultado:
            resultado[key] = {
                'periodo':     key,
                'kgs_totales': 0,
                'kg_export':   0,
                'usd_packing': 0,
                'usd_frio':    0,
                'usd_total':   0,
                'clp_total':   0,
                'moneda':      moneda,
                'tasa_usada':  c['tasa'],
            }
        r = resultado[key]
        r['kgs_totales'] += c['kgs']
        r['kg_export']   += c['kg_export']
        r['usd_packing'] += c['usd_packing']
        r['usd_frio']    += c['usd_frio']
        r['usd_total']   += c['usd_total']
        r['clp_total']   += c['clp_total']

    return jsonify(list(resultado.values()))


@login_required
@bp.route('/exportar')
def exportar():
    moneda = (request.args.get('moneda') or 'CLP').strip().upper()
    rows, tasas, unitarios, exportable = load_calc_data()

    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT TOP 1 numero_version,
                   CAST(fecha_actualizacion AS VARCHAR(10)) AS fa,
                   observacion
            FROM {schema}.ppto_version_log
            ORDER BY id DESC
        """ if CFG['DB_ENGINE'] == 'sqlserver' else f"""
            SELECT numero_version,
                   CAST(fecha_actualizacion AS VARCHAR(10)) AS fa,
                   observacion
            FROM {schema}.ppto_version_log
            ORDER BY id DESC
            LIMIT 1
        """)
        ver_row = cur.fetchone()

    output   = build_excel(rows, tasas, unitarios, exportable, ver_row, moneda)
    now_str  = datetime.now().strftime('%Y%m%d_%H%M')

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'Presupuesto_{moneda}_{now_str}.xlsx',
    )
