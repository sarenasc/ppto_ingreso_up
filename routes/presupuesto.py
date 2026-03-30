"""
routes/presupuesto.py — Endpoints de Presupuesto calculado.

  GET /api/ppto/resumen?agrupar=mes|semana|especie&moneda=CLP
  GET /api/ppto/arbol?moneda=CLP
  GET /api/ppto/exportar?moneda=CLP

Logica de ingresos:
  Si existe un registro en ppto_ingreso_usd para (exportadora, especie, mes),
  los valores USD de ese registro REEMPLAZAN los calculados automaticamente.
  Los kgs y kg_export siempre vienen del Excel (ppto_estimacion).
"""

from datetime import datetime
from flask import Blueprint, request, jsonify, send_file

from config            import CFG
from services.calculos import (load_calc_data, calcular_fila, MESES_NOMBRE,
                                sort_mes_temporada,
                                load_ingreso_data, aplicar_ingreso_manual)
from services.exportar import build_excel
from database          import get_db
from routes.auth       import login_required

bp = Blueprint('presupuesto', __name__, url_prefix='/api/ppto')


def _get_tasa_from_row(row, tasas, moneda):
    """Obtiene la tasa para calcular clp cuando hay override manual."""
    from services.calculos import get_tasa_for_row
    fecha  = str(row['fecha'])[:10] if row.get('fecha') else None
    semana = row.get('semana')
    mes    = row.get('mes')
    return get_tasa_for_row(tasas, fecha, semana, mes, moneda)


@bp.route('/resumen')
@login_required
def resumen():
    agrupar     = request.args.get('agrupar', 'mes')
    moneda      = (request.args.get('moneda') or 'CLP').strip().upper()

    rows, tasas, unitarios, exportable = load_calc_data()
    ingreso_map = load_ingreso_data()   # overrides manuales

    resultado: dict = {}

    for row in rows:
        esp         = row.get('especie')     or 'Sin Especie'
        exportadora = row.get('exportadora') or 'Sin Exportadora'
        mes         = row.get('mes')
        semana      = row.get('semana')

        c = calcular_fila(row, unitarios, exportable, tasas, moneda)
        c = aplicar_ingreso_manual(c, ingreso_map, exportadora, esp, mes)

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
                'tiene_manual': False,
            }
        r = resultado[key]
        r['kgs_totales'] += c['kgs']
        r['kg_export']   += c['kg_export']
        r['usd_packing'] += c['usd_packing']
        r['usd_frio']    += c['usd_frio']
        r['usd_total']   += c['usd_total']
        r['clp_total']   += c['clp_total']
        if c.get('es_manual'):
            r['tiene_manual'] = True

    return jsonify(list(resultado.values()))


@bp.route('/arbol')
@login_required
def arbol():
    """
    Jerarquia Mes -> Exportadora -> Especie con override de ingreso manual.
    """
    moneda = (request.args.get('moneda') or 'CLP').strip().upper()

    rows, tasas, unitarios, exportable = load_calc_data()
    ingreso_map = load_ingreso_data()

    tree: dict = {}

    for row in rows:
        mes         = row.get('mes')         or 0
        exportadora = row.get('exportadora') or 'Sin Exportadora'
        esp         = row.get('especie')     or 'Sin Especie'

        c = calcular_fila(row, unitarios, exportable, tasas, moneda)
        c = aplicar_ingreso_manual(c, ingreso_map, exportadora, esp, mes)

        tree.setdefault(mes, {})
        tree[mes].setdefault(exportadora, {})
        tree[mes][exportadora].setdefault(esp, {
            'kgs': 0, 'kg_export': 0,
            'usd_packing': 0, 'usd_frio': 0,
            'usd_total': 0, 'clp_total': 0,
            'es_manual': False,
        })
        d = tree[mes][exportadora][esp]
        d['kgs']        += c['kgs']
        d['kg_export']  += c['kg_export']
        d['usd_packing']+= c['usd_packing']
        d['usd_frio']   += c['usd_frio']
        d['usd_total']  += c['usd_total']
        d['clp_total']  += c['clp_total']
        if c.get('es_manual'):
            d['es_manual'] = True

    resultado = []
    for mes in sorted(tree.keys(), key=sort_mes_temporada):
        mes_data = {
            'mes': mes, 'mes_nombre': MESES_NOMBRE.get(mes, f'Mes {mes}'),
            'kgs': 0, 'kg_export': 0,
            'usd_packing': 0, 'usd_frio': 0,
            'usd_total': 0, 'clp_total': 0,
            'exportadoras': [],
        }
        for exp in sorted(tree[mes].keys()):
            exp_data = {
                'exportadora': exp,
                'kgs': 0, 'kg_export': 0,
                'usd_packing': 0, 'usd_frio': 0,
                'usd_total': 0, 'clp_total': 0,
                'especies': [],
            }
            for esp in sorted(tree[mes][exp].keys()):
                d = tree[mes][exp][esp]
                exp_data['especies'].append({'especie': esp, **d})
                for k in ('kgs','kg_export','usd_packing','usd_frio','usd_total','clp_total'):
                    exp_data[k] += d[k]
            mes_data['exportadoras'].append(exp_data)
            for k in ('kgs','kg_export','usd_packing','usd_frio','usd_total','clp_total'):
                mes_data[k] += exp_data[k]
        resultado.append(mes_data)

    return jsonify({'arbol': resultado, 'moneda': moneda})


@bp.route('/exportar')
@login_required
def exportar():
    moneda = (request.args.get('moneda') or 'CLP').strip().upper()

    rows, tasas, unitarios, exportable = load_calc_data()
    ingreso_map = load_ingreso_data()

    schema = CFG['DB_SCHEMA']
    db     = get_db()
    engine = CFG['DB_ENGINE']

    with db.get_conn() as conn:
        cur = conn.cursor()
        limit_sql = (
            f"SELECT TOP 1 numero_version, CAST(fecha_actualizacion AS VARCHAR(10)) AS fa, observacion "
            f"FROM {schema}.ppto_version_log ORDER BY id DESC"
        ) if engine == 'sqlserver' else (
            f"SELECT numero_version, CAST(fecha_actualizacion AS VARCHAR(10)) AS fa, observacion "
            f"FROM {schema}.ppto_version_log ORDER BY id DESC LIMIT 1"
        )
        cur.execute(limit_sql)
        ver_row = cur.fetchone()

    output  = build_excel(rows, tasas, unitarios, exportable, ver_row, moneda,
                          ingreso_map=ingreso_map)
    now_str = datetime.now().strftime('%Y%m%d_%H%M')

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'Presupuesto_{moneda}_{now_str}.xlsx',
    )
