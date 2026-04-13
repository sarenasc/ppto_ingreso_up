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
from services.calculos import (load_calc_data, MESES_NOMBRE,
                                sort_mes_temporada,
                                load_ingreso_data,
                                acumular_grupos, aplicar_overrides_a_grupos)
from services.exportar import build_excel
from database          import get_db
from routes.auth       import login_required

bp = Blueprint('presupuesto', __name__, url_prefix='/api/ppto')


@bp.route('/resumen')
@login_required
def resumen():
    agrupar = request.args.get('agrupar', 'mes')
    moneda  = (request.args.get('moneda') or 'CLP').strip().upper()

    rows, tasas, unitarios, exportable = load_calc_data()
    ingreso_map = load_ingreso_data()

    # Pasada 1: acumular automatico por (exportadora, especie, mes)
    grupos = acumular_grupos(rows, unitarios, exportable, tasas, moneda)
    # Pasada 2: aplicar overrides manuales una vez por grupo
    aplicar_overrides_a_grupos(grupos, ingreso_map)

    # Pasada 3: agregar al nivel pedido (mes / especie / semana)
    resultado: dict = {}

    for (exportadora, esp, mes), g in grupos.items():
        if agrupar == 'semana':
            # Distribuir cada semana del grupo
            for semana, ps in g['por_semana'].items():
                key = f'Semana {semana}'
                if key not in resultado:
                    resultado[key] = {
                        'periodo': key, 'semana': semana,
                        'kgs_totales': 0, 'kg_export': 0,
                        'usd_packing': 0, 'usd_frio': 0,
                        'usd_total': 0, 'clp_total': 0,
                        'moneda': moneda, 'tasa_usada': g['tasa'],
                        'tiene_manual': False,
                    }
                r = resultado[key]
                r['kgs_totales'] += ps['kgs']
                r['kg_export']   += ps['kg_export']
                r['usd_packing'] += ps['usd_packing']
                r['usd_frio']    += ps['usd_frio']
                r['usd_total']   += ps['usd_total']
                r['clp_total']   += ps['clp_total']
                if g['es_manual']:
                    r['tiene_manual'] = True
        else:
            key = esp if agrupar == 'especie' else MESES_NOMBRE.get(mes, f'Mes {mes}')
            if key not in resultado:
                resultado[key] = {
                    'periodo': key,
                    'kgs_totales': 0, 'kg_export': 0,
                    'usd_packing': 0, 'usd_frio': 0,
                    'usd_total': 0, 'clp_total': 0,
                    'moneda': moneda, 'tasa_usada': g['tasa'],
                    'tiene_manual': False,
                }
            r = resultado[key]
            r['kgs_totales'] += g['kgs']
            r['kg_export']   += g['kg_export']
            r['usd_packing'] += g['usd_packing']
            r['usd_frio']    += g['usd_frio']
            r['usd_total']   += g['usd_total']
            r['clp_total']   += g['clp_total']
            if g['es_manual']:
                r['tiene_manual'] = True

    return jsonify(list(resultado.values()))


@bp.route('/arbol')
@login_required
def arbol():
    """
    Jerarquia Mes -> Exportadora -> Especie con override de ingreso manual.
    El override se aplica una vez al total de cada grupo (exportadora, especie, mes),
    no fila a fila.
    """
    moneda = (request.args.get('moneda') or 'CLP').strip().upper()

    rows, tasas, unitarios, exportable = load_calc_data()
    ingreso_map = load_ingreso_data()

    # Pasada 1: acumular automatico por grupo
    grupos = acumular_grupos(rows, unitarios, exportable, tasas, moneda)
    # Pasada 2: aplicar overrides manuales
    aplicar_overrides_a_grupos(grupos, ingreso_map)

    # Pasada 3: construir arbol Mes → Exportadora → Especie desde grupos
    tree: dict = {}
    for (exportadora, esp, mes), g in grupos.items():
        mes = mes or 0
        tree.setdefault(mes, {})
        tree[mes].setdefault(exportadora, {})
        tree[mes][exportadora][esp] = {
            'kgs':        g['kgs'],
            'kg_export':  g['kg_export'],
            'usd_packing':g['usd_packing'],
            'usd_frio':   g['usd_frio'],
            'usd_total':  g['usd_total'],
            'clp_total':  g['clp_total'],
            'es_manual':  g['es_manual'],
        }

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
                for k in ('kgs', 'kg_export', 'usd_packing', 'usd_frio', 'usd_total', 'clp_total'):
                    exp_data[k] += d[k]
            mes_data['exportadoras'].append(exp_data)
            for k in ('kgs', 'kg_export', 'usd_packing', 'usd_frio', 'usd_total', 'clp_total'):
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
