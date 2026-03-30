"""
services/calculos.py — Logica de calculo de presupuesto.

Temporada agricola: Nov → Oct  (meses 11,12,1,2,3,4,5,6,7,8,9,10)
                    Semanas 44→52, luego 1→43

Unitarios: precio por (exportadora, especie)
  Lookup: (exportadora, especie) → {packing, frio}
  Si no existe la combinacion, usa 0.
"""

from datetime import datetime
from database import get_db
from config   import CFG

# ── Constantes ────────────────────────────────────────────────────────
MESES_NOMBRE: dict[int, str] = {
    1: "Enero",    2: "Febrero",  3: "Marzo",     4: "Abril",
    5: "Mayo",     6: "Junio",    7: "Julio",      8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}
MESES_ABREV: dict[int, str] = {k: v[:3].upper() for k, v in MESES_NOMBRE.items()}

# Orden de temporada agricola Nov → Oct
ORDEN_MESES_TEMPORADA: list[int] = [11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

def sort_mes_temporada(mes: int) -> int:
    try:
        return ORDEN_MESES_TEMPORADA.index(mes)
    except ValueError:
        return 99

def sort_semana_temporada(semana: int) -> int:
    if semana is None:
        return 999
    return semana if semana >= 44 else semana + 100


# ── Helpers de BD ─────────────────────────────────────────────────────
def next_version() -> int:
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(db.norm(
            f"SELECT MAX(numero_version) FROM {schema}.ppto_version_log"
        ))
        val = cur.fetchone()[0]
    return (val or 0) + 1


def load_calc_data() -> tuple[list, list, dict, dict]:
    """
    Carga desde BD:
      rows       → filas de ppto_estimacion
      tasas      → tasas activas
      unitarios  → {(exportadora, especie): {packing, frio}}
      exportable → {especie: porcentaje_decimal}
    """
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()

        cur.execute(f"SELECT * FROM {schema}.ppto_estimacion")
        rows = db.fetchall_dicts(cur)

        cur.execute(db.norm(
            f"SELECT * FROM {schema}.ppto_tasas_cambio WHERE activo=?"
        ), (1,))
        tasas = db.fetchall_dicts(cur)

        # Unitarios ahora por (exportadora, especie)
        cur.execute(
            f"SELECT exportadora, especie, precio_usd_packing, precio_usd_frio "
            f"FROM {schema}.ppto_unitarios"
        )
        unitarios = {
            (r['exportadora'], r['especie']): {
                'packing': r['precio_usd_packing'],
                'frio':    r['precio_usd_frio'],
            }
            for r in db.fetchall_dicts(cur)
        }

        cur.execute(
            f"SELECT especie, porcentaje FROM {schema}.ppto_exportable_pct"
        )
        exportable = {r['especie']: r['porcentaje'] for r in db.fetchall_dicts(cur)}

    return rows, tasas, unitarios, exportable


def load_ingreso_data() -> dict:
    """
    Carga los ingresos manuales USD desde ppto_ingreso_usd.
    Devuelve dict: {(exportadora, especie, mes): {usd_packing, usd_frio, usd_total}}
    Solo toma la temporada mas reciente si hay multiples para la misma combinacion.
    """
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT exportadora, especie, mes,
                   usd_packing, usd_frio, usd_total
            FROM {schema}.ppto_ingreso_usd
            ORDER BY temporada DESC, actualizado_en DESC
        """)
        rows = db.fetchall_dicts(cur)

    # Si hay duplicados (distintas temporadas para la misma combinacion),
    # el ORDER BY garantiza que el mas reciente queda primero → usamos setdefault
    ingreso_map = {}
    for r in rows:
        key = (r['exportadora'], r['especie'], r['mes'])
        ingreso_map.setdefault(key, {
            'usd_packing': float(r['usd_packing'] or 0),
            'usd_frio':    float(r['usd_frio']    or 0),
            'usd_total':   float(r['usd_total']   or 0),
        })
    return ingreso_map


def aplicar_ingreso_manual(calc: dict, ingreso_map: dict,
                            exportadora: str, especie: str, mes: int) -> dict:
    """
    Si existe un ingreso manual para (exportadora, especie, mes),
    reemplaza los valores USD calculados.
    Los kgs y kg_export NO cambian (siguen siendo del Excel).
    El clp_total se recalcula con el usd_total manual × tasa.
    """
    key = (exportadora, especie, mes)
    if key not in ingreso_map:
        return calc   # sin override, devuelve el calculo normal

    ing = ingreso_map[key]
    tasa = calc['tasa']
    manual = dict(calc)
    manual['usd_packing'] = ing['usd_packing']
    manual['usd_frio']    = ing['usd_frio']
    manual['usd_total']   = ing['usd_total']
    manual['clp_total']   = ing['usd_total'] * tasa if tasa else 0.0
    manual['es_manual']   = True
    return manual


# ── Calculo de tasa ───────────────────────────────────────────────────
def get_tasa_for_row(tasas, fecha_str, semana, mes, moneda='CLP'):
    tasas_m = [t for t in tasas if (t.get('moneda') or 'CLP').upper() == moneda.upper()]

    fecha = None
    if fecha_str:
        try:
            fecha = datetime.strptime(str(fecha_str)[:10], '%Y-%m-%d')
        except (ValueError, TypeError):
            pass

    prioridad = {'rango': 0, 'semanal': 1, 'mensual': 2, 'anual': 3}

    for t in sorted(tasas_m, key=lambda x: prioridad.get(x.get('tipo', ''), 9)):
        tipo = t.get('tipo', '')
        if tipo == 'rango' and fecha and t.get('fecha_inicio') and t.get('fecha_fin'):
            try:
                fi = datetime.strptime(str(t['fecha_inicio'])[:10], '%Y-%m-%d')
                ff = datetime.strptime(str(t['fecha_fin'])[:10],    '%Y-%m-%d')
                if fi <= fecha <= ff:
                    return float(t['valor'])
            except (ValueError, TypeError):
                pass
        elif tipo == 'semanal' and t.get('semana') is not None and t.get('semana') == semana:
            return float(t['valor'])
        elif tipo == 'mensual' and t.get('mes') is not None and t.get('mes') == mes:
            return float(t['valor'])
        elif tipo == 'anual':
            return float(t['valor'])

    return None


# ── Calculo de una fila ───────────────────────────────────────────────
def calcular_fila(row, unitarios, exportable, tasas, moneda='CLP'):
    """
    Calcula valores derivados de una fila de estimacion.
    unitarios: dict con clave (exportadora, especie) → {packing, frio}
    Si no existe la combinacion, usa 0.
    """
    esp         = row.get('especie')     or ''
    exportadora = row.get('exportadora') or ''
    kgs         = float(row.get('kgs_a_proc') or 0)
    semana      = row.get('semana')
    mes         = row.get('mes')
    fecha       = str(row['fecha'])[:10] if row.get('fecha') else None

    # Buscar precio por (exportadora, especie); si no hay, usar 0
    u = unitarios.get((exportadora, esp), {})

    pct            = float(exportable.get(esp) or 0)
    precio_packing = float(u.get('packing') or 0)
    precio_frio    = float(u.get('frio')    or 0)
    tasa           = get_tasa_for_row(tasas, fecha, semana, mes, moneda)

    kg_export   = kgs * pct
    usd_packing = kgs       * precio_packing
    usd_frio    = kg_export * precio_frio
    usd_total   = usd_packing + usd_frio
    clp_total   = usd_total * tasa if tasa is not None else 0.0

    return {
        'kgs':            kgs,
        'pct':            pct,
        'kg_export':      kg_export,
        'precio_packing': precio_packing,
        'precio_frio':    precio_frio,
        'usd_packing':    usd_packing,
        'usd_frio':       usd_frio,
        'usd_total':      usd_total,
        'tasa':           tasa,
        'clp_total':      clp_total,
        'moneda':         moneda,
    }
