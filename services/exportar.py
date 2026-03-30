"""
services/exportar.py — Generación del archivo Excel de presupuesto.

Función principal: build_excel() → BytesIO listo para send_file().
"""

import io
import xlsxwriter
from services.calculos import calcular_fila, MESES_ABREV, MESES_NOMBRE, sort_mes_temporada


def build_excel(
    rows:       list[dict],
    tasas:      list[dict],
    unitarios:  dict,
    exportable: dict,
    ver_row,
    moneda:     str = 'CLP',
    ingreso_map: dict | None = None,
) -> io.BytesIO:
    """
    Construye el Excel con 3 hojas:
      KilosMensual  → especie × mes con 6 métricas
      Detalle       → fila por fila de la estimación
      Tasas         → todas las tasas configuradas
    """
    output = io.BytesIO()
    wb     = xlsxwriter.Workbook(output)

    # ── Formatos ──────────────────────────────────────────────────────
    def F(p): return wb.add_format(p)
    fmt = {
        'title':   F({'bold': True, 'font_size': 13, 'bg_color': '#1a3a5c',
                      'font_color': 'white', 'valign': 'vcenter'}),
        'header':  F({'bold': True, 'bg_color': '#2d6a9f', 'font_color': 'white',
                      'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True}),
        'subhdr':  F({'bold': True, 'bg_color': '#c5d9f1', 'border': 1,
                      'align': 'center', 'valign': 'vcenter'}),
        'text':    F({'border': 1}),
        'num':     F({'num_format': '#,##0', 'border': 1}),
        'dec':     F({'num_format': '#,##0.00', 'border': 1}),
        'pct':     F({'num_format': '0.0%', 'border': 1}),
        'usd_pk':  F({'num_format': '$#,##0.00', 'border': 1, 'bg_color': '#dbeafe'}),
        'usd_fr':  F({'num_format': '$#,##0.00', 'border': 1, 'bg_color': '#e0f2fe'}),
        'usd_tot': F({'num_format': '$#,##0.00', 'border': 1, 'bg_color': '#ede9fe', 'bold': True}),
        'mon':     F({'num_format': '#,##0', 'border': 1, 'bg_color': '#e2efda'}),
        'tot':     F({'bold': True, 'bg_color': '#ffd966', 'num_format': '#,##0', 'border': 1}),
        'tot_upk': F({'bold': True, 'bg_color': '#ffd966', 'num_format': '$#,##0.00', 'border': 1}),
        'tot_ufr': F({'bold': True, 'bg_color': '#ffd966', 'num_format': '$#,##0.00', 'border': 1}),
        'tot_ut':  F({'bold': True, 'bg_color': '#ffd966', 'num_format': '$#,##0.00', 'border': 1}),
        'tot_mon': F({'bold': True, 'bg_color': '#ffd966', 'num_format': '#,##0', 'border': 1}),
        'tot_txt': F({'bold': True, 'bg_color': '#ffd966', 'border': 1}),
    }

    # ── Data map: especie → mes → métricas ───────────────────────────
    data_map: dict = {}
    from services.calculos import aplicar_ingreso_manual
    _ing = ingreso_map or {}

    for row in rows:
        esp         = row.get('especie')     or 'Sin Especie'
        exportadora = row.get('exportadora') or ''
        mes         = row.get('mes') or 0
        c   = calcular_fila(row, unitarios, exportable, tasas, moneda)
        c   = aplicar_ingreso_manual(c, _ing, exportadora, esp, mes)
        data_map.setdefault(esp, {})
        data_map[esp].setdefault(mes, {'kgs': 0, 'ke': 0, 'upk': 0, 'ufr': 0, 'ut': 0, 'mon': 0})
        m = data_map[esp][mes]
        m['kgs'] += c['kgs'];  m['ke']  += c['kg_export']
        m['upk'] += c['usd_packing']; m['ufr'] += c['usd_frio']
        m['ut']  += c['usd_total'];   m['mon'] += c['clp_total']

    all_meses = sorted({mes for v in data_map.values() for mes in v}, key=sort_mes_temporada)
    especies  = sorted(data_map.keys())
    n         = len(all_meses)

    ver_str = (f"V{ver_row[0]}  |  {ver_row[1]}  |  {ver_row[2]}"
               if ver_row else "Sin versión")

    # ── Hoja 1: KilosMensual ──────────────────────────────────────────
    ws1 = wb.add_worksheet('KilosMensual')
    ws1.set_tab_color('#1a3a5c')
    ws1.set_zoom(85)
    ws1.set_row(0, 22)
    ws1.merge_range(0, 0, 0, 2 + n,
                    f'ESTIMACIÓN DE INGRESOS  —  {ver_str}  |  Moneda: {moneda}',
                    fmt['title'])
    ws1.write(1, 0, 'Especie',  fmt['header'])
    ws1.write(1, 1, 'Métrica',  fmt['header'])
    for j, m in enumerate(all_meses):
        ws1.write(1, 2 + j, MESES_ABREV.get(m, str(m)), fmt['header'])
    ws1.write(1, 2 + n, 'TOTAL', fmt['header'])
    ws1.set_row(1, 28)
    ws1.set_column(0, 0, 20)
    ws1.set_column(1, 1, 18)
    ws1.set_column(2, 2 + n, 14)

    METRICS = [
        ('Kgs a Proc',      'kgs', fmt['num'],     fmt['tot']),
        ('Kg Export',       'ke',  fmt['num'],     fmt['tot']),
        ('USD Packing',     'upk', fmt['usd_pk'],  fmt['tot_upk']),
        ('USD Frío',        'ufr', fmt['usd_fr'],  fmt['tot_ufr']),
        ('USD Total',       'ut',  fmt['usd_tot'], fmt['tot_ut']),
        (f'Ingreso {moneda}','mon', fmt['mon'],    fmt['tot_mon']),
    ]

    r       = 2
    tot_k   = {m: 0 for m in all_meses}
    tot_upk = {m: 0 for m in all_meses}
    tot_ufr = {m: 0 for m in all_meses}
    tot_ut  = {m: 0 for m in all_meses}
    tot_mon = {m: 0 for m in all_meses}

    for esp in especies:
        md = data_map[esp]
        ws1.merge_range(r, 0, r + 5, 0, esp, fmt['subhdr'])
        for i, (label, key, cf, _tf) in enumerate(METRICS):
            vals = [md.get(m, {}).get(key, 0) for m in all_meses]
            ws1.write(r + i, 1, label, fmt['text'])
            for j, v in enumerate(vals):
                ws1.write(r + i, 2 + j, v, cf)
            ws1.write(r + i, 2 + n, sum(vals), _tf)
        for j, m in enumerate(all_meses):
            tot_k[m]   += md.get(m, {}).get('kgs', 0)
            tot_upk[m] += md.get(m, {}).get('upk', 0)
            tot_ufr[m] += md.get(m, {}).get('ufr', 0)
            tot_ut[m]  += md.get(m, {}).get('ut',  0)
            tot_mon[m] += md.get(m, {}).get('mon', 0)
        r += 6

    for lbl, d, tf in [
        ('Kgs a Proc',       tot_k,   fmt['tot']),
        ('USD Packing',      tot_upk, fmt['tot_upk']),
        ('USD Frío',         tot_ufr, fmt['tot_ufr']),
        ('USD Total',        tot_ut,  fmt['tot_ut']),
        (f'Ingreso {moneda}',tot_mon, fmt['tot_mon']),
    ]:
        ws1.write(r, 0, 'TOTAL GENERAL' if lbl == 'Kgs a Proc' else '', fmt['tot_txt'])
        ws1.write(r, 1, lbl, tf)
        for j, m in enumerate(all_meses):
            ws1.write(r, 2 + j, d[m], tf)
        ws1.write(r, 2 + n, sum(d.values()), tf)
        r += 1

    # ── Hoja 2: Detalle ───────────────────────────────────────────────
    ws2 = wb.add_worksheet('Detalle')
    ws2.set_tab_color('#27ae60')
    ws2.freeze_panes(1, 0)

    hd = ['Temporada', 'Exportadora', 'Especie', 'Variedad', 'Productor', 'Grupo',
          'Fecha', 'Semana', 'Mes', 'Envase', 'Kgs a Proc', '% Export', 'Kg Export',
          'P.Unit Packing', 'USD Packing', 'P.Unit Frío', 'USD Frío',
          'USD Total', f'Tasa {moneda}/USD', f'Ingreso {moneda}']
    wd = [10, 14, 16, 14, 22, 12, 12, 8, 6, 10, 13, 9, 13, 13, 13, 12, 12, 13, 12, 14]

    for i, (h, w) in enumerate(zip(hd, wd)):
        ws2.write(0, i, h, fmt['header'])
        ws2.set_column(i, i, w)

    fmts_ = ([fmt['text']] * 6 +
              [fmt['text'], fmt['num'], fmt['text'], fmt['text'],
               fmt['num'], fmt['pct'], fmt['num'],
               fmt['dec'], fmt['usd_pk'],
               fmt['dec'], fmt['usd_fr'],
               fmt['usd_tot'], fmt['dec'], fmt['mon']])

    for rn, row in enumerate(rows, 1):
        c = calcular_fila(row, unitarios, exportable, tasas, moneda)
        vals = [
            row.get('temporada'), row.get('exportadora'),
            row.get('especie'),   row.get('variedad'),
            row.get('productor'), row.get('grupo'),
            str(row['fecha'])[:10] if row.get('fecha') else '',
            row.get('semana'),
            MESES_ABREV.get(row.get('mes'), row.get('mes')),
            row.get('envase'),
            c['kgs'], c['pct'], c['kg_export'],
            c['precio_packing'], c['usd_packing'],
            c['precio_frio'],    c['usd_frio'],
            c['usd_total'], c['tasa'] or 0, c['clp_total'],
        ]
        for i, (v, f) in enumerate(zip(vals, fmts_)):
            ws2.write(rn, i, v if v is not None else '', f)

    # ── Hoja 3: Tasas ─────────────────────────────────────────────────
    ws3 = wb.add_worksheet('Tasas')
    ws3.set_tab_color('#e67e22')

    hd3 = ['Moneda', 'Nombre', 'Tipo', 'Valor/USD', 'Año', 'Mes', 'Semana', 'Desde', 'Hasta']
    for i, h in enumerate(hd3):
        ws3.write(0, i, h, fmt['header'])
        ws3.set_column(i, i, 15)

    for j, t in enumerate(tasas):
        ws3.write(j + 1, 0, t.get('moneda', 'CLP'), fmt['text'])
        ws3.write(j + 1, 1, t.get('nombre', ''),    fmt['text'])
        ws3.write(j + 1, 2, t.get('tipo', ''),      fmt['text'])
        ws3.write(j + 1, 3, t.get('valor', 0),      fmt['dec'])
        ws3.write(j + 1, 4, t.get('anio') or '',    fmt['text'])
        ws3.write(j + 1, 5,
                  MESES_ABREV.get(t.get('mes'), '') if t.get('mes') else '',
                  fmt['text'])
        ws3.write(j + 1, 6, t.get('semana') or '', fmt['text'])
        ws3.write(j + 1, 7, str(t.get('fecha_inicio') or '')[:10], fmt['text'])
        ws3.write(j + 1, 8, str(t.get('fecha_fin')    or '')[:10], fmt['text'])

    wb.close()
    output.seek(0)
    return output
