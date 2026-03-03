def build_yearly_source_disposition_chart_data(rows) -> dict:
    """
    Transform DB rows into parallel lists for Plotly.

    Line charts (side by side)
    ──────────────────────────
    Left  — total_net_generation
    Right — total_imports  (interstate import + international imports)
            total_exports  (interstate export + international exports)
            Both always positive. Higher export = state sends more out.

    Bar charts (side by side, all values positive)
    ───────────────────────────────────────────────
    Left  — interstate import and interstate export per year
    Right — international imports and international exports per year
    """
    sorted_rows = sorted(rows, key=lambda r: r["period"])

    years = [r["period"] for r in sorted_rows]

    def _val(row, col):
        v = row[col]
        return int(v) if v is not None else None

    total_net_generation = [_val(r, "total_net_generation") for r in sorted_rows]

    # ── Derive interstate import / export (always >= 0) ───────────────────
    net_interstate_import = []
    net_interstate_export = []
    for r in sorted_rows:
        nit = _val(r, "net_interstate_trade")
        if nit is None:
            net_interstate_import.append(None)
            net_interstate_export.append(None)
        else:
            net_interstate_import.append(max(0, nit))
            net_interstate_export.append(max(0, -nit))

    # ── International (already positive or null) ──────────────────────────
    intl_imports = [_val(r, "total_international_imports") for r in sorted_rows]
    intl_exports = [_val(r, "total_international_exports") for r in sorted_rows]

    # ── Aggregated lines for the right line chart ─────────────────────────
    total_imports = []
    total_exports = []
    for i in range(len(sorted_rows)):
        imp_inter = net_interstate_import[i] or 0
        imp_intl = intl_imports[i] or 0
        exp_inter = net_interstate_export[i] or 0
        exp_intl = intl_exports[i] or 0

        # Preserve None only if ALL source values are None
        all_imp_none = net_interstate_import[i] is None and intl_imports[i] is None
        all_exp_none = net_interstate_export[i] is None and intl_exports[i] is None

        total_imports.append(None if all_imp_none else imp_inter + imp_intl)
        total_exports.append(None if all_exp_none else exp_inter + exp_intl)

    return {
        "years": years,
        # Line charts
        "total_net_generation": total_net_generation,
        "total_imports": total_imports,
        "total_exports": total_exports,
        # Bar charts
        "net_interstate_import": net_interstate_import,
        "net_interstate_export": net_interstate_export,
        "intl_imports": intl_imports,
        "intl_exports": intl_exports,
    }
