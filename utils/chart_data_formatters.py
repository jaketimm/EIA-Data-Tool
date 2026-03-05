def build_yearly_source_disposition_chart_data(rows) -> dict:
    """
    Transform DB rows into parallel lists for Plotly.

    Line charts
    ───────────
    Left  — total_net_generation
    Right — total_imports  (interstate + international)
            total_exports  (interstate + international)

    Bar charts
    ──────────
    Left  — interstate import / export
    Right — international import / export
    """

    rows_by_year = sorted(rows, key=lambda row: row["period"])
    years = [row["period"] for row in rows_by_year]

    def get_int(row, column):
        value = row[column]
        return int(value) if value is not None else None

    # ── Net generation ─────────────────────────────────────────────
    total_net_generation = [
        get_int(row, "total_net_generation")
        for row in rows_by_year
    ]

    # ── Interstate trade (derived from net value) ──────────────────
    interstate_imports = []
    interstate_exports = []

    for row in rows_by_year:
        net_trade = get_int(row, "net_interstate_trade")

        if net_trade is None:
            interstate_imports.append(None)
            interstate_exports.append(None)
        else:
            interstate_imports.append(max(0, net_trade))
            # EIA represents exports as negative interstate trade
            interstate_exports.append(max(0, -net_trade))

    # ── International trade (already positive) ─────────────────────
    international_imports = [
        get_int(row, "total_international_imports")
        for row in rows_by_year
    ]

    international_exports = [
        get_int(row, "total_international_exports")
        for row in rows_by_year
    ]

    # ── Aggregate totals for line charts ───────────────────────────
    total_imports = []
    total_exports = []

    for idx in range(len(rows_by_year)):
        interstate_import = interstate_imports[idx] or 0
        international_import = international_imports[idx] or 0

        interstate_export = interstate_exports[idx] or 0
        international_export = international_exports[idx] or 0

        # Distinguish between missing data (None) and a real value of 0.
        # If both sources are None, keep None so charts show a gap instead of a false zero
        imports_all_none = (
            interstate_imports[idx] is None
            and international_imports[idx] is None
        )

        exports_all_none = (
            interstate_exports[idx] is None
            and international_exports[idx] is None
        )

        total_imports.append(
            None if imports_all_none else interstate_import + international_import
        )

        total_exports.append(
            None if exports_all_none else interstate_export + international_export
        )

    return {
        "years": years,

        # Line charts
        "total_net_generation": total_net_generation,
        "total_imports": total_imports,
        "total_exports": total_exports,

    }