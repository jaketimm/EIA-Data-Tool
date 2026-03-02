import json

from flask import Flask, render_template, request

from db.queries import (
    get_yearly_source_disposition,
    get_yearly_source_disposition_states,
    get_yearly_source_disposition_year_range,
)

app = Flask(__name__)


def _build_chart_data(rows) -> dict:
    """
    Transform DB rows into parallel lists for Plotly.

    Line chart series
    ─────────────────
    total_net_generation   — raw
    net_interstate_trade   — raw (positive = net importer, negative = net exporter)
    net_international_trade — derived: imports - exports (same sign convention)

    Stacked bar series
    ──────────────────
    Supply  (positive, stack above axis):
        total_net_generation
        net_interstate_import       — max(0,  net_interstate_trade)
        total_international_imports — raw

    Outflow (negative, stack below axis):
        net_interstate_export_neg       — min(0, -net_interstate_trade)  i.e. -(export amount)
        total_international_exports_neg — -(raw export value)
    """
    # Rows come back period DESC from the DB; charts read better chronologically
    sorted_rows = sorted(rows, key=lambda r: r["period"])

    years = [r["period"] for r in sorted_rows]

    def _val(row, col):
        """Return int value or None."""
        v = row[col]
        return int(v) if v is not None else None

    # ── Line chart ────────────────────────────────────────────────────────
    total_net_generation = [_val(r, "total_net_generation") for r in sorted_rows]
    net_interstate_trade = [_val(r, "net_interstate_trade") for r in sorted_rows]

    # net_international_trade: positive = net importer, negative = net exporter
    net_international_trade = []
    for r in sorted_rows:
        imp = _val(r, "total_international_imports") or 0
        exp = _val(r, "total_international_exports") or 0
        # Preserve None if both source columns are None
        if r["total_international_imports"] is None and r["total_international_exports"] is None:
            net_international_trade.append(None)
        else:
            net_international_trade.append(imp - exp)

    # ── Stacked bar ───────────────────────────────────────────────────────
    net_interstate_import = []
    net_interstate_export_neg = []
    for v in net_interstate_trade:
        if v is None:
            net_interstate_import.append(None)
            net_interstate_export_neg.append(None)
        else:
            net_interstate_import.append(max(0, v))        # positive portion
            net_interstate_export_neg.append(min(0, -v))   # negative portion

    total_international_imports = [_val(r, "total_international_imports") for r in sorted_rows]

    # Exports stored as negatives so Plotly stacks them below the axis
    total_international_exports_neg = [
        -_val(r, "total_international_exports")
        if _val(r, "total_international_exports") is not None
        else None
        for r in sorted_rows
    ]

    return {
        "years":                          years,
        "total_net_generation":           total_net_generation,
        "net_interstate_trade":           net_interstate_trade,
        "net_international_trade":        net_international_trade,
        "net_interstate_import":          net_interstate_import,
        "net_interstate_export_neg":      net_interstate_export_neg,
        "total_international_imports":    total_international_imports,
        "total_international_exports_neg": total_international_exports_neg,
    }


@app.route("/")
def index():
    states = get_yearly_source_disposition_states()
    min_year, max_year = get_yearly_source_disposition_year_range()

    selected_state = request.args.get("state", "")
    try:
        start_year = int(request.args.get("start_year", min_year))
        end_year   = int(request.args.get("end_year",   max_year))
    except ValueError:
        start_year, end_year = min_year, max_year

    rows = get_yearly_source_disposition(
        state=selected_state or None,
        start_year=start_year,
        end_year=end_year,
    ) if selected_state else []

    chart_data = _build_chart_data(rows) if rows else None

    # ── Console summary ───────────────────────────────────────────────────
    print(f"\n{'─' * 60}")
    print(f"  State    : {selected_state or 'none selected'}")
    print(f"  Years    : {start_year} – {end_year}")
    print(f"  Rows     : {len(rows)}")
    if rows:
        total_gen = sum(r["total_net_generation"] or 0 for r in rows)
        net_trade = sum(r["net_interstate_trade"] or 0 for r in rows)
        print(f"  Total net generation  : {total_gen:>20,.0f} MWh")
        print(f"  Sum net interstate    : {net_trade:>20,.0f} MWh")
    print(f"{'─' * 60}\n")

    return render_template(
        "index.html",
        rows=rows,
        states=states,
        selected_state=selected_state,
        start_year=start_year,
        end_year=end_year,
        min_year=min_year,
        max_year=max_year,
        chart_data=chart_data,
    )


if __name__ == "__main__":
    app.run(debug=True)