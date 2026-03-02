from flask import Flask, render_template, request
import threading

from db.db import (
    get_yearly_source_disposition,
    get_yearly_source_disposition_states,
    get_yearly_source_disposition_year_range,
)

from utils.fetch_yearly_source_disposition_data import fetch_eia_source_data

app = Flask(__name__)

_startup_lock = threading.Lock()
_startup_status = "pending"  # pending | running | ready | error
_startup_error: str | None = None


def _run_startup_fetch() -> None:
    global _startup_status, _startup_error

    try:
        fetch_eia_source_data()
    except SystemExit as exc:
        with _startup_lock:
            _startup_status = "error"
            _startup_error = f"Startup fetch exited early (code: {exc.code}). Check EIA_API_KEY and logs."
    except Exception as exc:  # noqa: BLE001
        with _startup_lock:
            _startup_status = "error"
            _startup_error = f"Startup fetch failed: {exc}"
    else:
        with _startup_lock:
            _startup_status = "ready"
            _startup_error = None


def _ensure_startup_fetch_started() -> None:
    global _startup_status

    with _startup_lock:
        if _startup_status in {"running", "ready", "error"}:
            return
        _startup_status = "running"

    worker = threading.Thread(target=_run_startup_fetch, daemon=True, name="eia-startup-fetch")
    worker.start()


def _get_startup_state() -> tuple[str, str | None]:
    with _startup_lock:
        return _startup_status, _startup_error


def _build_chart_data(rows) -> dict:
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
    Left  — interstate import / interstate export per year
    Right — international imports / international exports per year
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


@app.route("/")
def index():
    _ensure_startup_fetch_started()
    startup_status, startup_error = _get_startup_state()

    if startup_status != "ready":
        return (
            render_template(
                "loading.html",
                startup_status=startup_status,
                startup_error=startup_error,
            ),
            202,
        )

    states = get_yearly_source_disposition_states()
    min_year, max_year = get_yearly_source_disposition_year_range()

    selected_state = request.args.get("state", "")
    try:
        start_year = int(request.args.get("start_year", min_year))
        end_year = int(request.args.get("end_year", max_year))
    except ValueError:
        start_year, end_year = min_year, max_year

    rows = (
        get_yearly_source_disposition(
            state=selected_state or None,
            start_year=start_year,
            end_year=end_year,
        )
        if selected_state
        else []
    )

    chart_data = _build_chart_data(rows) if rows else None

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


@app.route("/startup-status")
def startup_status():
    _ensure_startup_fetch_started()
    status, error = _get_startup_state()
    return {"status": status, "ready": status == "ready", "error": error}


if __name__ == "__main__":
    app.run(debug=True)
