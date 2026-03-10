from flask import Flask, jsonify, render_template, request
import threading

from db.db import (
    get_yearly_source_disposition,
    get_yearly_state_comparison,
    get_yearly_source_disposition_states,
    get_yearly_source_disposition_year_range,
)
from utils.fetch_yearly_source_disposition_data import fetch_eia_source_data
from utils.fetch_yearly_generation_capacities_data import fetch_eia_capacities_data
from utils.chart_data_formatters import (
    build_state_comparison_chart_data,
    build_yearly_source_disposition_chart_data,
)
from utils.logger import get_logger
from utils.log_reader import read_log_records
logger = get_logger(__name__)

app = Flask(__name__)

_startup_lock = threading.Lock()
_startup_status = "pending"  # pending | running | ready | error
_startup_error: str | None = None


def _run_startup_fetch() -> None:
    global _startup_status, _startup_error

    try:
        fetch_eia_source_data()
        fetch_eia_capacities_data()
    except Exception as exc:
        logger.error("Startup fetch failed: %s", exc)
        with _startup_lock:
            _startup_status = "error"
            _startup_error = f"Startup fetch failed: {exc}"
    else:
        logger.info("Startup fetch completed successfully.")
        with _startup_lock:
            _startup_status = "ready"
            _startup_error = None


def _ensure_startup_fetch_started() -> None:
    global _startup_status

    with _startup_lock:
        if _startup_status in {"running", "ready", "error"}:
            return
        _startup_status = "running"

    logger.info("Starting background EIA data fetch.")
    worker = threading.Thread(target=_run_startup_fetch, daemon=True, name="eia-startup-fetch")
    worker.start()


def _get_startup_state() -> tuple[str, str | None]:
    with _startup_lock:
        return _startup_status, _startup_error


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
                next_path=request.full_path.rstrip("?"),
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
        logger.info(
            "Invalid year range params — falling back to full range (%s–%s).",
            min_year,
            max_year,
        )
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

    chart_data = build_yearly_source_disposition_chart_data(rows) if rows else None

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


@app.route("/state-comparison")
def state_comparison():
    _ensure_startup_fetch_started()
    startup_status, startup_error = _get_startup_state()

    if startup_status != "ready":
        return (
            render_template(
                "loading.html",
                startup_status=startup_status,
                startup_error=startup_error,
                next_path=request.full_path.rstrip("?"),
            ),
            202,
        )

    min_year, max_year = get_yearly_source_disposition_year_range()
    try:
        selected_year = int(request.args.get("year", max_year))
    except ValueError:
        selected_year = max_year

    selected_year = max(min_year, min(max_year, selected_year))

    return render_template(
        "state_comparison.html",
        selected_year=selected_year,
        min_year=min_year,
        max_year=max_year,
    )


@app.route("/api/state-comparison-data")
def state_comparison_data():
    _ensure_startup_fetch_started()
    startup_status, startup_error = _get_startup_state()
    if startup_status != "ready":
        return (
            jsonify(
                {
                    "error": "Data is still being prepared.",
                    "status": startup_status,
                    "details": startup_error,
                }
            ),
            503,
        )

    min_year, max_year = get_yearly_source_disposition_year_range()
    year_raw = request.args.get("year")
    if year_raw is None:
        year = max_year
    else:
        try:
            year = int(year_raw)
        except ValueError:
            return jsonify({"error": "Invalid year parameter."}), 400

    if year < min_year or year > max_year:
        return (
            jsonify(
                {"error": f"Year must be between {min_year} and {max_year}."}
            ),
            400,
        )

    rows = get_yearly_state_comparison(year)
    chart_data = build_state_comparison_chart_data(rows, year)
    return jsonify(chart_data)


@app.route("/startup-status")
def startup_status():
    _ensure_startup_fetch_started()
    status, error = _get_startup_state()
    return {"status": status, "ready": status == "ready", "error": error}


@app.route("/logs")
def logs():
    level = request.args.get("level") or None
    search = request.args.get("q") or None

    try:
        limit = int(request.args.get("limit", 25))
    except ValueError:
        limit = 25
    limit = max(10, min(limit, 1000))

    records = read_log_records(limit=limit, level=level, search=search)

    return render_template(
        "logs.html",
        records=records,
        level=level,
        search=search,
        limit=limit,
    )


if __name__ == "__main__":
    app.run(debug=True)
