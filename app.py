from flask import Flask, jsonify, render_template, Response, request
import csv, io
import threading

from db.source_disposition import(
    get_yearly_source_disposition,
    get_yearly_state_comparison,
    get_yearly_source_disposition_states,
    get_yearly_source_disposition_year_range)

from db.generation_capacities import(
    get_generation_capacities_state_list,
    get_generation_capacities_year_range,
    get_generation_capacities_for_state,
    get_generation_capacities_national)

from utils.eia_api.fetch_yearly_source_disposition_data import fetch_eia_source_data
from utils.eia_api.fetch_yearly_generation_capacities_data import fetch_eia_capacities_data
from utils.chart_formatters.source_disposition import (
    build_state_comparison_chart_data,
    build_yearly_source_disposition_chart_data
)
from utils.chart_formatters.generation_capacities import (
    build_state_capacities_chart_data,
    build_national_capacities_chart_data)

from utils.logger import get_logger
from utils.log_reader import read_log_records
logger = get_logger(__name__)

app = Flask(__name__)

_startup_lock = threading.Lock()
_startup_status = "pending"  # pending | running | ready | error
_startup_error: str | None = None


# True: bypass the EIA APIs and use the data in the existing db/eia.db
# False: Fetch fresh data from the EIA APIs - requires a .env file with an API key.
# Will check data for freshness on subsequent runs and update it every 30 days.
SKIP_FETCH = False


def _run_startup_fetch() -> None:
    global _startup_status, _startup_error

    # If SKIP_FETCH is True, use the existing database and mark startup as ready.
    if SKIP_FETCH:
        logger.info("SKIP_FETCH enabled — using existing database.")
        with _startup_lock:
            _startup_status = "ready"
        return
    
    # If SKIP_FETCH is False, fetch fresh data (if stale)
    logger.info("Fetching fresh data from EIA APIs.")
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
    """Main page showing yearly source disposition data with filters for state and year range."""
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

    selected_state = request.args.get("state", states[0]["state"] if states else "")
    start_year = int(request.args.get("start_year", min_year))
    end_year = int(request.args.get("end_year", max_year))

    rows = (
        get_yearly_source_disposition(
            state=selected_state,
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
    """Page showing comparison of net generation, imports, and exports across states for a given year. Includes a dropdown to select the year."""
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
    selected_year = int(request.args.get("year", max_year))

    return render_template(
        "state_comparison.html",
        selected_year=selected_year,
        min_year=min_year,
        max_year=max_year,
    )


@app.route("/api/state-comparison")
def state_comparison_data():
    """API endpoint to fetch data for the state comparison charts for a given year."""
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

    # API can be called directly, so validate input; routes use UI-constrained selector.
    min_year, max_year = get_yearly_source_disposition_year_range()
    year_raw = request.args.get("year")
    if year_raw is None:
        year = max_year
    else:
        try:
            year = int(year_raw)
        except ValueError:
            return jsonify({"error": "Invalid year parameter."}), 400

    rows = get_yearly_state_comparison(year)
    chart_data = build_state_comparison_chart_data(rows, year)
    return jsonify(chart_data)


@app.route("/generation-capacities-state")
def generation_capacities():
    """Page showing generation capacities by source for a given state and year range, with filters to select the state and years."""
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

    states = get_generation_capacities_state_list()
    if not states:
        return render_template(
            "generation_capacities_state.html",
            states=[],
            selected_state="",
            min_year=0,
            max_year=0,
        )

    selected_state = request.args.get("state") or states[0]["state"]
    selected_state = selected_state.upper()

    min_year, max_year = get_generation_capacities_year_range()

    start_year = int(request.args.get("start_year", min_year))
    end_year = int(request.args.get("end_year", max_year))

    if start_year > end_year:
        start_year, end_year = end_year, start_year

    return render_template(
        "generation_capacities_state.html",
        states=states,
        selected_state=selected_state,
        start_year=start_year,
        end_year=end_year,
        min_year=min_year,
        max_year=max_year,
    )


@app.route("/api/generation-capacities/state")
def generation_capacities_data():
    """API endpoint to fetch generation capacities data for a given state and year range."""
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

    states = get_generation_capacities_state_list()
    if not states:
        return jsonify({"error": "No generation capacity data is available."}), 404

    # API can be called directly, so validate inputs; routes use UI-constrained selectors.
    selected_state = request.args.get("state")
    if not selected_state:
        selected_state = states[0]["state"]
    selected_state = selected_state.upper()

    min_year, max_year = get_generation_capacities_year_range(selected_state)

    try:
        start_year = int(request.args.get("start_year") or min_year)
    except ValueError:
        return jsonify({"error": "Invalid start_year parameter."}), 400
    try:
        end_year = int(request.args.get("end_year") or max_year)
    except ValueError:
        return jsonify({"error": "Invalid end_year parameter."}), 400

    if start_year > end_year:
        start_year, end_year = end_year, start_year

    rows = get_generation_capacities_for_state(
        selected_state,
        start_year=start_year,
        end_year=end_year,
    )

    chart_data = build_state_capacities_chart_data(
        rows,
        state=selected_state,
        state_description=rows[0]["state_description"] if rows else "",
        year_range=(start_year, end_year),
    )
    chart_data.update({"start_year": start_year, "end_year": end_year})
    return jsonify(chart_data)


@app.route("/generation-capacities-national")
def generation_capacities_national():
    """Page showing national generation capacities by source for a given year. Includes a dropdown to select the year."""
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

    min_year, max_year = get_generation_capacities_year_range()
    selected_year = int(request.args.get("year", max_year))

    rows = get_generation_capacities_national(selected_year)

    return render_template(
        "generation_capacities_national.html",
        rows=rows,
        selected_year=selected_year,
        min_year=min_year,
        max_year=max_year,
    )


@app.route("/api/generation-capacities/national")
def generation_capacities_national_api():
    """API endpoint to fetch national generation capacities data for a given year."""
    _ensure_startup_fetch_started()
    startup_status, startup_error = _get_startup_state()

    if startup_status != "ready":
        return (
            jsonify({
                "error": "Data is still being prepared.",
                "status": startup_status,
                "details": startup_error,
            }),
            503,
        )

    min_year, max_year = get_generation_capacities_year_range()

    # API can be called directly, so validate input; routes use UI-constrained selector.
    year_raw = request.args.get("year")
    if year_raw is None:
        year = max_year
    else:
        try:
            year = int(year_raw)
        except ValueError:
            return jsonify({"error": "Invalid year parameter."}), 400

    rows = get_generation_capacities_national(year)

    chart_data = build_national_capacities_chart_data(
        rows, year
    )
    return jsonify(chart_data)


@app.route("/startup-status")
def startup_status():
    """Endpoint to check the status of the initial data fetch on app startup."""
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