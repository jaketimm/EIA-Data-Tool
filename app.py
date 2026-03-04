from flask import Flask, render_template, request
import threading

from db.db import (
    get_yearly_source_disposition,
    get_yearly_source_disposition_states,
    get_yearly_source_disposition_year_range,
)

from utils.fetch_yearly_source_disposition_data import fetch_eia_source_data
from utils.chart_data_formatters import build_yearly_source_disposition_chart_data
from utils.logger import get_logger

logger = get_logger(__name__)

app = Flask(__name__)

_startup_lock = threading.Lock()
_startup_status = "pending"  # pending | running | ready | error
_startup_error: str | None = None


def _run_startup_fetch() -> None:
    global _startup_status, _startup_error

    try:
        fetch_eia_source_data()
    except SystemExit as exc:
        msg = f"Startup fetch exited early (code: {exc.code}). Check EIA_API_KEY and logs."
        logger.error(msg)
        with _startup_lock:
            _startup_status = "error"
            _startup_error = msg
    except Exception as exc:  # noqa: BLE001
        msg = f"Startup fetch failed: {exc}"
        logger.error(msg)
        with _startup_lock:
            _startup_status = "error"
            _startup_error = msg
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


@app.route("/startup-status")
def startup_status():
    _ensure_startup_fetch_started()
    status, error = _get_startup_state()
    return {"status": status, "ready": status == "ready", "error": error}


if __name__ == "__main__":
    app.run(debug=True)