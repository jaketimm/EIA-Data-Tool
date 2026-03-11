def build_yearly_source_disposition_chart_data(rows) -> dict:
    """
    Transform DB rows into parallel lists for Plotly.

    Line charts
    ───────────
    Left  — total_net_generation
    Right — total_imports  (interstate + international)
            total_exports  (interstate + international)
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


def build_state_comparison_chart_data(rows, year: int) -> dict:
    """
    Transform one-year, all-state rows into Plotly-friendly lists.
    """
    def get_int(row, column):
        value = row[column]
        return int(value) if value is not None else 0

    generation_rows = sorted(
        rows,
        key=lambda row: get_int(row, "total_net_generation"),
        reverse=True,
    )

    generation_states = [row["state"] for row in generation_rows]
    generation_state_names = [row["state_description"] for row in generation_rows]
    total_generation = [get_int(row, "total_net_generation") for row in generation_rows]

    imports_by_state = []
    exports_by_state = []

    for row in rows:
        net_trade = get_int(row, "net_interstate_trade")
        international_imports = get_int(row, "total_international_imports")
        international_exports = get_int(row, "total_international_exports")

        interstate_imports = max(0, net_trade)
        # EIA represents exports as negative interstate trade
        interstate_exports = max(0, -net_trade)

        imports_by_state.append(
            {
                "state": row["state"],
                "state_description": row["state_description"],
                "total_imports": interstate_imports + international_imports,
            }
        )
        exports_by_state.append(
            {
                "state": row["state"],
                "state_description": row["state_description"],
                "total_exports": interstate_exports + international_exports,
            }
        )

    imports_rows = sorted(
        imports_by_state,
        key=lambda row: row["total_imports"],
        reverse=True,
    )
    exports_rows = sorted(
        exports_by_state,
        key=lambda row: row["total_exports"],
        reverse=True,
    )

    return {
        "year": year,
        "generation_states": generation_states,
        "generation_state_names": generation_state_names,
        "total_generation": total_generation,
        "import_states": [row["state"] for row in imports_rows],
        "import_state_names": [row["state_description"] for row in imports_rows],
        "total_imports": [row["total_imports"] for row in imports_rows],
        "export_states": [row["state"] for row in exports_rows],
        "export_state_names": [row["state_description"] for row in exports_rows],
        "total_exports": [row["total_exports"] for row in exports_rows],
    }


# Combine the generation data subcategories
CATEGORY_MAP = {
    "Natural Gas - CC": "Natural Gas",
    "Natural Gas - ST": "Natural Gas",
    "Natural Gas - IC": "Natural Gas",
    "Natural Gas - GT": "Natural Gas",
    "Natural Gas - OTH": "Natural Gas",
    "Petroleum - ST": "Petroleum",
    "Petroleum - IC": "Petroleum",
    "Petroleum - GT": "Petroleum",
    "Solar - PV": "Solar",
    "Other Biomass": "Other",
    "Other Gas": "Other",
    "Other": "Other",
}

def build_generation_capacities_chart_data(rows, state: str, state_description: str | None,
    year_range: tuple[int, int] | None = None,) -> dict:
    """
    Transform rows for one state into a per-year stacked bar dataset.
    """

    # Find the aggregate rows where the source is 'All'
    def _is_aggregate(row) -> bool:
        desc = (row["energy_source_description"] or "").strip().lower()
        source_id = (row["energy_source_id"] or "").strip().upper()
        return source_id == "ALL" or desc in {"all", "all?"}

    state_label = f"{state_description} ({state})" if state_description else state

    if not rows:
        return {
            "state": state,
            "state_label": state_label,
            "years": [],
            "sources": [],
            "unit": "MW",
        }

    ability = {}
    source_labels: dict[str, str] = {}

    for row in rows:
        # Skip charting the rows where the source is 'All'. The breakdown of sources is more informative
        if _is_aggregate(row):
            continue
        period = int(row["period"])
        source_desc = row["energy_source_description"] or row["energy_source_id"] or "UNKNOWN"
        grouped_label = CATEGORY_MAP.get(source_desc, source_desc)
        source_labels[grouped_label] = grouped_label
        value = float(row["capability"]) if row["capability"] is not None else 0.0
        ability.setdefault(grouped_label, {})[period] = \
            ability.get(grouped_label, {}).get(period, 0.0) + value


    # Bundle sources with an average generation of less than 200MW across all years into the 'Other' category
    # These are hard to see in the stacked bar chart and add clutter. Skip bundling if the state's total output is <2000MW
    overall_avg = sum(
        sum(vals.values()) / len(vals)
        for vals in ability.values()
    )

    # Bundle 'small_sources' into the 'Other' category
    if overall_avg >= 2000:
        avg_by_source = {
            source: sum(vals.values()) / len(vals)
            for source, vals in ability.items()
        }
        small_sources = {source for source, avg in avg_by_source.items() if avg < 300}

        for source in small_sources:
            for period, val in ability.pop(source).items():
                ability.setdefault("Other", {})[period] = \
                    ability.get("Other", {}).get(period, 0.0) + val
            source_labels.pop(source, None)

        source_labels["Other"] = "Other"

    if year_range:
        start_year, end_year = year_range
    else:
        periods = {int(row["period"]) for row in rows}
        start_year, end_year = min(periods), max(periods)

    years = list(range(start_year, end_year + 1))

    sources = []
    for source_id, source_desc in sorted(source_labels.items(), key=lambda item: item[1]):
        values = [ability.get(source_id, {}).get(year, 0.0) for year in years]
        sources.append({"id": source_id, "label": source_desc, "values": values})

    return {
        "state": state,
        "state_label": state_label,
        "years": years,
        "sources": sources,
        "unit": "MW",
    }
