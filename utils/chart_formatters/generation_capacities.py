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
    "Petroleum - OTH": "Petroleum",
    "Solar - PV": "Solar",
    "Solar - TH": "Solar",
    "Other Biomass": "Other",
    "Other Gas": "Other",
    "Other": "Other",
}


def build_generation_capacities_chart_data(rows, state: str, state_description: str | None,
    year_range: tuple[int, int] | None = None,) -> dict:
    """
    Transform rows for one state into a per-year stacked bar dataset.
    """

    # Find the aggregate rows where the source is 'All' so they can be excluded from the bar chart.
    # The breakdown of sources is more informative
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
        # Skip charting the rows where the source is 'All'.
        if _is_aggregate(row):
            continue
        period = int(row["period"])
        source_desc = row["energy_source_description"] or row["energy_source_id"] or "UNKNOWN"
        grouped_label = CATEGORY_MAP.get(source_desc, source_desc)
        source_labels[grouped_label] = grouped_label
        value = float(row["capability"]) if row["capability"] is not None else 0.0
        ability.setdefault(grouped_label, {})[period] = \
            ability.get(grouped_label, {}).get(period, 0.0) + value


    # Bundle sources with an average generation of less than 300MW across all years into the 'Other' category
    # These are hard to see in the stacked bar chart. Skip bundling if the state's total output is <2000MW,
    # at this point the small bars are not an issue
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
