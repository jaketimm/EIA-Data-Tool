const config = window.nationalCapacitiesConfig || {};
const yearSelect = document.getElementById("national-year");
const errorEl = document.getElementById("national-error");

if (!yearSelect || typeof Plotly === "undefined") {
  console.warn("Year selector or Plotly missing; skipping render.");
} else {

  // Color palette and category mapping to renewables, non-renewables
  const RENEWABLES = new Set(["Solar", "Wind", "Hydroelectric", "Wood", "Biomass", "Geothermal"]);
  const NON_RENEWABLES = new Set(["Natural Gas", "Coal", "Petroleum", "Nuclear", "Pumped Storage"]);

  const CATEGORY_COLORS = {
    "Coal": "#4a4a4a",
    "Natural Gas": "#5E81AC",
    "Petroleum": "#7a5450",
    "Nuclear": "#b85c5b",
    "Solar": "#c9a03a",
    "Wind": "#5a9e9a",
    "Hydroelectric": "#3a7a9c",
    "Wood": "#4e8a45",
    "Battery": "#8a6e9e",
    "Pumped Storage": "#2a4f6e",
    "Other": "#7a8490",
    "Biomass": "#bdaebf",
    "Geothermal": "#a986ad"
  };

  // Shared chart layout and settings
  const plotCfg = {
    responsive: true,
    displayModeBar: "hover",
    displaylogo: false,
    modeBarButtonsToRemove: [
      "pan2d", "select2d", "lasso2d", "zoomIn2d",
      "zoomOut2d", "resetScale2d", "hoverClosestCartesian",
      "hoverCompareCartesian",
    ],
  };

  function pieLayout() {
    return {
      font: { family: "system-ui, sans-serif", size: 12 },
      paper_bgcolor: "rgba(255,255,255,1)",
      plot_bgcolor: "rgba(255,255,255,1)",
      margin: { t: 40, r: 20, b: 1, l: 20 },
      hoverlabel: {
        bgcolor: "#ffffff",
        bordercolor: "#dee2e6",
        font: { color: "#212529", size: 12 },
      },
      legend: {
        orientation: "h",
        yanchor: "top",
        y: -0.2,
        xanchor: "center",
        x: 0.5,
        traceorder: "normal",
      },
      modebar: {
        color: "#6c757d",
        activecolor: "#212529",
      },
    };
  }

  function clearError() {
    errorEl.textContent = "";
    errorEl.classList.add("d-none");
  }

  function showError(message) {
    errorEl.textContent = message;
    errorEl.classList.remove("d-none");
  }

  // Fetch data for the selected year when the dropdown changes
  async function fetchData(year) {
    const response = await fetch(`/generation-capacities-national-data?year=${year}`, {
      cache: "no-store",
    });

    if (!response.ok) {
      let message = `Failed to load data for ${year}.`;
      try {
        const payload = await response.json();
        if (payload?.error) message = payload.error;
      } catch (err) { /* ignore */ }
      throw new Error(message);
    }

    return response.json();
  }

  function renderCharts(data) {
    if (!data || !data.sources || !data.sources.length) {
      showError("No data available for the selected year.");
      return;
    }

    const renewables = data.sources.filter(s => RENEWABLES.has(s.label));
    const nonRenewables = data.sources.filter(s => NON_RENEWABLES.has(s.label));

    // Top-level national pie chart
    const totalLabels = data.sources.map(s => s.label);
    const totalValues = data.sources.map(s => s.total);

    Plotly.newPlot("national-total-chart", [{
      labels: totalLabels,
      values: totalValues,
      type: "pie",
      hole: 0.4,
      textinfo: "none",
      marker: { colors: totalLabels.map(l => CATEGORY_COLORS[l] ?? "#999") },
      hovertemplate: "<b>%{label}</b><br>%{value:,.0f} MW<br>%{percent}<extra></extra>",
    }], { ...pieLayout(), height: 420 }, plotCfg);

    // Renewables pie chart
    Plotly.newPlot("national-renewables-chart", [{
      labels: renewables.map(s => s.label),
      values: renewables.map(s => s.total),
      type: "pie",
      hole: 0.4,
      textinfo: "none",
      marker: { colors: renewables.map(s => CATEGORY_COLORS[s.label] ?? "#999") },
      hovertemplate: "<b>%{label}</b><br>%{value:,.0f} MW<br>%{percent}<extra></extra>",
    }], { ...pieLayout(), height: 380 }, plotCfg);

    // Non-renewables pie chart
    Plotly.newPlot("national-nonrenewables-chart", [{
      labels: nonRenewables.map(s => s.label),
      values: nonRenewables.map(s => s.total),
      type: "pie",
      hole: 0.4,
      textinfo: "none",
      marker: { colors: nonRenewables.map(s => CATEGORY_COLORS[s.label] ?? "#999") },
      hovertemplate: "<b>%{label}</b><br>%{value:,.0f} MW<br>%{percent}<extra></extra>",
    }], { ...pieLayout(), height: 380 }, plotCfg);
  }

  async function refresh() {
    const year = Number(yearSelect.value);
    try {
      clearError();
      const data = await fetchData(year);
      renderCharts(data);
    } catch (err) {
      showError(err.message);
    }
  }

  yearSelect.value = String(config.selectedYear || yearSelect.value);
  yearSelect.addEventListener("change", refresh);
  refresh();
}