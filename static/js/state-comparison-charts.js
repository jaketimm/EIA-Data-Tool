const config = window.stateComparisonConfig || {};
const yearSelect = document.getElementById("year");
const errorEl = document.getElementById("comparison-error");

if (!yearSelect || typeof Plotly === "undefined") {
  console.warn("Year selector or Plotly is unavailable; skipping chart render.");
} else {

  // Color palette 
  const DARK_BLUE = "#1f4970";
  const GREEN = "#7c9e6e";

  const plotCfg = {
    responsive: true,
    displayModeBar: "hover",
    displaylogo: false,
    modeBarButtonsToRemove: [
      "pan2d",
      "select2d",
      "lasso2d",
      "zoomIn2d",
      "zoomOut2d",
      "resetScale2d",
      "hoverClosestCartesian",
      "hoverCompareCartesian",
      "toggleSpikelines",
    ],
  };

  function showError(message) {
    errorEl.textContent = message;
    errorEl.classList.remove("d-none");
  }

  function clearError() {
    errorEl.textContent = "";
    errorEl.classList.add("d-none");
  }

  // Fetch data for the selected year when the dropdown selection changes
  async function refreshCharts() {
    const year = Number(yearSelect.value);

    try {
      clearError();
      const data = await fetchComparisonData(year);
      renderChoroplethMap(data);
      renderTop10Imports(data);
      renderTop10Exports(data);
      renderTable(data);
    } catch (err) {
      showError(err.message);
    }
  }

  yearSelect.value = String(config.selectedYear || yearSelect.value);
  yearSelect.addEventListener("change", refreshCharts);
  refreshCharts();

  // Call Flask route to query DB and update data
  async function fetchComparisonData(year) {
    const response = await fetch(`/api/state-comparison?year=${encodeURIComponent(year)}`, {
      cache: "no-store",
    });

    if (!response.ok) {
      const fallback = `Failed to load data for ${year}.`;
      let message = fallback;
      try {
        const payload = await response.json();
        message = payload.error || fallback;
      } catch (err) {
        message = fallback;
      }
      throw new Error(message);
    }

    return response.json();
  }

  // Choropleth map showing imports (dark blue) and exports (green)
  function renderChoroplethMap(data) {
    // Build lookup maps
    const importMap = Object.fromEntries(
      data.import_states.map((state, i) => [state, data.total_imports[i]])
    );
    const exportMap = Object.fromEntries(
      data.export_states.map((state, i) => [state, data.total_exports[i]])
    );

    // Determine net position for each state
    const allStates = new Set([...data.import_states, ...data.export_states]);
    const locations = [];
    const netValues = [];
    const hoverTexts = [];

    allStates.forEach(state => {
      const imports = importMap[state] || 0;
      const exports = exportMap[state] || 0;
      const netPosition = exports - imports;

      locations.push(state);
      netValues.push(netPosition);

      const stateName = data.import_state_names?.[data.import_states.indexOf(state)] ||
        data.export_state_names?.[data.export_states.indexOf(state)] ||
        state;

      hoverTexts.push(
        `${stateName}<br>` +
        `Imports: ${imports.toLocaleString()} MWh<br>` +
        `Exports: ${exports.toLocaleString()} MWh<br>` +
        `Net: ${netPosition >= 0 ? '+' : ''}${netPosition.toLocaleString()} MWh`
      );
    });

    Plotly.newPlot(
      "state-map",
      [{
        type: "choropleth",
        locationmode: "USA-states",
        locations: locations,
        z: netValues,
        text: hoverTexts,
        hovertemplate: "%{text}<extra></extra>",
        zmin: -Math.max(...netValues.map(Math.abs)),
        zmax: Math.max(...netValues.map(Math.abs)),
        colorscale: [
          [0, DARK_BLUE],
          [0.4999, DARK_BLUE],
          [0.5, GREEN],
          [1, GREEN]
        ],
        colorbar: {
          title: "Net Position<br>(MWh)",
          thickness: 15,
          len: 0.7,
          tickformat: ",.0f",
        },
        marker: {
          line: {
            color: "#ffffff",
            width: 1.5
          }
        },
        hoverlabel: {
          bgcolor: "#ffffff",
          bordercolor: "#dee2e6",
          font: { color: "#212529", size: 12 },
        },
      }],
      {
        geo: {
          scope: "usa",
          showlakes: false,
          bgcolor: "rgba(255,255,255,1)"
        },
        font: { family: "system-ui, sans-serif", size: 12 },
        paper_bgcolor: "rgba(255,255,255,1)",
        margin: { t: 10, r: 15, b: 10, l: 15 },
        height: 500,
      },
      plotCfg
    );
  }

  // Bar chart — Top 10 Imports
  function renderTop10Imports(data) {
    // Get top 10 importers
    const combined = data.import_states.map((state, i) => ({
      state,
      name: data.import_state_names[i],
      value: data.total_imports[i]
    }));

    // Sort descending and take top 10
    combined.sort((a, b) => b.value - a.value);
    const top10 = combined.slice(0, 10);

    Plotly.newPlot(
      "imports-by-state",
      [{
        x: top10.map(d => d.value),
        y: top10.map(d => d.state),
        customdata: top10.map(d => d.name),
        type: "bar",
        orientation: "h",
        marker: { color: DARK_BLUE },
        hovertemplate: "%{customdata} (%{y})<br>%{x:,.0f} MWh<extra></extra>",
      }],
      {
        font: { family: "system-ui, sans-serif", size: 12 },
        paper_bgcolor: "rgba(255,255,255,1)",
        plot_bgcolor: "rgba(255,255,255,1)",
        margin: { t: 10, r: 15, b: 45, l: 70 },
        height: 400,
        hoverlabel: {
          bgcolor: "#ffffff",
          bordercolor: "#dee2e6",
          font: { color: "#212529", size: 12 },
        },
        xaxis: {
          title: "MWh",
          gridcolor: "#dfe2e6",
        },
        yaxis: {
          autorange: "reversed",
          automargin: true,
          side: "left",
          ticklen: 5,
          tickwidth: 1,
        },
        showlegend: false,
      },
      plotCfg
    );
  }

  // Bar chart — Top 10 Exports
  function renderTop10Exports(data) {
    // Get top 10 exporters
    const combined = data.export_states.map((state, i) => ({
      state,
      name: data.export_state_names[i],
      value: data.total_exports[i]
    }));

    // sort descending and take top 10
    combined.sort((a, b) => b.value - a.value);
    const top10 = combined.slice(0, 10);

    Plotly.newPlot(
      "exports-by-state",
      [{
        x: top10.map(d => d.value),
        y: top10.map(d => d.state),
        customdata: top10.map(d => d.name),
        type: "bar",
        orientation: "h",
        marker: { color: GREEN },
        hovertemplate: "%{customdata} (%{y})<br>%{x:,.0f} MWh<extra></extra>",
      }],
      {
        font: { family: "system-ui, sans-serif", size: 12 },
        paper_bgcolor: "rgba(255,255,255,1)",
        plot_bgcolor: "rgba(255,255,255,1)",
        margin: { t: 10, r: 15, b: 45, l: 60 },
        height: 400,
        hoverlabel: {
          bgcolor: "#ffffff",
          bordercolor: "#dee2e6",
          font: { color: "#212529", size: 12 },
        },
        xaxis: {
          title: "MWh",
          gridcolor: "#dfe2e6",
        },
        yaxis: {
          autorange: "reversed",
          automargin: true,
          side: "left",
          ticklen: 5,
          tickwidth: 1,
        },
        showlegend: false,
      },
      plotCfg
    );
  }

  // HTML table of raw data below the chart
  function renderTable(data) {
    const tbody = document.getElementById("comparison-table-body");
    if (!tbody) return;

    if (!data || !data.generation_states || !data.generation_states.length) {
      tbody.innerHTML = `<tr><td colspan="20" class="text-center text-muted">No data available.</td></tr>`;
      return;
    }

    // Build a lookup by state code
    const importMap = Object.fromEntries(data.import_states.map((source, i) => [source, data.total_imports[i]]));
    const exportMap = Object.fromEntries(data.export_states.map((source, i) => [source, data.total_exports[i]]));

    // Generate table rows showing state, generation, imports, and exports
    const rows = data.generation_states.map((state, i) => {
      const gen = data.total_generation[i];
      const imp = importMap[state] ?? 0;
      const exp = exportMap[state] ?? 0;
      const fmt = (v) => v > 0 ? v.toLocaleString("en-US", { maximumFractionDigits: 0 }) : '<span class="null-value">—</span>';
      return `
      <tr>
        <td><abbr title="${data.generation_state_names[i]}">${state}</abbr></td>
        <td class="text-end">${fmt(gen)}</td>
        <td class="text-end">${fmt(imp)}</td>
        <td class="text-end">${fmt(exp)}</td>
      </tr>
    `;
    }).join("");

    tbody.innerHTML = rows;
  }
}