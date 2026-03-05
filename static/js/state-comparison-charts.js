const config = window.stateComparisonConfig || {};
const yearSelect = document.getElementById("year");
const errorEl = document.getElementById("comparison-error");

if (!yearSelect || typeof Plotly === "undefined") {
  console.warn("Year selector or Plotly is unavailable; skipping chart render.");
} else {
  const BLUE = "#5E81AC";
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
      "toImage",
      "zoomIn2d",
      "zoomOut2d",
      "resetScale2d",
      "hoverClosestCartesian",
      "hoverCompareCartesian",
      "toggleSpikelines",
    ],
  };

  function baseLayout() {
    return {
      font: { family: "system-ui, sans-serif", size: 12 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      margin: { t: 10, r: 15, b: 45, l: 15 },
      xaxis: {
        title: "MWh",
        gridcolor: "#e9ecef",
        zeroline: true,
        zerolinecolor: "#adb5bd",
        zerolinewidth: 1,

      },
      yaxis: {
        automargin: true,
        ticklabelposition: "outside",
        ticklen: 4,        // length of tick mark
        tickwidth: 0,      // hide the tick mark itself if unwanted
        standoff: 10,      // gap between label and axis
      },
      hoverlabel: {
        bgcolor: "#ffffff",
        bordercolor: "#dee2e6",
        font: { color: "#212529", size: 12 },
      },
      legend: {
        orientation: "h",
        yanchor: "bottom",
        y: 1.02,
        xanchor: "left",
        x: 0,
      },
      modebar: {
        bgcolor: "rgba(248,249,250,0.92)",
        color: "#6c757d",
        activecolor: "#212529",
      },
      barmode: "group",
      bargap: 0.15,
    };
  }

  function chartHeight(categoryCount) {
    return Math.max(700, categoryCount * 12 + 120);
  }

  // Set a shared x-axis max for the net import and export charts
  // Creates a side-by-side comparison with a consistent scale
  function getSharedTradeAxisMax(data) {
    const importMax = Math.max(...(data.total_imports || [0]));
    const exportMax = Math.max(...(data.total_exports || [0]));
    const baseMax = Math.max(importMax, exportMax, 0);
    const minPadding = 250000;
    const pad = Math.max(baseMax * 0.08, minPadding);
    return baseMax + pad;
  }

  function showError(message) {
    errorEl.textContent = message;
    errorEl.classList.remove("d-none");
  }

  function clearError() {
    errorEl.textContent = "";
    errorEl.classList.add("d-none");
  }

  // Fetch data for the selected year when the dropdown selection changes
  async function fetchComparisonData(year) {
    const response = await fetch(`/api/state-comparison-data?year=${encodeURIComponent(year)}`, {
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

  function renderGenerationChart(data) {
    Plotly.newPlot(
      "generation-by-state",
      [
        {
          x: data.total_generation,
          y: data.generation_states,
          customdata: data.generation_state_names,
          type: "bar",
          orientation: "h",
          marker: { color: BLUE },
          hovertemplate: "%{customdata} (%{y})<br>%{x:,.0f} MWh<extra></extra>",
        },
      ],
      {
        ...baseLayout(),
        height: chartHeight(data.generation_states.length),
        yaxis: {
          ...baseLayout().yaxis,
          autorange: "reversed",
          tickmode: "array",
          tickvals: data.generation_states,
          ticktext: data.generation_states,
        },
        showlegend: false,
      },
      plotCfg
    );
  }

  function renderImportsChart(data) {
    const sharedMax = getSharedTradeAxisMax(data);

    Plotly.newPlot(
      "imports-by-state",
      [
        {
          x: data.total_imports,
          y: data.import_states,
          customdata: data.import_state_names,
          type: "bar",
          orientation: "h",
          name: "Total Imports (Interstate + Intl)",
          marker: { color: DARK_BLUE },
          hovertemplate: "%{customdata} (%{y})<br>%{x:,.0f} MWh<extra></extra>",
        },
      ],
      {
        ...baseLayout(),
        height: chartHeight(data.import_states.length),
        yaxis: {
          ...baseLayout().yaxis,
          autorange: "reversed",
          tickmode: "array",
          tickvals: data.import_states,
          ticktext: data.import_states,
        },
        xaxis: {
          ...baseLayout().xaxis,
          range: [0, sharedMax],
        },
        showlegend: false,
      },
      plotCfg
    );
  }

  function renderExportsChart(data) {
    const sharedMax = getSharedTradeAxisMax(data);

    Plotly.newPlot(
      "exports-by-state",
      [
        {
          x: data.total_exports,
          y: data.export_states,
          customdata: data.export_state_names,
          type: "bar",
          orientation: "h",
          name: "Total Exports (Interstate + Intl)",
          marker: { color: GREEN },
          hovertemplate: "%{customdata} (%{y})<br>%{x:,.0f} MWh<extra></extra>",
        },
      ],
      {
        ...baseLayout(),
        height: chartHeight(data.export_states.length),
        yaxis: {
          ...baseLayout().yaxis,
          autorange: "reversed",
          tickmode: "array",
          tickvals: data.export_states,
          ticktext: data.export_states,
        },
        xaxis: {
          ...baseLayout().xaxis,
          range: [0, sharedMax],
        },
        showlegend: false,
      },
      plotCfg
    );
  }

  async function refreshCharts() {
    const year = Number(yearSelect.value);

    try {
      clearError();
      const data = await fetchComparisonData(year);
      renderGenerationChart(data);
      renderImportsChart(data);
      renderExportsChart(data);
    } catch (err) {
      showError(err.message);
    }
  }

  yearSelect.value = String(config.selectedYear || yearSelect.value);
  yearSelect.addEventListener("change", refreshCharts);
  refreshCharts();
}
