const config = window.generationCapacitiesConfig || {};
const stateSelect = document.getElementById("capacity-state");
const startYearInput = document.getElementById("capacity-start-year");
const endYearInput = document.getElementById("capacity-end-year");
const refreshButton = document.getElementById("capacity-refresh");
const chartEl = document.getElementById("generation-capacities-chart");
const errorEl = document.getElementById("capacity-error");

if (!stateSelect || !startYearInput || !endYearInput || !chartEl || typeof Plotly === "undefined") {
  console.warn("Generation capacities elements or Plotly missing; skipping render.");
} else {

  const CATEGORY_COLORS = {
    "Coal": "#4a4a4a", // dark gray
    "Natural Gas": "#5E81AC", // steel blue
    "Petroleum": "#7a5450", // muted brown-red

    "Nuclear": "#b85c5b", // muted red
    "Solar": "#c9a03a", // muted gold
    "Wind": "#5a9e9a", // muted teal
    "Hydroelectric": "#3a7a9c", // muted water blue
    "Wood": "#4e8a45", // muted green

    "Battery": "#8a6e9e", // muted purple
    "Pumped Storage": "#2a4f6e", // deep muted navy

    "Other": "#7a8490", // muted blue-gray
  };
  const FALLBACK_PALETTE = ["#5E81AC", "#4a4a4a", "#b85c5b", "#4e8a45"];

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
    ],
  };

  function baseLayout() {
    return {
      font: { family: "system-ui, sans-serif", size: 12 },
      paper_bgcolor: "rgba(255,255,255,1)",
      plot_bgcolor: "rgba(255,255,255,1)",
      margin: { t: 20, r: 20, b: 50, l: 70 },
      xaxis: {
        title: "Year",
        gridcolor: "#dfe2e6",
        tickmode: "array",
        tickvals: [],
        tickangle: -45,
        automargin: true,
      },
      yaxis: {
        title: "Capacity (MW)",
        gridcolor: "#e9ecef",
        zerolinecolor: "#adb5bd",
        zerolinewidth: 1,
      },
      hoverlabel: {
        bgcolor: "#ffffff",
        bordercolor: "#dee2e6",
        font: { color: "#212529", size: 12 },
        align: "left",
      },
      legend: {
        orientation: "h",
        yanchor: "bottom",
        y: 1.02,
        xanchor: "left",
        x: 0,
        traceorder: "normal",
        itemwidth: 40
      },
      modebar: {
        color: "#6c757d",
        activecolor: "#212529",
      },
      barmode: "stack",
      bargap: 0.15,
    };
  }

  function chartHeight(data) {
    return Math.max(480, data.years.length * 26 + 180);
  }

  function clearError() {
    errorEl.textContent = "";
    errorEl.classList.add("d-none");
  }

  function showError(message) {
    errorEl.textContent = message;
    errorEl.classList.remove("d-none");
  }

  // Fetch data for the selected state when the filter button is pressed
  async function fetchData(state, startYear, endYear) {
    const params = new URLSearchParams({
      state,
      start_year: String(startYear),
      end_year: String(endYear),
    });

    const response = await fetch(`/api/generation-capacities-data?${params.toString()}`, {
      cache: "no-store",
    });

    if (!response.ok) {
      let message = `Failed to load data for ${state}.`;
      try {
        const payload = await response.json();
        if (payload && payload.error) {
          message = payload.error;
        }
      } catch (err) {
        // ignore
      }
      throw new Error(message);
    }

    return response.json();
  }

  function renderChart(data) {
    if (!data || !Array.isArray(data.sources) || !data.sources.length) {
      showError("No data is available for the selected filters.");
      return;
    }

    const traces = data.sources.map((source) => ({
      x: data.years,
      y: source.values,
      name: source.label,
      type: "bar",
      marker: {
        color: CATEGORY_COLORS[source.label] ?? FALLBACK_PALETTE[data.sources.indexOf(source) % FALLBACK_PALETTE.length],
        line: {
          color: "#ffffff",
          width: 0.5
        }
      },
      hovertemplate: "<b>%{fullData.name}</b><br>%{y:,.0f} " + (data.unit || "MW") + "<extra></extra>",
    }));

    Plotly.newPlot(
      chartEl,
      traces,
      {
        ...baseLayout(),
        height: chartHeight(data),
        xaxis: {
          ...baseLayout().xaxis,
          tickvals: data.years,
          ticktext: data.years,
        },
      },
      plotCfg
    );

    renderTable(data);
  }

  function renderTable(data) {
    const tbody = document.getElementById("capacity-table-body");
    if (!tbody) return;

    if (!data || !data.sources || !data.sources.length) {
      tbody.innerHTML = `<tr><td colspan="20" class="text-center text-muted">No data available.</td></tr>`;
      return;
    }
    
    const rows = [];
    // flip the order of the years for the table only
    [...data.years].reverse().forEach((year, i) => {
      const yearIndex = data.years.indexOf(year);
      const cells = data.sources.map(source => {
        const val = source.values[yearIndex];
        return `<td class="text-end">${val > 0 ? val.toLocaleString("en-US", { maximumFractionDigits: 1 }) : '<span class="null-value">—</span>'}</td>`;
      }).join("");
      rows.push(`<tr><td>${year}</td><td><abbr title="${data.state_label}">${data.state}</abbr></td>${cells}</tr>`);
    });

    tbody.innerHTML = rows.join("");

    const thead = document.querySelector("#capacity-table thead tr");
    if (thead) {
      const sourceCols = data.sources.map(s => `<th class="text-end">${s.label}</th>`).join("");
      thead.innerHTML = `<th>Year</th><th>State</th>${sourceCols}`;
    }

  }

  async function refreshChart() {
    const state = stateSelect.value;
    const startYear = Number(startYearInput.value);
    const endYear = Number(endYearInput.value);

    try {
      clearError();
      const data = await fetchData(state, startYear, endYear);
      renderChart(data);
    } catch (err) {
      showError(err.message);
    }
  }

  // Initialize with config values
  stateSelect.value = config.selectedState || stateSelect.value;
  startYearInput.value = config.selectedYearStart || startYearInput.value;
  endYearInput.value = config.selectedYearEnd || endYearInput.value;

  refreshButton.addEventListener("click", refreshChart);
  refreshChart();
}
