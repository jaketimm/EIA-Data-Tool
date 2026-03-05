const eia_data = window.chartData;

if (!eia_data || typeof Plotly === "undefined") {
  console.warn("Chart data or Plotly is unavailable; skipping chart render.");
} else {

const yearValues = (eia_data.years || [])
  .map((year) => Number(year))
  .filter((year) => Number.isFinite(year));

const yearMin = yearValues.length ? Math.min(...yearValues) : null;
const yearMax = yearValues.length ? Math.max(...yearValues) : null;

/* ── Colour palette ──────────────────────────────────────────────────────── */
const BLUE = "#2563eb";
const GREEN = "#16a34a";
const RED = "#dc2626";


/** Shared layout — opaque tooltips, clean grid. */
function baseLayout(yTitle) {
  return {
    font: { family: "system-ui, sans-serif", size: 12 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { t: 10, r: 15, b: 55, l: 75 },
    xaxis: {
      title: "Year",
      tickmode: "linear",
      dtick: 2,
      gridcolor: "#e9ecef",
      tickangle: -45,
      ...(yearMin !== null && yearMax !== null
        ? {
            range: [yearMin - 0.5, yearMax + 0.5],
            autorangeoptions: {
              minallowed: yearMin,
              maxallowed: yearMax,
            },
          }
        : {}),
    },
    yaxis: {
      title: yTitle,
      gridcolor: "#e9ecef",
      zeroline: true,
      zerolinecolor: "#adb5bd",
      zerolinewidth: 1,
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
    hovermode: "x unified",
  };
}

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

/* ── Line chart — Generation ─────────────────────────────────────────────── */
Plotly.newPlot(
  "line-generation",
  [
    {
      x: eia_data.years,
      y: eia_data.total_net_generation,
      name: "Net Generation",
      type: "scatter",
      mode: "lines",
      line: { color: BLUE, width: 2.5 },
      connectgaps: false,
      hovertemplate: "%{y:,.0f} MWh<extra></extra>",
    },
  ],
  baseLayout("MWh"),
  plotCfg
);

/* ── Line chart — Total Imports & Exports ────────────────────────────────── */
Plotly.newPlot(
  "line-trade",
  [
    {
      x: eia_data.years,
      y: eia_data.total_imports,
      name: "Total Imports",
      type: "scatter",
      mode: "lines",
      line: { color: GREEN, width: 2.5 },
      connectgaps: false,
      hovertemplate: "%{y:,.0f} MWh<extra></extra>",
    },
    {
      x: eia_data.years,
      y: eia_data.total_exports,
      name: "Total Exports",
      type: "scatter",
      mode: "lines",
      line: { color: RED, width: 2.5 },
      connectgaps: false,
      hovertemplate: "%{y:,.0f} MWh<extra></extra>",
    },
  ],
  baseLayout("MWh"),
  plotCfg
);
}
