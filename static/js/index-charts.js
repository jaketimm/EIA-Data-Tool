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
const GREEN_LIGHT = "#86efac";
const RED = "#dc2626";
const RED_LIGHT = "#fca5a5";

/* ── Helpers ─────────────────────────────────────────────────────────────── */
/**
 * Return true if every value in every array is null, 0, or undefined.
 * Pass as many arrays as you like: hasData(arr1, arr2, ...)
 */
function hasData(...arrays) {
  return arrays.some((arr) => arr.some((v) => v !== null && v !== undefined && v !== 0));
}

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

/**
 * Render a grayed-out empty chart with a centered "No data" message.
 * Also adds the .no-data class to the parent card.
 */
function renderNoData(divId, cardId) {
  const card = document.getElementById(cardId);
  if (card) card.classList.add("no-data");

  const layout = {
    ...baseLayout("MWh"),
    xaxis: { visible: false },
    yaxis: { visible: false },
    annotations: [
      {
        text: "No data available",
        xref: "paper",
        yref: "paper",
        x: 0.5,
        y: 0.5,
        showarrow: false,
        font: { size: 16, color: "#adb5bd" },
      },
    ],
  };

  Plotly.newPlot(divId, [], layout, {
    responsive: true,
    displaylogo: false,
    staticPlot: true,
  });
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

/* ── Bar chart — Interstate ──────────────────────────────────────────────── */
if (hasData(eia_data.net_interstate_import, eia_data.net_interstate_export)) {
  Plotly.newPlot(
    "bar-interstate",
    [
      {
        x: eia_data.years,
        y: eia_data.net_interstate_import,
        name: "Net Import",
        type: "bar",
        marker: { color: GREEN, opacity: 0.85 },
        hovertemplate: "%{y:,.0f} MWh<extra></extra>",
      },
      {
        x: eia_data.years,
        y: eia_data.net_interstate_export,
        name: "Net Export",
        type: "bar",
        marker: { color: RED, opacity: 0.85 },
        hovertemplate: "%{y:,.0f} MWh<extra></extra>",
      },
    ],
    {
      ...baseLayout("MWh"),
      barmode: "group",
    },
    plotCfg
  );
} else {
  renderNoData("bar-interstate", "card-bar-interstate");
}

/* ── Bar chart — International ───────────────────────────────────────────── */
if (hasData(eia_data.intl_imports, eia_data.intl_exports)) {
  Plotly.newPlot(
    "bar-international",
    [
      {
        x: eia_data.years,
        y: eia_data.intl_imports,
        name: "Imports",
        type: "bar",
        marker: { color: GREEN_LIGHT, line: { color: GREEN, width: 1 } },
        hovertemplate: "%{y:,.0f} MWh<extra></extra>",
      },
      {
        x: eia_data.years,
        y: eia_data.intl_exports,
        name: "Exports",
        type: "bar",
        marker: { color: RED_LIGHT, line: { color: RED, width: 1 } },
        hovertemplate: "%{y:,.0f} MWh<extra></extra>",
      },
    ],
    {
      ...baseLayout("MWh"),
      barmode: "group",
    },
    plotCfg
  );
} else {
  renderNoData("bar-international", "card-bar-international");
}
}
