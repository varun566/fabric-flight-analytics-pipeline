(() => {
  "use strict";

  const data = FLIGHT_ANALYTICS_DATA;
  const state = { route: "all", search: "", view: "overview" };
  const $ = (selector) => document.querySelector(selector);
  const $$ = (selector) => Array.from(document.querySelectorAll(selector));
  const numberFormatter = new Intl.NumberFormat("en-US");
  const dateFormatter = new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" });

  const escapeHtml = (value) => String(value).replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[character]);
  const percent = (value, digits = 1) => `${(Number(value) * 100).toFixed(digits)}%`;
  const decimal = (value, digits = 1) => Number(value).toFixed(digits);
  const wholeNumber = (value) => numberFormatter.format(Math.round(Number(value)));
  const displayDate = (isoDate) => dateFormatter.format(new Date(`${isoDate}T12:00:00`));
  const compactDate = (isoDate) => new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric" }).format(new Date(`${isoDate}T12:00:00`));

  function activeSummary() {
    return state.route === "all" ? data.metrics : data.routeMetrics.find((item) => item.route === state.route) || data.metrics;
  }

  function activeTrend() {
    return state.route === "all" ? data.trend : data.routeTrends[state.route] || data.trend;
  }

  function renderMetrics() {
    const summary = activeSummary();
    const selected = state.route !== "all";
    $("#onTimeMetric").textContent = percent(summary.onTimePct);
    $("#onTimeDetail").textContent = selected ? `${wholeNumber(summary.operatedFlights)} operated flights` : `${wholeNumber(summary.onTimeFlights)} on-time operated flights`;
    $("#delayMetric").textContent = `${decimal(summary.avgDelay)} min`;
    $("#cancellationMetric").textContent = percent(summary.cancellationRate);
    $("#cancellationDetail").textContent = `${wholeNumber(summary.cancelledFlights)} cancelled flights`;
    $("#passengerMetric").textContent = wholeNumber(summary.passengerCount);
    $("#passengerDetail").textContent = selected ? `${escapeHtml(summary.route)} passengers scheduled` : "Across all selected route-days";
  }

  function chartSvg(points) {
    const width = 760;
    const height = 275;
    const padding = { top: 18, right: 18, bottom: 38, left: 42 };
    const values = points.map((point) => Number(point.value));
    const rawMin = Math.min(...values);
    const rawMax = Math.max(...values);
    const valuePadding = Math.max((rawMax - rawMin) * .22, .025);
    const min = Math.max(0, rawMin - valuePadding);
    const max = Math.min(1, rawMax + valuePadding);
    const scaleX = (index) => padding.left + ((width - padding.left - padding.right) * index) / Math.max(points.length - 1, 1);
    const scaleY = (value) => height - padding.bottom - ((Number(value) - min) / Math.max(max - min, .001)) * (height - padding.top - padding.bottom);
    const coordinates = points.map((point, index) => `${scaleX(index).toFixed(1)},${scaleY(point.value).toFixed(1)}`);
    const baseline = height - padding.bottom;
    const grid = Array.from({ length: 4 }, (_, index) => {
      const value = min + ((max - min) * index) / 3;
      const y = scaleY(value).toFixed(1);
      return `<g><line class="chart-gridline" x1="${padding.left}" x2="${width - padding.right}" y1="${y}" y2="${y}"/><text class="chart-axis-text" x="0" y="${Number(y) + 4}">${percent(value, 0)}</text></g>`;
    }).join("");
    const labelIndexes = [...new Set([0, Math.floor((points.length - 1) / 2), points.length - 1])];
    const labels = labelIndexes.map((index) => `<text class="chart-axis-text" x="${scaleX(index)}" y="${height - 11}" text-anchor="${index === 0 ? "start" : index === points.length - 1 ? "end" : "middle"}">${compactDate(points[index].date)}</text>`).join("");
    const pointMarkup = points.map((point, index) => `<circle class="chart-point" cx="${scaleX(index).toFixed(1)}" cy="${scaleY(point.value).toFixed(1)}" r="2.2"><title>${displayDate(point.date)}: ${percent(point.value)}</title></circle>`).join("");
    return `<svg class="trend-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Daily on-time performance chart"><defs><linearGradient id="trendFill" x1="0" x2="0" y1="0" y2="1"><stop offset="0%" stop-color="#45c4ff" stop-opacity=".35"/><stop offset="100%" stop-color="#45c4ff" stop-opacity="0"/></linearGradient></defs>${grid}<path class="chart-area" d="M ${coordinates.join(" L ")} L ${scaleX(points.length - 1).toFixed(1)} ${baseline} L ${scaleX(0).toFixed(1)} ${baseline} Z"/><polyline class="chart-line" points="${coordinates.join(" ")}"/>${pointMarkup}${labels}</svg>`;
  }

  function renderTrend() {
    const trend = activeTrend();
    const selected = state.route !== "all";
    $("#trendTitle").textContent = selected ? `${state.route} on-time performance` : "On-time performance";
    $("#trendBadge").textContent = selected ? "Route focus" : "All routes";
    $("#trendChart").innerHTML = chartSvg(trend);
    const start = trend[0];
    const end = trend[trend.length - 1];
    $("#trendSummary").textContent = `${displayDate(start.date)} – ${displayDate(end.date)}`;
  }

  function renderAttention() {
    const filtered = data.anomalies.filter((item) => state.route === "all" || item.route === state.route);
    $("#anomalyCount").textContent = `${filtered.length} flagged`;
    const visible = filtered.slice(0, 4);
    $("#attentionList").innerHTML = visible.length ? visible.map((item) => `
      <div class="attention-row">
        <div><div class="attention-route">${escapeHtml(item.route)}</div><div class="attention-date">${displayDate(item.date)} · ${decimal(item.avgDelay)} min delay</div></div>
        <div class="attention-score">${decimal(item.zScore, 2)}σ</div>
      </div>`).join("") : `<p class="empty-state">No unusual delay patterns for this route.</p>`;
    $("#qualityStatus").textContent = `${data.quality.length}/${data.quality.length} passed`;
  }

  function renderRouteTable() {
    const routes = state.route === "all" ? data.routeMetrics.slice(0, 8) : data.routeMetrics.filter((item) => item.route === state.route);
    $("#routeTableBody").innerHTML = routes.map((item) => `
      <tr>
        <td class="route-name">${escapeHtml(item.route)}</td>
        <td class="delay-value">${decimal(item.avgDelay)} min</td>
        <td class="positive-value">${percent(item.onTimePct)}</td>
        <td>${wholeNumber(item.scheduledFlights)}</td>
      </tr>`).join("");
  }

  function renderPipeline() {
    const summary = activeSummary();
    const entries = [
      ["bronze", "Bronze landing", "Raw source records", data.pipeline.bronzeRows],
      ["silver", "Silver transformation", "Cleaned, typed records", data.pipeline.silverRows],
      ["gold", "Gold daily route", "Analytics-ready rows", data.pipeline.goldDailyRows],
      ["quality", "Quality log", "Checks passed", `${data.quality.length}/${data.quality.length}`],
    ];
    $("#pipelineList").innerHTML = entries.map(([tone, title, subtitle, value]) => `<div class="pipeline-item"><span class="pipeline-dot ${tone}"></span><div><p>${subtitle}</p><strong>${title}</strong></div><span>${typeof value === "number" ? wholeNumber(value) : value}</span></div>`).join("");
    if (state.route !== "all") {
      $("#pipelineList").insertAdjacentHTML("beforeend", `<div class="pipeline-item"><span class="pipeline-dot quality"></span><div><p>Current route</p><strong>${escapeHtml(summary.route)}</strong></div><span>${wholeNumber(summary.scheduledFlights)}</span></div>`);
    }
  }

  function renderAnomalies() {
    const query = state.search.trim().toLowerCase();
    const filtered = data.anomalies.filter((item) => {
      const routeMatches = state.route === "all" || item.route === state.route;
      const queryMatches = !query || `${item.date} ${item.route}`.toLowerCase().includes(query);
      return routeMatches && queryMatches;
    });
    $("#anomalyTableBody").innerHTML = filtered.slice(0, 18).map((item) => `
      <tr><td>${displayDate(item.date)}</td><td class="route-name">${escapeHtml(item.route)}</td><td>${decimal(item.avgDelay)} min</td><td>${decimal(item.zScore, 2)}</td><td>${percent(item.cancellationRate)}</td><td>${wholeNumber(item.flightCount)}</td></tr>`).join("");
    $("#anomalyEmpty").hidden = filtered.length !== 0;
  }

  function formatQualityValue(check) {
    if (check.type === "percent") return percent(check.actual, 2);
    return check.type === "minutes" ? `${decimal(check.actual, 1)} min` : wholeNumber(check.actual);
  }

  function renderQuality() {
    const layers = ["Bronze", "Silver", "Gold"];
    $("#qualityScore").textContent = `${data.quality.filter((item) => item.passed).length}/${data.quality.length}`;
    $("#qualitySummary").innerHTML = layers.map((layer) => {
      const checks = data.quality.filter((item) => item.layer === layer);
      return `<article class="quality-summary-card"><span>${layer.toUpperCase()} LAYER</span><strong>${checks.filter((item) => item.passed).length}/${checks.length}</strong><small>checks passing</small></article>`;
    }).join("");
    $("#qualityTableBody").innerHTML = data.quality.map((check) => `
      <tr><td>${check.layer}</td><td>${escapeHtml(check.name)}</td><td>${formatQualityValue(check)}</td><td>${check.type === "percent" ? percent(check.threshold, 2) : check.type === "minutes" ? `${decimal(check.threshold, 0)} min` : wholeNumber(check.threshold)}</td><td>${check.passed ? "PASS" : "FAIL"}</td></tr>`).join("");
  }

  function renderCoverage() {
    $("#coverageRange").textContent = `${displayDate(data.coverage.start)} – ${displayDate(data.coverage.end)}`;
    $("#coverageDetails").textContent = `${wholeNumber(data.coverage.days)} days · ${wholeNumber(data.routeMetrics.length)} routes`;
    $("#lastUpdated").textContent = `Generated ${new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" }).format(new Date(`${data.generatedAt}T12:00:00`))}`;
  }

  function render() {
    renderMetrics();
    renderTrend();
    renderAttention();
    renderRouteTable();
    renderPipeline();
    renderAnomalies();
  }

  function initializeControls() {
    $("#routeSelect").innerHTML = `<option value="all">All routes</option>${data.routeMetrics.map((item) => `<option value="${escapeHtml(item.route)}">${escapeHtml(item.route)}</option>`).join("")}`;
    $("#routeSelect").addEventListener("change", (event) => { state.route = event.target.value; render(); });
    $("#anomalySearch").addEventListener("input", (event) => { state.search = event.target.value; renderAnomalies(); });
    $("#resetFilters").addEventListener("click", () => { state.route = "all"; state.search = ""; $("#routeSelect").value = "all"; $("#anomalySearch").value = ""; render(); });
    $$('[data-view]').forEach((button) => button.addEventListener("click", () => {
      state.view = button.dataset.view;
      const quality = state.view === "quality";
      $("#overviewPanel").hidden = quality;
      $("#qualityPanel").hidden = !quality;
      $$('[data-view]').forEach((item) => { const active = item === button; item.classList.toggle("is-active", active); item.setAttribute("aria-selected", String(active)); });
    }));
  }

  renderCoverage();
  initializeControls();
  renderQuality();
  render();
})();
