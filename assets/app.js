const state = {
  payload: null,
  filters: { province: "ALL", municipality: "ALL", round: "ALL", search: "" },
};

const formatNumber = new Intl.NumberFormat("ko-KR");
const formatPercent = new Intl.NumberFormat("ko-KR", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const elements = {
  province: document.querySelector("#province-filter"),
  municipality: document.querySelector("#municipality-filter"),
  round: document.querySelector("#round-filter"),
  search: document.querySelector("#search-filter"),
  reset: document.querySelector("#reset-filters"),
  coverageRange: document.querySelector("#coverage-range"),
  coverageMunicipalities: document.querySelector("#coverage-municipalities"),
  coverageRounds: document.querySelector("#coverage-rounds"),
  kpiAverage: document.querySelector("#kpi-average"),
  kpiMax: document.querySelector("#kpi-max"),
  kpiMin: document.querySelector("#kpi-min"),
  kpiCount: document.querySelector("#kpi-count"),
  tableBody: document.querySelector("#table-body"),
  sourceList: document.querySelector("#source-list"),
};

const nationalChart = echarts.init(document.querySelector("#national-chart"));
const municipalityChart = echarts.init(document.querySelector("#municipality-chart"));
const rankingChart = echarts.init(document.querySelector("#ranking-chart"));

function unique(values) {
  return [...new Set(values)].sort((a, b) => a.localeCompare(b, "ko"));
}

function getRecords() {
  return state.payload?.records ?? [];
}

function getFilteredRecords() {
  const search = state.filters.search.trim().toLowerCase();
  return getRecords().filter((record) => {
    const provinceMatch = state.filters.province === "ALL" || record.province === state.filters.province;
    const municipalityMatch =
      state.filters.municipality === "ALL" || record.municipality_key === state.filters.municipality;
    const roundMatch = state.filters.round === "ALL" || String(record.election_round) === state.filters.round;
    const searchMatch = !search || `${record.province} ${record.municipality}`.toLowerCase().includes(search);
    return provinceMatch && municipalityMatch && roundMatch && searchMatch;
  });
}

function populateSelect(select, values, selected, allLabel) {
  select.innerHTML = "";
  const allOption = document.createElement("option");
  allOption.value = "ALL";
  allOption.textContent = allLabel;
  select.append(allOption);

  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    option.selected = value === selected;
    select.append(option);
  });
}

function updateFilters() {
  const records = getRecords();
  const provinces = unique(records.map((record) => record.province));
  const municipalities = unique(
    records
      .filter((record) => state.filters.province === "ALL" || record.province === state.filters.province)
      .map((record) => record.municipality_key),
  );
  const rounds = unique(records.map((record) => String(record.election_round)));

  populateSelect(elements.province, provinces, state.filters.province, "전체 광역");
  populateSelect(elements.municipality, municipalities, state.filters.municipality, "전체 기초");
  populateSelect(elements.round, rounds, state.filters.round, "전체 회차");

  if (state.filters.municipality !== "ALL" && !municipalities.includes(state.filters.municipality)) {
    state.filters.municipality = "ALL";
    populateSelect(elements.municipality, municipalities, "ALL", "전체 기초");
  }
}

function renderCoverage() {
  const coverage = state.payload.coverage;
  elements.coverageRange.textContent = `${coverage.from} ~ ${coverage.to}`;
  elements.coverageMunicipalities.textContent = formatNumber.format(coverage.municipality_count);
  elements.coverageRounds.textContent = formatNumber.format(coverage.election_rounds.length);
}

function renderKpis(records) {
  const rates = records.map((record) => record.turnout_rate);
  const average = rates.length ? rates.reduce((sum, value) => sum + value, 0) / rates.length : 0;
  const max = rates.length ? Math.max(...rates) : 0;
  const min = rates.length ? Math.min(...rates) : 0;

  elements.kpiAverage.textContent = `${formatPercent.format(average)}%`;
  elements.kpiMax.textContent = `${formatPercent.format(max)}%`;
  elements.kpiMin.textContent = `${formatPercent.format(min)}%`;
  elements.kpiCount.textContent = formatNumber.format(records.length);
}

function renderNationalChart() {
  const grouped = new Map();
  getRecords().forEach((record) => {
    const current = grouped.get(record.election_date) ?? { electorate: 0, votes: 0 };
    current.electorate += record.electorate;
    current.votes += record.votes;
    grouped.set(record.election_date, current);
  });

  const dates = [...grouped.keys()].sort();
  const values = dates.map((date) => {
    const entry = grouped.get(date);
    return Number(((entry.votes / entry.electorate) * 100).toFixed(2));
  });

  nationalChart.setOption({
    tooltip: { trigger: "axis", valueFormatter: (value) => `${value}%` },
    grid: { left: 48, right: 20, top: 30, bottom: 40 },
    xAxis: { type: "category", data: dates.map((date) => date.slice(0, 4)) },
    yAxis: {
      type: "value",
      min: 45,
      axisLabel: { formatter: "{value}%" },
      splitLine: { lineStyle: { color: "rgba(24,33,38,0.08)" } },
    },
    series: [
      {
        type: "line",
        smooth: true,
        data: values,
        lineStyle: { width: 4, color: "#0d6a73" },
        itemStyle: { color: "#c65f32" },
        areaStyle: { color: "rgba(13,106,115,0.15)" },
      },
    ],
  });
}

function renderMunicipalityChart(records) {
  const municipalityKey =
    state.filters.municipality !== "ALL"
      ? state.filters.municipality
      : records[0]?.municipality_key ?? getRecords()[0]?.municipality_key;

  const seriesRecords = getRecords()
    .filter((record) => record.municipality_key === municipalityKey)
    .sort((a, b) => a.election_date.localeCompare(b.election_date));

  municipalityChart.setOption({
    title: {
      text: municipalityKey || "선택 가능한 지자체가 없습니다.",
      left: 0,
      textStyle: { fontSize: 16, fontWeight: 700, color: "#182126" },
    },
    tooltip: { trigger: "axis", valueFormatter: (value) => `${value}%` },
    grid: { left: 48, right: 20, top: 56, bottom: 40 },
    xAxis: { type: "category", data: seriesRecords.map((record) => record.election_date.slice(0, 4)) },
    yAxis: {
      type: "value",
      min: 45,
      axisLabel: { formatter: "{value}%" },
      splitLine: { lineStyle: { color: "rgba(24,33,38,0.08)" } },
    },
    series: [
      {
        type: "line",
        data: seriesRecords.map((record) => record.turnout_rate),
        lineStyle: { width: 4, color: "#c65f32" },
        itemStyle: { color: "#0d6a73" },
      },
    ],
  });
}

function renderRankingChart(records) {
  const round =
    state.filters.round !== "ALL"
      ? Number(state.filters.round)
      : Math.max(...getRecords().map((record) => record.election_round));

  const ranking = records
    .filter((record) => record.election_round === round)
    .sort((a, b) => b.turnout_rate - a.turnout_rate)
    .slice(0, 20);

  rankingChart.setOption({
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, valueFormatter: (value) => `${value}%` },
    grid: { left: 180, right: 20, top: 20, bottom: 30 },
    xAxis: {
      type: "value",
      axisLabel: { formatter: "{value}%" },
      splitLine: { lineStyle: { color: "rgba(24,33,38,0.08)" } },
    },
    yAxis: {
      type: "category",
      data: ranking.map((record) => `${record.province} ${record.municipality}`).reverse(),
      axisLabel: { width: 160, overflow: "truncate" },
    },
    series: [
      {
        type: "bar",
        data: ranking.map((record) => record.turnout_rate).reverse(),
        itemStyle: { color: "#0d6a73", borderRadius: [0, 10, 10, 0] },
      },
    ],
  });
}

function renderTable(records) {
  elements.tableBody.innerHTML = "";
  records
    .slice()
    .sort((a, b) => a.election_date.localeCompare(b.election_date) || a.municipality_key.localeCompare(b.municipality_key, "ko"))
    .slice(0, 300)
    .forEach((record) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${record.election_round}회</td>
        <td>${record.election_date}</td>
        <td>${record.province}</td>
        <td>${record.municipality}</td>
        <td>${formatNumber.format(record.electorate)}</td>
        <td>${formatNumber.format(record.votes)}</td>
        <td>${formatNumber.format(record.invalid_votes)}</td>
        <td>${formatNumber.format(record.abstentions)}</td>
        <td>${formatPercent.format(record.turnout_rate)}%</td>
      `;
      elements.tableBody.append(row);
    });
}

function renderSources(sources) {
  elements.sourceList.innerHTML = "";
  sources.forEach((source) => {
    const item = document.createElement("li");
    item.innerHTML = `<a href="${source.url}" target="_blank" rel="noreferrer">${source.name}</a>`;
    elements.sourceList.append(item);
  });
}

function render() {
  updateFilters();
  const filtered = getFilteredRecords();
  renderCoverage();
  renderKpis(filtered);
  renderNationalChart();
  renderMunicipalityChart(filtered);
  renderRankingChart(filtered);
  renderTable(filtered);
}

function bindEvents() {
  elements.province.addEventListener("change", (event) => {
    state.filters.province = event.target.value;
    state.filters.municipality = "ALL";
    render();
  });

  elements.municipality.addEventListener("change", (event) => {
    state.filters.municipality = event.target.value;
    render();
  });

  elements.round.addEventListener("change", (event) => {
    state.filters.round = event.target.value;
    render();
  });

  elements.search.addEventListener("input", (event) => {
    state.filters.search = event.target.value;
    render();
  });

  elements.reset.addEventListener("click", () => {
    state.filters = { province: "ALL", municipality: "ALL", round: "ALL", search: "" };
    elements.search.value = "";
    render();
  });

  window.addEventListener("resize", () => {
    nationalChart.resize();
    municipalityChart.resize();
    rankingChart.resize();
  });
}

async function init() {
  const [payload, sources] = await Promise.all([
    fetch("./data/local-election-turnout-municipal.json").then((response) => response.json()),
    fetch("./data/sources.json").then((response) => response.json()),
  ]);

  state.payload = payload;
  renderSources(sources);
  bindEvents();
  render();
}

init().catch((error) => {
  console.error(error);
  document.body.innerHTML =
    '<div style="padding:24px;font-family:sans-serif">데이터를 불러오지 못했습니다. 정적 파일 경로와 JSON 파일을 확인해 주세요.</div>';
});
