---
title: The Hispanic Surge
theme: light
---

# Five years of mandatory state data show a systematic increase in traffic enforcement against Hispanic drivers in Missouri — not just in volume, but in intensity per stop.

The pattern emerges from the state's own Vehicle Stops Report, which every law enforcement agency is required to file annually. In 2020, Hispanic drivers were stopped at the lowest per-capita rate of any racial group: 11.3 stops per 100 residents, compared with 18.6 for white drivers and 30.2 for Black drivers. By 2024, the Hispanic rate had climbed to 13.5 — a 19 percent increase that outpaced every other group.

```js
const raw = await FileAttachment("data/story_analysis_results.csv").csv({typed: true});
const pop = await FileAttachment("data/missouri_population.csv").csv({typed: true});
const agencyPop = await FileAttachment("data/agency_population.csv").csv({typed: true});
const censusByYear = await FileAttachment("data/census_population_by_year.csv").csv({typed: true});
const contrabandData = await FileAttachment("data/contraband_hit_rates.csv").csv({typed: true});
const agencyContraband = await FileAttachment("data/agency_contraband_2024.csv").csv({typed: true});
const agencyComments = await FileAttachment("data/agency_comments.csv").csv({typed: true});
```

```js
const statewide = raw.filter(d => d.section === "statewide");
const byAgency = raw.filter(d => d.section === "by_agency");

function sw(metric) { return statewide.filter(d => d.metric === metric); }
function ag(metric) { return byAgency.filter(d => d.metric === metric); }

const storyRaces = ["White", "Black", "Hispanic"];
const raceColors = {domain: storyRaces, range: ["#888", "#333", "#e8a820"]};

const swStopsData = sw("total_stops");

const perCapita = storyRaces.flatMap(race => {
  return pop.map(p => {
    const stops = swStopsData.find(d => d.race === race && d.year === p.year)?.value;
    const population = p[race];
    if (!stops || !population) return null;
    return {year: p.year, race, value: +(stops / population * 100).toFixed(1), stops, population};
  }).filter(d => d);
});

function trendChart(data, {title, ylabel, fmt = ".1f", height = 380, annotateYear = 2024, baseline} = {}) {
  const filtered = data.filter(d => storyRaces.includes(d.race));
  const marks = [
    Plot.lineY(filtered, {x: "year", y: "value", stroke: "race", strokeWidth: 2.5}),
    Plot.dot(filtered, {x: "year", y: "value", fill: "race", r: 4}),
    Plot.text(filtered.filter(d => d.year === annotateYear), {
      x: "year", y: "value",
      text: d => `${d.race} ${d3.format(fmt)(d.value)}`,
      fill: "race", dx: 10, textAnchor: "start", fontWeight: 600, fontSize: 12
    }),
  ];
  if (baseline != null) marks.unshift(Plot.ruleY([baseline], {stroke: "#ddd", strokeDasharray: "4,3"}));
  return Plot.plot({title, width, height, x: {label: null, tickFormat: "d", ticks: 5}, y: {label: ylabel, grid: true, nice: true}, color: raceColors, marks});
}
```

---

<div class="grid grid-cols-3">
  <div class="card">
    <h2>+19%</h2>
    <p>Hispanic per-capita stop rate increase</p>
  </div>
  <div class="card">
    <h2>+90%</h2>
    <p>Investigative stops of Hispanic drivers</p>
  </div>
  <div class="card">
    <h2>1.8×</h2>
    <p>Hispanic-to-white arrest ratio, 2024</p>
  </div>
</div>

---

## The per-capita rate

In 2020, Hispanic drivers were stopped at the lowest per-capita rate of any group in Missouri — 11.3 stops per 100 residents, compared with 18.6 for white drivers and 30.2 for Black drivers. By 2024, the Hispanic rate had climbed to 13.5, closing nearly half the gap with white drivers. The white and Black rates barely moved.

```js
trendChart(perCapita, {title: "Stops per 100 residents", ylabel: "Stops per 100 residents"})
```

```js
const popTable = storyRaces.map(race => {
  const p2020 = pop.find(d => d.year === 2020)?.[race];
  const p2024 = pop.find(d => d.year === 2024)?.[race];
  const s2020 = swStopsData.find(d => d.race === race && d.year === 2020)?.value;
  const s2024 = swStopsData.find(d => d.race === race && d.year === 2024)?.value;
  return {
    Race: race,
    "Pop 2020": d3.format(",")(p2020), "Pop 2024": d3.format(",")(p2024),
    "Pop change": d3.format("+.0%")((p2024 - p2020) / p2020),
    "Stop change": d3.format("+.0%")((s2024 - s2020) / s2020),
    "Per capita 2020": (s2020 / p2020 * 100).toFixed(1),
    "Per capita 2024": (s2024 / p2024 * 100).toFixed(1),
    "Per capita change": d3.format("+.0%")((s2024 / p2024 - s2020 / p2020) / (s2020 / p2020)),
  };
});
Inputs.table(popTable)
```

Population: U.S. Census Bureau, American Community Survey. 2020 uses 5-year estimates (1-year not released due to COVID); 2021–2024 use 1-year estimates. "White" and "Black" are non-Hispanic.

---

## It's not a 2020 baseline problem

Was 2020 just artificially low because of COVID? The per-capita data says no. The Hispanic stop rate was 11.3 in 2020 *and* 11.3 in 2021 — identical. The climb doesn't start until 2022. The trend holds regardless of which year you use as a starting point.

```js
const from2020 = storyRaces.map(race => {
  const r20 = perCapita.find(d => d.race === race && d.year === 2020);
  const r24 = perCapita.find(d => d.race === race && d.year === 2024);
  return {Race: race, "Per capita change": d3.format("+.1f")(r24.value - r20.value), "% change": d3.format("+.0%")((r24.value - r20.value) / r20.value)};
});
const from2021 = storyRaces.map(race => {
  const r21 = perCapita.find(d => d.race === race && d.year === 2021);
  const r24 = perCapita.find(d => d.race === race && d.year === 2024);
  return {Race: race, "Per capita change": d3.format("+.1f")(r24.value - r21.value), "% change": d3.format("+.0%")((r24.value - r21.value) / r21.value)};
});
```

<div class="grid grid-cols-2">
<div class="card">

**2020 baseline**

```js
Inputs.table(from2020)
```

</div>
<div class="card">

**2021 baseline**

```js
Inputs.table(from2021)
```

</div>
</div>

Whether you start in 2020 or 2021, the Hispanic per-capita increase is roughly 5–6× larger than for white or Black drivers.

---

## The raw numbers

Missouri's Hispanic population grew about 32% between 2020 and 2024 — from roughly 263,000 to 347,000. But stops of Hispanic drivers grew 57%, nearly twice as fast.

```js
trendChart(sw("stop_index_2020_eq_100"), {title: "Traffic stops, indexed to 2020 = 100", ylabel: "Index", baseline: 100})
```

```js
const swStopsYoy = storyRaces.flatMap(race => {
  const sorted = swStopsData.filter(d => d.race === race).sort((a, b) => a.year - b.year);
  return sorted.slice(1).map((d, i) => ({
    year: d.year, race,
    value: +((d.value - sorted[i].value) / sorted[i].value * 100).toFixed(1),
    label: d3.format("+.1f")((d.value - sorted[i].value) / sorted[i].value * 100) + "%",
  }));
});

Plot.plot({
  title: "Year-over-year change in stops",
  subtitle: "Hispanic stops accelerated every year through 2023",
  width, height: 350,
  x: {label: null, tickFormat: "d"},
  y: {label: "% change from prior year", grid: true},
  color: raceColors,
  marks: [
    Plot.ruleY([0], {stroke: "#ddd"}),
    Plot.lineY(swStopsYoy, {x: "year", y: "value", stroke: "race", strokeWidth: 2.5, marker: "circle"}),
    Plot.text(swStopsYoy.filter(d => d.race === "Hispanic"), {
      x: "year", y: "value", text: "label", fill: "#e8a820", dy: -12, fontWeight: 600, fontSize: 12,
    }),
  ],
})
```

---

## Where the increase is concentrated: discretionary stops

Not all traffic stops are created equal. A moving violation leaves little room for discretion — you either ran the red light or you didn't. But equipment stops (tinted windows, broken taillight) and investigative stops (no traffic violation, just officer suspicion) are chosen by the officer. Those categories are where the Hispanic increase is sharpest.

```js
const reasons = ["reason_moving", "reason_equipment", "reason_investigative", "reason_license"];
const reasonLabels = {reason_moving: "Moving violation", reason_equipment: "Equipment", reason_investigative: "Investigative", reason_license: "License"};

const reasonIndexed = reasons.flatMap(r => {
  const hispRows = sw(r).filter(d => d.race === "Hispanic").sort((a, b) => a.year - b.year);
  const base = hispRows[0]?.value;
  if (!base) return [];
  return hispRows.map(d => ({year: d.year, reason: reasonLabels[r], index: +(d.value / base * 100).toFixed(1)}));
});

Plot.plot({
  title: "Hispanic stops by reason — indexed to 2020 = 100",
  width, height: 400,
  x: {label: null, tickFormat: "d", ticks: 5},
  y: {label: "Index (2020 = 100)", grid: true},
  color: {legend: true},
  marks: [
    Plot.ruleY([100], {stroke: "#ddd", strokeDasharray: "4,3"}),
    Plot.lineY(reasonIndexed, {x: "year", y: "index", stroke: "reason", strokeWidth: 2.5}),
    Plot.dot(reasonIndexed, {x: "year", y: "index", fill: "reason", r: 4}),
    Plot.text(reasonIndexed.filter(d => d.year === 2024), {
      x: "year", y: "index", text: d => `${d.reason} ${d.index}`,
      dx: 10, textAnchor: "start", fontWeight: 600, fontSize: 11
    }),
  ],
})
```

```js
const reasonCompare = reasons.flatMap(r => {
  return ["Hispanic", "White"].map(race => {
    const rRows = sw(r).filter(d => d.race === race).sort((a, b) => a.year - b.year);
    const v2020 = rRows.find(d => d.year === 2020)?.value;
    const v2024 = rRows.find(d => d.year === 2024)?.value;
    if (!v2020 || !v2024) return null;
    return {Reason: reasonLabels[r], Race: race, "2020": d3.format(",")(v2020), "2024": d3.format(",")(v2024), "Change": d3.format("+.0%")((v2024 - v2020) / v2020)};
  }).filter(d => d);
});
Inputs.table(reasonCompare, {sort: "Change", reverse: true})
```

Investigative stops of Hispanic drivers nearly doubled (985 → 1,868) while investigative stops of white drivers *fell* (29,565 → 25,158).

---

## It's not just one agency

The Missouri State Highway Patrol accounts for roughly a quarter of all traffic stops. Is the statewide surge just an MSHP story? No — it's actually worse without them. Hispanic stops by all other agencies combined grew 64%, compared to 44% for MSHP alone.

```js
const mshpData = byAgency.filter(d => d.agency === "Missouri State Highway Patrol");
const mshpStops = mshpData.filter(d => d.metric === "total_stops");

const mshpVsRest = storyRaces.flatMap(race => {
  return [2020, 2021, 2022, 2023, 2024].map(year => {
    const swVal = swStopsData.find(d => d.race === race && d.year === year)?.value;
    const mshpVal = mshpStops.find(d => d.race === race && d.year === year)?.value;
    if (!swVal || !mshpVal) return null;
    return {year, race, mshp: mshpVal, rest: swVal - mshpVal};
  }).filter(d => d);
});

const hispOnly = storyRaces.filter(r => r === "Hispanic").flatMap(race => {
  const rows = mshpVsRest.filter(d => d.race === race);
  const base_mshp = rows.find(d => d.year === 2020)?.mshp;
  const base_rest = rows.find(d => d.year === 2020)?.rest;
  return rows.flatMap(d => [
    {year: d.year, group: "MSHP", value: +(d.mshp / base_mshp * 100).toFixed(1)},
    {year: d.year, group: "All other agencies", value: +(d.rest / base_rest * 100).toFixed(1)},
  ]);
});

Plot.plot({
  title: "Hispanic stops indexed to 2020 — MSHP vs. everyone else",
  width, height: 380,
  x: {label: null, tickFormat: "d", ticks: 5},
  y: {label: "Index (2020 = 100)", grid: true},
  color: {legend: true},
  marks: [
    Plot.ruleY([100], {stroke: "#ddd", strokeDasharray: "4,3"}),
    Plot.lineY(hispOnly, {x: "year", y: "value", stroke: "group", strokeWidth: 2.5}),
    Plot.dot(hispOnly, {x: "year", y: "value", fill: "group", r: 4}),
    Plot.text(hispOnly.filter(d => d.year === 2024), {
      x: "year", y: "value", text: d => `${d.group} ${d.value}`,
      dx: 10, textAnchor: "start", fontWeight: 600, fontSize: 12
    }),
  ],
})
```

```js
const mshpSummary = storyRaces.map(race => {
  const rows = mshpVsRest.filter(d => d.race === race);
  const r20 = rows.find(d => d.year === 2020);
  const r24 = rows.find(d => d.year === 2024);
  return {
    Race: race,
    "MSHP 2020": d3.format(",")(r20.mshp), "MSHP 2024": d3.format(",")(r24.mshp),
    "MSHP change": d3.format("+.0%")((r24.mshp - r20.mshp) / r20.mshp),
    "Other 2020": d3.format(",")(r20.rest), "Other 2024": d3.format(",")(r24.rest),
    "Other change": d3.format("+.0%")((r24.rest - r20.rest) / r20.rest),
  };
});
Inputs.table(mshpSummary)
```

```js
const mshpSearch = mshpData.filter(d => d.metric === "search_rate_pct" && storyRaces.includes(d.race));
const mshpArrest = mshpData.filter(d => d.metric === "arrest_rate_pct" && storyRaces.includes(d.race));
```

<div class="grid grid-cols-2">

```js
trendChart(mshpSearch, {title: "MSHP search rate (%)", ylabel: "Search rate %", fmt: ".1f", height: 300})
```

```js
trendChart(mshpArrest, {title: "MSHP arrest rate (%)", ylabel: "Arrest rate %", fmt: ".2f", height: 300})
```

</div>

MSHP: 312,976 stops in 2024. Hispanic search rate 6.2% vs. white 3.2% (1.9×). Hispanic arrest rate 5.1% vs. white 2.7% (1.9×). MSHP has never submitted comments to the AG explaining its disparities.

---

## After the stop: searches and arrests

Once pulled over, Hispanic drivers face harsher outcomes than they did five years ago — and harsher than white drivers. In 2020, Hispanic drivers were searched at a *lower* rate than white drivers. By 2021, the lines crossed. By 2024, Hispanic drivers were searched at 6.4% vs. 4.7% for white drivers.

The arrest rate for Hispanic drivers climbed from 4.8% to 6.2%. For white drivers, it *fell* — from 3.7% to 3.4%.

<div class="grid grid-cols-2">

```js
trendChart(sw("search_rate_pct"), {title: "Search rate (% of stops)", ylabel: "Search rate %", fmt: ".1f", height: 320})
```

```js
trendChart(sw("arrest_rate_pct"), {title: "Arrest rate (% of stops)", ylabel: "Arrest rate %", fmt: ".2f", height: 320})
```

</div>

---

## Stopped more, found less

Contraband hit rates dropped for all groups between 2020 and 2024. But there's a crucial difference: white and Black searches *fell* (by 38% and 35%), while Hispanic searches *grew* by 32%. Police are searching more Hispanic drivers and finding contraband in a smaller share of those searches.

<div class="grid grid-cols-2">

```js
trendChart(contrabandData.map(d => ({...d, value: d.hit_rate})), {
  title: "Contraband hit rate (% of searches)", ylabel: "Hit rate %", fmt: ".1f", height: 320,
})
```

```js
Plot.plot({
  title: "Number of searches by race",
  subtitle: "Hispanic searches are the only ones going up",
  width, height: 320,
  x: {label: null, tickFormat: "d", ticks: 5},
  y: {label: "Searches", grid: true},
  color: raceColors,
  marks: [
    Plot.lineY(contrabandData, {x: "year", y: "searches", stroke: "race", strokeWidth: 2.5}),
    Plot.dot(contrabandData, {x: "year", y: "searches", fill: "race", r: 4}),
    Plot.text(contrabandData.filter(d => d.year === 2024), {
      x: "year", y: "searches",
      text: d => `${d.race} ${d3.format(",")(d.searches)}`,
      fill: "race", dx: 10, textAnchor: "start", fontWeight: 600, fontSize: 12
    }),
  ],
})
```

</div>

```js
const contrabandTable = storyRaces.map(race => {
  const r20 = contrabandData.find(d => d.race === race && d.year === 2020);
  const r24 = contrabandData.find(d => d.race === race && d.year === 2024);
  return {
    Race: race,
    "Searches 2020": d3.format(",")(r20.searches), "Searches 2024": d3.format(",")(r24.searches),
    "Search change": d3.format("+.0%")((r24.searches - r20.searches) / r20.searches),
    "Hit rate 2020": r20.hit_rate + "%", "Hit rate 2024": r24.hit_rate + "%",
  };
});
Inputs.table(contrabandTable)
```

Hispanic searches grew 32% (2,268 → 2,988) while contraband found fell 28% (798 → 574). White searches fell 38%. Hit rates dropped across the board, but only Hispanic drivers faced *more* searches despite the declining yield.

### Contraband hit rates by agency

Which agencies are searching Hispanic drivers the most and finding the least? The table below shows agencies with at least 20 Hispanic searches in 2024, sorted by hit rate. A low hit rate with a high search count suggests searches that aren't driven by strong probable cause.

```js
const hispContraband = agencyContraband
  .filter(d => d.race === "Hispanic" && d.searches >= 20)
  .sort((a, b) => a.hit_rate - b.hit_rate);

const whiteContraband = new Map(
  agencyContraband.filter(d => d.race === "White").map(d => [d.agency, d])
);

const contrabandAgTable = hispContraband.map(d => {
  const w = whiteContraband.get(d.agency);
  return {
    Agency: d.agency,
    "Hispanic searches": d.searches,
    "Hispanic hit rate": d.hit_rate + "%",
    "White hit rate": w ? w.hit_rate + "%" : "—",
    "Gap": w ? d3.format("+.1f")(d.hit_rate - w.hit_rate) + " pp" : "—",
  };
});
Inputs.table(contrabandAgTable, {sort: "Hispanic hit rate"})
```

---

## Where is this happening?

The surge isn't uniform. Some agencies saw Hispanic stops increase by hundreds of percent.

```js
const minStops = view(Inputs.range([50, 500], {label: "Min Hispanic stops in 2024", step: 25, value: 100}));
```

```js
const agPct = ag("pct_change_2020_to_2024").filter(d => d.race === "Hispanic");
const agStops2024 = ag("total_stops").filter(d => d.race === "Hispanic" && d.year === 2024);
const agencyLookup = new Map(agStops2024.map(d => [d.agency, d.value]));

const agencyChanges = agPct
  .filter(d => (agencyLookup.get(d.agency) ?? 0) >= minStops)
  .map(d => ({agency: d.agency, type: d.agency_type, pct_change: d.value, stops_2024: agencyLookup.get(d.agency)}))
  .sort((a, b) => b.pct_change - a.pct_change);
```

```js
Plot.plot({
  title: "Change in Hispanic stops, 2020–2024",
  width, height: Math.max(300, agencyChanges.slice(0, 25).length * 24),
  marginLeft: 240,
  x: {label: "% change since 2020", grid: true},
  y: {label: null},
  marks: [
    Plot.ruleX([0], {stroke: "#ddd"}),
    Plot.barX(agencyChanges.slice(0, 25), {
      y: "agency", x: "pct_change", fill: "#e8a820", sort: {y: "-x"},
      tip: true, title: d => `${d.agency}\n${d3.format("+,.0f")(d.pct_change)}%\n${d3.format(",")(d.stops_2024)} Hispanic stops in 2024`,
    }),
    Plot.text(agencyChanges.slice(0, 25), {
      y: "agency", x: "pct_change",
      text: d => d3.format("+,.0f")(d.pct_change) + "%",
      dx: 4, textAnchor: "start", fontSize: 11
    }),
  ],
})
```

```js
Inputs.table(agencyChanges, {
  columns: ["agency", "type", "stops_2024", "pct_change"],
  header: {agency: "Agency", type: "Type", stops_2024: "Hispanic stops (2024)", pct_change: "% change since 2020"},
  format: {pct_change: d => d3.format("+,.0f")(d) + "%", stops_2024: d => d3.format(",")(d)},
  sort: "pct_change", reverse: true,
})
```

---

## Stops vs. local population

If police stopped people in proportion to the local population, the "Hispanic share of stops" and "Hispanic share of population" would be roughly equal. Agencies above the diagonal line are stopping Hispanic drivers more than their share would predict.

```js
const censusYearLookup = new Map();
for (const row of censusByYear) censusYearLookup.set(`${row.name}||${row.year}`, row);

const popLookup = new Map(agencyPop.map(d => [d.agency, d]));

const disparity = ag("total_stops")
  .filter(d => d.year === 2024 && (d.race === "Hispanic" || d.race === "Total"))
  .reduce((acc, d) => {
    if (!acc.has(d.agency)) acc.set(d.agency, {agency: d.agency, type: d.agency_type});
    const obj = acc.get(d.agency);
    if (d.race === "Hispanic") obj.hisp_stops = d.value;
    if (d.race === "Total") obj.total_stops = d.value;
    return acc;
  }, new Map());

const scatterData = [...disparity.values()]
  .filter(d => d.hisp_stops && d.total_stops && d.total_stops >= 100)
  .map(d => {
    const p = popLookup.get(d.agency);
    if (!p || !p.pop_hispanic || !p.pop_total) return null;
    return {
      ...d,
      hisp_stop_pct: +(d.hisp_stops / d.total_stops * 100).toFixed(1),
      hisp_pop_pct: +(p.pop_hispanic / p.pop_total * 100).toFixed(1),
      pop_hispanic: p.pop_hispanic, pop_total: p.pop_total,
      census_match_name: p.census_match_name,
    };
  })
  .filter(d => d);

Plot.plot({
  title: "Hispanic share of stops vs. Hispanic share of local population (2024)",
  width, height: 500,
  x: {label: "Hispanic % of local population →", grid: true, nice: true},
  y: {label: "Hispanic % of stops →", grid: true, nice: true},
  marks: [
    Plot.link([{x1: 0, y1: 0, x2: 50, y2: 50}], {x1: "x1", y1: "y1", x2: "x2", y2: "y2", stroke: "#ddd", strokeDasharray: "4,3"}),
    Plot.dot(scatterData, {
      x: "hisp_pop_pct", y: "hisp_stop_pct",
      r: d => Math.sqrt(d.total_stops) / 8,
      fill: "#e8a820", fillOpacity: 0.6, stroke: "#333", strokeWidth: 0.5,
      tip: true,
      title: d => `${d.agency}\nHispanic stops: ${d.hisp_stop_pct}% (${d3.format(",")(d.hisp_stops)})\nHispanic pop: ${d.hisp_pop_pct}%\nTotal stops: ${d3.format(",")(d.total_stops)}`,
    }),
    Plot.text(scatterData.filter(d => d.hisp_stops > 400 || d.hisp_stop_pct > 15), {
      x: "hisp_pop_pct", y: "hisp_stop_pct", text: "agency", fontSize: 9, dy: -10,
    }),
  ],
})
```

Circle size = total stops. 587 of 612 agencies matched to Census geography.

```js
const gapData = scatterData
  .map(d => ({
    Agency: d.agency, Type: d.type,
    "Hispanic pop %": d.hisp_pop_pct, "Hispanic stop %": d.hisp_stop_pct,
    "Gap (pp)": +(d.hisp_stop_pct - d.hisp_pop_pct).toFixed(1),
    "Hispanic stops": d.hisp_stops,
  }))
  .filter(d => d["Hispanic stops"] >= 50)
  .sort((a, b) => b["Gap (pp)"] - a["Gap (pp)"]);
Inputs.table(gapData, {sort: "Gap (pp)", reverse: true})
```

---

## Agency per-capita trends over time

Using year-by-year Census estimates, we can track how the per-capita stop rate for Hispanic drivers changed at specific agencies. Select an agency to see whether its local Hispanic stop rate outpaced local population growth.

```js
const agPerCapitaList = [...new Set(scatterData.map(d => d.agency))].sort();
const selectedPerCapita = view(Inputs.select(agPerCapitaList, {label: "Agency", value: agPerCapitaList.find(a => a === "Missouri State Highway Patrol") ?? agPerCapitaList[0]}));
```

```js
const selAgPop = popLookup.get(selectedPerCapita);
const censusName = selAgPop?.census_match_name;

const agPerCapita = storyRaces.flatMap(race => {
  return [2020, 2021, 2022, 2023, 2024].map(year => {
    const stops = byAgency.find(d => d.agency === selectedPerCapita && d.metric === "total_stops" && d.race === race && d.year === year)?.value;
    const censusRow = censusYearLookup.get(`${censusName}||${year}`);
    const population = censusRow?.[race];
    if (!stops || !population || population === 0) return null;
    return {year, race, value: +(stops / population * 100).toFixed(2), stops, population};
  }).filter(d => d);
});
```

```js
trendChart(agPerCapita, {title: `${selectedPerCapita} — stops per 100 residents`, ylabel: "Stops per 100 residents", fmt: ".1f"})
```

```js
const agPcTable = storyRaces.flatMap(race => {
  return [2020, 2024].map(year => {
    const d = agPerCapita.find(r => r.race === race && r.year === year);
    if (!d) return null;
    return {Race: race, Year: year, Stops: d3.format(",")(d.stops), Population: d3.format(",")(d.population), "Per capita": d.value};
  }).filter(d => d);
});
Inputs.table(agPcTable)
```

---

## Arrest rate ratios by agency

Where are Hispanic drivers most disproportionately arrested? The chart shows the Hispanic arrest rate divided by the white arrest rate for agencies with at least 100 Hispanic stops in 2024.

```js
const agStopsAll = ag("total_stops").filter(d => d.year === 2024);
const agArrestAll = ag("arrest_rate_pct").filter(d => d.year === 2024);
const stopsMap = d3.group(agStopsAll, d => d.agency);
const arrestMap = d3.group(agArrestAll, d => d.agency);

const ratioRows = [];
for (const [agency, sRows] of stopsMap) {
  const hispStops = sRows.find(d => d.race === "Hispanic")?.value;
  if (!hispStops || hispStops < 100) continue;
  const aRows = arrestMap.get(agency);
  if (!aRows) continue;
  const hispArrest = aRows.find(d => d.race === "Hispanic")?.value;
  const whiteArrest = aRows.find(d => d.race === "White")?.value;
  if (!hispArrest || !whiteArrest || whiteArrest === 0) continue;
  ratioRows.push({agency, hisp_stops: hispStops, hisp_arrest_pct: hispArrest, white_arrest_pct: whiteArrest, ratio: +(hispArrest / whiteArrest).toFixed(2)});
}
ratioRows.sort((a, b) => b.ratio - a.ratio);
const topRatios = ratioRows.slice(0, 20);
```

```js
Plot.plot({
  title: "Hispanic-to-white arrest ratio by agency (2024)",
  subtitle: "Dashed line = statewide ratio (1.8×)",
  width, height: Math.max(300, topRatios.length * 26),
  marginLeft: 260,
  x: {label: "Arrest ratio (Hispanic ÷ White)", grid: true},
  y: {label: null},
  marks: [
    Plot.ruleX([1], {stroke: "#ddd"}),
    Plot.ruleX([1.8], {stroke: "#e8a820", strokeDasharray: "4,3", strokeWidth: 1.5}),
    Plot.barX(topRatios, {
      y: "agency", x: "ratio", fill: d => d.ratio >= 1.8 ? "#e8a820" : "#ccc", sort: {y: "-x"},
      tip: true,
      title: d => `${d.agency}\nHispanic arrest rate: ${d.hisp_arrest_pct}%\nWhite arrest rate: ${d.white_arrest_pct}%\nHispanic stops: ${d3.format(",")(d.hisp_stops)}`,
    }),
    Plot.text(topRatios, {
      y: "agency", x: "ratio", text: d => d.ratio.toFixed(1) + "×",
      dx: 4, textAnchor: "start", fontSize: 11, fontWeight: 600,
    }),
  ],
})
```

```js
Inputs.table(ratioRows, {
  columns: ["agency", "hisp_stops", "hisp_arrest_pct", "white_arrest_pct", "ratio"],
  header: {agency: "Agency", hisp_stops: "Hispanic stops", hisp_arrest_pct: "Hispanic arrest %", white_arrest_pct: "White arrest %", ratio: "Ratio"},
  format: {hisp_stops: d => d3.format(",")(d), ratio: d => d.toFixed(1) + "×"},
  sort: "ratio", reverse: true,
})
```

---

## Who's been asked to explain — and who responded

Each year, the attorney general invites agencies to submit comments explaining their data. In 2020–2023, roughly 500+ agencies were included in the comment file. In 2024, just 18 responded. The agencies with the largest Hispanic stop increases and disparity ratios are overwhelmingly silent.

```js
// For each agency in our ratioRows (high-disparity agencies), check comment history
const commentsByAgency = d3.group(agencyComments, d => d.agency);

const commentCheck = ratioRows.map(d => {
  const comments = commentsByAgency.get(d.agency) ?? [];
  const withText = comments.filter(c => c.has_comment === "True" || c.has_comment === true);
  const years = withText.map(c => c.year).sort();
  return {
    Agency: d.agency,
    "Hispanic stops": d.hisp_stops,
    "Arrest ratio": d.ratio.toFixed(1) + "×",
    "Years commented": years.length > 0 ? years.join(", ") : "Never",
    "Commented in 2024": years.includes(2024) || years.includes("2024") ? "Yes" : "No",
  };
});
Inputs.table(commentCheck)
```

```js
// 2024 response rate
const commented2024 = new Set(
  agencyComments.filter(d => d.year === 2024 && (d.has_comment === "True" || d.has_comment === true)).map(d => d.agency)
);
const totalAgencies2024 = new Set(ag("total_stops").filter(d => d.year === 2024 && d.race === "Total").map(d => d.agency));
```

<div class="grid grid-cols-3">
<div class="card">
  <h2>${commented2024.size}</h2>
  <p>Agencies commented in 2024</p>
</div>
<div class="card">
  <h2>${totalAgencies2024.size}</h2>
  <p>Total reporting agencies</p>
</div>
<div class="card">
  <h2>${d3.format(".0%")(commented2024.size / totalAgencies2024.size)}</h2>
  <p>Response rate</p>
</div>
</div>

MSHP — the state's largest agency with 312,976 stops — has rows in the comment file for 2020–2023 but never submitted actual text. They did not respond in 2024 either.

---

## Agency deep dive

Pick an agency to see how its stop numbers, search rates, and arrest rates have changed over time.

```js
const agencyList = [...new Set(byAgency.filter(d => d.race === "Hispanic" && d.metric === "total_stops").map(d => d.agency))].sort();
const selectedAgency = view(Inputs.select(agencyList, {label: "Agency", value: "Missouri State Highway Patrol"}));
```

```js
const agencyStops = byAgency.filter(d => d.agency === selectedAgency && d.metric === "total_stops" && storyRaces.includes(d.race));

Plot.plot({
  title: `${selectedAgency} — stops by race`,
  width, height: 380,
  x: {label: null, tickFormat: "d"},
  y: {label: "Stops", grid: true},
  color: raceColors,
  marks: [
    Plot.lineY(agencyStops, {x: "year", y: "value", stroke: "race", strokeWidth: 2.5}),
    Plot.dot(agencyStops, {x: "year", y: "value", fill: "race", r: 4}),
  ],
})
```

<div class="grid grid-cols-2">

```js
const agencySearch = byAgency.filter(d => d.agency === selectedAgency && d.metric === "search_rate_pct" && storyRaces.includes(d.race));
trendChart(agencySearch, {title: "Search rate %", ylabel: "Search rate %", fmt: ".1f", height: 280})
```

```js
const agencyArrest = byAgency.filter(d => d.agency === selectedAgency && d.metric === "arrest_rate_pct" && storyRaces.includes(d.race));
trendChart(agencyArrest, {title: "Arrest rate %", ylabel: "Arrest rate %", fmt: ".2f", height: 280})
```

</div>

---

## Reporting context

### The 287(g) wave

The surge in Hispanic stops predates — but now intersects with — a rapid expansion of immigration enforcement agreements in Missouri.

The Missouri State Highway Patrol signed a 287(g) task force agreement with ICE on March 21, 2025. Under the task force model, troopers can ask about immigration status during routine traffic stops. The program went operational September 26, 2025. Since then, MSHP has contacted ICE about 53 people and obtained detainers on 44.

As of March 2026, more than 60 agencies across Missouri have signed 287(g) agreements. At least 28 use the task force model — the most aggressive type, which embeds immigration enforcement into everyday traffic policing.

The data in this notebook shows the Hispanic stop pattern was already escalating from 2022–2024, before most agreements were signed. The question: was the enforcement culture already shifting before the formal agreements made it policy?

**Key agencies with 287(g) task force agreements:**
- Missouri State Highway Patrol (signed March 2025)
- Pulaski County Sheriff's Office (July 2025)
- Phelps County Sheriff's Office (October 2025)
- St. Charles County (considering, March 2026)

**Documented incidents where traffic stops led to ICE transfers:**
- O'Fallon PD: traffic stop → former DACA recipient transferred to ICE, held in rural jail
- St. Ann PD: traffic-related detention → transfer after officer found outstanding warrants

Austin Kocher, a Syracuse University researcher studying 287(g), warned that the task force model "can exacerbate community concerns around racial profiling, can lead to unlawful arrests, and can generate backlash of lawsuits that can and have cost taxpayers in these jurisdictions a lot of money."

*Sources: [KBIA](https://www.kbia.org/missouri-news/2026-03-18/ice-researcher-warns-of-risks-for-local-police-signing-onto-new-agreements-with-agency), [STLPR](https://www.stlpr.org/government-politics-issues/2026-03-11/immigration-enforcement-researcher-287-agreements), [KCUR](https://www.kcur.org/news/2026-03-18/missouri-police-ice-deportation-immigration-enforcement)*

### The AG tried to stop publishing the disparity data

In May 2025, the Missouri NAACP sued Attorney General Andrew Bailey's office for removing the disparity index — the core measure of whether police are stopping people in proportion to their population — from the 2023 vehicle stops report. The AG called it of "limited analytical value."

The NAACP settled in December 2025. The AG's office agreed to restore the disparity index to the 2023 and 2024 reports.

The disparity index is essentially what the "Stops vs. local population" scatter plot in this notebook computes. The state's own attorney general tried to stop publishing it.

*Sources: [Missouri Independent](https://missouriindependent.com/2025/05/29/naacp-lawsuit-accuses-missouri-ag-of-illegally-withholding-info-on-police-vehicle-stops/), [ABC17](https://abc17news.com/news/missouri/2025/12/29/naacp-settles-lawsuit-with-missouri-attorney-general-over-vehicle-stops-report/)*

### The advocacy landscape

Empower Missouri and the Coalition for Fair Policing have tracked the VSR for 19 years. They are pushing for the Fourth Amendment Affirmation Act, which would move beyond data collection to consequences for officers who engage in racial profiling. Their analysis has consistently flagged that non-white drivers — particularly Black and Hispanic — are searched at rates far exceeding their white counterparts, and that the contraband hit rate for those searches is lower.

*Source: [Empower Missouri](https://empowermissouri.org/by-the-numbers-the-missouri-vehicle-stops-report/)*

### Reporting targets

Agencies to contact, cross-referenced across multiple disparity metrics:

```js
// Cross-reference: which agencies appear in the worst lists across multiple metrics?
const stopChangeMap = new Map(agencyChanges.map(d => [d.agency, d.pct_change]));
const gapMap = new Map(gapData.map(d => [d.Agency, d["Gap (pp)"]]));
const ratioMap = new Map(ratioRows.map(d => [d.agency, d.ratio]));
const commentMap = new Map(commentCheck.map(d => [d.Agency, d["Years commented"]]));

// Agencies with 100+ Hispanic stops
const targetAgencies = [...new Set([
  ...agencyChanges.filter(d => d.pct_change > 50).map(d => d.agency),
  ...ratioRows.filter(d => d.ratio > 2).map(d => d.agency),
  ...gapData.filter(d => d["Gap (pp)"] > 3).map(d => d.Agency),
])];

const targets = targetAgencies.map(agency => {
  const hispContra = agencyContraband.find(d => d.agency === agency && d.race === "Hispanic");
  return {
    Agency: agency,
    "Stop change": stopChangeMap.has(agency) ? d3.format("+,.0f")(stopChangeMap.get(agency)) + "%" : "—",
    "Arrest ratio": ratioMap.has(agency) ? ratioMap.get(agency).toFixed(1) + "×" : "—",
    "Pop gap (pp)": gapMap.has(agency) ? gapMap.get(agency).toFixed(1) : "—",
    "Hisp hit rate": hispContra ? hispContra.hit_rate + "%" : "—",
    "Commented": commentMap.get(agency) ?? "Unknown",
    "Has 287(g)": ["Missouri State Highway Patrol", "Pulaski County Sheriff's Dept", "Phelps County Sheriff's Dept", "Pettis County Sheriff's Dept", "Callaway County Sheriff's Dept"].includes(agency) ? "Yes" : "—",
  };
}).sort((a, b) => {
  const aScore = (parseFloat(a["Stop change"]) || 0) + (parseFloat(a["Arrest ratio"]) || 0) * 20;
  const bScore = (parseFloat(b["Stop change"]) || 0) + (parseFloat(b["Arrest ratio"]) || 0) * 20;
  return bScore - aScore;
});

Inputs.table(targets)
```

### Past reporting

Nearly all prior coverage of the Missouri VSR has focused on Black-white disparities. The Hispanic trend is essentially unreported.

- **[Decades of Data Suggest Racial Profiling is Getting Worse, Not Better](https://flatlandkc.org/stopped-profiling-the-police/decades-of-data-suggest-racial-profiling-is-getting-worse-not-better/)** — Flatland KC, 2020. The most comprehensive investigation. Analyzed 20 years and 30+ million stops. Found Black drivers stopped 59% above expected rate. Hispanic drivers mentioned only in passing (arrested at 9.9% of stops — actually higher than Black drivers' 9.1%). Key finding: two decades of data collection produced no meaningful policy reform.

- **[Report shows Black drivers in Missouri more likely to be ticketed, arrested](https://missouriindependent.com/2023/06/03/report-shows-black-drivers-in-missouri-ticketed-arrested-at-much-higher-numbers-than-whites/)** — Missouri Independent, 2023. Covered the 2022 VSR. Entirely Black-white framing. Noted Hispanic citation rate was 50.4% vs. 38.2% for white but didn't investigate further.

- **[Missouri report on traffic stops revives call for anti-discrimination policies](https://www.stlpr.org/government-politics-issues/2018-06-04/missouri-report-on-traffic-stops-revives-call-for-anti-discrimination-policies)** — STLPR, 2018. Black drivers stopped 85% above white rate. Hispanic mentioned only as "searched at higher rates than average." Coalition for Fair Policing pushed the 4th Amendment Affirmation Act.

- **[Missouri Police Stop Black Drivers At Higher Rates](https://www.kcur.org/news/2019-06-04/missouri-police-stop-black-drivers-at-higher-rates-than-white-drivers)** — KCUR, 2019. Annual VSR coverage. Black-focused.

- **[Traffic stop report shows racial disparities as officials downplay numbers](https://www.columbiamissourian.com/news/local/traffic-stop-report-shows-racial-disparities-as-officials-downplay-numbers/article_6018752a-65ac-11e8-b3c1-d319bcc3cee9.html)** — Columbia Missourian, 2018. Boone County angle. Officials dismissed the data.

- **[NAACP lawsuit accuses Missouri AG of illegally withholding info on police vehicle stops](https://missouriindependent.com/2025/05/29/naacp-lawsuit-accuses-missouri-ag-of-illegally-withholding-info-on-police-vehicle-stops/)** — Missouri Independent, 2025. The AG removed the disparity index from the 2023 report. NAACP sued. Settled Dec 2025 — AG agreed to restore it.

- **[Missouri AG criticized for removing racial 'disparity index' from vehicle stops report](https://www.stltoday.com/news/local/government-politics/missouri-ag-criticized-for-removing-racial-disparity-index-from-vehicle-stops-report/article_0f8c58f6-5bdc-11ef-ba59-671894889699.html)** — St. Louis Post-Dispatch. The AG called the disparity index of "limited analytical value."

### Sources to pursue

**Agencies with the worst composite scores across multiple metrics:**
- **St. Charles PD** — 358 Hispanic stops (+426%), arrest rate 22.9% vs 6.7% White (3.4×), contraband hit rate 6.5%. Never commented.
- **St. Ann PD** — 275 Hispanic stops (+759%), 32.7% search rate, 29.5% arrest rate. Hit rate 11.1%. Never commented.
- **Perry County Sheriff** — searching and arresting 16.7% of Hispanic drivers. White arrest rate: 2.8%. 5.9× ratio.
- **Taney County Sheriff** — Branson area, tourism economy with large Hispanic workforce. +215%, 4.5× arrest ratio.
- **St. Peters PD, Lake St. Louis PD** — same St. Charles County corridor as St. Charles PD. Possible regional pattern. None ever commented.
- **Crawford County Sheriff, Jefferson County Sheriff, Moniteau County Sheriff** — rural counties with extreme arrest ratios and no comments.

**Researchers and advocates:**
- **Austin Kocher**, Syracuse University — 287(g) researcher, already on the record in Missouri (KBIA, STLPR, KCUR). Can contextualize the traffic stop → immigration enforcement pipeline.
- **Empower Missouri / Coalition for Fair Policing** — 19 years of VSR advocacy, focused on Black disparities. Have not flagged the Hispanic trend.
- **ACLU of Missouri** — filed 287(g) challenges, can speak to the legal framework.
- **Missouri NAACP** — just settled the disparity index lawsuit with the AG. They know the data and the politics.

**Community sources:**
- **Hispanic community organizations** in Jackson County (KC metro), Greene County (Springfield), Jasper County (Joplin), St. Charles County — the counties with the fastest Hispanic population growth and agency-level disparities. Ask what drivers are experiencing.
- **Defense attorneys** in high-disparity jurisdictions — they see the probable cause affidavits. They know whether "equipment violation" means a real broken taillight or pretext.

**Government sources:**
- **MSHP public affairs** — 312,976 stops, never submitted a comment to the AG, signed the 287(g). Ask what changed.
- **The AG's office** — why did agency comment responses drop from 500+ to 18 in 2024? Is that a reporting change or did agencies just stop responding?
- **Individual agency chiefs/sheriffs** on the reporting targets list — the data is the opening question: "Your Hispanic stops rose X% while your contraband hit rate fell. Can you explain what changed?"

**Records to request:**
- MSHP 287(g) implementation documents — when it went operational, training materials, number of ICE referrals from traffic stops by month
- Agency policies on investigative stops or equipment stops — to understand whether the discretionary increase reflects a policy change or individual officer behavior

---

Data: Missouri Attorney General, Vehicle Stops Report, 2020–2024. Population: U.S. Census Bureau, American Community Survey (2020: 5-year; 2021–2024: 1-year). "White" and "Black" refer to non-Hispanic populations.
