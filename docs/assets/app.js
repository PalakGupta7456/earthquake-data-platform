/* Seismic Ledger dashboard: reads data/dashboard.json published by the pipeline. */
(() => {
  const $ = (id) => document.getElementById(id);
  const css = (n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
  const DEPTHS = [[70, "--d1", "0–70 km"], [150, "--d2", "70–150"], [300, "--d3", "150–300"], [Infinity, "--d4", "300+"]];
  const depthColor = (d) => css((DEPTHS.find(([u]) => (d ?? 0) < u) || DEPTHS[3])[1]);
  const utc = (iso) => new Date(iso.endsWith("Z") || iso.includes("+") ? iso : iso + "Z");
  const ago = (iso) => {
    const m = Math.round((Date.now() - utc(iso)) / 60000);
    return m < 60 ? `${m} min ago` : m < 1440 ? `${Math.round(m / 60)} h ago` : `${Math.round(m / 1440)} d ago`;
  };
  const fmtDate = (iso) => utc(iso).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
  let data, map, grChart;

  fetch("data/dashboard.json", { cache: "no-store" })
    .then((r) => { if (!r.ok) throw new Error(r.status); return r.json(); })
    .then((d) => { data = d; render(); })
    .catch(() => { $("meta").textContent = "The warehouse hasn't published yet. The hourly pipeline creates it."; });

  function render() {
    $("meta").textContent = `Warehouse refreshed ${ago(data.generated_at)}, every hour`;
    hero();
    health();
    clusters();
    daily();
    gr();
  }

  function hero() {
    const ev = data.events;
    $("count").textContent = ev.length.toLocaleString();
    const big = ev.reduce((a, b) => (b.m > a.m ? b : a), ev[0]);
    if (big) $("largest").innerHTML = `Largest: <b>M${big.m.toFixed(1)}</b> ${big.p}, ${ago(big.t)}`;
    $("depth-legend").innerHTML = DEPTHS.map(([, v, label]) => `<li style="background:${css(v)}"><span>${label}</span></li>`).join("");

    map = L.map("map", { worldCopyJump: true, scrollWheelZoom: false, zoomControl: false }).setView([15, 150], 2);
    L.control.zoom({ position: "bottomright" }).addTo(map);
    L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", {
      attribution: "Tiles &copy; Esri, data USGS", maxZoom: 16,
    }).addTo(map);
    [...ev].sort((a, b) => a.m - b.m).forEach((e) => {
      const c = depthColor(e.d);
      L.circleMarker([e.lat, e.lon], { radius: Math.max(2.5, Math.pow(1.75, e.m) / 3.2), color: c, weight: 1, fillColor: c, fillOpacity: 0.55 })
        .bindTooltip(`<b>M${e.m.toFixed(1)}</b> ${e.p}<br>${fmtDate(e.t)}, depth ${e.d ?? "?"} km`)
        .on("click", () => window.open(e.u, "_blank", "noopener"))
        .addTo(map);
    });
    data.clusters.forEach((c) => {
      L.circle([c.lat, c.lon], { radius: Math.max(c.radius_km, 25) * 1000, color: css("--ink"), weight: 1.2, dashArray: "4 4", fill: false })
        .bindTooltip(`${c.kind}: ${c.events} events, max M${c.max_mag}`).addTo(map);
    });
  }

  function health() {
    const r = data.run || {}, q = r.quality || {}, dbt = r.dbt || {}, s = r.silver || {};
    $("last-run").textContent = `Last run ${ago(data.generated_at)}. Each run lands raw data, upserts, rebuilds the dbt models and runs every data test.`;
    const fresh = q.freshness_minutes;
    const items = [
      ["Rows in silver", (q.rows ?? 0).toLocaleString(), ""],
      ["New or revised this run", `${(s.inserted ?? 0).toLocaleString()} / ${(s.updated ?? 0).toLocaleString()}`, ""],
      ["dbt tests passing", `${dbt.tests_passed ?? "–"} of ${dbt.tests ?? "–"}`, dbt.tests && dbt.tests_passed === dbt.tests ? "ok" : "bad"],
      ["Newest event", fresh == null ? "–" : fresh < 60 ? `${Math.round(fresh)} min` : `${(fresh / 60).toFixed(1)} h`, q.checks?.fresh_under_3h ? "ok" : "bad"],
      ["Duplicate ids", q.duplicate_ids ?? "–", q.duplicate_ids === 0 ? "ok" : "bad"],
      ["Quarantined rows", (q.quarantined ?? 0).toLocaleString(), ""],
    ];
    $("stats").innerHTML = items.map(([k, v, cls]) => `<div><dt>${k}</dt><dd class="${cls}">${v}</dd></div>`).join("");
    const runs = [...data.runs].reverse();
    const max = Math.max(...runs.map((x) => x.seconds), 1);
    $("runs").innerHTML = runs.map((x) => `<i class="${x.ok ? "" : "fail"}" style="height:${(x.seconds / max) * 100}%" title="${fmtDate(x.started)}: ${x.seconds.toFixed(1)} s${x.ok ? "" : " (failed)"}"></i>`).join("");
  }

  function clusters() {
    const cs = data.clusters;
    $("clusters").innerHTML = cs.length ? cs.slice(0, 6).map((c, i) => `<li><button type="button" data-i="${i}">
        <span class="kind">${c.kind}</span><span class="mag">M${c.max_mag.toFixed(1)}</span>
        <span class="where">${c.main_place}. ${c.events} events, ${fmtDate(c.start)} to ${fmtDate(c.end)}</span></button></li>`).join("")
      : `<li class="sub">No clustered activity in the last 30 days.</li>`;
    $("clusters").addEventListener("click", (e) => {
      const b = e.target.closest("button");
      if (!b) return;
      const c = cs[+b.dataset.i];
      map.flyTo([c.lat, c.lon], 6, { duration: 1 });
      document.querySelector(".hero").scrollIntoView({ behavior: "smooth" });
    });
  }

  function daily() {
    const d = data.daily, muted = css("--muted");
    new Chart($("daily"), {
      data: { labels: d.date, datasets: [
        { type: "bar", label: "Events", data: d.events, backgroundColor: css("--rule"), barPercentage: 1, categoryPercentage: 1 },
        { type: "line", label: "7-day average", data: d.avg7, borderColor: css("--accent"), borderWidth: 2, pointRadius: 0, tension: .3 },
      ] },
      options: { maintainAspectRatio: false, interaction: { mode: "index", intersect: false },
        plugins: { legend: { labels: { color: muted, boxWidth: 12 } } },
        scales: { x: { ticks: { color: muted, maxTicksLimit: 6, maxRotation: 0 }, grid: { display: false } },
                  y: { ticks: { color: muted }, grid: { color: css("--rule") } } } },
    });
  }

  function gr() {
    const fits = data.gutenberg_richter;
    const pct = (p) => p >= 0.995 ? ">99%" : p < 0.005 ? "<1%" : `${Math.round(p * 100)}%`;
    $("gr-table").querySelector("tbody").innerHTML = fits.map((f, i) => `<tr data-i="${i}" tabindex="0">
      <td>${f.region}</td><td>${f.n.toLocaleString()}</td><td>${f.b.toFixed(2)} <small>± ${f.b_se.toFixed(2)}</small></td><td>${f.mc.toFixed(1)}</td>
      <td><span class="pbar" style="width:${Math.max(2, f.p30_m5 * 60)}px"></span>${pct(f.p30_m5)}</td><td>${pct(f.p30_m6)}</td></tr>`).join("");
    const tbody = $("gr-table").querySelector("tbody");
    const pick = (i) => {
      tbody.querySelectorAll("tr").forEach((tr) => tr.setAttribute("aria-selected", tr.dataset.i == i));
      drawGR(fits[i]);
    };
    tbody.addEventListener("click", (e) => { const tr = e.target.closest("tr"); if (tr) pick(+tr.dataset.i); });
    tbody.addEventListener("keydown", (e) => { const tr = e.target.closest("tr"); if (tr && e.key === "Enter") pick(+tr.dataset.i); });
    if (fits.length) pick(0);
  }

  function drawGR(f) {
    $("gr-title").textContent = `${f.region}: b = ${f.b.toFixed(2)}, complete above M${f.mc.toFixed(1)}`;
    const muted = css("--muted");
    if (grChart) grChart.destroy();
    grChart = new Chart($("grchart"), {
      type: "scatter",
      data: { datasets: [
        { label: "Observed: events ≥ M", data: f.curve.mag.map((m, i) => ({ x: m, y: f.curve.observed[i] })).filter((p) => p.y > 0),
          backgroundColor: css("--ink"), pointRadius: 3 },
        { label: "Gutenberg-Richter fit", type: "line", data: f.curve.mag.map((m, i) => ({ x: m, y: f.curve.fitted[i] })).filter((p) => p.y >= 0.3),
          borderColor: css("--d3"), borderWidth: 2, pointRadius: 0 },
      ] },
      options: { maintainAspectRatio: false, plugins: { legend: { labels: { color: muted, boxWidth: 12 } } },
        scales: { x: { title: { display: true, text: "Magnitude", color: muted }, ticks: { color: muted }, grid: { color: css("--rule") } },
                  y: { type: "logarithmic", title: { display: true, text: "Number of events (log scale)", color: muted },
                       ticks: { color: muted, callback: (v) => [1, 10, 100, 1000, 10000].includes(v) ? v.toLocaleString() : "" }, grid: { color: css("--rule") } } } },
    });
  }
})();
