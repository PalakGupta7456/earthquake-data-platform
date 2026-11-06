"""Statistical seismology and ML on the gold layer.

1. Gutenberg-Richter fits per region: magnitude of completeness (Mc), the
   b-value by maximum likelihood (Aki-Utsu) with its standard error, and the
   implied probability of at least one M≥5 / M≥6 event in the next 30 days
   (Poisson model).
2. Spatio-temporal clustering (DBSCAN on a combined distance-and-time metric)
   to find aftershock sequences and earthquake swarms in the last 30 days.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN

DM = 0.1          # magnitude bin width
EARTH_R_KM = 6371.0


# ---------- Gutenberg-Richter ----------
def magnitude_of_completeness(mags: np.ndarray) -> float:
    """Maximum-curvature method (+0.2 correction, Woessner & Wiemer 2005)."""
    bins = np.round(np.floor(mags / DM) * DM, 1)
    values, counts = np.unique(bins, return_counts=True)
    return float(values[counts.argmax()] + 0.2)


def b_value(mags: np.ndarray, mc: float) -> tuple[float, float, int]:
    """Aki-Utsu maximum-likelihood b-value with Shi & Bolt (1982) standard error."""
    m = mags[mags >= mc - 1e-9]
    n = len(m)
    if n < 30:
        return float("nan"), float("nan"), n
    mean = m.mean()
    b = np.log10(np.e) / (mean - (mc - DM / 2))
    se = 2.3 * b**2 * np.sqrt(((m - mean) ** 2).sum() / (n * (n - 1)))
    return float(b), float(se), n


def gutenberg_richter(events: pd.DataFrame, min_events: int = 150, top: int = 12) -> list[dict]:
    span_years = max((events["event_time"].max() - events["event_time"].min()).days / 365.25, 1 / 12)
    fits = []
    for region, g in events.groupby("region"):
        if len(g) < min_events or region == "Unknown":
            continue
        mags = g["mag"].to_numpy()
        mc = max(magnitude_of_completeness(mags), 2.5)
        b, se, n = b_value(mags, mc)
        if not np.isfinite(b):
            continue
        a = np.log10(n / span_years) + b * mc            # log10 annual rate of M >= 0

        def rate(m, a=a, b=b):
            return 10 ** (a - b * m)                     # annual rate of M >= m

        def p30(m):
            return 1 - np.exp(-rate(m) * 30 / 365.25)    # P(at least one in 30 days), Poisson
        bins = np.round(np.arange(mc, mags.max() + DM, DM), 1)
        observed = [int((mags >= x - 1e-9).sum()) for x in bins]
        fits.append({
            "region": region, "n": int(len(g)), "n_above_mc": n, "mc": round(mc, 1),
            "b": round(b, 3), "b_se": round(se, 3), "a": round(float(a), 3),
            "annual_rate_m5": round(float(rate(5.0)), 2), "annual_rate_m6": round(float(rate(6.0)), 3),
            "p30_m5": round(float(p30(5.0)), 4), "p30_m6": round(float(p30(6.0)), 4),
            "curve": {"mag": bins.tolist(), "observed": observed,
                      "fitted": [round(float(10 ** (a - b * x) * span_years), 2) for x in bins]},
        })
    return sorted(fits, key=lambda f: -f["n"])[:top]


# ---------- Spatio-temporal clustering ----------
def _haversine_km(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    la, lo = np.radians(lat)[:, None], np.radians(lon)[:, None]
    d = np.sin((la - la.T) / 2) ** 2 + np.cos(la) * np.cos(la.T) * np.sin((lo - lo.T) / 2) ** 2
    return 2 * EARTH_R_KM * np.arcsin(np.sqrt(np.clip(d, 0, 1)))


def find_clusters(events: pd.DataFrame, days: int = 30, km_scale: float = 60.0, day_scale: float = 4.0,
                  eps: float = 1.0, min_samples: int = 6) -> tuple[list[dict], pd.Series]:
    """DBSCAN where two events are 'close' when (km/60)^2 + (days/4)^2 <= 1."""
    recent = events[events["event_time"] >= events["event_time"].max() - pd.Timedelta(days=days)].reset_index(drop=True)
    if len(recent) < min_samples:
        return [], pd.Series(dtype=int)
    km = _haversine_km(recent["lat"].to_numpy(), recent["lon"].to_numpy())
    t = recent["event_time"].astype("int64").to_numpy() / 86_400e9
    dt = np.abs(t[:, None] - t[None, :])
    dist = np.sqrt((km / km_scale) ** 2 + (dt / day_scale) ** 2)
    labels = DBSCAN(eps=eps, min_samples=min_samples, metric="precomputed").fit_predict(dist)
    recent["cluster"] = labels
    clusters = []
    for cid, g in recent[recent["cluster"] >= 0].groupby("cluster"):
        g = g.sort_values("event_time")
        main = g.loc[g["mag"].idxmax()]
        before = (g["event_time"] < main["event_time"]).sum()
        gap = main["mag"] - g["mag"].drop(main.name).max() if len(g) > 1 else 0
        # Mainshock-aftershock if the largest event comes early and stands clearly above the rest
        kind = "Aftershock sequence" if before <= max(1, 0.2 * len(g)) and gap >= 0.5 else "Swarm"
        clusters.append({
            "id": int(cid), "kind": kind, "events": int(len(g)), "max_mag": round(float(main["mag"]), 1),
            "main_place": main["place"], "region": main["region"],
            "lat": round(float(g["lat"].mean()), 3), "lon": round(float(g["lon"].mean()), 3),
            "radius_km": round(float(_haversine_km(g["lat"].to_numpy(), g["lon"].to_numpy()).max() / 2), 1),
            "start": g["event_time"].min().isoformat(), "end": g["event_time"].max().isoformat(),
            "event_ids": g["event_id"].tolist(),
        })
    clusters.sort(key=lambda c: (-c["events"], -c["max_mag"]))
    return clusters, recent.set_index("event_id")["cluster"]
