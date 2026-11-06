"""Synthetic USGS-shaped GeoJSON features for tests and offline runs.

Magnitudes follow Gutenberg-Richter (b≈1), events concentrate on plate
boundaries, and planted mainshocks trigger Omori-law aftershock sequences.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

ZONES = [  # (lat, lon, spread_deg, weight, region label, depth_mean)
    (38.0, 142.5, 4, 1.2, "Japan", 40), (-6.0, 130.0, 6, 1.4, "Indonesia", 90), (60.0, -152.0, 5, 1.1, "Alaska", 50),
    (-20.0, -70.0, 5, 0.9, "Chile", 70), (-28.0, -177.0, 4, 1.0, "Kermadec Islands", 120), (36.0, -118.0, 3, 0.8, "California", 8),
    (19.4, -155.3, 1, 0.5, "Hawaii", 10), (13.0, 125.0, 4, 0.8, "Philippines", 50), (37.5, 27.5, 3, 0.4, "Turkey", 12),
    (28.0, 84.0, 3, 0.35, "Nepal", 15), (-15.0, -173.0, 3, 0.6, "Tonga", 150), (15.0, -94.0, 4, 0.7, "Mexico", 40),
]


def generate(days: int = 120, per_day: float = 70, seed: int = 11, end: pd.Timestamp | None = None) -> list[dict]:
    rng = np.random.default_rng(seed)
    end = end or pd.Timestamp.now(tz="UTC").tz_localize(None).floor("min")
    start = end - pd.Timedelta(days=days)
    w = np.array([z[3] for z in ZONES])
    n = rng.poisson(per_day * days)
    zi = rng.choice(len(ZONES), n, p=w / w.sum())
    times = start + pd.to_timedelta(rng.uniform(0, days * 86400, n), unit="s")
    mags = 2.5 + rng.exponential(1 / np.log(10), n)  # b = 1
    rows = [(times[i], mags[i], zi[i], None) for i in range(n)]
    # Planted mainshocks with Omori aftershocks
    for k in range(4):
        z = rng.integers(len(ZONES))
        t0 = start + pd.Timedelta(days=float(rng.uniform(days - 28, days - 2)))
        m0 = rng.uniform(6.2, 7.3)
        rows.append((t0, m0, z, (k, 0.0)))
        for _ in range(int(10 ** (m0 - 5))):
            dt_days = 0.02 * ((1 - rng.random()) ** (-1 / 0.1) - 1)  # Omori-Utsu, p≈1.1
            if dt_days < 14 and t0 + pd.Timedelta(days=dt_days) <= end:
                rows.append((t0 + pd.Timedelta(days=dt_days), min(2.5 + rng.exponential(0.5), m0 - 0.6), z, (k, 0.25)))
    feats = []
    for i, (t, m, z, seq) in enumerate(rows):
        lat0, lon0, spread, _, label, depth = ZONES[z]
        if seq is None:
            lat, lon = lat0 + rng.normal(0, spread), lon0 + rng.normal(0, spread)
        else:
            lat0s = lat0 + (seq[0] - 1.5) * 0.7
            lat, lon = lat0s + rng.normal(0, seq[1]), lon0 + rng.normal(0, seq[1])
        ms = int(t.value // 1_000_000)
        feats.append({
            "type": "Feature", "id": f"syn{i:07d}",
            "properties": {"mag": round(float(m), 1), "place": f"{rng.integers(5, 120)} km NE of Somewhere, {label}",
                           "time": ms, "updated": ms + int(rng.integers(60_000, 86_400_000)), "magType": "mb", "type": "earthquake",
                           "sig": int(m * 100), "tsunami": int(m > 7), "alert": "green" if m > 6 else None,
                           "status": "reviewed" if rng.random() < 0.7 else "automatic", "felt": None,
                           "url": "https://earthquake.usgs.gov/"},
            "geometry": {"type": "Point", "coordinates": [float(((lon + 180) % 360) - 180), float(np.clip(lat, -89, 89)),
                                                          float(max(0, rng.normal(depth, depth * 0.4)))]},
        })
    return feats
