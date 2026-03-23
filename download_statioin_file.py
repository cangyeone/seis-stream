#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import time
from typing import List, Dict, Optional

from obspy.clients.fdsn import Client
from obspy import UTCDateTime
from obspy.core.inventory import Inventory

# ----------------------------
# Config
# ----------------------------
START = "2022-06-08T00:00:00"
END   = "2022-06-15T00:00:00"

OUTDIR = "data/compare_dataset/continous_usa"

MAX_RETRY = 5
SLEEP_BASE = 2          # seconds
TIMEOUT_SEC = 60        # station-level 通常 30-60s 足够

# Networks: BK/NC -> NCEDC, CI -> SCEDC
NETWORKS = [
    ("BK", "NCEDC"),
    ("NC", "NCEDC"),
    ("CI", "SCEDC"),
]

# ----------------------------
# Helpers
# ----------------------------
def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)

def retry_get_stations(client: Client, **kwargs) -> Inventory:
    last_err: Optional[Exception] = None
    for i in range(1, MAX_RETRY + 1):
        try:
            return client.get_stations(**kwargs)
        except Exception as e:
            last_err = e
            sleep_s = SLEEP_BASE * (2 ** (i - 1))
            print(f"[WARN] get_stations failed (try {i}/{MAX_RETRY}): {e}. Sleep {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"get_stations failed after {MAX_RETRY} retries: {last_err}")

def flatten_station(inv: Inventory) -> List[Dict]:
    rows = []
    for net in inv:
        for sta in net:
            rows.append({
                "net": net.code,
                "sta": sta.code,
                "lat": getattr(sta, "latitude", None),
                "lon": getattr(sta, "longitude", None),
                "elev_m": getattr(sta, "elevation", None),
                "start": str(sta.start_date) if sta.start_date else "",
                "end": str(sta.end_date) if sta.end_date else "",
            })
    return rows

def write_csv(rows: List[Dict], path: str):
    if not rows:
        print(f"[WARN] no rows -> {path}")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

# ----------------------------
# Main
# ----------------------------
def main():
    t0 = UTCDateTime(START)
    t1 = UTCDateTime(END)
    ensure_outdir(OUTDIR)

    all_inv: Optional[Inventory] = None
    all_rows: List[Dict] = []

    for net_code, center in NETWORKS:
        print(f"\n[INFO] Fetch station positions: net={net_code}, center={center}")
        client = Client(center, timeout=TIMEOUT_SEC)

        inv = retry_get_stations(
            client,
            network=net_code,
            station="*",
            starttime=t0,
            endtime=t1,
            level="station"   # 只要台站级别：位置/高程/有效期
        )

        # merge inventory
        if all_inv is None:
            all_inv = inv
        else:
            all_inv += inv

        rows = flatten_station(inv)
        print(f"[OK] {net_code}: stations={len(rows)}")
        all_rows.extend(rows)

    # write outputs
    csv_path = os.path.join(OUTDIR, "stations_one_week_less_event.csv")
    xml_path = os.path.join(OUTDIR, "stations_one_week_less_event.xml")

    write_csv(all_rows, csv_path)
    print(f"[OK] wrote CSV: {csv_path}")

    if all_inv is not None:
        all_inv.write(xml_path, format="STATIONXML")
        print(f"[OK] wrote StationXML: {xml_path}")

    print(f"\n[DONE] output dir: {OUTDIR}/")

if __name__ == "__main__":
    main()
