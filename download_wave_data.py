#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional, Set

from obspy import UTCDateTime, Stream
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.header import FDSNNoDataException
import numpy as np
import numpy.ma as ma

import threading, random
SCEDC_SEM = threading.Semaphore(1)   # 1 或 2 都行；建议先 1

# ----------------------------
# User config
# ----------------------------
#STATION files 
CSV_PATH = "data/compare_dataset/continous_usa/stations_one_week_less_event.csv"
START = "2022-06-08T00:00:00"
END   = "2022-06-15T00:00:00"

OUTDIR = "data/compare_dataset/continous_usa"
LOCATION = "*"              # 用 "*" 最保险；如果你只想要空loc，可改成 ""
MAX_WORKERS = 2
MAX_RETRY = 4
SLEEP_BASE = 2
TIMEOUT_SEC = 120
CHUNK_SECONDS = 86400       # 按天切片

F_TAG = "yzy"

# 自动通道发现缓存，建议开启
CACHE_CHANNELS = True
CHANNEL_CACHE_DIR = os.path.join(OUTDIR, "_channel_cache")


# ----------------------------
# Clients routing
# ----------------------------
def pick_client(net: str) -> Client:
    if net in ("BK", "NC"):
        return Client("NCEDC", timeout=TIMEOUT_SEC)
    if net == "CI":
        return Client("SCEDC", timeout=TIMEOUT_SEC)
    return Client("IRIS", timeout=TIMEOUT_SEC)


# ----------------------------
# Utilities
# ----------------------------
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def read_station_csv(path: str) -> List[Tuple[str, str]]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            net = row["net"].strip()
            sta = row["sta"].strip()
            if net and sta:
                out.append((net, sta))
    return sorted(set(out))

def daterange_chunks(t0: UTCDateTime, t1: UTCDateTime, chunk_s: int):
    cur = UTCDateTime(t0)
    while cur < t1:
        nxt = cur + chunk_s
        yield cur, min(nxt, t1)
        cur = nxt

def ymd_dirs(base: str, t0: UTCDateTime) -> str:
    y = f"{t0.year:04d}"
    m = f"{t0.month:02d}"
    d = f"{t0.day:02d}"
    out = os.path.join(base, y, m, d)
    ensure_dir(out)
    return out

def format_filename(net: str, sta: str, loc: str, cha: str, t0: UTCDateTime) -> str:
    # net.sta.loc.cha.YY.MM.DD.HH.MM.SS.f.mseed
    loc2 = loc if loc else "--"
    yy = t0.year % 100
    return (
        f"{net}.{sta}.{loc2}.{cha}."
        f"{yy:02d}.{t0.month:02d}.{t0.day:02d}."
        f"{t0.hour:02d}.{t0.minute:02d}.{t0.second:02d}."
        f"{F_TAG}.mseed"
    )

def retry_call(fn, **kwargs):
    last_err = None
    for i in range(1, MAX_RETRY + 1):
        try:
            return fn(**kwargs)
        except Exception as e:
            last_err = e
            time.sleep(SLEEP_BASE * (2 ** (i - 1)))
    raise last_err

def retry_get_waveforms(client: Client, **kwargs) -> Optional[Stream]:
    last_err = None
    for i in range(1, MAX_RETRY + 1):
        try:
            st = client.get_waveforms(**kwargs)
            if st is None or len(st) == 0:
                return None
            return st
        except FDSNNoDataException:
            # 204 No data -> 正常无数据
            return None
        except Exception as e:
            last_err = e
            time.sleep(SLEEP_BASE * (2 ** (i - 1)))
    # 如果最终是 no-data，就不 raise；否则 raise
    raise last_err


def write_mseed(st: Stream, path: str):
    ensure_dir(os.path.dirname(path))
    s = st.copy()
    s.sort()

    for tr in s:
        if isinstance(tr.data, ma.MaskedArray):
            tr.data = tr.data.astype(np.float32).filled(np.nan)
        else:
            # 如果是整数波形，也统一转 float32（更稳，且不插值）
            if np.issubdtype(tr.data.dtype, np.integer):
                tr.data = tr.data.astype(np.float32, copy=False)

    s.write(path, format="MSEED", encoding="FLOAT32")



# ----------------------------
# Channel discovery (auto)
# ----------------------------
def channel_cache_path(net: str, sta: str) -> str:
    ensure_dir(CHANNEL_CACHE_DIR)
    return os.path.join(CHANNEL_CACHE_DIR, f"{net}.{sta}.channels.txt")

def discover_channels(client: Client, net: str, sta: str, t0: UTCDateTime, t1: UTCDateTime) -> List[Tuple[str, str]]:
    """
    自动发现某台站在时间窗内的 (loc, channel_code)，例如 [("", "EHZ"), ("00","HHN"), ...]
    """
    cache_file = channel_cache_path(net, sta)
    if CACHE_CHANNELS and os.path.exists(cache_file) and os.path.getsize(cache_file) > 0:
        pairs = []
        with open(cache_file, "r", encoding="utf-8") as f:
            for line in f:
                loc, cha = line.strip().split()
                pairs.append(("" if loc == "--" else loc, cha))
        return pairs

    inv = retry_call(
        client.get_stations,
        network=net,
        station=sta,
        location="*",
        channel="*",
        starttime=t0,
        endtime=t1,
        level="channel",
    )

    pairs_set: Set[Tuple[str, str]] = set()
    for n in inv:
        for s in n:
            for ch in s:
                loc = ch.location_code or ""
                cha = ch.code or ""
                if len(cha) >= 3:
                    pairs_set.add((loc, cha))

    pairs = sorted(pairs_set)

    if CACHE_CHANNELS:
        with open(cache_file, "w", encoding="utf-8") as f:
            for loc, cha in pairs:
                f.write(f"{loc if loc else '--'} {cha}\n")

    return pairs


# ----------------------------
# Split and save per concrete channel (no gap filling)
# ----------------------------
def split_by_channel(st: Stream) -> Dict[Tuple[str, str, str, str], Stream]:
    """
    按 (net, sta, loc, channel_code) 分组，并对每组 merge(method=0) 做拼接（不填补缺失）。
    """
    buckets: Dict[Tuple[str, str, str, str], Stream] = defaultdict(Stream)
    for tr in st:
        net = tr.stats.network
        sta = tr.stats.station
        loc = tr.stats.location or ""
        cha = tr.stats.channel or ""
        if not cha:
            continue
        buckets[(net, sta, loc, cha)] += tr

    out: Dict[Tuple[str, str, str, str], Stream] = {}
    for key, s in buckets.items():
        # 不插值：method=0 仅拼接，不填 gap
        try:
            s2 = s.copy()
            s2.merge(method=0)
        except Exception:
            s2 = s
        out[key] = s2
    return out
def chunk_list(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]
def file_ok(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 0


# ----------------------------
# Core download per station per chunk
# ----------------------------
def download_one(net: str, sta: str, t0: UTCDateTime, t1: UTCDateTime) -> Tuple[str, str]:
    client = pick_client(net)
    outdir = ymd_dirs(OUTDIR, t0)

    # 发现通道列表
    try:
        pairs = discover_channels(client, net, sta, t0, t1)
    except Exception as e:
        return ("ERR", f"{net}.{sta} discover_channels failed: {e}")

    if not pairs:
        return ("NODATA", f"{net}.{sta} {t0} {t1} no channels in metadata")

    # 按 loc 分批请求（避免一次 channel 列表过长）
    loc_to_chans: Dict[str, List[str]] = defaultdict(list)
    already = 0
    need = 0

    for loc, cha in pairs:
        fn = format_filename(net, sta, loc, cha, t0)
        path = os.path.join(outdir, fn)
        if file_ok(path):
            already += 1
            continue
        loc_to_chans[loc].append(cha)
        need += 1
    if need == 0:
        return ("SKIP", f"{net}.{sta} {t0} {t1} already_done={already}")

    wrote = 0
    any_ok = False
    last_err = None

    for loc, chans in loc_to_chans.items():
        chans = sorted(set(chans))
        if not chans:
            continue

        try:
            any_batch_ok = False

            # 分批请求，避免 channel 列表过长（URL 过长/413/414）
            for chans_batch in chunk_list(chans, 20):
                chan_expr = ",".join(chans_batch)

                st = retry_get_waveforms(
                    client,
                    network=net,
                    station=sta,
                    location=loc if loc != "" else "*",
                    channel=chan_expr,
                    starttime=t0,
                    endtime=t1,
                    attach_response=False
                )
                if st is None or len(st) == 0:
                    continue

                # 按具体 channel 拆分并保存（每批都保存，避免 st 被覆盖只留最后一批）
                by_ch = split_by_channel(st)
                for (k_net, k_sta, k_loc, k_cha), s_ch in by_ch.items():
                    fn = format_filename(k_net, k_sta, k_loc, k_cha, t0)
                    path = os.path.join(outdir, fn)
                    write_mseed(s_ch, path)
                    wrote += 1
                    any_ok = True
                    any_batch_ok = True

            # 该 loc 下所有 batch 都无数据则继续下一个 loc
            if not any_batch_ok:
                continue

        except Exception as e:
            last_err = e
            continue

    if any_ok:
        return ("OK", f"{net}.{sta} {t0} {t1} wrote_files={wrote}")
    if last_err is not None:
        return ("ERR", f"{net}.{sta} {t0} {t1} err={last_err}")
    return ("NODATA", f"{net}.{sta} {t0} {t1} no waveform data")



def main():
    ensure_dir(OUTDIR)
    stations = read_station_csv(CSV_PATH)
    print(f"[INFO] stations loaded: {len(stations)}")

    t_start = UTCDateTime(START)
    t_end = UTCDateTime(END)

    tasks = [(net, sta, a, b) for net, sta in stations for a, b in daterange_chunks(t_start, t_end, CHUNK_SECONDS)]
    print(f"[INFO] total tasks: {len(tasks)}")

    stats = defaultdict(int)
    err_samples = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(download_one, *x) for x in tasks]
        for fut in as_completed(futs):
            status, msg = fut.result()
            stats[status] += 1
            if status == "ERR" and len(err_samples) < 20:
                err_samples.append(msg)

    print("\n========== SUMMARY ==========")
    for k in ["OK", "SKIP", "NODATA", "ERR"]:
        print(f"{k}: {stats.get(k, 0)}")

    if err_samples:
        print("\n[ERR] samples:")
        for s in err_samples:
            print("  ", s)

    print(f"\n[DONE] output dir: {OUTDIR}/")
    if CACHE_CHANNELS:
        print(f"[INFO] channel cache dir: {CHANNEL_CACHE_DIR}/")


if __name__ == "__main__":
    main()
