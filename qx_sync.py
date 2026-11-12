#!/usr/bin/env python3
"""
qx_sync.py - keeps YOUR OWN candle files up to date from a qx_server (no Quotex login on this device).

It writes the same files that qx_live.py writes (<ASSET>_60s_live.csv and <ASSET>_60s_partial.csv), so a bot that reads
those files (LUMINEX, KRONOS ...) works unchanged. Only candles older than QX_KEEP_DAYS (default 90) are removed, once a day.

  set once:   export QX_SERVER=https://your-server.example.com      export QX_API_KEY=qx_xxxxxxxx
  python qx_sync.py                  run forever (every minute: new + corrected candles; every 2 s: the forming candle)
  python qx_sync.py --once           one round, then stop
  python qx_sync.py --pairs EURUSD USDMXN_otc     only these pairs
  python qx_sync.py --dir /path      folder for the files (default: QX_DATA or the folder of this script)
Only the standard library is used. Needs nothing from Quotex.
"""
import os, sys, csv, json, time, glob, urllib.request, urllib.error, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
KEEP_DAYS = float(os.getenv("QX_KEEP_DAYS") or "90")
HEADER = ["timestamp", "datetime", "open", "high", "low", "close", "ticks"]
REVISE = 8                 # the newest candles are compared with the server every round (the server swaps them for Quotex's official ones)
BACKFILL = 4000            # rows asked for a pair that has no / old data


class Api:
    def __init__(self, base, key):
        self.base, self.key = base.rstrip("/"), key

    def get(self, path, timeout=30):
        req = urllib.request.Request(self.base + path, headers={"X-API-Key": self.key, "User-Agent": "qx_sync"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())


def last_ts(path):
    if not os.path.isfile(path):
        return 0
    with open(path, "rb") as f:
        f.seek(0, 2)
        f.seek(max(0, f.tell() - 4000))
        tail = f.read().decode("utf-8", "ignore").splitlines()
    for ln in reversed(tail):
        p = ln.split(",")
        if p and p[0].isdigit():
            return int(p[0])
    return 0


def merge_tail(path, rows, limit=600):
    """add new candles at the end and swap changed ones in the last `limit` rows. Nothing else is touched."""
    if not rows:
        return 0
    new = not os.path.isfile(path) or os.path.getsize(path) == 0
    if new:
        with open(path, "w", newline="") as f:
            csv.writer(f, lineterminator="\n").writerow(HEADER)
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        start = max(0, size - limit * 110)
        f.seek(start)
        raw = f.read()
    if start > 0:
        k = raw.find(b"\n")
        if k < 0:
            return 0
        start += k + 1
        raw = raw[k + 1:]
    have = {}
    for ln in raw.decode("utf-8", "ignore").splitlines():
        p = ln.split(",")
        if len(p) >= 7 and p[0].isdigit():
            have[int(p[0])] = p[:7]
    lo = min(have) if have else 0
    n = 0
    for r in rows:
        t = int(r[0])
        r = [str(x) for x in r[:7]]
        if t < lo:
            continue                                            # older than the window: only added by the backfill below
        old = have.get(t)
        if old is None:
            have[t] = r
            n += 1
        elif old[2:6] != r[2:6]:
            if r[6] in ("0", ""):
                r[6] = old[6]
            have[t] = r
            n += 1
    if not n:
        return 0
    body = "".join(",".join(have[t]) + "\n" for t in sorted(have))
    with open(path, "r+b") as f:
        f.seek(start)
        f.truncate(start)
        f.write(body.encode())
    return n


def merge_all(path, rows):
    """backfill: put rows that are older than the file's start (or a file that does not exist yet) in their place (sorted, atomic)"""
    have = {}
    if os.path.isfile(path):
        with open(path, newline="") as f:
            r = csv.reader(f)
            next(r, None)
            for c in r:
                if len(c) >= 7 and c[0].isdigit():
                    have[int(c[0])] = c[:7]
    n = 0
    for r in rows:
        t = int(r[0])
        if t not in have:
            have[t] = [str(x) for x in r[:7]]
            n += 1
    if not n:
        return 0
    tmp = path + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(HEADER)
        for t in sorted(have):
            w.writerow(have[t])
    os.replace(tmp, path)
    return n


def first_ts(path):
    try:
        with open(path) as f:
            f.readline()
            ln = f.readline().split(",")
        return int(ln[0]) if ln and ln[0].isdigit() else 0
    except OSError:
        return 0


def prune(path, days=None):
    """remove rows older than `days` (default 90). Atomic; only done when at least 1 day can be dropped."""
    cut = int(time.time() - (days or KEEP_DAYS) * 86400)
    ft = first_ts(path)
    if not ft or ft >= cut - 86400:
        return 0
    n = 0
    tmp = path + ".tmp"
    with open(path, newline="") as src, open(tmp, "w", newline="") as dst:
        for i, ln in enumerate(src):
            if i == 0:
                dst.write(ln)
                continue
            p = ln.split(",", 1)[0]
            if p.isdigit() and int(p) >= cut:
                dst.write(ln)
            else:
                n += 1
    os.replace(tmp, path)
    return n


def sync_round(api, folder, pairs=None, log=print):
    info = api.get("/pairs")
    total = 0
    for it in info:
        a = it["asset"]
        if pairs and a.lower() not in pairs:
            continue
        path = os.path.join(folder, "%s_60s_live.csv" % a)
        last = last_ts(path)
        try:
            if last and time.time() - last < BACKFILL * 60 - 3600:
                rows = api.get("/candles?asset=%s&since=%d" % (a, max(0, last - REVISE * 60)), 60)
                total += merge_tail(path, rows)
            else:                                               # no file, or very old: take everything the server has
                rows = api.get("/candles?asset=%s&n=%d" % (a, BACKFILL), 120)
                total += merge_all(path, rows) + merge_tail(path, rows)
        except urllib.error.HTTPError as e:
            log("  %s: HTTP %s" % (a, e.code))
    return total


def sync_partials(api, folder, pairs=None):
    d = api.get("/partials", 10)
    for a, r in d.items():
        if pairs and a.lower() not in pairs:
            continue
        p = os.path.join(folder, "%s_60s_partial.csv" % a)
        with open(p + ".tmp", "w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(HEADER)
            w.writerow(r[:7])
        os.replace(p + ".tmp", p)
    return len(d)


def main():
    args = sys.argv[1:]
    folder = os.getenv("QX_DATA") or HERE
    pairs = None
    if "--dir" in args:
        k = args.index("--dir")
        folder = args[k + 1]
        del args[k:k + 2]
    if "--pairs" in args:
        k = args.index("--pairs")
        pairs = {x.lower() for x in args[k + 1:] if not x.startswith("--")}
        args = args[:k]
    once = "--once" in args
    base, key = os.getenv("QX_SERVER"), os.getenv("QX_API_KEY")
    if not base or not key:
        sys.exit("set QX_SERVER (address of the server) and QX_API_KEY first")
    os.makedirs(folder, exist_ok=True)
    api = Api(base, key)
    print("qx_sync: %s -> %s (keeps %d days)" % (base, folder, KEEP_DAYS), flush=True)
    last_pr, last_min, last_day = 0.0, 0, 0
    while True:
        now = time.time()
        try:
            m = int(now // 60)
            if m != last_min and (now % 60 >= 7 or last_min == 0):    # a few seconds after the minute: the server has the official candles by then
                n = sync_round(api, folder, pairs)
                last_min = m
                print("  %s  +%d candle(s)" % (time.strftime("%H:%M:%S"), n), flush=True)
                if once:
                    return
            if now - last_pr >= 2:
                sync_partials(api, folder, pairs)
                last_pr = now
            if now - last_day > 86400:
                for f in glob.glob(os.path.join(folder, "*_60s_live.csv")):
                    k = prune(f)
                    if k:
                        print("  pruned %d old row(s) from %s" % (k, os.path.basename(f)), flush=True)
                last_day = now
        except urllib.error.HTTPError as e:
            print("  server answered %s (%s)" % (e.code, "key?" if e.code == 401 else "wait"), flush=True)
            time.sleep(10)
        except (urllib.error.URLError, OSError, ValueError) as e:
            print("  server not reachable: %s" % str(e)[:60], flush=True)
            time.sleep(10)
        except KeyboardInterrupt:
            return
        time.sleep(1)


if __name__ == "__main__":
    main()
