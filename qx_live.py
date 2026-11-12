#!/usr/bin/env python3
"""
qx_live.py - LIVE Quotex OTC M1 candles.  One login, a permanent price stream, one finished candle appended
to <ASSET>_otc_60s_live.csv every minute (same columns as qx_tool.py: timestamp,datetime,open,high,low,close,ticks).

Put this file in the SAME folder as qx_tool.py (QUOTEX_HISTORICAL_ENGINE/github_reference).  Keep it running in its own
Termux session; your bot / KRONOS then only READ the CSV files (they never log in, candles are always fresh).

  python qx_live.py                    live for every pair that already has a *_otc_60s_live.csv here
  python qx_live.py USDMXN USDNGN      only these pairs (names with or without _otc)
  python qx_live.py --fill             first fill every missing minute of the last 3 hours (qx_tool.py heal), then go live
  python qx_live.py status             last candle and age of every file

NEW v5 - (1) the candle that is FORMING right now is written once per second to <ASSET>_60s_partial.csv (one row: minute, open,
high, low, last price, ticks) - the bot draws it as a normal candle and analyses it. (2) Every minute, a few seconds after it
closed, the OFFICIAL Quotex candles of the last minutes are fetched and replace the tick-built ones (so the data matches Quotex).
OLD DATA IS NEVER DELETED: only the timestamps that Quotex returns are overwritten/added, and every CSV gets a one-time
<file>.bak copy before the first change. Partial files are separate, so the *_live.csv files only ever hold CLOSED candles.

NEW v4 - REAL market (forex, not OTC) from Quotex, 16 pairs (EURUSD EURGBP EURJPY AUDJPY CADJPY GBPJPY USDJPY GBPUSD AUDUSD
GBPAUD EURAUD AUDCAD USDCAD USDCHF EURCAD GBPCAD). Files are named EURUSD_60s_live.csv (no _otc):
  python qx_live.py --real --hist 7    download 7 days of history for the 16 real pairs (once), then stop
  python qx_live.py --real             live stream of the 16 real pairs (+ every file that already exists here)
  python qx_live.py --hist 7           7 days of history for every file that exists here (OTC and real)
Everything that was missed while this program was OFF is filled automatically after the login (up to QX_HEAL_HOURS, default 72 h).

Run with a clean environment (your bot token stays invisible):
  env -i HOME="$HOME" PATH="$PATH" PREFIX="$PREFIX" ANDROID_ROOT="$ANDROID_ROOT" ANDROID_DATA="$ANDROID_DATA" python qx_live.py

Use a SEPARATE Quotex account: a connection that stays open for hours is automation and may get an account blocked.
Candles are built from the live ticks: open = first tick of the minute, close = last tick, ticks = number of ticks.
A minute that was only partly watched (start-up / reconnect) is NOT written, so there are no half-candles in your data.

SELF-HEALING (new): while it runs, qx_live.py looks for missing minutes in every file every ~20 s and fills them from the
Quotex history, through the SAME open connection (no second login, no need to stop anything). After every (re)connect it also
fills everything that was missed while it was offline (up to QX_HEAL_HOURS hours, default 6). A hole that the history cannot
fill (1-3 minutes only) is closed with a flat candle (ticks = 0) after 5 minutes, like quiet minutes already are.
"""
import os, sys, csv, glob, time, asyncio, logging, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)                # session.json and .env always live next to this file
sys.path.insert(0, HERE)
TZ_OFFSET = 6 * 3600          # the 'datetime' column is Dhaka time (UTC+6), exactly like the files qx_tool.py writes
STALE_SECS = 25               # no tick at all for this long -> reconnect
START_GRACE = 3               # the first tick of a minute must come within this many seconds, otherwise the minute is partial
CLOSE_AFTER = 3               # a candle is finished when ticks of the NEXT minute (any pair) are CLOSE_AFTER seconds old
MAX_RECONNECTS_PER_HOUR = 6   # protects the account: never hammer the login
BACKOFF0 = 20                 # seconds before the first reconnect try (doubles each time, max 300)
HEADER = ["timestamp", "datetime", "open", "high", "low", "close", "ticks"]
RECON_AFTER = 6               # v5: seconds after a minute closes before the official candles are fetched
RECON_BACK = 4                # v5: how many of the latest closed minutes are compared with Quotex every time
PARTIAL_EVERY = 1.0           # v5: seconds between two writes of the forming candle
SEED_HOURS = float(os.getenv("QX_SEED_HOURS") or "0")   # v6 (server): a pair with NO data yet is filled with this many hours of history at start
HEAL_EVERY = 20               # seconds between two hole checks
HEAL_GRACE = 90               # a minute is a hole only when it should have been written at least this many seconds ago
HEAL_LOOKBACK = 90            # minutes the periodic hole check looks back
HEAL_HOURS = float(os.getenv("QX_HEAL_HOURS") or "72")   # after a (re)connect: how far back the offline time is filled
FLAT_MAX = 3                  # longest hole (minutes) that may be closed with a flat candle when the history has nothing
QUIET_MAX = int(os.getenv("QX_QUIET_MAX") or "60")   # minutes: a longer silence is a closed market, never flat-filled
REAL_ASSETS = ["EURUSD", "EURGBP", "EURJPY", "AUDJPY", "CADJPY", "GBPJPY", "USDJPY", "GBPUSD", "AUDUSD", "GBPAUD", "EURAUD", "AUDCAD",
               "USDCAD", "USDCHF", "EURCAD", "GBPCAD"]
FLAT_AFTER = 300              # seconds a hole must be old before it is flat-filled (gives the history time to supply it)


def load_env():
    path = os.path.join(HERE, ".env")
    try:
        from dotenv import load_dotenv
        load_dotenv(path)
        return
    except ImportError:
        pass
    if os.path.isfile(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("\"'"))


def asset_of(p, real=False):
    p = p.strip().replace("/", "")
    if p.lower().endswith("_otc"):
        return p[:-4].upper() + "_otc"
    return p.upper() if real else p.upper() + "_otc"


def live_path(asset):
    return os.path.join(HERE, "%s_60s_live.csv" % asset)


def fmt(x):
    return ("%.6f" % x).rstrip("0").rstrip(".")


def last_ts(path):
    """timestamp of the newest candle in the file (0 if none); reads only the end of the file"""
    if not os.path.isfile(path):
        return 0
    with open(path, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        f.seek(max(0, size - 400))
        tail = f.read().decode("utf-8", "ignore").strip().splitlines()
    for line in reversed(tail):
        p = line.split(",")
        if p and p[0].isdigit():
            return int(p[0])
    return 0


class Writer:
    """turns ticks into finished one-minute candles and appends them to the CSV files"""

    def __init__(self, assets, folder=None):
        self.assets = list(assets)
        self.folder = folder or HERE
        self.cur = {}                  # asset -> [minute, open, high, low, close, ticks, complete?]
        self.last = {a: last_ts(self.path(a)) for a in self.assets}
        self.px = {}                   # asset -> last price seen in this session
        self.lastc = {}                # asset -> close of the last candle written
        self.t0 = None                 # first minute that was watched from its very first second (set by the first tick)
        self.srv = 0.0                 # newest tick time seen (server clock)
        self.saved = 0
        self.ticks = 0

    def path(self, a):
        return os.path.join(self.folder, "%s_60s_live.csv" % a)

    def ppath(self, a):
        return os.path.join(self.folder, "%s_60s_partial.csv" % a)

    def flush_partial(self, now=None, force=False):
        """v5: write the forming candle of every pair (atomic, ~1 per second). Only watched minutes are shown."""
        now = time.time() if now is None else now
        if not force and now - getattr(self, "_pf", 0) < PARTIAL_EVERY:
            return 0
        self._pf = now
        n = 0
        for a, c in list(self.cur.items()):
            m, o, h, l, cl, k, complete = c
            if not complete or now - m >= 120:                  # a half-watched minute, or a stale one: do not show
                continue
            p = self.ppath(a)
            try:
                with open(p + ".tmp", "w", newline="") as f:
                    w = csv.writer(f, lineterminator="\n")
                    w.writerow(HEADER)
                    w.writerow([m, dt.datetime.fromtimestamp(m + TZ_OFFSET, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), fmt(o), fmt(h), fmt(l), fmt(cl), k])
                os.replace(p + ".tmp", p)
                n += 1
            except OSError:
                pass
        return n

    def reset_session(self):
        """after a (re)connect: whatever candle is open now was only partly watched"""
        self.cur = {}
        self.px = {}
        self.t0 = None

    def feed(self, asset, t, price):
        self.ticks += 1
        self.srv = max(self.srv, t)
        if self.t0 is None:                                     # from the next full minute on, the connection is watched from second 0
            self.t0 = (int(t // 60) + 1) * 60
        b = int(t // 60) * 60
        c = self.cur.get(asset)
        if c is not None and b < c[0]:
            return                                              # a late tick of an old minute, ignore
        if c is not None and b == c[0]:
            c[2], c[3], c[4] = max(c[2], price), min(c[3], price), price
            c[5] += 1
        else:
            if c is not None:
                self._close(asset, c)
            self._open(asset, b, t, price)
        self.px[asset] = price

    def _open(self, asset, b, t, price):
        """start a candle. A quiet pair may send its first tick of the minute late (OTC): when the connection was up for the
        whole minute, that is NOT a partial candle - it opened at the last price we saw (this was the cause of missing minutes)."""
        watched = b >= self.t0
        prev = self.px.get(asset)
        if watched and prev is not None:
            o, h, l = prev, max(prev, price), min(prev, price)
        else:
            o = h = l = price
        self.cur[asset] = [b, o, h, l, price, 1, watched or (t - b) <= START_GRACE]

    def sweep(self):
        """finish candles of quiet pairs once another pair shows that the minute is over"""
        for a, c in list(self.cur.items()):
            if self.srv >= c[0] + 60 + CLOSE_AFTER:
                self._close(a, c)
                del self.cur[a]

    def _write(self, asset, m, o, h, l, cl, n):
        p = self.path(asset)
        new = not os.path.isfile(p) or os.path.getsize(p) == 0
        with open(p, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(HEADER)
            w.writerow([m, dt.datetime.fromtimestamp(m + TZ_OFFSET, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), fmt(o), fmt(h), fmt(l), fmt(cl), n])
        self.last[asset] = m
        self.lastc[asset] = cl
        self.saved += 1

    def _close(self, asset, c):
        m, o, h, l, cl, n, complete = c
        if not complete or m <= self.last.get(asset, 0):
            return                                              # partial minute, or already in the file
        # minutes in between that had NO tick at all (quiet pair, connection was up): the price did not move -> flat candle
        prev_c = self.lastc.get(asset)
        if prev_c is None and self.last.get(asset):
            prev_c = o                                          # first candle after a restart: nothing to compare with
        if self.last.get(asset) and self.t0 is not None:
            k = max(self.last[asset] + 60, self.t0)
            if m - k > QUIET_MAX * 60:
                k = m                                           # a long silence = closed market (real pairs): never invent candles
            while k < m and prev_c is not None:
                self._write(asset, k, prev_c, prev_c, prev_c, prev_c, 0)
                k += 60
        self._write(asset, m, o, h, l, cl, n)


def tail_ts(path, minutes):
    """timestamps of the candles in the last `minutes` minutes of a file (reads only the end)"""
    if not os.path.isfile(path):
        return set()
    with open(path, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        start = max(0, size - minutes * 110 - 2000)
        f.seek(start)
        lines = f.read().decode("utf-8", "ignore").splitlines()
    if start > 0:
        lines = lines[1:]                                       # first line is cut in the middle
    out = set()
    for line in lines:
        p = line.split(",")
        if p and p[0].isdigit():
            out.add(int(p[0]))
    return out


def find_holes(W, asset, now, minutes):
    """minutes that should be in the file but are not (never the minute that is still forming)"""
    last = W.last.get(asset, 0)
    if not last:
        return []
    hi = max(last, ((int(now) - HEAL_GRACE) // 60) * 60)
    lo = max(((int(now) // 60) * 60) - int(minutes) * 60, 0)
    have = tail_ts(W.path(asset), int(minutes) + 5)
    if have:
        lo = max(lo, min(have))                                 # nothing is known before the oldest candle we looked at
    return [t for t in range(((lo + 59) // 60) * 60, hi + 1, 60) if t not in have]


def tail_ticks(path, minutes):
    """{timestamp: ticks} of the last `minutes` minutes of a file (reads only the end)"""
    out = {}
    if not os.path.isfile(path):
        return out
    with open(path, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        start = max(0, size - minutes * 110 - 2000)
        f.seek(start)
        lines = f.read().decode("utf-8", "ignore").splitlines()
    if start > 0:
        lines = lines[1:]
    for line in lines:
        p = line.split(",")
        if len(p) >= 7 and p[0].isdigit():
            try:
                out[int(p[0])] = int(float(p[6]))
            except ValueError:
                pass
    return out


def find_suspects(path, now, minutes=90):
    """candles that are probably WRONG: built from only a few ticks (the stream stalled for part of that minute, so the
    open was missed - this is how a green broker candle became a red one) or flat placeholders (ticks = 0)"""
    tk = tail_ticks(path, minutes + 5)
    good = sorted(v for v in tk.values() if v > 0)
    if len(good) < 10:
        return []
    med = good[len(good) // 2]
    if med < 30:
        return []
    lim = ((int(now) - 150) // 60) * 60                         # the history needs a moment to have the newest minutes
    floor = int(now) - minutes * 60
    return [t for t, v in sorted(tk.items()) if floor <= t <= lim and (v == 0 or v < 0.35 * med)]


def merge_rows(path, add, flat=(), replace=None):
    """put candles into a file at their place (sorted, atomic).
    `add` = {ts: row} new minutes (an existing minute is never touched by this);
    `flat` = timestamps to close with a flat candle (open = high = low = close = the candle before);
    `replace` = {ts: row} suspect candles that are swapped for the official history candle.
    -> number of minutes added or replaced"""
    rows = {}
    with open(path, newline="") as f:
        r = csv.reader(f)
        next(r, None)
        for c in r:
            if len(c) >= 7 and c[0].isdigit():
                rows[int(c[0])] = c[:7]
    n = 0
    for t, row in add.items():
        if t not in rows:
            rows[t] = row
            n += 1
    for t, row in (replace or {}).items():
        if t in rows:
            rows[t] = row
            n += 1
    for t in sorted(flat):
        prev = rows.get(t - 60)
        if t not in rows and prev is not None:
            c = prev[5]
            rows[t] = [t, dt.datetime.fromtimestamp(t + TZ_OFFSET, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), c, c, c, c, 0]
            n += 1
    if not n:
        return 0
    tmp = path + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(HEADER)
        for t in sorted(rows):
            w.writerow(rows[t])
    os.replace(tmp, path)
    return n


def runs_of(ts):
    """consecutive minutes -> list of lists"""
    out, cur = [], []
    for t in sorted(ts):
        if cur and t - cur[-1] != 60:
            out.append(cur)
            cur = []
        cur.append(t)
    if cur:
        out.append(cur)
    return out


async def deep(client, name, secs):
    """candles of the last `secs` seconds from the Quotex history, on the connection that is already open"""
    secs = int(secs)
    tmo = 40 + secs / 30.0
    try:
        return await asyncio.wait_for(client.get_candles_deep(name, secs, 60, progress_callback=lambda *a: None), timeout=tmo)
    except TypeError:
        return await asyncio.wait_for(client.get_candles_deep(name, secs, 60), timeout=tmo)


async def heal_pair(client, name, W, asset, holes, tries, sus=()):
    path = W.path(asset)
    hs = set(holes)
    ss = set(sus)
    got = await deep(client, name, time.time() - min(hs | ss) + 180)
    cut = int(time.time()) // 60 * 60
    add, rpl = {}, {}
    for c in got or []:
        try:
            t = float(c["time"])
            t = int(t / 1000.0 if t > 1e11 else t) // 60 * 60
            o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
        except (KeyError, TypeError, ValueError):
            continue
        if t < cut and None not in (o, h, l, cl):
            row = [t, dt.datetime.fromtimestamp(t + TZ_OFFSET, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), fmt(o), fmt(h), fmt(l), fmt(cl), c.get("ticks", 0) or 0]
            if t in hs:
                add[t] = row
            elif t in ss:
                rpl[t] = row
    for t in hs:
        tries[(asset, t)] = tries.get((asset, t), 0) + 1
    for t in ss:
        tries[(asset, t, "s")] = tries.get((asset, t, "s"), 0) + 1
    still = hs - set(add)
    flat = []
    now = time.time()
    for run in runs_of(still):                                  # the history had nothing: close SHORT holes with a flat candle
        if len(run) <= FLAT_MAX and now - run[-1] > FLAT_AFTER and all(tries.get((asset, t), 0) >= 2 for t in run):
            flat += run
    n = merge_rows(path, add, flat, rpl)
    if n:
        print("\n  healed %s: %d missing minute(s) filled (%d from history, %d flat), %d weak candle(s) replaced by the official one" % (
            asset, len(add) + len(flat), len(add), len(flat), len(rpl)), flush=True)
    return n


async def healer(client, names, W):
    """background task: keeps every file free of holes. Runs on the open connection, one pair at a time."""
    tries = {}
    first = True
    await asyncio.sleep(8)                                      # let the stream settle after (re)connecting
    while True:
        try:
            now = time.time()
            for a, name in list(names.items()):
                last = W.last.get(a, 0)
                if first and not last and SEED_HOURS > 0:
                    try:
                        got = await deep(client, name, SEED_HOURS * 3600)
                        add = hist_rows(got, int(time.time()) // 60 * 60)
                        p_ = W.path(a)
                        if add:
                            if not os.path.isfile(p_) or os.path.getsize(p_) == 0:
                                with open(p_, "w", newline="") as f_:
                                    csv.writer(f_, lineterminator="\n").writerow(HEADER)
                            merge_rows(p_, add)
                            W.last[a] = max(W.last.get(a, 0), max(add))
                            last = W.last[a]
                            print("\n  seeded %s: %d candles of history" % (a, len(add)), flush=True)
                    except asyncio.TimeoutError:
                        print("\n  seed %s: the history answered too slowly" % a, flush=True)
                    except (ConnectionError, OSError):
                        raise
                    except Exception as e:
                        print("\n  seed %s: %s: %s" % (a, type(e).__name__, str(e)[:60]), flush=True)
                    await asyncio.sleep(1.0)
                mins = min(HEAL_HOURS * 60, (now - last) / 60.0 + 10) if (first and last) else HEAL_LOOKBACK
                holes = [t for t in find_holes(W, a, now, mins) if now - t < 900 or tries.get((a, t), 0) < 2]   # a closed market is tried twice, then ignored
                sus = [t for t in find_suspects(W.path(a), now) if tries.get((a, t, "s"), 0) < 2]
                if holes or sus:
                    try:
                        await heal_pair(client, name, W, a, holes, tries, sus)
                    except asyncio.TimeoutError:
                        print("\n  heal %s: the history answered too slowly, will retry" % a, flush=True)
                    except (ConnectionError, OSError):
                        raise
                    except Exception as e:
                        print("\n  heal %s: %s: %s" % (a, type(e).__name__, str(e)[:60]), flush=True)
                    await asyncio.sleep(1.0)
            first = False
            if len(tries) > 5000:
                tries.clear()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("\n  healer problem: %s: %s" % (type(e).__name__, str(e)[:60]), flush=True)
        await asyncio.sleep(HEAL_EVERY)


def patch_tail(path, official, W=None, asset=None, limit=400):
    """v5: swap / add the official candles in the END of a file. Nothing is ever deleted: only the timestamps in `official`
    ({ts: row}) are touched, everything else stays byte for byte. The first run makes a one-time <file>.bak copy.
    -> number of candles changed or added"""
    if not official or not os.path.isfile(path):
        return 0
    bak = path + ".bak"
    if not os.path.isfile(bak):
        import shutil
        shutil.copy2(path, bak)
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
    keep_head = raw.decode("utf-8", "ignore")
    rows = {}
    order = []
    for line in keep_head.splitlines():
        c = line.split(",")
        if len(c) >= 7 and c[0].isdigit():
            rows[int(c[0])] = c[:7]
    if not rows:
        return 0
    lo = min(rows)
    n = 0
    for t, row in official.items():
        if t < lo:
            continue                                            # older than the tail window: the healer takes care of that
        row = [str(x) for x in row]
        old = rows.get(t)
        if old is None:
            rows[t] = row
            n += 1
        elif old[2:6] != row[2:6]:
            row[6] = old[6] if str(row[6]) in ("0", "") else row[6]
            rows[t] = row
            n += 1
    if not n:
        return 0
    body = "".join(",".join(rows[t]) + "\n" for t in sorted(rows))
    with open(path, "r+b") as f:
        f.seek(start)
        f.truncate(start)
        f.write(body.encode())
        f.flush()
        os.fsync(f.fileno())
    if W is not None and asset is not None:
        W.last[asset] = max(W.last.get(asset, 0), max(rows))
    return n


def official_rows(got, cut, back=RECON_BACK):
    out = {}
    for c in got or []:
        try:
            t = float(c["time"])
            t = int(t / 1000.0 if t > 1e11 else t) // 60 * 60
            o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
        except (KeyError, TypeError, ValueError):
            continue
        if t < cut and None not in (o, h, l, cl):
            out[t] = [t, dt.datetime.fromtimestamp(t + TZ_OFFSET, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), fmt(o), fmt(h), fmt(l), fmt(cl), c.get("ticks", 0) or 0]
    keys = sorted(out)[-back:]
    return {t: out[t] for t in keys}


async def reconciler(client, names, W):
    """v5: a few seconds after every minute, copy the OFFICIAL Quotex candles of the last minutes into the files"""
    await asyncio.sleep(12)
    while True:
        try:
            now = time.time()
            nxt = (int(now) // 60 + 1) * 60 + RECON_AFTER
            await asyncio.sleep(max(0.5, nxt - now))
            cut = int(time.time()) // 60 * 60
            ch = 0
            for a, name in list(names.items()):
                try:
                    got = await deep(client, name, (RECON_BACK + 2) * 60)
                    ch += patch_tail(W.path(a), official_rows(got, cut), W, a)
                except asyncio.TimeoutError:
                    pass
                except (ConnectionError, OSError):
                    raise
                except Exception as e:
                    print("\n  reconcile %s: %s: %s" % (a, type(e).__name__, str(e)[:60]), flush=True)
                await asyncio.sleep(0.25)
            if ch:
                print("\n  matched Quotex: %d candle(s) corrected" % ch, flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("\n  reconcile problem: %s: %s" % (type(e).__name__, str(e)[:60]), flush=True)
            await asyncio.sleep(10)


def status():
    now = time.time()
    files = sorted(glob.glob(os.path.join(HERE, "*_60s_live.csv")))
    if not files:
        print("no *_60s_live.csv files in", HERE)
    for f in files:
        ts = last_ts(f)
        age = (now - ts) / 60.0 if ts else float("inf")
        print("%-26s last %s  (%s)" % (os.path.basename(f).replace("_60s_live.csv", ""),
                                       dt.datetime.fromtimestamp(ts + TZ_OFFSET, dt.timezone.utc).strftime("%m-%d %H:%M") if ts else "none",
                                       ("%.0f min old" % age) if ts else "empty"))


def fill_gap(assets):
    """close the gap since the last saved candle with qx_tool.py update, INSIDE this process
    (starting another python from a clean environment is blocked on Android, and this keeps the same clean environment)"""
    import runpy
    tool = os.path.join(HERE, "qx_tool.py")
    if not os.path.isfile(tool):
        print("  qx_tool.py not found next to qx_live.py: gap not filled")
        return
    hours = os.getenv("QX_HEAL_HOURS") or "3"
    print("filling the holes of the last %s hours with qx_tool.py heal ..." % hours)
    old_argv = sys.argv
    sys.argv = [tool, "heal", hours] + [a[:-4] for a in assets]
    try:
        runpy.run_path(tool, run_name="__main__")
    except SystemExit:
        pass
    except Exception as e:
        print("  gap fill did not finish (%s: %s). Going live anyway." % (type(e).__name__, str(e)[:80]))
    finally:
        sys.argv = old_argv


def clean_env():
    keys = ("HOME", "PATH", "PREFIX", "ANDROID_ROOT", "ANDROID_DATA", "TMPDIR", "LANG", "LD_PRELOAD", "PYTHONPATH")
    return {k: os.environ[k] for k in keys if k in os.environ}


def drop_session():
    """pyquotex re-uses the token in session.json. After a dropped connection that token is often dead
    ('Token rejected'), and every reconnect fails with it. Move it away so the next login is a fresh one."""
    moved = False
    for d in {HERE, os.getcwd()}:
        sp = os.path.join(d, "session.json")
        if os.path.isfile(sp):
            try:
                os.replace(sp, sp + ".bad")
                moved = True
            except OSError:
                pass
    return moved


async def try_open(client, a):
    """subscribe one asset. -> platform name, or None when it is closed / not found (a closed real market must NOT fall back to its OTC twin)"""
    real = not a.lower().endswith("_otc")
    try:
        name, data = await client.get_available_asset(a, force_open=not real)
    except Exception as e:
        print("  skip %s (%s)" % (a, str(e)[:50]))
        return None
    if not name:
        return None
    if str(name).lower() != a.lower():
        return None                                              # only the other variant (OTC <-> real) is open: wrong data for this file
    if real:
        try:
            if not data or not data[2]:
                return None
        except Exception:
            pass
    client.start_candles_stream(name, 0)
    return name


async def login_client():
    """one login (with the stale-session retry); -> connected client"""
    from pyquotex.stable_api import Quotex
    email, pw = os.getenv("QUOTEX_EMAIL", ""), os.getenv("QUOTEX_PASSWORD", "")
    if not email or not pw:
        sys.exit("QUOTEX_EMAIL / QUOTEX_PASSWORD missing: fill the .env file in " + HERE)

    async def attempt():
        c = Quotex(email=email, password=pw, lang="en")
        c.debug_ws_enable = False
        ok_, msg_ = await c.connect()
        return c, ok_, msg_

    client, ok, msg = await attempt()
    if not ok:
        try:
            await client.close()
        except Exception:
            pass
        if drop_session():                                       # stale token: ONE fresh login right away
            print("  old session token removed, trying one fresh login ...", flush=True)
            await asyncio.sleep(3)
            client, ok, msg = await attempt()
    if not ok:
        raise ConnectionError("login failed: %s" % str(msg)[:80])
    return client


async def open_stream(assets):
    """one login, subscribe every pair; returns (client, {asset: platform name}, [closed assets])"""
    client = await login_client()
    names, skipped = {}, []
    for a in assets:
        name = await try_open(client, a)
        if name:
            names[a] = name
            await asyncio.sleep(0.15)
        else:
            skipped.append(a)
            print("  skip %s (closed / not available now - it is checked again every few minutes)" % a)
    if not names:
        raise ConnectionError("no pair could be subscribed")
    return client, names, skipped


def hist_rows(got, cut):
    add = {}
    for c in got or []:
        try:
            t = float(c["time"])
            t = int(t / 1000.0 if t > 1e11 else t) // 60 * 60
            o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
        except (KeyError, TypeError, ValueError):
            continue
        if t < cut and None not in (o, h, l, cl):
            add[t] = [t, dt.datetime.fromtimestamp(t + TZ_OFFSET, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), fmt(o), fmt(h), fmt(l), fmt(cl), c.get("ticks", 0) or 0]
    return add


async def hist_download(assets, days):
    """download `days` days of M1 history for every asset into <ASSET>_60s_live.csv (merged, nothing is overwritten)"""
    client = await login_client()
    bad = 0
    try:
        for n, a in enumerate(assets, 1):
            path = live_path(a)
            if not os.path.isfile(path):
                with open(path, "w", newline="") as f:
                    csv.writer(f, lineterminator="\n").writerow(HEADER)
            done = False
            for attempt in range(1, 4):
                try:
                    name, data = await client.get_available_asset(a, force_open=a.lower().endswith("_otc"))
                    if not name or str(name).lower() != a.lower():
                        print("SKIP %s: not found on Quotex" % a, flush=True)
                        bad += 1
                        done = True
                        break
                    secs = int(days * 86400)
                    print("@ %s (%d/%d) downloading %.1f days%s ..." % (a, n, len(assets), days, "" if attempt == 1 else " (try %d)" % attempt), flush=True)
                    tmo = 120 + days * 120
                    got = await asyncio.wait_for(client.get_candles_deep(name, secs, 60, progress_callback=lambda *x: None), timeout=tmo)
                    add = hist_rows(got, int(time.time()) // 60 * 60)
                    k = merge_rows(path, add)
                    ts = sorted(add)
                    print("DONE %s: %d candles received, %d new%s" % (a, len(add), k, (", %s -> %s" % (
                        dt.datetime.fromtimestamp(ts[0] + TZ_OFFSET, dt.timezone.utc).strftime("%m-%d %H:%M"),
                        dt.datetime.fromtimestamp(ts[-1] + TZ_OFFSET, dt.timezone.utc).strftime("%m-%d %H:%M"))) if ts else ""), flush=True)
                    done = True
                    break
                except Exception as e:
                    print("FAIL %s: %s" % (a, (str(e)[:70] or type(e).__name__)), flush=True)
                    try:
                        await client.close()
                    except Exception:
                        pass
                    await asyncio.sleep(8)
                    try:
                        client = await login_client()
                    except Exception as e2:
                        print("login problem: %s" % str(e2)[:70], flush=True)
                        await asyncio.sleep(30)
            if not done:
                bad += 1
                print("GIVE-UP %s: run the same command again later" % a, flush=True)
            await asyncio.sleep(1.5)
    finally:
        try:
            await client.close()
        except Exception:
            pass
    return bad


async def run(assets):
    W = Writer(assets)
    for a in assets:
        age = (time.time() - W.last[a]) / 60.0 if W.last[a] else None
        if age is None or age > 3:
            print("  note: %s has a gap (%s): it is filled automatically right after the login." % (a, ("%.0f min" % age) if age else "no file yet"))
    stamps = []                                              # reconnect times (rate limit)
    backoff = BACKOFF0
    client = None
    htask = None
    rtask = None
    while True:
        try:
            if client is None:
                now = time.time()
                stamps = [t for t in stamps if now - t < 3600]
                while len(stamps) >= MAX_RECONNECTS_PER_HOUR:       # protect the account, but do NOT quit: wait, then try again
                    wait = int(3600 - (now - min(stamps))) + 5
                    print("\n  too many logins in one hour: waiting %d s (account protection), then trying again" % wait, flush=True)
                    await asyncio.sleep(min(wait, 600))
                    now = time.time()
                    stamps = [t for t in stamps if now - t < 3600]
                stamps.append(now)
                print("connecting ... (login)")
                client, names, skipped = await open_stream(assets)
                last_retry = time.time()
                W.reset_session()
                if htask is not None:
                    htask.cancel()
                htask = asyncio.create_task(healer(client, names, W))     # fills what was missed while offline, then keeps watching
                if rtask is not None:
                    rtask.cancel()
                rtask = asyncio.create_task(reconciler(client, names, W))  # v5: official Quotex candles replace tick-built ones
                print("LIVE: %d pair(s) streaming. Candles are saved every minute. Ctrl+C to stop." % len(names))
                backoff = BACKOFF0
                last_tick_at = time.time()
                t_start, n_start = time.time(), W.ticks
            got = 0
            for a, name in names.items():
                lst = client.api.realtime_price.get(name)
                if not lst:
                    continue
                n = len(lst)
                batch = lst[:n]
                del lst[:n]
                for tk in batch:
                    t = float(tk["time"])
                    t = t / 1000.0 if t > 1e11 else t
                    W.feed(a, t, float(tk["price"]))
                    got += 1
            W.sweep()
            W.flush_partial()
            if skipped and time.time() - last_retry > 300:                 # a real market that was closed may have opened
                last_retry = time.time()
                for a in list(skipped):
                    nm = await try_open(client, a)
                    if nm:
                        names[a] = nm
                        skipped.remove(a)
                        print("\n  %s is open now: streaming" % a, flush=True)
            if got:
                last_tick_at = time.time()
            elif time.time() - last_tick_at > STALE_SECS:
                raise ConnectionError("no price for %d s" % STALE_SECS)
            if int(time.time()) % 30 == 0 and got:
                rate = (W.ticks - n_start) / max(1.0, time.time() - t_start)
                sys.stdout.write("\r  ticks/s %.1f | candles saved %d | %s " % (rate, W.saved, time.strftime("%H:%M:%S")))
                sys.stdout.flush()
            await asyncio.sleep(0.4)
        except (ConnectionError, TimeoutError, OSError, RuntimeError) as e:
            print("\n  connection problem: %s -> reconnecting in %d s" % (str(e)[:60], backoff))
            if htask is not None:
                htask.cancel()
                htask = None
            if rtask is not None:
                rtask.cancel()
                rtask = None
            try:
                if client is not None:
                    await client.close()
            except Exception:
                pass
            client = None
            await asyncio.sleep(backoff)
            backoff = min(300, backoff * 2)


def main():
    args = sys.argv[1:]
    if args and args[0] == "status":
        return status()
    fill = "--fill" in args
    real = "--real" in args
    days = 0.0
    if "--hist" in args:
        k = args.index("--hist")
        try:
            days = float(args[k + 1])
            args = args[:k + 1] + args[k + 2:]
        except (IndexError, ValueError):
            days = 7.0
    pairs = [a for a in args if not a.startswith("--")]
    load_env()
    logging.disable(logging.DEBUG)
    found = sorted(os.path.basename(f).replace("_60s_live.csv", "") for f in glob.glob(os.path.join(HERE, "*_60s_live.csv")))
    if pairs:
        assets = [asset_of(p, real) for p in pairs]
    else:
        assets = list(found)
        if real:
            assets += [a for a in REAL_ASSETS if a not in assets]
    assets = list(dict.fromkeys(assets))
    if not assets:
        sys.exit("no pairs: use  python qx_live.py --real --hist 7  (real market) or give names (python qx_live.py USDMXN)")
    if days:
        print("history: %d asset(s), %.1f days each" % (len(assets), days))
        try:
            bad = asyncio.run(hist_download(assets, days))
        except KeyboardInterrupt:
            print("\nstopped.")
            return
        print("history finished%s. Now start it live:  python qx_live.py%s" % (" (%d problem(s): run the same command again)" % bad if bad else "", " --real" if real else ""))
        return
    if fill:
        fill_gap([a for a in assets if a.lower().endswith("_otc")] or assets)
    try:
        asyncio.run(run(assets))
    except KeyboardInterrupt:
        print("\nstopped.")


if __name__ == "__main__":
    main()
