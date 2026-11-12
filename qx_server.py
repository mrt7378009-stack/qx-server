#!/usr/bin/env python3
"""
qx_server.py - Quotex candle SERVER (relay).  One Quotex login, many readers.

It runs qx_live.py (v5) inside, and puts a small HTTP API in front of the candle files, protected by API keys.
The server only keeps the LAST few hours of candles (QX_KEEP_HOURS, default 48): the long history (e.g. 90 days)
lives on the devices that read it (see qx_sync.py). After a restart the server refills what it missed from the Quotex history,
so it works on hosts WITHOUT a permanent disk (Render free ...).

  python qx_server.py                      start (pairs from QX_PAIRS, default: 16 real + 15 OTC pairs)
  python qx_server.py key add NAME         make a key for a person / bot  (printed ONCE, only its hash is stored)
  python qx_server.py key list
  python qx_server.py key revoke NAME

API  (key in header  X-API-Key: ...   or  ?key=...)
  GET /health                  no key: {ok, age_s, pairs}            (use it for an uptime monitor)
  GET /pairs                   [{asset, last, age_s}]
  GET /candles?asset=EURUSD&n=300          newest n closed candles   (or &since=TS: everything after TS)
  GET /all?since=TS            {asset: [rows]} for every pair (max 400 rows each)
  GET /partial?asset=EURUSD    the candle that is forming now
  GET /partials                {asset: row} every forming candle
A row is [timestamp, datetime(Dhaka), open, high, low, close, ticks].
"""
import os, sys, json, time, glob, hmac, hashlib, secrets, threading, asyncio, logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
sys.path.insert(0, HERE)
os.environ.setdefault("QX_HEAL_HOURS", "6")
os.environ.setdefault("QX_SEED_HOURS", "6")
KEYS_FILE = os.path.join(HERE, "keys.json")
KEEP_HOURS = float(os.getenv("QX_KEEP_HOURS") or "48")
RATE_PER_MIN = int(os.getenv("QX_RATE") or "240")
MAX_ROWS = 5000
ALL_ROWS = 400
DEFAULT_REAL = ["EURUSD", "EURGBP", "EURJPY", "AUDJPY", "CADJPY", "GBPJPY", "USDJPY", "GBPUSD", "AUDUSD", "GBPAUD", "EURAUD", "AUDCAD",
                "USDCAD", "USDCHF", "EURCAD", "GBPCAD"]
DEFAULT_OTC = ["AUDNZD", "BRLUSD", "GBPNZD", "NZDJPY", "USDARS", "USDBDT", "USDCOP", "USDDZD", "USDEGP", "USDINR", "USDMXN", "USDNGN",
               "USDPHP", "USDPKR", "USDZAR"]
START = time.time()


# ------------------------------------------------------------------ keys
def _h(k):
    return hashlib.sha256(k.encode()).hexdigest()


def load_keys():
    """{hash: name}. From keys.json (hashes) and from the env QX_API_KEYS='name:key,name2:key2' (for hosts without a disk)"""
    out = {}
    try:
        with open(KEYS_FILE, encoding="utf-8") as f:
            for name, rec in json.load(f).items():
                if rec.get("active", True):
                    out[rec["hash"]] = name
    except (OSError, ValueError, KeyError):
        pass
    for part in (os.getenv("QX_API_KEYS") or "").split(","):
        if ":" in part:
            n, k = part.split(":", 1)
            if n.strip() and k.strip():
                out[_h(k.strip())] = n.strip()
    return out


def key_cmd(args):
    try:
        data = json.load(open(KEYS_FILE, encoding="utf-8"))
    except (OSError, ValueError):
        data = {}
    if not args or args[0] == "list":
        for n, r in data.items():
            print("%-20s %s  created %s" % (n, "active " if r.get("active", True) else "REVOKED", r.get("created")))
        print("(%d key(s) in keys.json; keys from QX_API_KEYS are not listed)" % len(data))
        return
    if args[0] == "add" and len(args) > 1:
        k = "qx_" + secrets.token_urlsafe(24)
        data[args[1]] = {"hash": _h(k), "created": time.strftime("%Y-%m-%d %H:%M"), "active": True}
        tmp = KEYS_FILE + ".tmp"
        json.dump(data, open(tmp, "w", encoding="utf-8"), indent=1)
        os.replace(tmp, KEYS_FILE)
        print("key for %s (shown ONCE, copy it now):\n  %s\n\nfor Render / hosts without a disk put it in the env:  QX_API_KEYS=%s:%s" % (args[1], k, args[1], k))
        return
    if args[0] == "revoke" and len(args) > 1 and args[1] in data:
        data[args[1]]["active"] = False
        json.dump(data, open(KEYS_FILE, "w", encoding="utf-8"), indent=1)
        print("revoked", args[1])
        return
    print("usage: key add NAME | key list | key revoke NAME")


# ------------------------------------------------------------------ reading the files
def live_path(a):
    return os.path.join(HERE, "%s_60s_live.csv" % a)


def read_rows(path, n=None, since=None):
    """newest rows of a candle file (reads only the end). since = return rows with timestamp > since"""
    if not os.path.isfile(path):
        return []
    want = n if n else (int(((time.time() - since) // 60) + 6) if since else 300)
    want = max(1, min(want, MAX_ROWS))
    with open(path, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        start = max(0, size - (want + 4) * 100)
        f.seek(start)
        lines = f.read().decode("utf-8", "ignore").splitlines()
    if start > 0:
        lines = lines[1:]
    rows = []
    for ln in lines:
        p = ln.split(",")
        if len(p) >= 7 and p[0].isdigit():
            rows.append([int(p[0]), p[1], p[2], p[3], p[4], p[5], p[6]])
    if since is not None:
        rows = [r for r in rows if r[0] > since]
    return rows[-want:]


def assets_here():
    return sorted(os.path.basename(f).replace("_60s_live.csv", "") for f in glob.glob(os.path.join(HERE, "*_60s_live.csv")))


def partial_of(a):
    try:
        with open(os.path.join(HERE, "%s_60s_partial.csv" % a)) as f:
            ln = f.read().splitlines()[1]
        p = ln.split(",")
        if len(p) >= 7 and p[0].isdigit() and time.time() - int(p[0]) < 120:
            return [int(p[0]), p[1], p[2], p[3], p[4], p[5], p[6]]
    except (OSError, IndexError):
        pass
    return None


def health():
    now = time.time()
    ages = []
    for a in assets_here():
        r = read_rows(live_path(a), 1)
        if r:
            ages.append(now - r[-1][0])
    age = min(ages) if ages else None
    return {"ok": age is not None and age < 240, "age_s": None if age is None else int(age), "pairs": len(ages), "uptime_s": int(now - START)}


# ------------------------------------------------------------------ http
class Bucket:
    def __init__(self):
        self.t = {}
        self.lock = threading.Lock()

    def ok(self, name):
        now = time.time()
        with self.lock:
            q = [x for x in self.t.get(name, []) if now - x < 60]
            if len(q) >= RATE_PER_MIN:
                self.t[name] = q
                return False
            q.append(now)
            self.t[name] = q
            return True


BUCKET = Bucket()
KEYCACHE = [0.0, {}]


def who(key):
    if time.time() - KEYCACHE[0] > 20:
        KEYCACHE[1] = load_keys()
        KEYCACHE[0] = time.time()
    for h, n in KEYCACHE[1].items():
        if hmac.compare_digest(h, _h(key or "")):
            return n
    return None


class H(BaseHTTPRequestHandler):
    server_version = "qx_server"

    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        b = json.dumps(obj, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        try:
            if u.path in ("/", "/health"):
                return self._send(200, health())
            name = who(self.headers.get("X-API-Key") or q.get("key"))
            if not name:
                return self._send(401, {"error": "bad or missing API key"})
            if not BUCKET.ok(name):
                return self._send(429, {"error": "too many requests"})
            if u.path == "/pairs":
                now = time.time()
                out = []
                for a in assets_here():
                    r = read_rows(live_path(a), 1)
                    out.append({"asset": a, "last": r[-1][0] if r else None, "age_s": int(now - r[-1][0]) if r else None})
                return self._send(200, out)
            if u.path == "/candles":
                a = q.get("asset", "")
                if a not in assets_here():
                    return self._send(404, {"error": "unknown asset"})
                since = int(q["since"]) if q.get("since", "").isdigit() else None
                n = int(q["n"]) if q.get("n", "").isdigit() else None
                return self._send(200, read_rows(live_path(a), n, since))
            if u.path == "/all":
                since = int(q["since"]) if q.get("since", "").isdigit() else int(time.time()) - 3600
                return self._send(200, {a: read_rows(live_path(a), ALL_ROWS, since) for a in assets_here()})
            if u.path == "/partial":
                return self._send(200, partial_of(q.get("asset", "")))
            if u.path == "/partials":
                d = {a: partial_of(a) for a in assets_here()}
                return self._send(200, {a: r for a, r in d.items() if r})
            return self._send(404, {"error": "not found"})
        except Exception as e:
            return self._send(500, {"error": str(e)[:100]})


def serve(port):
    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print("API on port %d" % port, flush=True)
    return srv


# ------------------------------------------------------------------ keeping the disk small
def prune_file(path, keep_s):
    """drop rows older than keep_s. Runs inside the event loop (no await), so it can never race with the writer."""
    cut = int(time.time()) - int(keep_s)
    try:
        with open(path, newline="") as f:
            lines = f.read().splitlines()
    except OSError:
        return 0
    if len(lines) < 3:
        return 0
    try:
        first = int(lines[1].split(",")[0])
    except ValueError:
        return 0
    if first >= cut - 3600:
        return 0                                                # rewrite only when there is at least an hour to drop
    keep = [lines[0]] + [l for l in lines[1:] if l.split(",")[0].isdigit() and int(l.split(",")[0]) >= cut]
    tmp = path + ".tmp"
    with open(tmp, "w", newline="") as f:
        f.write("\n".join(keep) + "\n")
    os.replace(tmp, path)
    return len(lines) - len(keep)


async def pruner():
    while True:
        await asyncio.sleep(1800)
        try:
            for a in assets_here():
                prune_file(live_path(a), KEEP_HOURS * 3600)
        except Exception as e:
            print("prune: %s" % str(e)[:60], flush=True)


def restore_session():
    """hosts without a disk: the Quotex session token comes from the env QX_SESSION_JSON (log in once somewhere else, copy session.json)"""
    sj = os.getenv("QX_SESSION_JSON")
    sp = os.path.join(HERE, "session.json")
    if sj and not os.path.isfile(sp):
        try:
            json.loads(sj)
            open(sp, "w").write(sj)
            print("session restored from QX_SESSION_JSON", flush=True)
        except ValueError:
            print("QX_SESSION_JSON is not valid JSON", flush=True)


async def amain(assets):
    import qx_live as Q
    asyncio.create_task(pruner())
    await Q.run(assets)


def main():
    a = sys.argv[1:]
    if a and a[0] == "key":
        return key_cmd(a[1:])
    import qx_live as Q
    Q.load_env()
    logging.disable(logging.DEBUG)
    restore_session()
    env_pairs = [p.strip() for p in (os.getenv("QX_PAIRS") or "").split(",") if p.strip()]
    if env_pairs:
        assets = [Q.asset_of(p, True) if not p.lower().endswith("_otc") else Q.asset_of(p) for p in env_pairs]
    else:
        assets = list(DEFAULT_REAL) + [x + "_otc" for x in DEFAULT_OTC]
    if not load_keys():
        print("WARNING: no API key yet. Make one:  python qx_server.py key add NAME   (or set QX_API_KEYS)", flush=True)
    serve(int(os.getenv("PORT") or os.getenv("QX_PORT") or "8787"))
    try:
        asyncio.run(amain(assets))
    except KeyboardInterrupt:
        print("\nstopped.")


if __name__ == "__main__":
    main()
