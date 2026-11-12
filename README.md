# qx_server + qx_sync  (Quotex candle relay)

One device logs in to Quotex (the **server**). Everybody else (your bot, a friend's bot) only reads candles through an API
key (**qx_sync**) and keeps its own CSV files (90 days by default). Nobody else ever sees your Quotex password.

```
Quotex --(1 login)--> qx_server.py (+qx_live.py)  --HTTP + API key-->  qx_sync.py (your phone / friend)  -->  bot reads *_60s_live.csv
```

Files: `qx_live.py` (v6: data collector, also works alone as before) · `qx_server.py` · `qx_sync.py` · `render.yaml` · `start_server.sh`

## 1. Server

1. Use a SEPARATE Quotex account. Put `QUOTEX_EMAIL` / `QUOTEX_PASSWORD` in a `.env` file next to the scripts (see `.env.example`).
2. `pip install pyquotex python-dotenv`
3. First login by hand once (Quotex may ask an e-mail code): `python qx_live.py --real` - when candles start to be saved, press Ctrl+C.
   This creates `session.json` (the login token).
4. Make a key for each person / bot:  `python qx_server.py key add friend1`  (shown once; revoke with `key revoke friend1`).
5. Start: `python qx_server.py`  (Termux: `bash start_server.sh`, it restarts after a crash; also `termux-wake-lock`, battery optimisation OFF, phone on the charger).
6. Check: open `http://SERVER:8787/health`  ->  `{"ok":true,"age_s":..}`

The server keeps only the last `QX_KEEP_HOURS` (48) hours. After a restart it refills about 6 hours from the Quotex history itself,
so it also works where files are lost on restart.

### Reach it from outside without opening your router (free, no card)
Install `cloudflared` and run `cloudflared tunnel --url http://localhost:8787` -> it prints an `https://....trycloudflare.com` address.
(That address changes when you restart the tunnel; a named tunnel with your own domain keeps it fixed.)

### Render (free web service) - try it
- New Web Service from your repo with these files, `render.yaml` is included. Env: `QUOTEX_EMAIL`, `QUOTEX_PASSWORD`, `QX_API_KEYS=me:qx_key1,friend:qx_key2`
  and `QX_SESSION_JSON` = the whole content of `session.json` from step 3 (no disk on free plans, and nobody can type the e-mail code there).
- A free service sleeps after 15 minutes without a request: let a monitor (e.g. UptimeRobot) call `/health` every 5 minutes.
- Unknown until tried: whether Quotex accepts the login from Render's IP, and how long the session token lives. If the token dies, repeat step 3 and update `QX_SESSION_JSON`.

## 2. Client (your device / a friend)

```
export QX_SERVER=https://your-server        export QX_API_KEY=qx_xxxxxxxx
python qx_sync.py                      # runs forever; --once = one round; --pairs EURUSD USDMXN_otc ; --dir FOLDER
```
It writes `<ASSET>_60s_live.csv` (closed candles, corrected to Quotex's official ones a few seconds after each minute) and
`<ASSET>_60s_partial.csv` (the forming candle, every 2 s) - exactly what LUMINEX v1.4s reads. Rows older than `QX_KEEP_DAYS` (90) are removed once a day; nothing else is ever deleted.
The first run takes what the server has (about 2 days). For more history on your own device run `python qx_live.py --real --hist 30` (needs your own Quotex login).

## 3. API  (header `X-API-Key: KEY`)
| GET | answer |
|---|---|
| `/health` (no key) | `{ok, age_s, pairs, uptime_s}` |
| `/pairs` | `[{asset,last,age_s}]` |
| `/candles?asset=EURUSD&n=300` or `&since=TS` | rows `[ts, datetime(Dhaka), o, h, l, c, ticks]` (max 5000) |
| `/all?since=TS` | `{asset: rows}` (max 400 rows each) |
| `/partial?asset=EURUSD`, `/partials` | the forming candle(s) |

Python: `requests.get(URL+"/candles", params={"asset":"EURUSD","n":300}, headers={"X-API-Key":KEY}).json()`
Node: `fetch(URL+"/candles?asset=EURUSD&n=300",{headers:{"X-API-Key":KEY}}).then(r=>r.json())`
Limit: 240 requests per minute per key (`QX_RATE`).

## Notes
- A connection that stays open for hours is automation: Quotex may block the account. Use a separate account and share data, never the login.
- Giving Quotex data to other people can break Quotex's terms. That is your decision and risk.
