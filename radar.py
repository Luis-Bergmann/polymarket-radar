#!/usr/bin/env python3
"""
Polymarket insider radar (read-only).

Watches open politics/geopolitics markets and flags trades that look like
"someone knows something": big buys at long-shot prices by fresh wallets,
and several fresh wallets piling onto the same outcome.

Uses only public, unauthenticated endpoints. No account, key or wallet.
Python 3.9+, standard library only.

  python3 radar.py                 # one scan of the last 24h
  python3 radar.py --watch 10      # rescan every 10 minutes
  python3 radar.py --hours 72 --min-usd 2000
  python3 radar.py --watch 5 --ntfy my-secret-topic   # push alerts to your phone
  python3 radar.py --site _site    # also write _site/data.json for the dashboard
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com"
HERE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, "radar_state.json")
ALERTS_CSV = os.path.join(HERE, "alerts.csv")

DEFAULT_TAGS = ["politics", "geopolitics", "elections", "world"]


# ---------------------------------------------------------------- HTTP

def get(base, path, params=None, tries=4):
    """GET JSON with polite retries on 429/503."""
    url = base + path
    if params:
        url += "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    req = urllib.request.Request(url, headers={"User-Agent": "polyradar/1.0", "Accept": "application/json"})
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < tries - 1:
                wait = int(e.headers.get("Retry-After") or 2 ** (attempt + 1))
                time.sleep(min(wait, 60))
                continue
            if e.code == 404:
                return None
            raise
        except urllib.error.URLError:
            if attempt < tries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            raise
    return None


def data_rows(resp):
    """v2 wraps payloads in {data, pagination}; tolerate bare lists too."""
    if resp is None:
        return [], None
    if isinstance(resp, list):
        return resp, None
    rows = resp.get("data") or []
    nxt = (resp.get("pagination") or {}).get("next_cursor")
    return rows, nxt


def pick(d, *keys, default=None):
    """Read a field that may be snake_case or camelCase."""
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


# ---------------------------------------------------------------- Discovery

def tag_ids(slugs):
    ids = {}
    for s in slugs:
        t = get(GAMMA, f"/tags/slug/{s}")
        if t and t.get("id"):
            ids[s] = str(t["id"])
    return ids


def open_events(tag_id, max_events):
    events, cursor = [], None
    while len(events) < max_events:
        resp = get(GAMMA, "/events/keyset", {
            "tag_id": tag_id, "closed": "false", "limit": 100, "after_cursor": cursor,
        })
        if not resp:
            break
        batch = resp.get("events", resp if isinstance(resp, list) else [])
        events.extend(batch)
        cursor = resp.get("next_cursor") if isinstance(resp, dict) else None
        if not cursor or not batch:
            break
    return events[:max_events]


def parse_ts(value):
    """ISO date string -> epoch seconds, or None."""
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def yes_price(m):
    """Current YES price of a market from Gamma fields, or None."""
    for key in ("lastTradePrice", "last_trade_price"):
        if m.get(key) not in (None, ""):
            try:
                return float(m[key])
            except (TypeError, ValueError):
                pass
    prices = m.get("outcomePrices")
    if isinstance(prices, str):
        try:
            prices = json.loads(prices)
        except ValueError:
            prices = None
    if prices:
        try:
            return float(prices[0])
        except (TypeError, ValueError, IndexError):
            pass
    return None


def num(v):
    """Float from a Gamma field that may be missing or a string."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def market_index(events):
    """condition_id -> {question, end, price, link, changes, volume} from the events' markets."""
    idx = {}
    for e in events.values():
        e_end = parse_ts(e.get("endDate"))
        for m in e.get("markets") or []:
            cid = m.get("conditionId") or m.get("condition_id")
            if not cid or m.get("closed"):
                continue
            idx[cid] = {
                "question": m.get("question") or e.get("title") or "?",
                "end": parse_ts(m.get("endDate")) or e_end,
                "price": yes_price(m),
                "link": "https://polymarket.com/event/" + str(e.get("slug") or ""),
                "event": e.get("title") or "",
                "ch_1h": num(m.get("oneHourPriceChange")),
                "ch_1d": num(m.get("oneDayPriceChange")),
                "ch_1w": num(m.get("oneWeekPriceChange")),
                "vol_1d": num(m.get("volume24hr")) or 0,
                "vol_1w": num(m.get("volume1wk")) or 0,
                "live": m.get("acceptingOrders") is not False,
            }
    return idx


# ---------------------------------------------------------------- Notify

def notify(topic, title, body, link=None):
    """Push to the ntfy app (free, no account). Subscribe to the same topic on your phone."""
    if not topic:
        return
    headers = {"Title": title.encode("ascii", "replace").decode("ascii"), "Priority": "high"}
    if link:
        headers["Click"] = link
    req = urllib.request.Request("https://ntfy.sh/" + topic, data=body.encode("utf-8"),
                                 headers=headers, method="POST")
    try:
        urllib.request.urlopen(req, timeout=15).close()
    except urllib.error.URLError as e:
        print(f"  (push failed: {e})")


# ---------------------------------------------------------------- Trades

def recent_trades(event_ids, since_ts, min_usd):
    """All BUY trades >= min_usd since since_ts for up to 20 events per call."""
    out = []
    for i in range(0, len(event_ids), 20):
        chunk = ",".join(event_ids[i:i + 20])
        cursor = None
        while True:
            params = {"event_id": chunk, "side": "BUY", "filter_type": "CASH",
                      "filter_amount": min_usd, "limit": 500}
            if cursor:
                params = {"event_id": chunk, "cursor": cursor, "side": "BUY",
                          "filter_type": "CASH", "filter_amount": min_usd}
            rows, cursor = data_rows(get(DATA, "/v2/trades", params))
            fresh = [r for r in rows if int(pick(r, "timestamp", default=0)) >= since_ts]
            out.extend(fresh)
            # feed is newest-first: stop once we page past the window
            if not cursor or len(fresh) < len(rows):
                break
    return out


_wallet_cache = {}


def wallet_profile(addr):
    """Distinct markets traded and join date, cached per run."""
    if addr in _wallet_cache:
        return _wallet_cache[addr]
    resp = get(DATA, "/v2/user-stats", {"user": addr})
    d = (resp or {}).get("data") if isinstance(resp, dict) else None
    prof = {
        "markets": int(pick(d or {}, "trades", default=0) or 0),
        "join": pick(d or {}, "join_date", "joinDate"),
    }
    _wallet_cache[addr] = prof
    return prof


# ---------------------------------------------------------------- Scoring

def usd_of(t):
    v = pick(t, "usdc_size", "usdcSize")
    if v is not None:
        return float(v)
    return float(pick(t, "size", default=0)) * float(pick(t, "price", default=0))


def score_trades(trades, args, now, markets=None):
    """Return a list of alerts, highest score first."""
    alerts = []
    # pass 1: per-trade signals
    enriched = []
    for t in trades:
        wallet = pick(t, "proxy_wallet", "proxyWallet", default="")
        price = float(pick(t, "price", default=0))
        usd = usd_of(t)
        prof = wallet_profile(wallet) if wallet else {"markets": 999, "join": None}
        age_days = None
        if prof["join"]:
            age_days = (now - int(prof["join"])) / 86400
        fresh = prof["markets"] <= args.fresh_markets or (age_days is not None and age_days <= args.fresh_days)

        score, why = 0, []
        if price <= args.longshot:
            score += 2
            why.append(f"long shot @ {price:.2f}")
        if usd >= args.big_usd:
            score += 2
            why.append(f"big ${usd:,.0f}")
        elif usd >= args.min_usd:
            score += 1
        if fresh:
            score += 2
            why.append(f"fresh wallet ({prof['markets']} mkts" +
                       (f", {age_days:.0f}d old)" if age_days is not None else ")"))
        # payoff multiple: what this wins if right
        if price > 0:
            payoff = usd / price
            if payoff >= args.big_usd * 5:
                score += 1
                why.append(f"pays ${payoff:,.0f} if right")
        # insider bets cluster on markets that resolve soon ("strike by Friday")
        info = (markets or {}).get(pick(t, "condition_id", "conditionId"))
        if info and info["end"]:
            days_left = (info["end"] - now) / 86400
            if 0 <= days_left <= args.soon_days:
                score += 2
                why.append(f"resolves in {days_left:.0f}d")

        enriched.append((t, wallet, price, usd, fresh, score, why))

    # pass 2: clusters of fresh wallets on the same outcome
    cluster = defaultdict(set)
    for t, wallet, _, _, fresh, _, _ in enriched:
        if fresh:
            key = (pick(t, "condition_id", "conditionId"), pick(t, "outcome", default="?"))
            cluster[key].add(wallet)

    for t, wallet, price, usd, fresh, score, why in enriched:
        key = (pick(t, "condition_id", "conditionId"), pick(t, "outcome", default="?"))
        n = len(cluster.get(key, ()))
        if n >= args.cluster:
            score += 3
            why.append(f"{n} fresh wallets on this side")
        if score >= args.threshold:
            alerts.append({
                "score": score,
                "time": datetime.fromtimestamp(int(pick(t, "timestamp", default=0)), timezone.utc)
                        .strftime("%Y-%m-%d %H:%M UTC"),
                "market": pick(t, "title", default="?"),
                "outcome": pick(t, "outcome", default="?"),
                "price": round(price, 3),
                "usd": round(usd),
                "wallet": wallet,
                "trader": pick(t, "name", "pseudonym", default=""),
                "why": "; ".join(why),
                "link": "https://polymarket.com/event/" + str(pick(t, "event_slug", "eventSlug", default="")),
                "tx": pick(t, "transaction_hash", "transactionHash", default=""),
            })
    alerts.sort(key=lambda a: (-a["score"], -a["usd"]))
    return alerts


# ---------------------------------------------------------------- Output

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"seen": []}


def save_state(state):
    state["seen"] = state["seen"][-5000:]
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def report(alerts):
    if not alerts:
        print("  nothing suspicious in this window.")
        return
    for a in alerts:
        print(f"\n  [{a['score']}] {a['market']}")
        print(f"      BUY {a['outcome']} @ {a['price']}  ${a['usd']:,}  {a['time']}")
        print(f"      why:    {a['why']}")
        print(f"      wallet: {a['wallet']} {('(' + a['trader'] + ')') if a['trader'] else ''}")
        print(f"      {a['link']}")


def append_csv(alerts):
    new = not os.path.exists(ALERTS_CSV)
    with open(ALERTS_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(alerts[0].keys()))
        if new:
            w.writeheader()
        w.writerows(alerts)


# ---------------------------------------------------------------- Dashboard

def log_events(state, kind, items):
    """Keep a rolling log of moves and alerts for the dashboard."""
    now = int(time.time())
    log = state.setdefault("log", [])
    log.extend(dict(item, kind=kind, at=now) for item in items)
    state["log"] = log[-300:]


def top_movers(markets, key, vol_key, min_vol, now, n=40):
    rows = []
    for m in markets.values():
        ch = m[key]
        if ch is None or not m["live"] or m[vol_key] < min_vol:
            continue
        if m["end"] and m["end"] < now:
            continue
        rows.append({"question": m["question"], "event": m["event"], "price": m["price"],
                     "change": round(ch, 4), "volume": round(m[vol_key]), "link": m["link"]})
    rows.sort(key=lambda r: -abs(r["change"]))
    return rows[:n]


def write_site(site_dir, markets, state, min_vol):
    """Write data.json for the static dashboard in site/index.html."""
    now = int(time.time())
    data = {
        "updated": now,
        "markets": len(markets),
        "movers": {
            "1h": top_movers(markets, "ch_1h", "vol_1d", min_vol, now),
            "24h": top_movers(markets, "ch_1d", "vol_1d", min_vol, now),
            "7d": top_movers(markets, "ch_1w", "vol_1w", min_vol * 5, now),
        },
        "log": list(reversed(state.get("log", []))),
    }
    os.makedirs(site_dir, exist_ok=True)
    with open(os.path.join(site_dir, "data.json"), "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"  dashboard data written to {site_dir}/data.json")


# ---------------------------------------------------------------- Main

def price_moves(markets, state, min_move):
    """Compare YES prices to the previous scan; return big moves and store new prices."""
    old = state.get("prices", {})
    moves = []
    if min_move > 0:
        for cid, m in markets.items():
            p = m["price"]
            if p is None or cid not in old:
                continue
            if abs(p - old[cid]) >= min_move:
                moves.append({"old": old[cid], "new": p, "question": m["question"], "link": m["link"]})
    state["prices"] = {cid: m["price"] for cid, m in markets.items() if m["price"] is not None}
    moves.sort(key=lambda mv: -abs(mv["new"] - mv["old"]))
    return moves


def scan(args, state):
    now = int(time.time())
    since = now - int(args.hours * 3600)
    print(f"\n=== scan {datetime.now().strftime('%Y-%m-%d %H:%M')}, last {args.hours:g}h ===")

    tags = tag_ids(args.tags)
    if not tags:
        print("  could not resolve any tags; check the tag slugs.")
        return
    events = {}
    for slug, tid in tags.items():
        for e in open_events(tid, args.max_events):
            events[str(e["id"])] = e
    markets = market_index(events)
    print(f"  watching {len(events)} open events ({len(markets)} markets) across tags: {', '.join(tags)}")

    # price moves since the previous scan
    moves = price_moves(markets, state, args.move)
    if moves:
        print(f"\n  price moves >= {args.move * 100:.0f} pts since last scan:")
        for mv in moves:
            print(f"    {mv['old']:.2f} -> {mv['new']:.2f}  {mv['question']}")
            notify(args.ntfy, f"Move {mv['old']:.2f} -> {mv['new']:.2f}", mv["question"], mv["link"])
        log_events(state, "move", moves)

    trades = recent_trades(list(events), since, args.min_usd)
    seen = set(state["seen"])
    trades = [t for t in trades if pick(t, "transaction_hash", "transactionHash") not in seen]
    print(f"  {len(trades)} new buys >= ${args.min_usd:,.0f}")

    alerts = score_trades(trades, args, now, markets)
    report(alerts)
    if alerts:
        append_csv(alerts)
        print(f"\n  {len(alerts)} alert(s) appended to {os.path.basename(ALERTS_CSV)}")
        for a in alerts:
            notify(args.ntfy, f"[{a['score']}] {a['outcome']} @ {a['price']} ${a['usd']:,}",
                   f"{a['market']}\n{a['why']}", a["link"])
        log_events(state, "alert", alerts)
    state["seen"].extend(pick(t, "transaction_hash", "transactionHash", default="") for t in trades)
    save_state(state)
    if args.site:
        write_site(args.site, markets, state, args.site_min_vol)


def main():
    p = argparse.ArgumentParser(description="Flag suspicious buys on Polymarket politics markets.")
    p.add_argument("--tags", nargs="+", default=DEFAULT_TAGS, help="Gamma tag slugs to watch")
    p.add_argument("--hours", type=float, default=24, help="look-back window")
    p.add_argument("--watch", type=float, default=0, help="rescan every N minutes (0 = once)")
    p.add_argument("--max-events", type=int, default=300, help="cap per tag")
    p.add_argument("--min-usd", type=float, default=1000, help="ignore buys smaller than this")
    p.add_argument("--big-usd", type=float, default=10000, help="size that counts as big")
    p.add_argument("--longshot", type=float, default=0.15, help="price at or below = long shot")
    p.add_argument("--fresh-markets", type=int, default=3, help="wallet with <= N markets = fresh")
    p.add_argument("--fresh-days", type=float, default=14, help="wallet younger than N days = fresh")
    p.add_argument("--cluster", type=int, default=3, help="fresh wallets on one side = cluster")
    p.add_argument("--threshold", type=int, default=6, help="minimum score to alert")
    p.add_argument("--soon-days", type=float, default=14, help="market ending within N days = +2")
    p.add_argument("--move", type=float, default=0.10,
                   help="alert when a price moves this much between scans (0.10 = 10 pts, 0 = off)")
    p.add_argument("--ntfy", default=os.environ.get("RADAR_NTFY"),
                   help="ntfy.sh topic for phone pushes (or set RADAR_NTFY)")
    p.add_argument("--site", help="write dashboard data.json into this folder")
    p.add_argument("--site-min-vol", type=float, default=5000,
                   help="dashboard movers: min 24h volume in USD (7d list uses 5x)")
    args = p.parse_args()

    state = load_state()
    while True:
        try:
            scan(args, state)
        except urllib.error.URLError as e:
            print(f"  network error: {e}. Polymarket may be blocked on this network.")
        if not args.watch:
            break
        time.sleep(args.watch * 60)


if __name__ == "__main__":
    sys.exit(main())
