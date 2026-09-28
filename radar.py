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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
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


def midpoint(m):
    """Mid of best bid/ask when the book is tight enough to trust, else None."""
    bid, ask = num(m.get("bestBid")), num(m.get("bestAsk"))
    if bid is None or ask is None or ask < bid or ask - bid > 0.10:
        return None
    return round((bid + ask) / 2, 4)


def json_list(v):
    """Gamma stores some lists as JSON strings."""
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return []
    return v if isinstance(v, list) else []


def fee_schedule(m):
    """(rate, exponent) of the taker fee; fee per share = rate * (p * (1 - p)) ** exponent."""
    sched = m.get("feeSchedule") or {}
    if not m.get("feesEnabled") or not sched:
        return (0.0, 1.0)
    return (num(sched.get("rate")) or 0.0, num(sched.get("exponent")) or 1.0)


def market_index(events):
    """condition_id -> {question, end, price, link, changes, volume} from the events' markets."""
    idx = {}
    for e in events.values():
        e_end = parse_ts(e.get("endDate"))
        # scheduled votes: everyone knows when they resolve, so "resolves soon" means nothing
        election = any("election" in str(t.get("slug", "")) for t in e.get("tags") or [])
        for m in e.get("markets") or []:
            cid = m.get("conditionId") or m.get("condition_id")
            if not cid or m.get("closed"):
                continue
            idx[cid] = {
                "question": m.get("question") or e.get("title") or "?",
                "end": parse_ts(m.get("endDate")) or e_end,
                "price": yes_price(m),
                "mid": midpoint(m),
                "election": election,
                "link": "https://polymarket.com/event/" + str(e.get("slug") or ""),
                "event": e.get("title") or "",
                "ch_1h": num(m.get("oneHourPriceChange")),
                "ch_1d": num(m.get("oneDayPriceChange")),
                "ch_1w": num(m.get("oneWeekPriceChange")),
                "vol_1d": num(m.get("volume24hr")) or 0,
                "vol_1w": num(m.get("volume1wk")) or 0,
                "live": m.get("acceptingOrders") is not False,
                "tokens": json_list(m.get("clobTokenIds")),
                "outcomes": json_list(m.get("outcomes")) or ["Yes", "No"],
                "fee": fee_schedule(m),
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
WALLET_TTL = 2 * 86400  # re-check a wallet's history after this long


def load_wallets(state, now):
    """Seed the wallet cache from the previous runs, dropping stale entries."""
    for addr, (markets, join, fetched) in state.get("wallets", {}).items():
        if now - fetched < WALLET_TTL:
            _wallet_cache[addr] = {"markets": markets, "join": join, "fetched": fetched}


def dump_wallets(state):
    state["wallets"] = {a: [p["markets"], p["join"], p["fetched"]] for a, p in _wallet_cache.items()}


def wallet_profile(addr):
    """Distinct markets traded and join date, cached across runs via the state file."""
    if addr in _wallet_cache:
        return _wallet_cache[addr]
    resp = get(DATA, "/v2/user-stats", {"user": addr})
    d = (resp or {}).get("data") if isinstance(resp, dict) else None
    join = pick(d or {}, "join_date", "joinDate")
    prof = {
        "markets": int(pick(d or {}, "trades", default=0) or 0),
        "join": int(join) if join else None,
        "fetched": int(time.time()),
    }
    _wallet_cache[addr] = prof
    return prof


def prefetch_wallets(addrs):
    """Look up many uncached wallets in parallel."""
    todo = [a for a in set(addrs) if a and a not in _wallet_cache]
    if todo:
        with ThreadPoolExecutor(8) as pool:
            list(pool.map(wallet_profile, todo))
    return len(todo)


def wallet_age_days(prof, now):
    return (now - prof["join"]) / 86400 if prof["join"] else None


def is_fresh(prof, args, now):
    age = wallet_age_days(prof, now)
    return prof["markets"] <= args.fresh_markets or (age is not None and age <= args.fresh_days)


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
    prefetch_wallets(pick(t, "proxy_wallet", "proxyWallet") for t in trades)
    for t in trades:
        wallet = pick(t, "proxy_wallet", "proxyWallet", default="")
        price = float(pick(t, "price", default=0))
        usd = usd_of(t)
        prof = wallet_profile(wallet) if wallet else {"markets": 999, "join": None}
        age_days = wallet_age_days(prof, now)
        fresh = is_fresh(prof, args, now)

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
        if info and info["end"] and not info["election"]:
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
        # big bets on favourites are just conviction; the insider tell is buying the unlikely side
        elif price > args.max_price:
            continue
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
    for cid, m in markets.items():
        ch = m[key]
        if ch is None or abs(ch) < 0.01 or not m["live"] or m[vol_key] < min_vol:
            continue
        if m["end"] and m["end"] < now:
            continue
        rows.append({"cid": cid, "question": m["question"], "event": m["event"], "end": m["end"],
                     "price": m["mid"] if m["mid"] is not None else m["price"],
                     "change": round(ch, 4), "volume": round(m[vol_key]), "link": m["link"]})
    rows.sort(key=lambda r: -abs(r["change"]))
    return rows[:n]


def window_trades(cids, since_ts, min_usd, max_pages):
    """Trades (both sides) >= min_usd since since_ts on these markets. Returns (trades, capped)."""
    out, capped = [], False
    for i in range(0, len(cids), 20):
        cursor, pages = None, 0
        while True:
            params = {"condition": ",".join(cids[i:i + 20]), "filter_type": "CASH",
                      "filter_amount": min_usd, "limit": 500, "cursor": cursor}
            rows, cursor = data_rows(get(DATA, "/v2/trades", params))
            fresh = [r for r in rows if int(pick(r, "timestamp", default=0)) >= since_ts]
            out.extend(fresh)
            pages += 1
            if not cursor or len(fresh) < len(rows):
                break
            if pages >= max_pages:
                capped = True
                break
    return out, capped


def add_flow(rows, window, min_usd, args, now):
    """Annotate movers with who bought in the direction of the move: how much came from fresh wallets."""
    if not rows:
        return
    trades, capped = window_trades([r["cid"] for r in rows], now - window, min_usd, args.flow_pages)
    prefetch_wallets(pick(t, "proxy_wallet", "proxyWallet") for t in trades)
    by_cid = defaultdict(list)
    for t in trades:
        by_cid[pick(t, "condition_id", "conditionId")].append(t)
    for r in rows:
        up = r["change"] > 0
        total = fresh_usd = 0.0
        fresh_wallets = defaultdict(float)
        for t in by_cid.get(r["cid"], []):
            # buying YES or selling NO pushes the YES price up
            yes_side = int(pick(t, "outcome_index", "outcomeIndex", default=0)) == 0
            pushes_up = (pick(t, "side") == "BUY") == yes_side
            if pushes_up != up:
                continue
            usd = usd_of(t)
            total += usd
            wallet = pick(t, "proxy_wallet", "proxyWallet", default="")
            if wallet and is_fresh(wallet_profile(wallet), args, now):
                fresh_usd += usd
                fresh_wallets[wallet] += usd
        r["flow"] = {
            "usd": round(total),
            "fresh_usd": round(fresh_usd),
            "fresh_pct": round(fresh_usd / total, 3) if total else 0,
            "fresh_wallets": len(fresh_wallets),
            "top_fresh": round(max(fresh_wallets.values())) if fresh_wallets else 0,
            "partial": capped,
        }


def fill(asks, stake, fee):
    """Spend `stake` USDC on the cheapest asks, paying the taker fee on top.
    Returns (shares, usdc spent)."""
    rate, exp = fee
    shares = spent = 0.0
    for price, size in asks:
        per_share = price + rate * (price * (1 - price)) ** exp
        take = min(size, (stake - spent) / per_share)
        shares += take
        spent += take * per_share
        if stake - spent < 0.01:
            break
    return shares, spent


def add_payoff(rows, markets, stakes):
    """If you follow the push now and the market resolves that way, what do you make?"""
    wanted = {}
    for r in rows:
        m = markets[r["cid"]]
        side = 0 if r["change"] > 0 else 1
        if len(m["tokens"]) == 2:
            wanted[(r["cid"], side)] = m["tokens"][side]
    books = {}

    def load(item):
        key, token = item
        b = get(CLOB, "/book", {"token_id": token}) or {}
        asks = sorted((float(a["price"]), float(a["size"])) for a in b.get("asks") or [])
        books[key] = asks

    with ThreadPoolExecutor(8) as pool:
        list(pool.map(load, wanted.items()))

    for r in rows:
        m = markets[r["cid"]]
        side = 0 if r["change"] > 0 else 1
        asks = books.get((r["cid"], side))
        if not asks:
            continue
        out = []
        for stake in stakes:
            shares, spent = fill(asks, stake, m["fee"])
            out.append({
                "stake": stake,
                "spent": round(spent, 2),
                "avg": round(spent / shares, 4) if shares else None,
                "profit": round(shares - spent, 2),
                "filled": spent >= stake - 0.01,
            })
        r["payoff"] = {"side": m["outcomes"][side], "best": asks[0][0],
                       "fee_rate": m["fee"][0], "stakes": out}


def write_site(site_dir, markets, state, args):
    """Write data.json for the static dashboard in site/index.html."""
    now = int(time.time())
    min_vol = args.site_min_vol
    movers = {
        "1h": top_movers(markets, "ch_1h", "vol_1d", min_vol, now),
        "24h": top_movers(markets, "ch_1d", "vol_1d", min_vol, now),
        "7d": top_movers(markets, "ch_1w", "vol_1w", min_vol * 5, now),
    }
    t0 = time.time()
    add_flow(movers["1h"], 3600, args.flow_min_usd, args, now)
    add_flow(movers["24h"], 86400, args.flow_min_usd, args, now)
    add_flow(movers["7d"], 7 * 86400, args.flow_min_usd * 5, args, now)
    print(f"  move flow analysed in {time.time() - t0:.0f}s ({len(_wallet_cache)} wallets cached)")
    t0 = time.time()
    for rows in movers.values():
        add_payoff(rows, markets, args.stakes)
    print(f"  payoffs priced off order books in {time.time() - t0:.0f}s")
    data = {
        "updated": now,
        "markets": len(markets),
        "movers": movers,
        "log": list(reversed(state.get("log", []))),
    }
    os.makedirs(site_dir, exist_ok=True)
    with open(os.path.join(site_dir, "data.json"), "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"  dashboard data written to {site_dir}/data.json")


# ---------------------------------------------------------------- Main

def price_moves(markets, state, min_move, min_vol):
    """Compare order-book midpoints to the previous scan; return big moves and store new mids.
    Midpoints ignore one-off odd trades, and the volume floor skips markets too thin to matter."""
    old = state.get("mids", {})
    moves = []
    if min_move > 0:
        for cid, m in markets.items():
            p = m["mid"]
            if p is None or cid not in old or m["vol_1d"] < min_vol:
                continue
            if abs(p - old[cid]) >= min_move:
                moves.append({"old": old[cid], "new": p, "question": m["question"], "link": m["link"]})
    state["mids"] = {cid: m["mid"] for cid, m in markets.items() if m["mid"] is not None}
    state.pop("prices", None)
    moves.sort(key=lambda mv: -abs(mv["new"] - mv["old"]))
    return moves


def scan(args, state):
    now = int(time.time())
    since = now - int(args.hours * 3600)
    load_wallets(state, now)
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
    moves = price_moves(markets, state, args.move, args.move_min_vol)
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
    if args.site:
        write_site(args.site, markets, state, args)
    dump_wallets(state)
    save_state(state)


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
    p.add_argument("--max-price", type=float, default=0.35,
                   help="only alert on buys at or below this price (fresh-wallet clusters excepted)")
    p.add_argument("--soon-days", type=float, default=14, help="market ending within N days = +2")
    p.add_argument("--move", type=float, default=0.10,
                   help="alert when a price moves this much between scans (0.10 = 10 pts, 0 = off)")
    p.add_argument("--move-min-vol", type=float, default=10000,
                   help="price-move pushes: ignore markets with less 24h volume (USD)")
    p.add_argument("--ntfy", default=os.environ.get("RADAR_NTFY"),
                   help="ntfy.sh topic for phone pushes (or set RADAR_NTFY)")
    p.add_argument("--site", help="write dashboard data.json into this folder")
    p.add_argument("--site-min-vol", type=float, default=5000,
                   help="dashboard movers: min 24h volume in USD (7d list uses 5x)")
    p.add_argument("--flow-min-usd", type=float, default=100,
                   help="dashboard flow: ignore trades smaller than this (7d uses 5x)")
    p.add_argument("--stakes", type=float, nargs="+", default=[100, 1000],
                   help="dashboard: stakes (USD) to price 'follow the push' payoffs for")
    p.add_argument("--flow-pages", type=int, default=10,
                   help="dashboard flow: max pages of 500 trades per 20 markets")
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
