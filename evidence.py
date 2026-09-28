#!/usr/bin/env python3
"""
Does the radar work? Written-down hypotheses, tested two ways.

  backtest  replay markets that resolved in the last N days and score every
            signal they would have raised
                python3 evidence.py backtest --days 180 --out backtest.json
  live      radar.py --track DIR logs every signal it raises from now on with
            its price at that moment, and scores it when the market resolves

A hypothesis counts as supported only if its side won clearly more often than
the entry prices implied: one-sided test, p < 0.05, at least 30 resolved signals.
Each market side counts once per hypothesis, at the first moment it qualified.

Known biases, stated up front:
  - "Fresh" uses each wallet's join date (known at the time) or its current
    market count <= 3. The count only grows, so a wallet that is under the
    limit today was under it then; older wallets are never wrongly flagged.
  - The backtest enters at the trade price that triggered the signal and ignores
    fees and slippage. Live signals enter at the order-book midpoint.
  - Markets inside one event (say, every candidate for one office) are correlated,
    so the p-values are a bit optimistic.
"""

import argparse
import json
import math
import os
import re
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import radar as R

# ---------------------------------------------------------------- Definitions

MOVE = 0.05          # a push: Yes price moves this much within WINDOW
WINDOW = 86400
TRADE_MIN = 500      # only trades this size or larger count as money behind a move
FRESH_USD = 2000     # H1: fresh money behind the push, at least
FRESH_SHARE = 0.5    # H1: ...and at least this share of it
LONGSHOT = 0.35      # H3: bought at or below
BIG = 1000           # H3/H4: single buy of at least
CLUSTER = 3          # H4: distinct fresh wallets on one side within WINDOW
FRESH_MARKETS = 3    # fresh wallet: traded <= this many markets...
FRESH_DAYS = 14      # ...or account younger than this
ENTRY_RANGE = (0.03, 0.97)   # near-certain prices carry no information
MIN_N = 30
ALPHA = 0.05

HYPOTHESES = [
    {"id": "H1", "title": "Fresh money pushing a price knows the outcome",
     "claim": "When the Yes price moves 5¢ or more within 24 hours and at least half the money pushing it "
              "(and at least $2,000) comes from fresh wallets, the side it was pushed toward wins more often "
              "than its price at that moment implied."},
    {"id": "H2", "title": "Any big push knows the outcome",
     "claim": "When the Yes price moves 5¢ or more within 24 hours, whoever pushed it, the side it was pushed "
              "toward wins more often than its price implied.",
     "note": "The control. If H1 does no better than this, fresh money adds nothing beyond momentum."},
    {"id": "H3", "title": "A fresh wallet buying a long shot knows something",
     "claim": "A single buy of $1,000 or more at 35¢ or less from a fresh wallet wins more often than "
              "its price implied."},
    {"id": "H4", "title": "Several fresh wallets on one side know something",
     "claim": "When 3 or more different fresh wallets each buy $1,000 or more of the same side within "
              "24 hours, that side wins more often than its price implied."},
]


def is_fresh(prof, at):
    if prof["markets"] <= FRESH_MARKETS:
        return True
    return bool(prof["join"]) and 0 <= (at - prof["join"]) / 86400 <= FRESH_DAYS


# ---------------------------------------------------------------- Signal detection

def detect(trades, profile):
    """Replay one market's trades (oldest first) and return the signals it raises.
    trades: (ts, is_buy, outcome_index, price, usd, wallet) tuples, all >= TRADE_MIN."""
    out, fired = [], set()

    def fire(h, side, entry, at):
        if (h, side) in fired or not ENTRY_RANGE[0] <= entry <= ENTRY_RANGE[1]:
            return
        fired.add((h, side))
        out.append({"h": h, "side": side, "entry": round(entry, 4), "at": at})

    window, ref = deque(), None
    sums = {1: [0.0, 0.0], -1: [0.0, 0.0]}   # direction -> [usd, fresh usd]
    buyers = {0: deque(), 1: deque()}        # side -> recent (ts, wallet) big fresh buys
    for at, buy, idx, price, usd, wallet in trades:
        yes = price if idx == 0 else 1 - price
        push = 1 if buy == (idx == 0) else -1   # buying Yes or selling No pushes Yes up
        fresh = is_fresh(profile(wallet), at)
        window.append((at, yes, push, usd, fresh))
        sums[push][0] += usd
        sums[push][1] += usd if fresh else 0
        while window[0][0] < at - WINDOW:
            _, old_yes, old_push, old_usd, old_fresh = window.popleft()
            ref = old_yes   # last price at or before 24h ago
            sums[old_push][0] -= old_usd
            sums[old_push][1] -= old_usd if old_fresh else 0

        move = yes - (ref if ref is not None else window[0][1])
        if abs(move) >= MOVE:
            d = 1 if move > 0 else -1
            side = 0 if d > 0 else 1
            entry = yes if side == 0 else 1 - yes
            fire("H2", side, entry, at)
            usd_d, fresh_d = sums[d]
            if fresh_d >= FRESH_USD and fresh_d >= FRESH_SHARE * usd_d:
                fire("H1", side, entry, at)

        if buy and fresh and usd >= BIG:
            if price <= LONGSHOT:
                fire("H3", idx, price, at)
            q = buyers[idx]
            q.append((at, wallet))
            while q[0][0] < at - WINDOW:
                q.popleft()
            if len({w for _, w in q}) >= CLUSTER:
                fire("H4", idx, price, at)
    return out


# ---------------------------------------------------------------- Scoring

def score(signals):
    """Did the signalled side win more often than its entry price implied?"""
    done = [s for s in signals if s.get("won") is not None]
    n = len(done)
    res = {"n": n, "open": len(signals) - n}
    if not n:
        return dict(res, verdict="untested")
    wins = sum(1 for s in done if s["won"])
    expected = sum(s["entry"] for s in done)
    var = sum(s["entry"] * (1 - s["entry"]) for s in done)
    z = (wins - expected) / math.sqrt(var) if var else 0.0
    p = 0.5 * math.erfc(z / math.sqrt(2))   # one-sided: more wins than priced in
    profit = sum((100 / s["entry"] if s["won"] else 0) - 100 for s in done)
    verdict = "too_few" if n < MIN_N else "supported" if p < ALPHA and wins > expected else "not_supported"
    buckets = []
    for lo, hi in ((0, 0.2), (0.2, 0.5), (0.5, 0.8), (0.8, 1.01)):
        b = [s for s in done if lo <= s["entry"] < hi]
        if b:
            buckets.append({"lo": lo, "hi": min(hi, 1), "n": len(b),
                            "won": sum(1 for s in b if s["won"]),
                            "expected": round(sum(s["entry"] for s in b), 2)})
    return dict(res, won=wins, expected=round(expected, 2), win_rate=round(wins / n, 4),
                implied=round(expected / n, 4), p=round(p, 4), profit_100=round(profit),
                per_bet=round(profit / n, 2), verdict=verdict, buckets=buckets)


def score_all(signals):
    return {h["id"]: score([s for s in signals if s["h"] == h["id"]]) for h in HYPOTHESES}


# ---------------------------------------------------------------- Backtest

def parse_time(value):
    """Gamma mixes '2026-08-08T20:31:58Z' and '2026-08-08 20:31:58+00'."""
    if not value:
        return None
    s = str(value).replace("Z", "+00:00")
    if re.search(r"[+-]\d\d$", s):
        s += ":00"
    try:
        return int(datetime.fromisoformat(s).timestamp())
    except ValueError:
        return None


def winner(m):
    """Index of the winning outcome of a cleanly resolved binary market, else None."""
    prices = [R.num(p) for p in R.json_list(m.get("outcomePrices"))]
    if len(prices) != 2 or sorted(prices) != [0.0, 1.0]:
        return None
    return prices.index(1.0)


def resolved_markets(tags, days, min_volume):
    cut = time.time() - days * 86400
    out = {}
    for slug, tid in R.tag_ids(tags).items():
        cursor, pages = None, 0
        while True:
            resp = R.get(R.GAMMA, "/events/keyset", {"tag_id": tid, "closed": "true", "limit": 100,
                                                    "after_cursor": cursor})
            pages += 1
            if not resp:
                break
            for e in resp.get("events") or []:
                for m in e.get("markets") or []:
                    closed = parse_time(m.get("closedTime"))
                    cid = m.get("conditionId")
                    if (not cid or cid in out or not closed or closed < cut
                            or m.get("umaResolutionStatus") != "resolved"
                            or (R.num(m.get("volumeNum")) or 0) < min_volume):
                        continue
                    w = winner(m)
                    if w is None:
                        continue
                    out[cid] = {"question": m.get("question") or e.get("title") or "?",
                                "link": "https://polymarket.com/event/" + str(e.get("slug") or ""),
                                "closed": closed, "winner": w, "volume": round(R.num(m.get("volumeNum")) or 0)}
            cursor = resp.get("next_cursor")
            if not cursor:
                break
        print(f"  {slug}: scanned {pages} pages of closed events, {len(out)} markets so far", flush=True)
    return out


def market_trades(cid, max_pages):
    rows, cursor, pages = [], None, 0
    while pages < max_pages:
        batch, cursor = R.data_rows(R.get(R.DATA, "/v2/trades", {
            "condition": cid, "filter_type": "CASH", "filter_amount": TRADE_MIN, "limit": 500, "cursor": cursor}))
        pages += 1
        for t in batch:
            rows.append((int(R.pick(t, "timestamp", default=0)), R.pick(t, "side") == "BUY",
                         int(R.pick(t, "outcome_index", "outcomeIndex", default=0)),
                         float(R.pick(t, "price", default=0)), R.usd_of(t),
                         R.pick(t, "proxy_wallet", "proxyWallet", default="")))
        if not cursor:
            break
    rows.sort()
    return rows, bool(cursor)


def load_wallet_file(path):
    try:
        with open(path) as f:
            for addr, (markets, join, fetched) in json.load(f).items():
                R._wallet_cache[addr] = {"markets": markets, "join": join, "fetched": fetched}
    except (OSError, ValueError):
        pass


def save_wallet_file(path):
    with open(path, "w") as f:
        json.dump({a: [p["markets"], p["join"], p["fetched"]] for a, p in R._wallet_cache.items()}, f)


def backtest(args):
    t0 = time.time()
    print(f"backtest: markets resolved in the last {args.days:g} days, volume >= ${args.min_volume:,.0f}", flush=True)
    markets = resolved_markets(args.tags, args.days, args.min_volume)
    cids = sorted(markets, key=lambda c: -markets[c]["volume"])[:args.max_markets]
    print(f"  {len(cids)} markets to replay", flush=True)

    trades, truncated = {}, 0

    def fetch(cid):
        trades[cid] = market_trades(cid, args.max_pages)

    with ThreadPoolExecutor(6) as pool:
        for i, _ in enumerate(pool.map(fetch, cids), 1):
            if i % 200 == 0:
                print(f"  trades fetched for {i}/{len(cids)} markets", flush=True)
    truncated = sum(1 for _, cut in trades.values() if cut)
    print(f"  {sum(len(t) for t, _ in trades.values()):,} trades >= ${TRADE_MIN} "
          f"({truncated} markets capped at {args.max_pages} pages)", flush=True)

    load_wallet_file(args.wallets)
    wallets = sorted({t[5] for rows, _ in trades.values() for t in rows if t[5]} - set(R._wallet_cache))
    print(f"  {len(wallets):,} wallets to look up ({len(R._wallet_cache):,} cached)", flush=True)
    for i in range(0, len(wallets), 5000):
        R.prefetch_wallets(wallets[i:i + 5000])
        save_wallet_file(args.wallets)
        print(f"  wallets {min(i + 5000, len(wallets)):,}/{len(wallets):,}", flush=True)

    signals = []
    for cid in cids:
        m = markets[cid]
        for s in detect(trades[cid][0], R.wallet_profile):
            signals.append(dict(s, cid=cid, question=m["question"], link=m["link"],
                                won=s["side"] == m["winner"]))
    result = {
        "generated": int(time.time()),
        "days": args.days,
        "min_volume": args.min_volume,
        "markets": len(cids),
        "truncated": truncated,
        "hypotheses": score_all(signals),
        "signals": signals,
    }
    with open(args.out, "w") as f:
        json.dump(result, f, separators=(",", ":"))
    print(f"  {len(signals)} signals, written to {args.out} in {(time.time() - t0) / 60:.0f} min", flush=True)
    for h in HYPOTHESES:
        r = result["hypotheses"][h["id"]]
        if r["n"]:
            print(f"  {h['id']}: won {r['won']}/{r['n']} ({r['win_rate']:.0%}) vs {r['implied']:.0%} priced in, "
                  f"p={r['p']:.3f}, $100 each: {r['profit_100']:+,} -> {r['verdict']}")
        else:
            print(f"  {h['id']}: no signals")


# ---------------------------------------------------------------- Live track record

def load_track(folder):
    try:
        with open(os.path.join(folder, "signals.json")) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"since": int(time.time()), "signals": []}


def save_track(folder, track):
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, "signals.json"), "w") as f:
        json.dump(track, f, indent=0, separators=(",", ":"))


def log_live(track, state, markets, movers_24h, trades, now):
    """Record new signals from this scan. Same definitions as the backtest, fed from live data."""
    have = {(s["h"], s["cid"], s["side"]) for s in track["signals"]}
    new = []

    def add(h, cid, side, entry):
        if (h, cid, side) in have or entry is None or not ENTRY_RANGE[0] <= entry <= ENTRY_RANGE[1]:
            return
        m = markets.get(cid, {})
        have.add((h, cid, side))
        new.append({"h": h, "cid": cid, "side": side, "side_name": (m.get("outcomes") or ["Yes", "No"])[side],
                    "entry": round(entry, 4), "at": now, "question": m.get("question", "?"),
                    "link": m.get("link", ""), "won": None})

    for r in movers_24h:
        if abs(r["change"]) < MOVE or r["price"] is None:
            continue
        side = 0 if r["change"] > 0 else 1
        entry = r["price"] if side == 0 else 1 - r["price"]
        add("H2", r["cid"], side, entry)
        f = r.get("flow") or {}
        if f.get("fresh_usd", 0) >= FRESH_USD and f.get("fresh_pct", 0) >= FRESH_SHARE:
            add("H1", r["cid"], side, entry)

    recent = [b for b in state.get("fresh_buys", []) if b[0] >= now - WINDOW]
    for t in trades:
        wallet = R.pick(t, "proxy_wallet", "proxyWallet", default="")
        usd, price = R.usd_of(t), float(R.pick(t, "price", default=0))
        at = int(R.pick(t, "timestamp", default=now))
        if not wallet or usd < BIG or not is_fresh(R.wallet_profile(wallet), at):
            continue
        cid = R.pick(t, "condition_id", "conditionId")
        idx = int(R.pick(t, "outcome_index", "outcomeIndex", default=0))
        if price <= LONGSHOT:
            add("H3", cid, idx, price)
        recent.append([at, cid, idx, wallet])
        if len({b[3] for b in recent if b[1] == cid and b[2] == idx}) >= CLUSTER:
            add("H4", cid, idx, price)
    state["fresh_buys"] = recent[-2000:]
    track["signals"].extend(new)
    return new


def resolve_live(track):
    """Fill in won/lost for signals whose market has closed."""
    open_cids = sorted({s["cid"] for s in track["signals"] if s["won"] is None})
    results = {}
    for i in range(0, len(open_cids), 20):
        params = [("condition_ids", c) for c in open_cids[i:i + 20]] + [("closed", "true"), ("limit", 100)]
        url = R.GAMMA + "/markets?" + "&".join(f"{k}={v}" for k, v in params)
        req = R.urllib.request.Request(url, headers={"User-Agent": "polyradar/1.0"})
        try:
            with R.urllib.request.urlopen(req, timeout=30) as resp:
                for m in json.loads(resp.read().decode("utf-8")) or []:
                    if m.get("closed") and m.get("umaResolutionStatus") == "resolved":
                        results[m.get("conditionId")] = winner(m)
        except (R.urllib.error.URLError, ValueError) as e:
            print(f"  (could not check resolutions: {e})")
    changed = 0
    for s in track["signals"]:
        if s["won"] is None and s["cid"] in results:
            w = results[s["cid"]]
            s["won"] = None if w is None else s["side"] == w
            s["void"] = w is None
            s["resolved"] = int(time.time())
            changed += 1
    track["signals"] = [s for s in track["signals"] if not s.get("void")]
    return changed


def update_live(folder, state, markets, movers_24h, trades, now):
    """One scan's worth of evidence work. Returns the block for data.json."""
    track = load_track(folder)
    new = log_live(track, state, markets, movers_24h, trades, now)
    resolved = resolve_live(track)
    save_track(folder, track)
    print(f"  evidence: {len(new)} new signals, {resolved} resolved, {len(track['signals'])} tracked")
    try:
        with open(os.path.join(folder, "backtest.json")) as f:
            bt = json.load(f)
    except (OSError, ValueError):
        bt = None
    return {
        "since": track["since"],
        "hypotheses": [dict(h, live=score([s for s in track["signals"] if s["h"] == h["id"]]),
                            backtest=bt["hypotheses"].get(h["id"]) if bt else None) for h in HYPOTHESES],
        "backtest": {k: bt[k] for k in ("generated", "days", "markets", "min_volume")} if bt else None,
        "recent": sorted(track["signals"], key=lambda s: -s["at"])[:60],
        "rules": {"min_n": MIN_N, "alpha": ALPHA},
    }


# ---------------------------------------------------------------- Main

def main():
    p = argparse.ArgumentParser(description="Test the radar's hypotheses on resolved markets.")
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backtest", help="replay resolved markets")
    b.add_argument("--days", type=float, default=180, help="markets resolved within this many days")
    b.add_argument("--min-volume", type=float, default=50000, help="skip markets with less lifetime volume")
    b.add_argument("--max-markets", type=int, default=100000, help="cap, biggest markets first")
    b.add_argument("--max-pages", type=int, default=60, help="pages of 500 trades per market")
    b.add_argument("--tags", nargs="+", default=R.DEFAULT_TAGS)
    b.add_argument("--wallets", default="backtest_wallets.json", help="wallet lookup cache")
    b.add_argument("--out", default="backtest.json")
    args = p.parse_args()
    if args.cmd == "backtest":
        backtest(args)


if __name__ == "__main__":
    sys.exit(main())
