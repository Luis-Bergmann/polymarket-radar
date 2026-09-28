#!/usr/bin/env python3
"""
Does the radar work? Written-down hypotheses, tested two ways.

  backtest  replay markets that resolved in the last N days and score every
            signal they would have raised
                python3 evidence.py backtest --days 180 --out backtest.json
  live      radar.py --track DIR logs every signal it raises from now on with
            its price at that moment, and scores it when the market resolves

Scoring rules:
  - A signal wins if the side it points to wins. The test asks whether signals won
    more often than their entry prices implied (one-sided).
  - Independence: signals within one event are one bet (Andersson No and
    Kristersson Yes are the same call). Only the strongest signal per event per
    UTC day counts, and n is the number of those independent bets.
  - Multiple testing: four hypotheses, so each needs p < 0.05 / 4 = 0.0125, and at
    least 30 independent resolved bets.
  - H1 vs H2: fresh-money pushes are also tested directly against pushes that were
    never flagged as fresh money, on how far each beat its entry prices.

"Fresh" is judged at the moment of each trade from the wallet's own trade history
before that moment: first trade under 14 days earlier, or at most 3 markets traded
before it. Nothing from after the trade is used.

Known biases, stated up front:
  - The backtest enters at the trade price that triggered the signal and ignores
    fees and slippage. Live signals enter at the order-book midpoint.
  - Only markets that resolved are replayed, and the live record fills up with
    short-dated markets first; both are reported next to the results.
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
ALPHA = 0.05 / 4     # Bonferroni over H1-H4; the H1-vs-H2 comparison uses the same bar

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
    """Fresh at time `at`, judged only from the wallet's history before it."""
    return R.fresh_at(prof, at, FRESH_MARKETS, FRESH_DAYS)


# ---------------------------------------------------------------- Signal detection

def detect(trades, profile):
    """Replay one market's trades (oldest first) and return the signals it raises.
    trades: (ts, is_buy, outcome_index, price, usd, wallet) tuples, all >= TRADE_MIN."""
    out, fired = [], set()

    def fire(h, side, entry, at, strength):
        if (h, side) in fired or not ENTRY_RANGE[0] <= entry <= ENTRY_RANGE[1]:
            return
        fired.add((h, side))
        out.append({"h": h, "side": side, "entry": round(entry, 4), "at": at,
                    "strength": round(strength, 4)})

    window, ref = deque(), None
    sums = {1: [0.0, 0.0], -1: [0.0, 0.0]}   # direction -> [usd, fresh usd]
    buyers = {0: deque(), 1: deque()}        # side -> recent (ts, wallet, usd) big fresh buys
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
            fire("H2", side, entry, at, abs(move))
            usd_d, fresh_d = sums[d]
            if fresh_d >= FRESH_USD and fresh_d >= FRESH_SHARE * usd_d:
                fire("H1", side, entry, at, fresh_d)

        if buy and fresh and usd >= BIG:
            if price <= LONGSHOT:
                fire("H3", idx, price, at, usd)
            q = buyers[idx]
            q.append((at, wallet, usd))
            while q[0][0] < at - WINDOW:
                q.popleft()
            if len({w for _, w, _ in q}) >= CLUSTER:
                fire("H4", idx, price, at, sum(u for _, _, u in q))
    return out


# ---------------------------------------------------------------- Scoring

def event_of(sig):
    """Event a signal belongs to: the event slug in its Polymarket link."""
    return sig.get("event") or (sig.get("link") or "").rstrip("/").rsplit("/", 1)[-1] or sig["cid"]


def independent(signals):
    """One bet per event per UTC day: keep the strongest signal."""
    best = {}
    for sig in signals:
        key = (event_of(sig), int(sig["at"]) // 86400)
        if key not in best or sig.get("strength", 0) > best[key].get("strength", 0):
            best[key] = sig
    return list(best.values())


def days_to_resolution(sig):
    end = sig.get("closed") or sig.get("end")
    return (end - sig["at"]) / 86400 if end else None


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    mid = len(xs) // 2
    return round(xs[mid] if len(xs) % 2 else (xs[mid - 1] + xs[mid]) / 2, 1)


def one_sided_p(z):
    return 0.5 * math.erfc(z / math.sqrt(2))


def score(signals):
    """Did the signalled side win more often than its entry price implied?
    Counts independent bets only (strongest signal per event per day)."""
    bets = independent(signals)
    done = [s for s in bets if s.get("won") is not None]
    n = len(done)
    res = {"n": n, "open": len(bets) - n, "raw": len(signals),
           "raw_resolved": sum(1 for s in signals if s.get("won") is not None),
           "days_resolved": median(days_to_resolution(s) for s in done),
           "days_all": median(days_to_resolution(s) for s in bets)}
    if not n:
        return dict(res, verdict="untested")
    wins = sum(1 for s in done if s["won"])
    expected = sum(s["entry"] for s in done)
    var = sum(s["entry"] * (1 - s["entry"]) for s in done)
    z = (wins - expected) / math.sqrt(var) if var else 0.0
    p = one_sided_p(z)
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
                implied=round(expected / n, 4), p=round(p, 5), profit_100=round(profit),
                per_bet=round(profit / n, 2), verdict=verdict, buckets=buckets)


def compare(fresh_pushes, all_pushes):
    """H1 vs H2: do fresh-money pushes beat their prices by more than pushes that were
    never flagged as fresh money? One-sided Welch test on (won - entry) per bet."""
    flagged = {(s["cid"], s["side"]) for s in fresh_pushes}
    plain = [s for s in all_pushes if (s["cid"], s["side"]) not in flagged]
    groups = []
    for sigs in (fresh_pushes, plain):
        done = [s for s in independent(sigs) if s.get("won") is not None]
        edge = [(1.0 if s["won"] else 0.0) - s["entry"] for s in done]
        n = len(edge)
        mean = sum(edge) / n if n else 0.0
        var = sum((e - mean) ** 2 for e in edge) / (n - 1) if n > 1 else 0.0
        groups.append((n, mean, var))
    (n1, m1, v1), (n2, m2, v2) = groups
    res = {"n_fresh": n1, "n_plain": n2, "edge_fresh": round(m1, 4), "edge_plain": round(m2, 4)}
    if n1 < 2 or n2 < 2:
        return dict(res, verdict="untested" if not n1 or not n2 else "too_few")
    se = math.sqrt(v1 / n1 + v2 / n2)
    p = one_sided_p((m1 - m2) / se) if se else 0.5
    verdict = ("too_few" if min(n1, n2) < MIN_N
               else "supported" if p < ALPHA and m1 > m2 else "not_supported")
    return dict(res, diff=round(m1 - m2, 4), p=round(p, 5), verdict=verdict)


def score_all(signals):
    by_h = {h["id"]: [s for s in signals if s["h"] == h["id"]] for h in HYPOTHESES}
    out = {h: score(sigs) for h, sigs in by_h.items()}
    out["H1_vs_H2"] = compare(by_h["H1"], by_h["H2"])
    return out


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
                                "event": str(e.get("slug") or e.get("id") or cid),
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
    """Wallet histories from earlier backtests. First entries never change, so no expiry,
    except for wallets that had few markets at lookup time and may have entered more since."""
    try:
        with open(path) as f:
            for addr, prof in json.load(f).items():
                if isinstance(prof, dict) and "entries" in prof:
                    R._wallet_cache[addr] = prof
    except (OSError, ValueError):
        pass


def save_wallet_file(path):
    with open(path, "w") as f:
        json.dump(R._wallet_cache, f, separators=(",", ":"))


def backtest(args):
    t0 = time.time()
    print(f"backtest: markets resolved in the last {args.days:g} days, volume >= ${args.min_volume:,.0f}", flush=True)
    markets = resolved_markets(args.tags, args.days, args.min_volume)
    cids = sorted(markets, key=lambda c: -markets[c]["volume"])[:args.max_markets]
    print(f"  {len(cids)} markets to replay", flush=True)

    trades, failed = {}, []

    def fetch(cid):
        try:
            trades[cid] = market_trades(cid, args.max_pages)
        except (R.urllib.error.URLError, ValueError) as e:
            failed.append(cid)
            print(f"  skipped a market, trades would not load: {e}", flush=True)

    with ThreadPoolExecutor(4) as pool:
        for i, _ in enumerate(pool.map(fetch, cids), 1):
            if i % 200 == 0:
                print(f"  trades fetched for {i}/{len(cids)} markets", flush=True)
    cids = [c for c in cids if c in trades]
    truncated = sum(1 for _, cut in trades.values() if cut)
    print(f"  {sum(len(t) for t, _ in trades.values()):,} trades >= ${TRADE_MIN} "
          f"({truncated} markets capped at {args.max_pages} pages)", flush=True)

    load_wallet_file(args.wallets)
    wallets = sorted({t[5] for rows, _ in trades.values() for t in rows if t[5]} - set(R._wallet_cache))
    print(f"  {len(wallets):,} wallets to look up ({len(R._wallet_cache):,} cached)", flush=True)
    for i in range(0, len(wallets), 5000):
        R.prefetch_wallets(wallets[i:i + 5000], workers=8)
        save_wallet_file(args.wallets)
        print(f"  wallets {min(i + 5000, len(wallets)):,}/{len(wallets):,}", flush=True)

    def profile(addr):
        """Cached profile; a wallet whose lookup failed twice counts as unknown, not fresh."""
        try:
            return R.wallet_profile(addr)
        except (R.urllib.error.URLError, ValueError):
            return R.UNKNOWN

    signals = []
    for cid in cids:
        m = markets[cid]
        for s in detect(trades[cid][0], profile):
            signals.append(dict(s, cid=cid, question=m["question"], link=m["link"], event=m["event"],
                                closed=m["closed"], won=s["side"] == m["winner"]))
    result = {
        "generated": int(time.time()),
        "days": args.days,
        "min_volume": args.min_volume,
        "markets": len(cids),
        "truncated": truncated,
        "skipped": len(failed),
        "hypotheses": score_all(signals),
        "signals": signals,
    }
    with open(args.out, "w") as f:
        json.dump(result, f, separators=(",", ":"))
    print(f"  {len(signals)} signals, written to {args.out} in {(time.time() - t0) / 60:.0f} min", flush=True)
    print_results(result["hypotheses"])


def print_results(res):
    for h in HYPOTHESES:
        r = res[h["id"]]
        if r["n"]:
            print(f"  {h['id']}: {r['n']} independent bets ({r['raw']} raw signals): won {r['won']} "
                  f"({r['win_rate']:.1%}) vs {r['implied']:.1%} priced in, p={r['p']:.4f}, "
                  f"$100 each: {r['profit_100']:+,} ({r['per_bet']:+.2f}/bet), "
                  f"median {r['days_resolved']}d to resolution -> {r['verdict']}")
        else:
            print(f"  {h['id']}: no resolved signals ({r['raw']} raw)")
    c = res["H1_vs_H2"]
    if "p" in c:
        print(f"  H1 vs H2: fresh edge {c['edge_fresh']:+.3f} (n={c['n_fresh']}) vs plain {c['edge_plain']:+.3f} "
              f"(n={c['n_plain']}), p={c['p']:.4f} -> {c['verdict']}")
    else:
        print(f"  H1 vs H2: {c['verdict']} (n={c['n_fresh']} vs {c['n_plain']})")


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

    def add(h, cid, side, entry, strength):
        if (h, cid, side) in have or entry is None or not ENTRY_RANGE[0] <= entry <= ENTRY_RANGE[1]:
            return
        m = markets.get(cid, {})
        have.add((h, cid, side))
        sig = {"h": h, "cid": cid, "side": side, "side_name": (m.get("outcomes") or ["Yes", "No"])[side],
               "entry": round(entry, 4), "at": now, "strength": round(strength, 4),
               "question": m.get("question", "?"), "link": m.get("link", ""),
               "end": m.get("end"), "won": None}
        sig["event"] = event_of(sig)
        new.append(sig)

    for r in movers_24h:
        if abs(r["change"]) < MOVE or r["price"] is None:
            continue
        side = 0 if r["change"] > 0 else 1
        entry = r["price"] if side == 0 else 1 - r["price"]
        add("H2", r["cid"], side, entry, abs(r["change"]))
        f = r.get("flow") or {}
        if f.get("fresh_usd", 0) >= FRESH_USD and f.get("fresh_pct", 0) >= FRESH_SHARE:
            add("H1", r["cid"], side, entry, f["fresh_usd"])

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
            add("H3", cid, idx, price, usd)
        recent.append([at, cid, idx, wallet, usd])
        cluster = [b for b in recent if b[1] == cid and b[2] == idx]
        if len({b[3] for b in cluster}) >= CLUSTER:
            add("H4", cid, idx, price, sum(b[4] for b in cluster if len(b) > 4))
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
                        results[m.get("conditionId")] = (winner(m), parse_time(m.get("closedTime")))
        except (R.urllib.error.URLError, ValueError) as e:
            print(f"  (could not check resolutions: {e})")
    changed = 0
    for s in track["signals"]:
        if s["won"] is None and s["cid"] in results:
            w, closed = results[s["cid"]]
            s["won"] = None if w is None else s["side"] == w
            s["void"] = w is None
            s["closed"] = closed or int(time.time())
            changed += 1
    track["signals"] = [s for s in track["signals"] if not s.get("void")]
    return changed


def update_live(folder, state, markets, movers_24h, trades, now):
    """One scan's worth of evidence work. Returns the block for data.json."""
    track = load_track(folder)
    for sig in track["signals"]:   # signals logged before event/end/strength existed
        sig.setdefault("event", event_of(sig))
        if sig.get("end") is None and sig["cid"] in markets:
            sig["end"] = markets[sig["cid"]]["end"]
    new = log_live(track, state, markets, movers_24h, trades, now)
    resolved = resolve_live(track)
    save_track(folder, track)
    print(f"  evidence: {len(new)} new signals, {resolved} resolved, {len(track['signals'])} tracked")
    try:
        with open(os.path.join(folder, "backtest.json")) as f:
            bt = json.load(f)
    except (OSError, ValueError):
        bt = None
    live = score_all(track["signals"])
    for sig in track["signals"]:
        sig["days"] = None if days_to_resolution(sig) is None else round(days_to_resolution(sig), 1)
    return {
        "since": track["since"],
        "hypotheses": [dict(h, live=live[h["id"]], backtest=bt["hypotheses"].get(h["id"]) if bt else None)
                       for h in HYPOTHESES],
        "compare": {"live": live["H1_vs_H2"], "backtest": bt["hypotheses"].get("H1_vs_H2") if bt else None},
        "live_days": {"resolved": median(s["days"] for s in track["signals"] if s["won"] is not None),
                      "all": median(s["days"] for s in track["signals"]),
                      "resolved_n": sum(1 for s in track["signals"] if s["won"] is not None),
                      "n": len(track["signals"])},
        "backtest": {k: bt.get(k) for k in ("generated", "days", "markets", "min_volume", "skipped")} if bt else None,
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
