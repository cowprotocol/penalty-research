#!/usr/bin/env python
"""Derive the PAIRS table that is hardcoded in notebooks/fixed_caps_from_revert_target.ipynb.

Prints a ready-to-paste literal. The notebook does NOT call this: the pair list only changes
when CoW's traded pairs shift, so it is pasted in and reviewed rather than recomputed on every
notebook run.

What it does, in order:
  1. read the DB extracts and keep penalty-eligible attempts (fill-or-kill, in-market, not
     excluded from penalties) inside [--start, --end)
  2. resolve each token address to the Binance book that prices it -- explicit paths only:
     the HARDCODED_BOOK address map, an exact CMS-symbol match, or the WRAPPED 1:1 wrappers
  3. tier each attempt by CoW's own correlated-token buckets (correlated = both legs in the
     same bucket)
  4. merge every USD-pegged stable into one "USD" leg and drop trade direction, so a row is an
     unordered asset pair
  5. rank the uncorrelated pairs by attempts and take the top --top

Usage:
    python scripts/derive_pairs.py                     # notebook's window (see DEFAULT_*)
    python scripts/derive_pairs.py --start 2026-05-01 --end 2026-08-01 --top 10
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_orderbook_data import CHAINS  # noqa: E402  (chain list, shared with the fetcher)

REPO = Path(__file__).resolve().parent.parent

# Keep these in step with MONTHS in the notebook: the flow weights should come from the same
# period as the prices the caps are fitted on.
DEFAULT_START, DEFAULT_END = "2026-05-01", "2026-07-31"

# CMS network name per chain, for the correlated-token bucket lists.
CMS_NET = {"ethereum": "MAINNET", "arbitrum": "ARBITRUM", "base": "BASE", "gnosis": "GNOSIS",
           "polygon": "POLYGON", "bnb": "BNB", "avalanche_c": "AVALANCHE"}
NATIVE = {"ethereum": "ETH", "arbitrum": "ETH", "base": "ETH", "gnosis": "XDAI",
          "polygon": "POL", "bnb": "BNB", "avalanche_c": "AVAX"}

# Tokens outside the CMS buckets that carry real flow, mapped to the book that prices them.
# Addresses are unique across chains in practice (cbBTC deliberately shares one address on
# ethereum and base).
HARDCODED_BOOK = {
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "BTC",   # WBTC (ethereum)
    "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": "BTC",   # WBTC (arbitrum)
    "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6": "BTC",   # WBTC (polygon)
    "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf": "BTC",   # cbBTC (ethereum & base)
    "0x18084fba666a33d37592fa2633fd49a74dd93a88": "BTC",   # tBTC (ethereum)
    "0x8236a87084f8b84306f72007f36f2618a5634494": "BTC",   # LBTC (ethereum)
    "0x5ee5bf7ae06d1be5997a1a72006fe6c607ec6de8": "BTC",   # aEthWBTC (ethereum)
    "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c": "BTC",   # BTCB (bnb)
    "0x514910771af9ca656af840dff83e8264ecf986ca": "LINK",  # (ethereum)
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": "AAVE",  # (ethereum)
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": "UNI",   # (ethereum)
    "0x45804880de22913dafe09f4980848ece6ecbaf78": "PAXG",  # (ethereum)
    "0x68749665ff8d2d112fa859aa293f07a622782f38": "XAUT",  # (ethereum)
    "0x77e06c9eccf2e797fd462a92b6d7642ef85b0a44": "TAO",   # wTAO (ethereum)
    "0x56072c95faa701256059aa122697b133aded9279": "SKY",   # (ethereum)
    "0xd533a949740bb3306d119cc777fa900ba034cd52": "CRV",   # (ethereum)
    "0x6982508145454ce325ddbe47a25d4ec3d2311933": "PEPE",  # (ethereum)
    "0xfaba6f8e4a5e8ab82f62fe7c39859fa577269be3": "ONDO",  # (ethereum)
    "0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab": "COW",   # (ethereum)
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": "BNB",   # WBNB (bnb)
    "0x9c58bacc331c9aa871afd802db6379a98e80cedb": "GNO",   # (gnosis)
    "0x940181a94a35a4569e4529a3cdfb74e38fd98631": "AERO",  # (base)
    "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd": "USDC",  # sUSDS (ethereum)
}

# 1:1 wrappers only. Rate-bearing wrappers (wstETH, sDAI) are deliberately absent: they are not
# the same asset as their underlying and must not inherit its book.
WRAPPED = {"WETH": "ETH", "WBTC": "BTC", "WPOL": "POL", "WMATIC": "POL",
           "WBNB": "BNB", "WAVAX": "AVAX"}

# USDC on each chain, used to find the bucket that defines "USD-pegged".
USDC_ADDRS = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",   # ethereum
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",   # base
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831",   # arbitrum
    "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",   # polygon
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",   # polygon (USDC.e)
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",   # bnb
    "0xddafbb505ad214d7b80b1f830fccc89b60fb7a83",   # gnosis
}

# Non-USD fiat pegs that sit inside CoW's "Stables" buckets alongside USD ones.
NON_USD_FX = ("EUR", "GBP", "CHF", "JPY", "AUD", "CAD", "BRL", "TRY", "SGD", "MXN", "PEN",
              "COP", "CLP", "ARS", "KRW", "GYEN", "CJPY", "PHT", "PAR", "XSGD", "ZCHF", "WARS")


def cached_json(path: Path, url: str):
    """Fetch and cache a JSON endpoint under data/ (delete the file to refresh)."""
    if not path.exists():
        req = urllib.request.Request(url, headers={"User-Agent": "penalty-research"})
        with urllib.request.urlopen(req) as r:
            path.write_bytes(r.read())
    return json.loads(path.read_text())


def pick_extract(files: list[str], start: str, end: str) -> str:
    """The extract overlapping [start, end) most -- not the widest, which may be older."""
    w0, w1 = pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC")

    def overlap(path: str):
        a, b = re.findall(r"\d{4}-\d{2}-\d{2}", os.path.basename(path))[:2]
        lo, hi = pd.Timestamp(a, tz="UTC"), pd.Timestamp(b, tz="UTC")
        return max(pd.Timedelta(0), min(hi, w1) - max(lo, w0))

    return max(files, key=overlap)


def load_attempts(data_dir: Path, start: str, end: str) -> pd.DataFrame:
    """Penalty-eligible attempts in [start, end), one row each, across all chains found."""
    frames = []
    for chain in CHAINS:
        files = glob.glob(str(data_dir / f"{chain}_????-??-??_????-??-??*.csv"))
        if not files:
            print(f"[warn] no extract for {chain}; run scripts/fetch_orderbook_data.py",
                  file=sys.stderr)
            continue
        chosen = pick_extract(files, start, end)
        df = pd.read_csv(chosen, low_memory=False,
                         usecols=["sell_token", "buy_token", "partially_fillable",
                                  "is_out_of_market", "is_excluded_from_penalties",
                                  "auction_timestamp"])
        df = df[df.partially_fillable.eq(False)          # fill-or-kill only
                & df.is_out_of_market.eq(False)          # rows with NaN flags drop out
                & df.is_excluded_from_penalties.eq(False)].copy()
        df["chain"] = chain
        frames.append(df)
        print(f"  {chain:12s} {os.path.basename(chosen)}")
    att = pd.concat(frames, ignore_index=True)
    att["ts"] = pd.to_datetime(att.auction_timestamp, utc=True, format="mixed")
    covered_lo, covered_hi = att.ts.min(), att.ts.max()
    att = att[(att.ts >= pd.Timestamp(start, tz="UTC"))
              & (att.ts < pd.Timestamp(end, tz="UTC"))].copy()
    att["sell_token"] = att.sell_token.str.lower()
    att["buy_token"] = att.buy_token.str.lower()

    print(f"requested window : {start} .. {end}")
    print(f"extract coverage : {covered_lo.date()} .. {covered_hi.date()}")
    if covered_lo > pd.Timestamp(start, tz="UTC") or covered_hi < pd.Timestamp(end, tz="UTC"):
        print(f"[WARN] the extracts do not span the requested window -- weights are computed "
              f"only over {max(covered_lo, pd.Timestamp(start, tz='UTC')).date()} .. "
              f"{min(covered_hi, pd.Timestamp(end, tz='UTC')).date()}.\n"
              f"       Re-fetch with scripts/fetch_orderbook_data.py --end {end} to close the "
              f"gap.", file=sys.stderr)
    return att


def build_resolvers(data_dir: Path):
    """(book_of, bucket_of, usd_books) -- address -> Binance book, and the USD-pegged set."""
    cms = cached_json(data_dir / "cow_correlated_tokens.json",
                      "https://cms.cow.finance/api/correlated-tokens"
                      "?pagination%5BpageSize%5D=100")
    info = cached_json(data_dir / "binance_exchangeinfo.json",
                       "https://api.binance.com/api/v3/exchangeInfo")
    bases = {s["baseAsset"] for s in info["symbols"]
             if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
             and s["isSpotTradingAllowed"]}

    buckets = {c: {b["attributes"]["name"]:
                   {a.lower(): s for a, s in b["attributes"]["tokens"].items()}
                   for b in cms["data"] if f"for {CMS_NET[c]}" in b["attributes"]["name"]}
               for c in CMS_NET}
    symbol = {}
    for c in CMS_NET:
        symbol[c] = {}
        for toks in buckets[c].values():
            symbol[c].update(toks)
        symbol[c].setdefault("0x" + "e" * 40, NATIVE[c])   # native-token sentinel

    def book_of(chain, addr):
        if addr in HARDCODED_BOOK:
            return HARDCODED_BOOK[addr]
        sym = symbol.get(chain, {}).get(addr)
        if not sym:
            return None
        u = sym.upper()
        if u == "USDT" or u in bases:
            return u
        return WRAPPED.get(u)

    def bucket_of(chain, addr):
        return next((n for n, t in buckets.get(chain, {}).items() if addr in t), None)

    usd_books = set()
    for c in CMS_NET:
        for toks in buckets[c].values():
            if any(a in USDC_ADDRS for a in toks):        # the bucket USDC lives in
                usd_books.update(filter(None, (book_of(c, a) for a in toks)))

    def symbol_of(chain, addr):
        return symbol.get(chain, {}).get(addr)

    return book_of, bucket_of, usd_books, symbol_of


def peg_kind(sym: str | None) -> str:
    """Rough split of a stable's peg, for reporting what the correlated tier contains."""
    u = (sym or "").upper()
    if u.endswith("ON") and len(u) > 3:      # tokenised equities/ETFs: AAPLon, TSLAon, ...
        return "equity"
    if "EUR" in u:
        return "eur"
    if any(k in u for k in NON_USD_FX):
        return "other-fx"
    return "usd"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", default=DEFAULT_START, help="inclusive, YYYY-MM-DD (UTC)")
    p.add_argument("--end", default=DEFAULT_END, help="exclusive, YYYY-MM-DD (UTC)")
    p.add_argument("--top", type=int, default=10, help="uncorrelated pairs to emit")
    p.add_argument("--exclude", action="append", default=[], metavar="ASSET",
                   help="skip any pair with this leg (repeatable). Use for assets with no "
                        "usable Binance 1s archive, e.g. --exclude AERO")
    p.add_argument("--data-dir", type=Path, default=REPO / "data")
    args = p.parse_args()

    att = load_attempts(args.data_dir, args.start, args.end)
    book_of, bucket_of, usd_books, symbol_of = build_resolvers(args.data_dir)
    total = len(att)
    print(f"attempts in window: {total:,}\n")

    pairs = (att.groupby(["chain", "sell_token", "buy_token"]).size()
                .rename("attempts").reset_index())
    pairs["book_s"] = [book_of(c, a) for c, a in zip(pairs.chain, pairs.sell_token)]
    pairs["book_b"] = [book_of(c, a) for c, a in zip(pairs.chain, pairs.buy_token)]
    pairs["tier"] = ["correlated" if (bucket_of(c, s) is not None
                                      and bucket_of(c, s) == bucket_of(c, b)) else "uncorrelated"
                     for c, s, b in zip(pairs.chain, pairs.sell_token, pairs.buy_token)]
    # a str|None column becomes object with NaN, so test with isinstance rather than `is None`
    res = pairs[[isinstance(a, str) and isinstance(b, str) and a != b
                 for a, b in zip(pairs.book_s, pairs.book_b)]].copy()
    print(f"resolvable to two distinct Binance books: {res.attempts.sum():,} "
          f"({res.attempts.sum() / total:.1%} of the window)\n")

    # ---- uncorrelated: merge USD-pegged legs, drop direction, rank by flow
    merged = res[res.tier == "uncorrelated"].copy()
    merged["a"] = merged.book_s.map(lambda b: "USD" if b in usd_books else b)
    merged["b"] = merged.book_b.map(lambda b: "USD" if b in usd_books else b)
    merged = merged[merged.a != merged.b]                 # USD<->USD is the correlated tier
    merged["key"] = ["|".join(sorted([a, b])) for a, b in zip(merged.a, merged.b)]
    unc = (merged.groupby("key").attempts.sum()
                 .sort_values(ascending=False).reset_index())
    if args.exclude:
        skip = {a.upper() for a in args.exclude}
        dropped = unc[[bool(skip & set(k.split("|"))) for k in unc.key]]
        unc = unc[[not (skip & set(k.split("|"))) for k in unc.key]].reset_index(drop=True)
        print(f"excluded by --exclude {sorted(skip)}: "
              f"{', '.join(f'{r.key} ({int(r.attempts):,})' for r in dropped.itertuples())}\n")
    unc["share"] = unc.attempts / unc.attempts.sum()
    unc["cum"] = unc.share.cumsum()
    print(f"uncorrelated: {len(unc)} merged pairs, {unc.attempts.sum():,} attempts")
    print(unc.head(args.top + 6).to_string(index=False,
                                           float_format=lambda v: f"{v:8.3f}"), "\n")

    # ---- correlated: the whole tier, not just the part with a Binance feed. Most EUR
    # stables (EURe, EURC) and their counterparties (wxDAI, USDC.e, sDAI) resolve to no book
    # at all, so restricting this to `res` would hide nearly all of them.
    corr = pairs[pairs.tier == "correlated"].copy()
    corr["kind"] = [
        "usd<->usd" if {peg_kind(symbol_of(c, s)), peg_kind(symbol_of(c, b))} == {"usd"}
        else " / ".join(sorted({peg_kind(symbol_of(c, s)), peg_kind(symbol_of(c, b))}))
        for c, s, b in zip(corr.chain, corr.sell_token, corr.buy_token)]
    corr["resolvable"] = [isinstance(a, str) and isinstance(b, str) and a != b
                          for a, b in zip(corr.book_s, corr.book_b)]
    by_kind = corr.groupby("kind").agg(
        attempts=("attempts", "sum"),
        with_binance_feed=("attempts", lambda s_: int(
            corr.loc[s_.index].query("resolvable").attempts.sum())))
    by_kind["share"] = by_kind.attempts / by_kind.attempts.sum()
    print(f"correlated tier by peg ({corr.attempts.sum():,} attempts; "
          f"'with_binance_feed' is the subset both legs of which price on Binance):")
    print(by_kind.sort_values("attempts", ascending=False)
                 .to_string(float_format=lambda v: f"{v:7.3f}"), "\n")
    usd_usd = int(by_kind.loc["usd<->usd", "attempts"]) if "usd<->usd" in by_kind.index else 0

    # ---- the literal
    top = unc.head(args.top)
    print("=" * 78)
    excl = "".join(f" --exclude {a}" for a in args.exclude)
    print(f"# derived by scripts/derive_pairs.py --start {args.start} --end {args.end}{excl}")
    print(f"# {total:,} penalty-eligible attempts in the window; the {args.top} pairs below are "
          f"{top.attempts.sum() / unc.attempts.sum():.0%} of merged uncorrelated flow.")
    print("PAIRS = [")
    print("    # (a, b, tier, attempts)")
    for r in top.itertuples():
        a, b = r.key.split("|")
        a, b = (b, a) if a == "USD" else (a, b)           # read as ASSET<->USD
        print(f'    ("{a}", "{b}", "uncorrelated", {int(r.attempts):>7}),')
    print(f'    ("USDC", "USD", "correlated",   {usd_usd:>7}),'
          f'   # all usd<->usd flow; priced off USDCUSDT')
    print("]")
    print("=" * 78)
    print("\nBefore pasting, check each pair has a 1s kline archive on data.binance.vision:")
    print("  " + "  ".join(sorted({f"{x}USDT" for r in top.itertuples()
                                   for x in r.key.split('|') if x != "USD"})))


if __name__ == "__main__":
    main()
