# penalty-research

Python notebooks running in a reproducible environment managed by [uv](https://docs.astral.sh/uv/),
pinned to Python 3.13. Notebook outputs are stripped automatically on commit via
[nbstripout](https://github.com/kynan/nbstripout), keeping diffs clean.

## Repository layout

- `notebooks/` — all analysis notebooks (outputs are stripped on commit; run them to reproduce
  results). The two behind the penalty-cap CIP are described [below](#the-penalty-cap-notebooks)
- `data/` — local datasets and download caches (gitignored); notebooks reference it as `../data`
- `scripts/` — data-fetching scripts (see `scripts/README.md`)
- `sql/` — queries behind the datasets (see `docs/dataset.md`)

## Recommended: Dev Container

Requires Docker + an editor with Dev Containers support (VS Code "Dev Containers"
extension, or any tool that reads `.devcontainer/`).

1. Open the folder and **Reopen in Container** when prompted.
2. The container builds, then `postCreateCommand` runs `uv sync` (installs Python 3.13
   and the dependencies from `uv.lock`) and `nbstripout --install` (wires up the git filters).
3. Open `notebooks/example.ipynb` and select the `.venv` interpreter to run cells.

No host Python is needed — uv inside the container provides it.

## Alternative: local setup (no container)

With [uv installed](https://docs.astral.sh/uv/getting-started/installation/) on your machine:

```bash
uv sync                    # creates .venv with Python 3.13 + dependencies
uv run nbstripout --install   # enable output stripping for this clone (one-time)
```

Run JupyterLab with `uv run jupyter lab`, or point your editor at `.venv/bin/python`.

## Working with the project

```bash
uv add pandas              # add a dependency (updates pyproject.toml + uv.lock)
uv run jupyter lab         # launch JupyterLab
uv run python script.py    # run anything inside the environment
```

Commit `pyproject.toml`, `uv.lock`, and `.python-version` so the environment stays
reproducible for everyone.

### About nbstripout

`nbstripout --install` sets up git filters (stored in `.git/config`, which is **not**
committed), so each fresh clone needs it run once — the dev container does this
automatically. The matching `*.ipynb filter=nbstripout` rules live in the committed
`.gitattributes`. To check it's active: `uv run nbstripout --status`.

## The penalty-cap notebooks

Two notebooks carry the [penalty-cap redesign CIP](https://forum.cow.fi/t/cip-draft-penalty-cap-redesign/3520):
`fixed_caps_from_revert_target.ipynb` derives the caps, and
`penalties_analysis_counterfactual.ipynb` prices them against what solvers were actually paid.

Run both from `notebooks/` — they resolve `../data` and `../scripts` relative to the notebook
file, which is what Jupyter does by default. Both need network access; only the counterfactual
needs the database.

### `fixed_caps_from_revert_target.ipynb` — deriving the caps

One cap per tier (correlated / uncorrelated) as a function of the exclusivity window `T`: the
smallest cap whose flow-weighted, price-driven revert rate meets `TARGET_REVERT_RATE`. It prints
the cap table over `T`, then shows what holding a fixed number costs — drift per month and per
day, and spread across token pairs.

- **Inputs:** Binance 1-second klines only, pulled from `data.binance.vision` into
  `data/binance_klines_1s/`. No database, no API key.
- **First run** downloads 920 daily archives (10 books × 92 days), ~1.2 GB on disk. Days Binance
  does not publish get a `.missing` marker and are not requested again. With the cache warm the
  notebook takes about a minute and holds ~640 MB of price series in memory.
- **Knobs:** `TARGET_REVERT_RATE` (8%), `FIT_MONTH` and `MONTHS` (fit vs. validation months),
  `CHAIN_WINDOWS` (each chain's `T`), `T_REFERENCE` (the `T` section 2 fixes).

`PAIRS` — the pairs the fit is weighted over — is a hardcoded literal, not derived at runtime.
Regenerating it is the only part of this analysis that touches the database:

```bash
uv run python scripts/derive_pairs.py --start 2026-05-01 --end 2026-08-01 --exclude AERO
```

It reads the `data/{chain}_{start}_{end}_db.csv` extracts written by
`scripts/fetch_orderbook_data.py` and prints a literal to paste back in. Pair selection moves the
uncorrelated cap by ~1%, so this is rarely worth redoing.

### `penalties_analysis_counterfactual.ipynb` — what the caps would have paid

Replays four accounting weeks with the proposed volume-based cap in place of today's flat one,
and reports per chain and per solver what each pays: penalties, consistency rewards (the budget
grows as penalties do), and the total. Solvers are labelled by payment rank rather than by
address, since the charts go into the CIP.

- **Inputs:** three CSVs per chain from cow-analytics-db, plus CoW's correlated-token list from
  the CMS API. Copy `.env.example` to `.env` and fill in `ANALYTICS_DB_URL`; `DUNE_API_KEY` is not
  used here.
- The notebook fetches whatever is missing itself, shelling out to
  `scripts/fetch_counterfactual_data.py` once per chain. Seven chains over
  `2026-06-30 .. 2026-07-28` is 21 files and ~105 MB, about ten minutes end to end (arbitrum is
  most of it). Cached files are reused — delete them to re-fetch. The analysis itself then runs in
  seconds.
- **Knobs:** `UNCORRELATED_BPS_BY_CHAIN` and `CORRELATED_BPS` (the caps from the first notebook),
  `PER_ORDER_CAP_USD` (the $20 ceiling on a single failed order's cap), `NATIVE_TOKEN_USD_PRICE`
  (a fixed CoinGecko snapshot rather than a live feed, so the counterfactual stays reproducible),
  `START_DATE` / `END_DATE`.

`START_DATE` and `END_DATE` must both be **Tuesdays**: accounting periods run Tuesday to Tuesday
and the consistency-reward split is per period, so a partial period mis-attributes it. The fetch
script refuses other dates rather than silently snapping them.

#### Without database access

The 21 CSVs for the window above are published as a release asset, so the counterfactual can be
reproduced without cow-analytics-db:
[`counterfactual_2026-06-30_2026-07-28.zip`](https://github.com/cowprotocol/penalty-research/releases/download/counterfactual-data-2026-06-30_2026-07-28/counterfactual_2026-06-30_2026-07-28.zip)
(17.8 MB, 105 MB unpacked).

```bash
curl -L -o /tmp/counterfactual.zip https://github.com/cowprotocol/penalty-research/releases/download/counterfactual-data-2026-06-30_2026-07-28/counterfactual_2026-06-30_2026-07-28.zip
unzip -d data /tmp/counterfactual.zip
```

Unpack it into `data/` and the notebook runs as-is: `load_inputs` only shells out to the fetch
script for chains whose CSVs are missing, so with all 21 present it never touches the database and
`ANALYTICS_DB_URL` can stay empty. Changing `START_DATE` / `END_DATE` away from the published
window puts you back on the database.
