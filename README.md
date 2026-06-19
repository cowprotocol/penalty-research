# penalty-research

Python notebooks running in a reproducible environment managed by [uv](https://docs.astral.sh/uv/),
pinned to Python 3.14. Notebook outputs are stripped automatically on commit via
[nbstripout](https://github.com/kynan/nbstripout), keeping diffs clean.

## Recommended: Dev Container

Requires Docker + an editor with Dev Containers support (VS Code "Dev Containers"
extension, or any tool that reads `.devcontainer/`).

1. Open the folder and **Reopen in Container** when prompted.
2. The container builds, then `postCreateCommand` runs `uv sync` (installs Python 3.14
   and the dependencies from `uv.lock`) and `nbstripout --install` (wires up the git filters).
3. Open `notebooks/example.ipynb` and select the `.venv` interpreter to run cells.

No host Python is needed — uv inside the container provides it.

## Alternative: local setup (no container)

With [uv installed](https://docs.astral.sh/uv/getting-started/installation/) on your machine:

```bash
uv sync                    # creates .venv with Python 3.14 + dependencies
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
