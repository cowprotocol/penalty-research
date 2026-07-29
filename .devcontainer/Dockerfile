FROM mcr.microsoft.com/devcontainers/base:bookworm

# Bring in the uv / uvx binaries from the official image (pinned for reproducibility).
COPY --from=ghcr.io/astral-sh/uv:0.11.22 /uv /uvx /usr/local/bin/

# copy       = play nice with the bind-mounted workspace (no hardlinks across filesystems)
# 1          = pre-compile to .pyc for faster first import
# .venv      = keep the virtualenv inside the workspace so VS Code finds it
# python dir = install managed Pythons to a system-wide path so the interpreter baked
#              in below (during the root build) is visible to the runtime vscode user
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/workspaces/penalty-research/.venv \
    UV_PYTHON_INSTALL_DIR=/opt/uv-python

# Bake the project's Python interpreter into the image at build time, so the first
# `uv sync` finds it locally instead of downloading it on container create. The
# version is read from .python-version (copied from the build context, which
# devcontainer.json sets to the repo root) to keep a single source of truth.
COPY .python-version /tmp/.python-version
RUN cd /tmp && uv python install && rm /tmp/.python-version

# Put the project venv on PATH for interactive shells.
ENV PATH=/workspaces/penalty-research/.venv/bin:$PATH

# Install chromium (needed for plotting)
RUN apt-get update && apt-get install -y chromium
