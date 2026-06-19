FROM mcr.microsoft.com/devcontainers/base:bookworm

# Bring in the uv / uvx binaries from the official image (pinned for reproducibility).
COPY --from=ghcr.io/astral-sh/uv:0.5.26 /uv /uvx /usr/local/bin/

# copy   = play nice with the bind-mounted workspace (no hardlinks across filesystems)
# 1      = pre-compile to .pyc for faster first import
# .venv  = keep the virtualenv inside the workspace so VS Code finds it
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/workspaces/penalty-research/.venv

# Put the project venv on PATH for interactive shells.
ENV PATH=/workspaces/penalty-research/.venv/bin:$PATH
