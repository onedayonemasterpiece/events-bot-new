FROM python:3.12-slim

ARG STATIC_SITE_IMAGE_REPO_SHA

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080
WORKDIR /app
RUN test "$(printf '%s' "$STATIC_SITE_IMAGE_REPO_SHA" | wc -c)" -eq 40 \
    && ! printf '%s' "$STATIC_SITE_IMAGE_REPO_SHA" | grep -q '[^0-9a-f]'
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Region Talk renders at most three JavaScript-only article pages per bounded
# materializer invocation.  Install only Chromium and its system dependencies;
# the worker is not a general-purpose crawler.
RUN python -m playwright install --with-deps chromium
COPY . .
RUN printf '%s\n' "$STATIC_SITE_IMAGE_REPO_SHA" > /app/.static-site-repo-sha
# With ENABLE_PROD_OPS_MCP unset, the wrapper immediately execs the historical
# `python main.py` entrypoint. The optional MCP sidecar is therefore zero-cost
# and unreachable by default.
CMD ["python", "-m", "prod_ops_mcp.entrypoint"]
