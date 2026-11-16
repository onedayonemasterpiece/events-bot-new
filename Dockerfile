FROM python:3.12-slim

ARG STATIC_SITE_IMAGE_REPO_SHA

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080
WORKDIR /app
RUN test "$(printf '%s' "$STATIC_SITE_IMAGE_REPO_SHA" | wc -c)" -eq 40 \
    && ! printf '%s' "$STATIC_SITE_IMAGE_REPO_SHA" | grep -q '[^0-9a-f]'
RUN apt-get update && apt-get install -y --no-install-recommends webp libatomic1 && rm -rf /var/lib/apt/lists/*
# Native ChatGPT-login client for the daily VK excursion writer. The service
# supplies its login at runtime; the image contains only the OpenAI distribution.
RUN pip install --no-cache-dir --only-binary=:all: --no-deps openai-codex-cli-bin==0.160.0 \
    && python -c "from codex_cli_bin import bundled_codex_path; from pathlib import Path; Path('/usr/local/bin/codex').symlink_to(bundled_codex_path())" \
    && codex --version | grep -Fx "codex-cli 0.160.0"
COPY requirements.txt .
COPY vendor-private/ /tmp/vendor-private/
RUN test -f /tmp/vendor-private/ai_resource_control-0.1.4-py3-none-any.whl     && pip install --no-cache-dir /tmp/vendor-private/ai_resource_control-0.1.4-py3-none-any.whl     && pip install --no-cache-dir -r requirements.txt     && rm -rf /tmp/vendor-private
# Region Talk renders at most three JavaScript-only article pages per bounded
# materializer invocation.  Install only Chromium and its system dependencies;
# the worker is not a general-purpose crawler.
RUN python -m playwright install --with-deps chromium
COPY . .
RUN printf '%s\n' "$STATIC_SITE_IMAGE_REPO_SHA" > /app/.static-site-repo-sha \
    && STATIC_SITE_IMAGE_REPO_SHA="$STATIC_SITE_IMAGE_REPO_SHA" python -c "import os; from pathlib import Path; from scripts.run_static_site_builder_kaggle import write_image_source_manifest; write_image_source_manifest(Path('/app/.static-site-source-manifest.json'), repo_sha=os.environ['STATIC_SITE_IMAGE_REPO_SHA'])"
CMD ["python", "main.py"]
