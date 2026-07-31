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
COPY . .
RUN printf '%s\n' "$STATIC_SITE_IMAGE_REPO_SHA" > /app/.static-site-repo-sha
CMD ["python", "main.py"]
